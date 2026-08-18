package k8sexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountxfer"
	"github.com/xavidop/senro/internal/kubeapi"
)

// Workspace transfer: a tar over the apiserver's exec subresource, in both
// directions, into containers that exist for no other purpose.
//
// Why not the alternatives: a CAS-pulling init container needs the content
// store reachable FROM THE CLUSTER (a second network path, credential and
// service), and is the right answer the day senro grows an OCI-backed cache.
// A PVC is a persistent object senro would then own; RWO forces
// whole-workflow node affinity and destroys parallelism, RWX means a
// networked filesystem and real money. tar over exec needs nothing of the
// cluster that running a pod does not already need, and shares everything
// that must be identical with sshexec through internal/executor/mountxfer.
//
// The cost: every byte crosses the apiserver TWICE per attempt, and the
// apiserver is shared cluster-wide (sshexec's tar goes point to point). It
// is bounded by what a snapshot carries, so .git and node_modules are not in
// it (see mountxfer.Send), and a step that mounts nothing pays nothing. A
// genuinely large workspace is better fetched in the pod (NoSnapshot).
//
// Two containers, because no single one can do both: content must be in the
// volume BEFORE the step's process runs, which only an init container
// orders, and a container sharing the volume must still be alive AFTER it
// exits, which only an ordinary container of a restartPolicy Never pod is.
// Both run the STEP's own image, already pinned and on the node, so neither
// adds an image to pull or a digest to a cache key. What they ask of that
// image is `sh` and `tar`, as sshexec asks of a remote host; a distroless
// image with neither cannot carry a workspace.

// StageContainer is the init container that receives workspaces, and
// IOContainer the one that hands them back. Fixed names, because the exec
// requests, the pod spec and the classifier all have to mean the same
// container, exactly as StepContainer is fixed.
const (
	StageContainer = "senro-stage"
	IOContainer    = "senro-io"
)

// WorkspaceStageRoot is where the two senro containers see the step's
// workspaces, one directory per mount, numbered by position. A private root
// rather than the declared mount path: a step may mount a workspace
// read-only and the staging container has to WRITE it, and a declared path
// is the pipeline's to choose.
const WorkspaceStageRoot = "/senro/ws"

// stagedFlag is the file whose appearance releases the staging container. It
// is inside the container's own filesystem rather than on a volume, so
// nothing the step can see is involved.
const stagedFlag = "/tmp/senro-staged"

// stagePath is where mount i is mounted inside the two senro containers.
func stagePath(i int) string { return WorkspaceStageRoot + "/" + strconv.Itoa(i) }

// stageCommand is what the staging init container runs: nothing, until
// senro has finished with it. The wait is bounded so a coordinator killed
// between creating the pod and staging it does not leave an init container
// idling indefinitely. One second of polling granularity: sub-second sleeps
// are not POSIX, and a `sleep 0.2` that fails on some image would spin a
// core; the wait is paid once per step, not once per mount.
func stageCommand() []string {
	return []string{"sh", "-c", `i=0
while [ ! -f ` + stagedFlag + ` ]; do
  i=$((i+1))
  if [ "$i" -gt 900 ]; then
    echo "senro: no workspace arrived within 15 minutes; the coordinator went away" >&2
    exit 1
  fi
  sleep 1
done`}
}

// ioCommand is what the reader container runs: nothing, for a bounded time.
// This container keeps the pod alive after the step exits; Close deletes
// the pod on every path, so the bound is only reached by a killed
// coordinator, and a day outlives any step. Find one sooner with
// `kubectl get pods -l senro.dev/run=<id>`.
func ioCommand() []string {
	return []string{"sh", "-c", "sleep 86400"}
}

// holdCommand is what the STEP's container runs when senro will exec the
// step's own process into it instead of making it the container's command: a
// func step re-entering a staged binary (staging.go), and a `senro shell`
// session (interactive.go). The exec subresource reaches a container that is
// already RUNNING, so such a container has to be started doing nothing, and
// that is also the window the binary arrives in.
//
// The same wait, bound and bargain as ioCommand's, and deliberately the same
// command: two idle commands that drifted apart is how one acquires a bug
// the other does not have. It lives here beside the one it wraps rather than
// in either caller's file.
func holdCommand() []string { return ioCommand() }

// stageWorkspaces puts every mount's content into the pod, then releases
// the staging container so the step can start. Called after the pod is
// created and before awaitStart, the only window there is: the volume does
// not exist until the pod is scheduled, and the step's container does not
// start until the init container has exited.
func (s *sandbox) stageWorkspaces(ctx context.Context) error {
	if len(s.mounts) == 0 {
		return nil
	}
	if err := s.awaitContainer(ctx, StageContainer); err != nil {
		return err
	}
	for i, m := range s.mounts {
		if err := s.copyIn(ctx, i, m); err != nil {
			return err
		}
	}
	return s.release(ctx)
}

// copyIn streams one workspace's current content into the staging container.
//
// The tar is workspace.WriteTar's, through mountxfer.Send, so the bytes on
// the wire are the same normalized form a snapshot digests and the exclusions
// that apply here are the ones that will apply when it comes back.
func (s *sandbox) copyIn(ctx context.Context, i int, m senroexec.Mount) error {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := mountxfer.Send(pw, m)
		// CloseWithError(nil) is Close: the far side sees EOF only once Send
		// has finished, and the writer's error otherwise, so tar in the pod
		// does not report a truncated archive as a corrupt one.
		_ = pw.CloseWithError(err)
		done <- err
	}()

	var errb bytes.Buffer
	exit, execErr := s.ex.cli.Exec(ctx, kubeapi.ExecSpec{
		Namespace: s.ex.spec.Namespace, Pod: s.pod, Container: StageContainer,
		Command: []string{"tar", "-x", "-m", "-f", "-", "-C", stagePath(i)},
		Stdin:   pr, Stdout: &errb, Stderr: &errb,
	})
	_ = pr.CloseWithError(io.EOF)
	sendErr := <-done

	if sendErr != nil {
		return fmt.Errorf("k8sexec: %w: step %q: reading workspace %q to send it to pod %s/%s: %w",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Namespace, s.pod, sendErr)
	}
	if execErr != nil {
		return fmt.Errorf("k8sexec: %w: step %q: sending workspace %q into pod %s/%s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Namespace, s.pod,
			execErr, detail(errb.String()))
	}
	if exit != 0 {
		return fmt.Errorf(
			"k8sexec: %w: step %q: sending workspace %q into pod %s/%s failed (tar exited %d). "+
				"This executor needs tar and sh in the step's image, exactly as the ssh executor "+
				"needs tar on a remote host%s",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Namespace, s.pod,
			exit, detail(errb.String()))
	}
	return nil
}

// release lets the staging container finish, which is what starts the step.
func (s *sandbox) release(ctx context.Context) error {
	var out bytes.Buffer
	exit, err := s.ex.cli.Exec(ctx, kubeapi.ExecSpec{
		Namespace: s.ex.spec.Namespace, Pod: s.pod, Container: StageContainer,
		Command: []string{"touch", stagedFlag}, Stdout: &out, Stderr: &out,
	})
	if err != nil {
		return fmt.Errorf("k8sexec: %w: step %q: releasing the staging container of pod %s/%s: %w",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Namespace, s.pod, err)
	}
	if exit != 0 {
		return fmt.Errorf(
			"k8sexec: %w: step %q: releasing the staging container of pod %s/%s failed (exit %d)%s",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Namespace, s.pod, exit, detail(out.String()))
	}
	return nil
}

// Snapshot reads one workspace back out of the pod and captures it. The
// capture itself is mountxfer.Capture's (digest of what came back,
// replacement not merge, read-only snapshotted without swapping); this
// executor's own part is copyOut, plus one refusal: a name no mount
// carries, for which an invented digest would be a confident wrong answer.
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	i, m, ok := s.findMount(name)
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"k8sexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	if s.ex.snap == nil {
		return senroexec.Snapshot{}, fmt.Errorf(
			"k8sexec: %w: step %q cannot snapshot workspace %q: this executor was built without a "+
				"snapshotter", senroexec.ErrInfra, s.spec.StepID, name)
	}
	if !s.ran() {
		return senroexec.Snapshot{}, fmt.Errorf(
			"k8sexec: %w: step %q cannot snapshot workspace %q before its pod has run",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	snap, err := mountxfer.Capture(ctx, s.ex.snap, m,
		func(ctx context.Context, dest string) error { return s.copyOut(ctx, i, m, dest) })
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, err)
	}
	return snap, nil
}

// ReadMount pulls one mount's copy out of the pod into dest, without a
// digest and without replacing the coordinator's own directory. It is
// Snapshot's read-back with mountxfer.Capture's two workspace halves left
// off, over the same copyOut and therefore the same tar: a scratch cache is
// never evidence, so a digest of it would be a number nothing may read, and
// the engine keeps what came back aside rather than swapping it over a
// directory a sibling step may be sending out at the same moment.
//
// The cost is the transfer itself, once more per step: a scratch cache is
// often a large dependency tree, and every byte of it crosses the SHARED
// apiserver twice per attempt, exactly as a workspace does. See
// internal/engine's readScratch.
func (s *sandbox) ReadMount(ctx context.Context, name, dest string) error {
	i, m, ok := s.findMount(name)
	if !ok {
		return fmt.Errorf("k8sexec: %w: step %q has no mount named %q to read back",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	if !s.ran() {
		return fmt.Errorf("k8sexec: %w: step %q cannot read mount %q back before its pod has run",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	return s.copyOut(ctx, i, m, dest)
}

// copyOut streams one workspace out of the reader container into dest.
//
// `tar -c .` rather than `tar -c <dir>`, so the entries are relative and the
// archive is the shape mountxfer.Receive expects.
func (s *sandbox) copyOut(ctx context.Context, i int, m senroexec.Mount, dest string) error {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := mountxfer.Receive(pr, dest)
		// Closed WITH the error so the exec's own writes fail rather than
		// filling a pipe nobody drains, which would hold this open until the
		// whole workspace had been sent to a reader that gave up early.
		_ = pr.CloseWithError(err)
		done <- err
	}()

	var errb bytes.Buffer
	exit, execErr := s.ex.cli.Exec(ctx, kubeapi.ExecSpec{
		Namespace: s.ex.spec.Namespace, Pod: s.pod, Container: IOContainer,
		Command: []string{"tar", "-c", "-f", "-", "-C", stagePath(i), "."},
		Stdout:  pw, Stderr: &errb,
	})
	_ = pw.Close()
	readErr := <-done

	// Exec already errors when the apiserver reported no status, which is
	// the property this depends on: a tar stream cut in the middle is a
	// parseable prefix of a tarball, and the missing status is what tells it
	// from a whole one. The read error matters for the other direction: a
	// reader that refused an entry closes the pipe and fails the exec's
	// writes, and printing only the exec's complaint would send the reader
	// looking at the cluster instead of at the tarball.
	if execErr != nil || readErr != nil || exit != 0 {
		return fmt.Errorf(
			"k8sexec: %w: step %q: reading workspace %q back out of pod %s/%s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Namespace, s.pod,
			joinTransfer(execErr, readErr, exit), detail(errb.String()))
	}
	return nil
}

// joinTransfer folds a transfer's three ways of failing into one error. All
// are reported because they cause each other in both directions: reporting
// only one would print "read on closed pipe" for a cluster that went away,
// or "unexpected EOF" for a tarball senro refused, and the person reading
// it would be looking at the wrong end.
func joinTransfer(execErr, readErr error, exit int) error {
	var exitErr error
	if exit != 0 {
		exitErr = fmt.Errorf("tar in the pod exited %d", exit)
	}
	return errors.Join(execErr, readErr, exitErr)
}
