package k8sexec

import (
	"context"
	"fmt"
	"io"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
)

// A process EXEC'd into a container that is holding, rather than a container
// whose command it is. Two callers need that: a func step re-entering a
// staged binary (staging.go), and `senro shell` standing in a step's
// workspaces.
//
// A session is a pod of its OWN. The engine gives every session a new
// sandbox carrying the step's mounts at the step's paths
// (internal/engine/shell.go), because the step's own sandbox is closed by
// the time anybody asks; here that sandbox is a second pod running the
// step's image. Exec'ing into the STEP's live pod is the tempting shortcut
// and is wrong twice over: that pod projects the step's Secret and carries
// the SENRO_SECRET_* paths in its environment, so a session there would hand
// an operator the credential the engine deliberately withholds, and its
// workspaces are mounted as the STEP asked for them, so a session could
// write bytes the ledger's digest already claims to describe.
//
// A session's own pod has neither. The engine calls PutSecret for a step and
// never for a session, so createSecret below has nothing to create and no
// secret volume is added (containerexec's session is empty for the same
// reason). Every mount arrives read-only because the engine marks it so, and
// a volumeMount enforces that in the kernel.
//
// What a session costs: the workspace crosses the apiserver a second time,
// and the cluster needs capacity for a second pod. A step that mounts
// nothing pays only the pod.

// RunInteractive runs one command in the step's container with a stdin
// attached: senroexec.Interactive. A remote func step re-enters a staged
// binary through it (internal/engine's invokeRemote), and `senro shell`
// opens a session with it.
//
// It differs from Run on one point, and everything else follows: the process
// is EXEC'd into the container instead of being the container's command. The
// exec subresource is the only channel here that carries a stdin, keeps
// stdout and stderr apart and reports an exit code of its own, and a step
// child needs all three (the pod log merges the streams, which would shred
// its frames). So the container is started HOLDING (holdCommand), which is
// also the state in which it can be given the binary; see staging.go.
//
// The step's environment and working directory are still the container's, as
// they are for Run: a process started through the exec subresource inherits
// both from the container it lands in.
func (s *sandbox) RunInteractive(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, stdout, stderr io.Writer,
) (int, error) {
	return s.exec(ctx, c, kubeapi.ExecSpec{Stdin: stdin, Stdout: stdout, Stderr: stderr})
}

// exec is RunInteractive and RunTerminal in one: create the pod, stage the
// workspaces, wait for the holding container, hand over the binary if this
// command IS one, and run the caller's exec inside it.
//
// Everything the two kinds of session differ by travels in spec, which the
// caller fills in; the three fields naming WHERE the command runs are set
// here, so neither caller can name the wrong container.
func (s *sandbox) exec(ctx context.Context, c senroexec.Cmd, spec kubeapi.ExecSpec) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("k8sexec: %w: empty command", senroexec.ErrInfra)
	}
	// A sandbox holds one pod, and a pod's container runs one command:
	// whether that command was the step's own or an earlier session's, there
	// is nothing here left to enter. Refused rather than left to fail as a
	// 409 from the create, which would name neither the step nor what to do
	// instead. Neither caller asks, since both get a sandbox of their own.
	if s.ran() {
		return 0, fmt.Errorf(
			"k8sexec: %w: the sandbox for step %q already created pod %s/%s, and a pod's container "+
				"runs one command and ends, so there is nothing left to run a process in. A session "+
				"gets a sandbox of its own; once the run is over, `senro ws pull` writes the "+
				"workspace out instead",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Namespace, s.pod)
	}
	workDir := s.spec.WorkDir
	if c.Dir != "" {
		workDir = c.Dir
	}
	if err := checkWorkDir(s.spec.StepID, workDir); err != nil {
		return 0, err
	}
	// A session's command is `sh`, never the staged binary's path, so a
	// session's pod gets no bin volume: podSpec's third argument is false for
	// one and true only for the func step this binary IS.
	host, staged := s.ex.stagedHost(c.Args)

	if err := s.createSecret(ctx); err != nil {
		return 0, err
	}
	pod, err := s.ex.cli.CreatePod(ctx, s.ex.spec.Namespace,
		s.podSpec(senroexec.Cmd{Args: holdCommand(), Env: c.Env}, workDir, staged))
	if err != nil {
		return 0, fmt.Errorf("k8sexec: %w: creating pod %s/%s: %w",
			senroexec.ErrInfra, s.ex.spec.Namespace, s.pod, err)
	}
	s.mu.Lock()
	s.created = true
	s.mu.Unlock()

	s.adoptSecret(ctx, pod)

	bg := context.WithoutCancel(ctx)
	// Cancellation takes the pod with it, exactly as Run's awaitExit does.
	// Closing the exec connection ends THIS call, and whether it also ends
	// the process on the far side is the runtime's business, so the pod is
	// deleted rather than trusted to notice. For a session that is the
	// vanished-client path: an operator who walked away must not leave a
	// shell running in somebody else's cluster.
	sessionOver := make(chan struct{})
	defer close(sessionOver)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.ex.cli.DeletePod(bg, s.ex.spec.Namespace, s.pod, 0)
		case <-sessionOver:
		}
	}()

	// The workspaces first, then the binary, then the process: the volumes do
	// not exist until the pod is scheduled, the step's container does not
	// start until the staging init container has exited, and an exec reaches
	// only a container that is already running.
	if err := s.stageWorkspaces(ctx); err != nil {
		return 0, err
	}
	if err := s.awaitContainer(ctx, StepContainer); err != nil {
		return 0, err
	}
	if staged {
		if err := s.copyBinary(ctx, host, c.Args[0]); err != nil {
			return 0, err
		}
	}

	spec.Namespace, spec.Pod, spec.Container = s.ex.spec.Namespace, s.pod, StepContainer
	spec.Command = c.Args
	exit, execErr := s.ex.cli.Exec(ctx, spec)
	switch {
	case ctx.Err() != nil:
		return exit, fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, ctx.Err())
	case execErr != nil:
		return exit, fmt.Errorf("k8sexec: %w: step %q: running %s in pod %s/%s: %w",
			senroexec.ErrInfra, s.spec.StepID, c.Args[0], s.ex.spec.Namespace, s.pod, execErr)
	}
	return exit, nil
}

// The engine reaches a session through an interface assertion (see
// internal/engine/shell.go), so a sandbox that quietly stopped implementing
// this would turn every `senro shell` on a cluster into a refusal with
// nothing failing at build time.
var _ senroexec.Interactive = (*sandbox)(nil)
