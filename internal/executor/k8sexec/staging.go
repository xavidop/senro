package k8sexec

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/workspace"
)

// BinDir is where a staged step binary appears INSIDE the pod, on an
// emptyDir of its own.
//
// The same private root containerexec.BinDir uses, and the leaf name is
// senroexec.StagedName's, so one digest is one file name wherever senro puts
// it. A volume rather than the image's own filesystem: an emptyDir is
// writable whatever the image's root looks like and whichever uid
// k8s.User("1000:1000") named, and it is deleted with the pod.
const BinDir = "/senro/bin"

// binVolume is that emptyDir's name in the pod spec. Fixed, like
// StepContainer, because nothing outside the spec refers to it.
const binVolume = "senro-bin"

// StageBinary records the coordinator-side binary this executor's pods will
// re-enter, and reports the path it will hold inside one:
// senroexec.BinaryStager.
//
// Nothing is transferred here, and nothing can be: a pod is created per
// ATTEMPT (see Sandbox and RunInteractive), so there is no target yet. The
// bytes cross in RunInteractive, as a one-entry tar over the same exec
// subresource a workspace crosses on (transfer.go). What this call does is
// stat the file, so a missing or non-regular one is a refusal before a pod
// is created for a command that could never run.
//
// What that costs, in the two places it is paid:
//
//   - Once per RUN at most, the cross-compile. internal/binprov ships
//     os.Executable() unchanged when the cluster's platform is the
//     coordinator's, and otherwise builds once per platform and keeps the
//     result on disk keyed by the coordinator's own digest, so a later run of
//     the same release reuses it.
//   - Once per POD, which is once per attempt of a func step, the transfer:
//     the whole binary over the apiserver. A pod's filesystem does not
//     outlive the pod and senro owns no cluster object to keep a copy in, so
//     there is nothing to amortize it against and Staging.Reused is false on
//     every call. sshexec's per-host staging directory is what a target with
//     somewhere to keep it looks like.
func (e *Executor) StageBinary(
	_ context.Context, b senroexec.StagedBinary,
) (senroexec.Staging, error) {
	name, err := senroexec.StagedName(b.Digest)
	if err != nil {
		return senroexec.Staging{}, fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, err)
	}
	fi, err := os.Stat(b.Path)
	if err != nil {
		return senroexec.Staging{}, fmt.Errorf(
			"k8sexec: %w: reading %s to send it into this step's pod: %w",
			senroexec.ErrInfra, b.Path, err)
	}
	if !fi.Mode().IsRegular() {
		return senroexec.Staging{}, fmt.Errorf(
			"k8sexec: %w: %s is not a regular file, so it cannot be sent into a pod as this "+
				"step's binary", senroexec.ErrInfra, b.Path)
	}

	p := BinDir + "/" + name
	e.stageMu.Lock()
	defer e.stageMu.Unlock()
	if e.staged == nil {
		e.staged = map[string]string{}
	}
	e.staged[p] = b.Path
	return senroexec.Staging{Path: p, Reused: false}, nil
}

// stagedHost is the coordinator-side file a command IS a staged binary of,
// or no file at all. Keyed on the command being run, exactly as
// containerexec's binaryBind is: this puts senro's own executable inside an
// image somebody else built, and only the container whose command is that
// binary has any business receiving it.
func (e *Executor) stagedHost(args []string) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	e.stageMu.Lock()
	defer e.stageMu.Unlock()
	host, ok := e.staged[args[0]]
	return host, ok
}

// copyBinary streams the staged binary into the step's container, into the
// emptyDir at BinDir. It is what sshexec's pushBinary does over a shell, on
// this executor's one transport.
//
// A tar rather than a `cat` redirect: tar is what this executor already asks
// of the step's image (see transfer.go), it needs no shell quoting, and it
// carries the MODE with the bytes, so the file arrives executable without a
// chmod the image may not have.
func (s *sandbox) copyBinary(ctx context.Context, host, podPath string) error {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := writeBinaryTar(pw, path.Base(podPath), host)
		// CloseWithError(nil) is Close: tar in the pod sees EOF only once the
		// whole file has been written, and the writer's error otherwise, so a
		// truncated archive is not reported as a corrupt one.
		_ = pw.CloseWithError(err)
		done <- err
	}()

	var errb bytes.Buffer
	exit, execErr := s.ex.cli.Exec(ctx, kubeapi.ExecSpec{
		Namespace: s.ex.spec.Namespace, Pod: s.pod, Container: StepContainer,
		Command: []string{"tar", "-x", "-f", "-", "-C", BinDir},
		Stdin:   pr, Stdout: &errb, Stderr: &errb,
	})
	_ = pr.CloseWithError(io.EOF)
	sendErr := <-done

	if sendErr != nil {
		return fmt.Errorf("k8sexec: %w: step %q: reading the step binary to send it to pod %s/%s: %w",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Namespace, s.pod, sendErr)
	}
	if execErr != nil {
		return fmt.Errorf("k8sexec: %w: step %q: sending the step binary into pod %s/%s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Namespace, s.pod,
			execErr, detail(errb.String()))
	}
	if exit != 0 {
		return fmt.Errorf(
			"k8sexec: %w: step %q: sending the step binary into pod %s/%s failed (tar exited %d). "+
				"A func step needs tar and sh in the step's image, exactly as carrying a workspace "+
				"does%s",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Namespace, s.pod,
			exit, detail(errb.String()))
	}
	return nil
}

// writeBinaryTar writes the one-entry archive: the binary, mode 0700, at
// internal/workspace's fixed epoch and with no uid or gid, which is how
// every other tar senro sends into a pod is normalized.
//
// 0700 rather than 0755 for sshexec's reason: this is senro's own executable
// landing in somebody else's filesystem, and a window in which it is
// world-readable is one in which it is world-RUNNABLE.
func writeBinaryTar(w io.Writer, name, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("reading %s: %w", src, err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("reading %s: %w", src, err)
	}

	tw := tar.NewWriter(w)
	if err := tw.WriteHeader(&tar.Header{
		Name: name, Typeflag: tar.TypeReg, Mode: 0o700,
		Size: fi.Size(), ModTime: workspace.Epoch, Format: tar.FormatPAX,
	}); err != nil {
		return fmt.Errorf("writing the header for %s: %w", src, err)
	}
	n, err := io.Copy(tw, f)
	if err != nil {
		return fmt.Errorf("copying %s: %w", src, err)
	}
	if n != fi.Size() {
		// The file changed under the transfer. Every byte after this point is
		// misaligned in the archive, so this is fatal rather than a warning.
		return fmt.Errorf("%s changed size while it was being staged (%d bytes declared, %d copied)",
			src, fi.Size(), n)
	}
	return tw.Close()
}

// MountPath reports where a mount was realized inside the pod:
// senroexec.MountLocator. Every mount is at its declared path here, claim and
// emptyDir alike, so the answer is the declaration; it is still asked rather
// than assumed because a re-entered func child cannot open Mount.Path, which
// is a directory on the coordinator.
func (s *sandbox) MountPath(name string) (string, bool) {
	_, m, ok := s.findMount(name)
	if !ok {
		return "", false
	}
	return m.At, true
}

var (
	_ senroexec.BinaryStager = (*Executor)(nil)
	_ senroexec.MountLocator = (*sandbox)(nil)
)
