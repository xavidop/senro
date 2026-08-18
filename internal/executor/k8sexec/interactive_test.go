package k8sexec_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
)

// What `senro shell` stands on, against a real cluster: a session is a pod
// of this sandbox's own, it carries bytes both ways for as long as somebody
// is on the other end, it sees the step's workspaces at the step's paths and
// cannot write them, and a client that vanishes leaves nothing running.

// interactive is the optional capability a session needs, asserted rather
// than skipped: an executor that stopped being able to host one is the
// regression this file exists to catch, and a skip reads like a pass.
func interactive(t *testing.T, sb senroexec.Sandbox) senroexec.Interactive {
	t.Helper()
	in, ok := sb.(senroexec.Interactive)
	if !ok {
		t.Fatalf("%T does not implement executor.Interactive, so senro shell on a cluster can only refuse", sb)
	}
	return in
}

// waitForOutput blocks until the session has written want, or fails.
func waitForOutput(t *testing.T, buf *syncBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in the session's output; got %q", want, buf.String())
}

// The property a one-shot test cannot see: a shell answers twice on one
// connection and leaves when its input ends. The container executor gets the
// identical test for the identical reason.
func TestASessionCarriesBytesInBothDirections(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	stdin, stdinW := io.Pipe()
	out, errOut := &syncBuffer{}, &syncBuffer{}

	done := make(chan int, 1)
	go func() {
		exit, err := interactive(t, sb).RunInteractive(context.Background(),
			senroexec.Cmd{Args: []string{"sh"}}, stdin, out, errOut)
		if err != nil {
			t.Errorf("RunInteractive: %v", err)
		}
		done <- exit
	}()

	if _, err := io.WriteString(stdinW, "echo first\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, out, "first")
	if _, err := io.WriteString(stdinW, "echo second\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, out, "second")

	_ = stdinW.Close()
	select {
	case exit := <-done:
		if exit != 0 {
			t.Errorf("exit = %d, want 0 for a shell that reached EOF", exit)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("RunInteractive did not return after stdin closed")
	}
}

// The workload-versus-infrastructure split, on the session path: a command
// that exits 7 is a verdict, not a broken sandbox.
func TestASessionReportsTheCommandsOwnExitCode(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	out, errOut := &syncBuffer{}, &syncBuffer{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	exit, err := interactive(t, sb).RunInteractive(ctx,
		senroexec.Cmd{Args: []string{"sh", "-c", "exit 7"}}, strings.NewReader(""), out, errOut)
	if err != nil {
		t.Fatalf("a command exiting non-zero is not an infrastructure failure: %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
}

// The reason any of this exists: standing in the step's workspace. The mount
// is staged into the session's own pod from the coordinator's copy, so the
// file the step left behind is readable at the path the step saw it, even
// though the step's own pod is long gone.
//
// And it is READ-ONLY, which this executor genuinely enforces: a session
// that could rewrite a workspace would be rewriting bytes a digest already
// in the ledger claims to describe.
func TestASessionSeesTheStepsWorkspaceAtItsOwnPathAndCannotWriteIt(t *testing.T) {
	ws := t.TempDir()
	write(t, filepath.Join(ws, "left-behind.txt"), "evidence\n")

	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{
		StepID: t.Name() + "/shell/s1", WorkDir: "/src",
		Mounts: []senroexec.Mount{{Name: "src", Path: ws, At: "/src", RO: true}},
	})
	out, errOut := &syncBuffer{}, &syncBuffer{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	exit, err := interactive(t, sb).RunInteractive(ctx, senroexec.Cmd{
		Args: []string{"sh", "-c", "cat left-behind.txt; echo tampered > /src/planted.txt"},
	}, strings.NewReader(""), out, errOut)
	if err != nil {
		t.Fatalf("RunInteractive: %v", err)
	}
	if !strings.Contains(out.String(), "evidence") {
		t.Errorf("stdout = %q, stderr = %q: a session must see the step's workspace at the step's "+
			"own path, in the step's own working directory", out.String(), errOut.String())
	}
	if exit == 0 {
		t.Errorf("a write through a read-only mount succeeded: exit = %d, output = %q",
			exit, out.String()+errOut.String())
	}
	if _, err := os.Stat(filepath.Join(ws, "planted.txt")); err == nil {
		t.Error("a session wrote a file into a read-only workspace")
	}
}

// The disconnect path. The pod is a workload on somebody else's cluster, so
// ending the connection stops nothing by itself: the pod must go, or an
// operator who closed a laptop leaves a shell holding a node until the
// holding command's own day expires.
func TestACancelledSessionEndsAndTakesItsPodWithIt(t *testing.T) {
	c := kindtest.Require(t)
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Never closed, standing in for a client that vanished without saying
	// anything.
	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	out, errOut := &syncBuffer{}, &syncBuffer{}

	done := make(chan error, 1)
	go func() {
		_, err := interactive(t, sb).RunInteractive(ctx, senroexec.Cmd{
			Args: []string{"sh", "-c", "echo READY; sleep 600"},
		}, stdin, out, errOut)
		done <- err
	}()

	waitForOutput(t, out, "READY")
	cancel()

	select {
	case err := <-done:
		if !senroexec.IsInfra(err) {
			t.Errorf("a cancelled session must report infrastructure, since nothing about the "+
				"command was decided: %v", err)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("RunInteractive did not return after its context was cancelled: a disconnected " +
			"client would leave a shell running in the cluster")
	}

	poll, pollCancel := context.WithTimeout(context.Background(), time.Minute)
	defer pollCancel()
	name := podNameOf(t, sb)
	for {
		pod, err := c.Client.GetPod(poll, kindtest.Namespace, name)
		if kubeapi.IsNotFound(err) {
			return
		}
		select {
		case <-poll.Done():
			t.Fatalf("the pod survived a cancelled session: %s is still %s (get error %v)",
				name, pod.Status.Phase, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// A sandbox whose pod already ran its step has nothing left to open a
// session in, and says so rather than failing as a 409 from a second create
// with the same name. This is the settled-step refusal: the engine never
// asks, because a session gets a sandbox of its own, and once the run is
// over the workspace comes out with `senro ws pull`.
func TestASessionIsRefusedOnASandboxWhosePodHasAlreadyRun(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{})
	if exit, out, err := run(t, sb, "sh", "-c", "echo the step itself"); err != nil || exit != 0 {
		t.Fatalf("Run: exit = %d, err = %v, output = %q", exit, err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := interactive(t, sb).RunInteractive(ctx,
		senroexec.Cmd{Args: []string{"sh"}}, strings.NewReader(""), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("a session opened in the sandbox that already ran the step")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("the refusal does not carry ErrInfra: %v", err)
	}
	if !strings.Contains(err.Error(), "senro ws pull") {
		t.Errorf("the refusal does not say what to use instead: %v", err)
	}
}
