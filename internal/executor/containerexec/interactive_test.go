package containerexec_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
)

// newInteractiveSandbox returns a container sandbox and its Interactive
// capability, failing rather than skipping if the executor does not have
// one: an executor that stopped being able to host a shell is the exact
// regression this file exists to catch, and a skip reads like a pass.
func newInteractiveSandbox(t *testing.T, spec executor.SandboxSpec) (executor.Sandbox, executor.Interactive) {
	t.Helper()
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	sb, err := ex.Sandbox(context.Background(), spec)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	t.Cleanup(func() { _ = sb.Close(context.Background(), false) })
	in, ok := sb.(executor.Interactive)
	if !ok {
		t.Fatalf("%T does not implement executor.Interactive, so no shell can be opened in a container", sb)
	}
	return sb, in
}

// The container half of the local executor's property: a shell answers
// twice on one connection and leaves when stdin closes. The two executors
// get the same test deliberately: a container implementation could pass a
// one-shot test because the container's output all arrives after it exits.
func TestRunInteractiveCarriesBytesInBothDirections(t *testing.T) {
	_, in := newInteractiveSandbox(t, executor.SandboxSpec{StepID: "shell", Attempt: 1})
	stdin, stdinW := io.Pipe()
	var out, errb lockedBuffer

	done := make(chan int, 1)
	go func() {
		exit, err := in.RunInteractive(context.Background(),
			executor.Cmd{Args: []string{"sh"}}, stdin, &out, &errb)
		if err != nil {
			t.Errorf("RunInteractive: %v", err)
		}
		done <- exit
	}()

	if _, err := io.WriteString(stdinW, "echo first\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitFor(t, &out, "first")
	if _, err := io.WriteString(stdinW, "echo second\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitFor(t, &out, "second")

	_ = stdinW.Close()
	select {
	case exit := <-done:
		if exit != 0 {
			t.Errorf("exit = %d, want 0 for a shell that reached EOF", exit)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("RunInteractive did not return after stdin closed")
	}
}

// The same deadlock check the local executor gets, and it matters more
// here: the attach stream is a single socket carrying framed stdout and
// stderr, so an implementation that stops reading it while writing stdin
// wedges at the first output larger than a socket buffer.
func TestRunInteractiveSurvivesALotOfOutput(t *testing.T) {
	_, in := newInteractiveSandbox(t, executor.SandboxSpec{StepID: "shell-big", Attempt: 1})
	stdin, stdinW := io.Pipe()
	var out, errb lockedBuffer

	done := make(chan error, 1)
	go func() {
		_, err := in.RunInteractive(context.Background(),
			executor.Cmd{Args: []string{"sh"}}, stdin, &out, &errb)
		done <- err
	}()

	const lines = 20000
	if _, err := io.WriteString(stdinW, "i=0; while [ $i -lt "+strconv.Itoa(lines)+" ]; do "+
		"echo 0123456789012345678901234567890123456789012345678901234567890123456789; "+
		"i=$((i+1)); done; echo DONE\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitFor(t, &out, "DONE")
	_ = stdinW.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunInteractive: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("RunInteractive did not return after a large output run")
	}
	if got := strings.Count(out.String(), "\n"); got < lines {
		t.Errorf("stdout carried %d lines, want at least %d: output was lost", got, lines)
	}
}

// The disconnect path: the container is a separate process tree on the
// daemon's side of a socket, so closing a connection stops nothing. It must
// be killed AND removed, or a client that walked away leaves a labelled
// container running on the host indefinitely.
func TestRunInteractiveEndsAndKillsOnContextCancel(t *testing.T) {
	c := dockertest.Require(t)
	sb, in := newInteractiveSandbox(t, executor.SandboxSpec{StepID: "shell-cancel", Attempt: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Never closed, standing in for a client that vanished without saying
	// anything.
	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	var out, errb lockedBuffer

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = in.RunInteractive(ctx,
			executor.Cmd{Args: []string{"sh", "-c", "echo READY; sleep 300"}}, stdin, &out, &errb)
	}()

	waitFor(t, &out, "READY")
	cancel()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("RunInteractive did not return after its context was cancelled: " +
			"a disconnected client would leave a container running on the host")
	}

	// Close is what removes it, exactly as for a step; asserted because this
	// is the one path where the container was killed rather than exiting on
	// its own.
	if err := sb.Close(context.Background(), false); err != nil {
		t.Fatalf("Close after a cancelled session: %v", err)
	}
	ids, err := c.ContainerList(context.Background(), map[string]string{"senro.run": testRunID(t)})
	if err != nil {
		t.Fatalf("ContainerList: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("%d container(s) survived a cancelled session: %v", len(ids), ids)
	}
}

// The workload/infra split the Sandbox interface requires: a session whose
// command exits 7 is not a broken sandbox.
func TestRunInteractiveReportsTheCommandsOwnExitCode(t *testing.T) {
	_, in := newInteractiveSandbox(t, executor.SandboxSpec{StepID: "shell-exit", Attempt: 1})
	var out, errb bytes.Buffer

	exit, err := in.RunInteractive(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "exit 7"}}, strings.NewReader(""), &out, &errb)
	if err != nil {
		t.Fatalf("a command exiting non-zero is not an infrastructure failure: %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
}

// The reason any of this exists: standing in a failed step's workspace. The
// mount is realized from the coordinator's own directory, so the file the
// step left behind is readable even though the step's container is gone.
func TestRunInteractiveSeesTheSandboxMounts(t *testing.T) {
	wsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(wsDir, "left-behind.txt"), []byte("evidence"), 0o644); err != nil {
		t.Fatalf("seeding the workspace: %v", err)
	}
	_, in := newInteractiveSandbox(t, executor.SandboxSpec{
		StepID: "shell-mounts", Attempt: 1, WorkDir: "/src",
		Mounts: []executor.Mount{{Name: "src", Path: wsDir, At: "/src", RO: true}},
	})

	var out, errb bytes.Buffer
	exit, err := in.RunInteractive(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "cat left-behind.txt"}}, strings.NewReader(""), &out, &errb)
	if err != nil {
		t.Fatalf("RunInteractive: %v", err)
	}
	if exit != 0 || !strings.Contains(out.String(), "evidence") {
		t.Errorf("exit = %d, stdout = %q, stderr = %q: a session must see the step's workspace at the step's own path",
			exit, out.String(), errb.String())
	}
}

// Read-only is real on this executor (the local one cannot enforce it; see
// executor.Mount.RO): a session that could rewrite a workspace would be
// rewriting bytes a digest already in the ledger claims to describe.
func TestRunInteractiveCannotWriteThroughAReadOnlyMount(t *testing.T) {
	wsDir := t.TempDir()
	_, in := newInteractiveSandbox(t, executor.SandboxSpec{
		StepID: "shell-ro", Attempt: 1, WorkDir: "/src",
		Mounts: []executor.Mount{{Name: "src", Path: wsDir, At: "/src", RO: true}},
	})

	var out, errb bytes.Buffer
	exit, err := in.RunInteractive(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "echo tampered > /src/new-file"}},
		strings.NewReader(""), &out, &errb)
	if err != nil {
		t.Fatalf("RunInteractive: %v", err)
	}
	if exit == 0 {
		t.Errorf("a write through a read-only mount succeeded: exit = %d, stderr = %q", exit, errb.String())
	}
	if _, err := os.Stat(filepath.Join(wsDir, "new-file")); err == nil {
		t.Error("a session wrote a file into a read-only workspace")
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func waitFor(t *testing.T, buf *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in the session's output; got %q", want, buf.String())
}
