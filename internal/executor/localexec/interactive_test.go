package localexec_test

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/executor"
)

// interactive is the optional Sandbox capability an interactive shell
// needs, asserted so a sandbox that lacks it fails clearly (see
// TestEveryExecutorInThisBuildCanHostAShell in internal/engine).
func interactive(t *testing.T, sb executor.Sandbox) executor.Interactive {
	t.Helper()
	in, ok := sb.(executor.Interactive)
	if !ok {
		t.Fatalf("%T does not implement executor.Interactive, so no shell can be opened on it", sb)
	}
	return in
}

// The whole point of the capability in one test: a shell reads a command
// off stdin and its answer comes back on stdout.
func TestRunInteractiveCarriesBytesInBothDirections(t *testing.T) {
	sb := newSandbox(t)
	stdin, stdinW := io.Pipe()
	var out, errb lockedBuffer

	done := make(chan int, 1)
	go func() {
		exit, err := interactive(t, sb).RunInteractive(context.Background(),
			executor.Cmd{Args: []string{"/bin/sh"}}, stdin, &out, &errb)
		if err != nil {
			t.Errorf("RunInteractive: %v", err)
		}
		done <- exit
	}()

	if _, err := io.WriteString(stdinW, "echo first\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitFor(t, &out, "first")

	// A SECOND command down the same session: one round trip could be
	// satisfied by a process that read its whole stdin, ran, and exited; a
	// session is still there for the next line.
	if _, err := io.WriteString(stdinW, "echo second\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitFor(t, &out, "second")

	// Closing stdin is how a shell is told to leave, and it must actually
	// leave: a session outliving its client is the leak to avoid.
	_ = stdinW.Close()
	select {
	case exit := <-done:
		if exit != 0 {
			t.Errorf("exit = %d, want 0 for a shell that reached EOF", exit)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunInteractive did not return after stdin closed")
	}
}

// Fails if either side of the session deadlocks on a full pipe: the classic
// way an interactive implementation passes every small test and hangs the
// first time someone runs `find /`.
func TestRunInteractiveSurvivesALotOfOutput(t *testing.T) {
	sb := newSandbox(t)
	stdin, stdinW := io.Pipe()
	var out, errb lockedBuffer

	done := make(chan error, 1)
	go func() {
		_, err := interactive(t, sb).RunInteractive(context.Background(),
			executor.Cmd{Args: []string{"/bin/sh"}}, stdin, &out, &errb)
		done <- err
	}()

	// Roughly 1MiB, well past any pipe buffer on any platform this runs on.
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
	case <-time.After(30 * time.Second):
		t.Fatal("RunInteractive did not return after a large output run")
	}
	if got := strings.Count(out.String(), "\n"); got < lines {
		t.Errorf("stdout carried %d lines, want at least %d: output was lost", got, lines)
	}
}

// The client-disconnect path: the engine cancels a session's context when
// its client goes away, and a command that never reads stdin (a tail, a
// sleep, a server) will not notice EOF, so cancellation must kill it or a
// disconnected client leaves a process running unwatched.
func TestRunInteractiveEndsAndKillsOnContextCancel(t *testing.T) {
	sb := newSandbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Never closed: this stands in for a client that is gone without having
	// closed anything cleanly.
	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	var out, errb lockedBuffer

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = interactive(t, sb).RunInteractive(ctx,
			executor.Cmd{Args: []string{"/bin/sh", "-c", "echo READY; sleep 300"}}, stdin, &out, &errb)
	}()

	waitFor(t, &out, "READY")
	cancel()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("RunInteractive did not return after its context was cancelled: " +
			"a disconnected client would leave this process running")
	}
}

// A non-zero exit is the workload's verdict, not an infrastructure error:
// `senro shell -- false` reports 1 rather than claiming the sandbox broke.
func TestRunInteractiveReportsTheCommandsOwnExitCode(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer

	exit, err := interactive(t, sb).RunInteractive(context.Background(),
		executor.Cmd{Args: []string{"/bin/sh", "-c", "exit 7"}}, strings.NewReader(""), &out, &errb)
	if err != nil {
		t.Fatalf("a command exiting non-zero is not an infrastructure failure: %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
}

// Mirrors Run's own guard: an empty argv is a caller bug, not something to
// discover as an exec error.
func TestRunInteractiveRefusesAnEmptyCommand(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer
	_, err := interactive(t, sb).RunInteractive(context.Background(),
		executor.Cmd{}, strings.NewReader(""), &out, &errb)
	if err == nil {
		t.Fatal("RunInteractive accepted an empty command")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want an infrastructure failure", err)
	}
}

// A bare `ls` in a shell must mean what it means in the step: the session
// runs where the step ran.
func TestRunInteractiveStartsInTheSandboxWorkingDirectory(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer
	if _, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"/bin/sh", "-c", "echo marker > only-here"}}, &out, &errb); err != nil {
		t.Fatalf("seeding the sandbox: %v", err)
	}

	out.Reset()
	exit, err := interactive(t, sb).RunInteractive(context.Background(),
		executor.Cmd{Args: []string{"/bin/sh", "-c", "cat only-here"}}, strings.NewReader(""), &out, &errb)
	if err != nil {
		t.Fatalf("RunInteractive: %v", err)
	}
	if exit != 0 || !strings.Contains(out.String(), "marker") {
		t.Errorf("exit = %d, stdout = %q: a session must start in the same directory the step ran in",
			exit, out.String())
	}
}

// A session is held to Run's "empty means empty" rule: a shell is the
// likeliest place for the coordinator's environment to be read back out.
func TestRunInteractiveDoesNotInheritTheCoordinatorsEnvironment(t *testing.T) {
	t.Setenv("SENRO_TEST_COORDINATOR_ONLY", "leaked")
	sb := newSandbox(t)
	var out, errb bytes.Buffer

	if _, err := interactive(t, sb).RunInteractive(context.Background(),
		executor.Cmd{Args: []string{"/bin/sh", "-c", "echo [$SENRO_TEST_COORDINATOR_ONLY]"}},
		strings.NewReader(""), &out, &errb); err != nil {
		t.Fatalf("RunInteractive: %v", err)
	}
	if strings.Contains(out.String(), "leaked") {
		t.Errorf("stdout = %q: the coordinator's own environment reached an interactive session", out.String())
	}
}

// lockedBuffer is a bytes.Buffer safe for a session writing on its own
// goroutine while the test polls for a marker; a bare bytes.Buffer is a
// data race under -race in exactly that shape.
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

// waitFor polls buf until it contains want: the thing being waited for is a
// real process scheduling and flushing, so there is nothing to subscribe to.
func waitFor(t *testing.T, buf *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in the session's output; got %q", want, buf.String())
}
