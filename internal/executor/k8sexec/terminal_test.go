package k8sexec_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// A terminal session on a cluster: the pty is the container runtime's, and
// the size travels the exec's own resize channel. These are the three
// properties that separate it from a pipe-backed session, and each is a
// thing `senro shell --tty` would silently lose.

// terminal is the second optional capability, asserted rather than skipped
// for interactive's reason.
func terminal(t *testing.T, sb senroexec.Sandbox) senroexec.Terminal {
	t.Helper()
	term, ok := sb.(senroexec.Terminal)
	if !ok {
		t.Fatalf("%T does not implement executor.Terminal, so senro shell --tty on a cluster can only refuse", sb)
	}
	return term
}

// runTerminal runs one script to completion on a terminal and returns
// everything it wrote.
//
// The client's input is a pipe that stays OPEN for the whole session, which
// is what a terminal needs: closing it sends the VEOF byte, and a shell
// answers that by exiting. Errorf rather than Fatalf, because one caller runs
// this on a goroutine of its own and only the test's own goroutine may Fatal.
func runTerminal(
	t *testing.T, sb senroexec.Sandbox, script string,
	ws senroexec.WinSize, resize <-chan senroexec.WinSize,
) string {
	t.Helper()
	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	out := &syncBuffer{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := terminal(t, sb).RunTerminal(ctx,
		senroexec.Cmd{Args: []string{"sh", "-c", script}}, stdin, out, ws, resize); err != nil {
		t.Errorf("RunTerminal: %v", err)
	}
	return out.String()
}

// The whole point of the capability: the command gets a REAL tty, which a
// pipe-backed session can never give it. `test -t 0` is false against a pipe
// and true against a terminal.
func TestATerminalSessionGivesTheCommandATty(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	out := runTerminal(t, sb, "test -t 0 && echo IS_TTY; test -t 1 && echo OUT_TTY",
		senroexec.WinSize{Cols: 80, Rows: 24}, nil)

	for _, want := range []string{"IS_TTY", "OUT_TTY"} {
		if !strings.Contains(out, want) {
			t.Errorf("the command did not see a tty on both ends: %q", out)
		}
	}
}

// stdout and stderr arrive on ONE stream, which is what a terminal is: the
// exec asks for no stderr at all, and losing those bytes instead of merging
// them is the failure this catches.
func TestATerminalMergesTheTwoOutputStreams(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	out := runTerminal(t, sb, "echo to-stdout; echo to-stderr >&2",
		senroexec.WinSize{Cols: 80, Rows: 24}, nil)

	if !strings.Contains(out, "to-stdout") || !strings.Contains(out, "to-stderr") {
		t.Errorf("one of the streams did not arrive: %q", out)
	}
}

// The size the caller asked for is the size the command reads. A pty whose
// creator sets none reports "0 0", and a full-screen program reading that
// draws nothing; the exec subresource has no parameter for it, so the first
// value on the resize channel is what carries it.
func TestTheCommandReadsTheInitialWindowSize(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	// Polled rather than read once, because the size arrives as a message on
	// the same connection: a single read can lose the race with a terminal
	// that has just been created.
	// One second of granularity, never a fractional sleep: sub-second sleeps
	// are not POSIX, and transfer.go avoids them in a pod for that reason.
	script := `i=0; while [ $i -lt 30 ]; do s=$(stty size); ` +
		`case "$s" in "40 120") echo "GOT $s"; exit 0;; esac; i=$((i+1)); sleep 1; done; ` +
		`echo "LAST $(stty size)"`
	out := runTerminal(t, sb, script, senroexec.WinSize{Cols: 120, Rows: 40}, nil)

	if !strings.Contains(out, "GOT 40 120") {
		t.Errorf("the command read %q, want 40 rows and 120 columns", out)
	}
}

// A later size must reach the command, or an operator's window change would
// leave the remote program drawing at the old width forever.
//
// The command POLLS its size rather than trapping SIGWINCH, for localexec's
// reason: a POSIX shell runs a trap only after the current command
// completes, so a trap-based version would assert "applied within twenty
// seconds" while looking like it asserted delivery.
func TestALaterWindowSizeReachesTheCommand(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	resize := make(chan senroexec.WinSize, 1)

	script := `echo READY; while :; do s=$(stty size); ` +
		`case "$s" in "60 200") echo "GOT $s"; exit 0;; esac; sleep 1; done`

	outCh := make(chan string, 1)
	go func() { outCh <- runTerminal(t, sb, script, senroexec.WinSize{Cols: 80, Rows: 24}, resize) }()

	// After a moment, so the command is certainly in its loop and has read
	// the ORIGINAL size first: without that this would pass against an
	// implementation that only ever sends the initial size.
	time.Sleep(2 * time.Second)
	resize <- senroexec.WinSize{Cols: 200, Rows: 60}

	select {
	case out := <-outCh:
		if !strings.Contains(out, "GOT 60 200") {
			t.Errorf("the command read %q, want 60 rows and 200 columns after the resize", out)
		}
	case <-time.After(5 * time.Minute):
		t.Fatal("the command never read the new window size")
	}
}

// A terminal has no EOF, so the client's end of input is delivered as the
// VEOF byte: a shell answers it by exiting, which is how ^D ends a session
// without anything being killed. Without it a session would hang until its
// context was cancelled and report infrastructure instead of an exit code.
func TestEndOfInputEndsATerminalSession(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{StepID: t.Name() + "/shell/s1"})
	stdin, stdinW := io.Pipe()
	out := &syncBuffer{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	done := make(chan int, 1)
	go func() {
		exit, err := terminal(t, sb).RunTerminal(ctx, senroexec.Cmd{Args: []string{"sh"}},
			stdin, out, senroexec.WinSize{Cols: 80, Rows: 24}, nil)
		if err != nil {
			t.Errorf("RunTerminal: %v", err)
		}
		done <- exit
	}()

	if _, err := io.WriteString(stdinW, "echo in-a-terminal\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, out, "in-a-terminal")
	_ = stdinW.Close()

	select {
	case exit := <-done:
		if exit != 0 {
			t.Errorf("exit = %d, want 0 for a shell that was sent VEOF", exit)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("RunTerminal did not return when the client's input ended")
	}
}
