//go:build unix

package localexec_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/executor"
)

// terminal is the optional capability a pty session needs, asserted so an
// executor that cannot host one fails clearly instead of quietly behaving
// like a pipe.
func terminal(t *testing.T, sb executor.Sandbox) executor.Terminal {
	t.Helper()
	term, ok := sb.(executor.Terminal)
	if !ok {
		t.Fatalf("%T does not implement executor.Terminal", sb)
	}
	return term
}

// The whole point of the capability: the child gets a REAL tty, which is
// what a pipe-backed session can never give it. `test -t 0` is false against
// a pipe and true against a terminal.
func TestATerminalSessionGivesTheChildATty(t *testing.T) {
	sb := newSandbox(t)
	out := runTerminal(t, sb,
		"test -t 0 && echo IS_TTY; test -t 1 && echo OUT_TTY",
		executor.WinSize{Cols: 80, Rows: 24}, nil)

	for _, want := range []string{"IS_TTY", "OUT_TTY"} {
		if !strings.Contains(out, want) {
			t.Errorf("the child did not see a tty on both ends: %q", out)
		}
	}
}

// stdout and stderr arrive on ONE stream, which is what a terminal is. This
// is the property that makes a terminal a different session KIND rather than
// a flag on the pipe-backed one.
func TestATerminalMergesTheTwoOutputStreams(t *testing.T) {
	sb := newSandbox(t)
	out := runTerminal(t, sb,
		"echo to-stdout; echo to-stderr >&2",
		executor.WinSize{Cols: 80, Rows: 24}, nil)

	if !strings.Contains(out, "to-stdout") || !strings.Contains(out, "to-stderr") {
		t.Errorf("one of the streams did not arrive: %q", out)
	}
}

// The size the caller asked for is the size the child reads. A pty whose
// creator sets none reports "0 0", and a full-screen program that reads that
// draws nothing at all.
func TestTheChildReadsTheInitialWindowSize(t *testing.T) {
	sb := newSandbox(t)
	out := runTerminal(t, sb, "stty size </dev/tty",
		executor.WinSize{Cols: 120, Rows: 40}, nil)

	// stty prints "rows cols".
	if !strings.Contains(out, "40") || !strings.Contains(out, "120") {
		t.Errorf("the child read %q, want 40 rows and 120 columns", out)
	}
}

// A later size must reach the child, or an operator's window change would
// leave the remote program drawing at the old width forever.
//
// The child POLLS its size rather than trapping SIGWINCH: a POSIX shell
// runs a trap only after the current command completes, so a trap-based
// version asserted "applied within twenty seconds" while looking like it
// asserted delivery.
func TestALaterWindowSizeReachesTheChild(t *testing.T) {
	sb := newSandbox(t)
	resize := make(chan executor.WinSize, 1)

	script := `while :; do s=$(stty size </dev/tty); ` +
		`case "$s" in "60 200") echo "GOT $s"; exit 0;; esac; sleep 0.1; done`

	outCh := make(chan string, 1)
	go func() {
		outCh <- runTerminal(t, sb, script, executor.WinSize{Cols: 80, Rows: 24}, resize)
	}()

	// After a moment, so the child is certainly in its loop and reading the
	// ORIGINAL size first: without that this could pass against an
	// implementation that only ever set the initial size.
	time.Sleep(300 * time.Millisecond)
	resize <- executor.WinSize{Cols: 200, Rows: 60}

	select {
	case out := <-outCh:
		if !strings.Contains(out, "GOT 60 200") {
			t.Errorf("the child read %q, want 60 rows and 200 columns after the resize", out)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("the child never read the new window size")
	}
}

// A command's exit status is still the verdict, and reading a pty master
// after the last slave closes must not be reported as a failure: on Linux
// that is EIO, which is the platform's way of spelling EOF for this device.
func TestATerminalSessionReportsTheCommandsExitCode(t *testing.T) {
	sb := newSandbox(t)
	term := terminal(t, sb)

	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	var out lockedBuffer

	exit, err := term.RunTerminal(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "exit 3"}},
		stdin, &out, executor.WinSize{Cols: 80, Rows: 24}, nil)
	if err != nil {
		t.Fatalf("RunTerminal: %v", err)
	}
	if exit != 3 {
		t.Errorf("exit = %d, want 3", exit)
	}
}

// A session with no resize channel is ordinary, not an error: most clients
// that are not a terminal never send one.
func TestANilResizeChannelIsFine(t *testing.T) {
	sb := newSandbox(t)
	out := runTerminal(t, sb, "echo fine", executor.WinSize{Cols: 80, Rows: 24}, nil)
	if !strings.Contains(out, "fine") {
		t.Errorf("out = %q", out)
	}
}

// runTerminal runs one script to completion and returns everything it wrote.
func runTerminal(
	t *testing.T, sb executor.Sandbox, script string,
	ws executor.WinSize, resize <-chan executor.WinSize,
) string {
	t.Helper()
	term := terminal(t, sb)

	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	var out lockedBuffer

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := term.RunTerminal(ctx,
		executor.Cmd{Args: []string{"sh", "-c", script}},
		stdin, &out, ws, resize); err != nil {
		t.Fatalf("RunTerminal: %v", err)
	}
	return out.String()
}
