package conformance_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// TestATerminalIsAtItsDeclaredSizeBeforeTheCommandReadsIt.
//
// Terminal's doc gives the reason initial exists: "a pty whose creator sets
// no size reports '0 0' and a full-screen program that reads that draws
// nothing". A full-screen program reads its size ONCE, at startup, so a
// terminal that reaches the right size a moment later is a terminal that
// reached it too late. The command here is what such a program does first
// and nothing else.
//
// Two promises, because the substrates differ in what they can offer and
// pretending otherwise would hide which is which:
//
//   - An executor that can size the pty at CREATION must be exact, every
//     time. localexec opens the device itself; containerexec passes
//     HostConfig.ConsoleSize. Neither has an excuse.
//   - An executor whose platform only accepts a size AFTER the command is
//     running (ptySizedAfterStart: Kubernetes, whose exec subresource takes
//     no initial size) must still reach the declared size promptly, so a
//     program that redraws on SIGWINCH is correct within a moment and the
//     size is never simply lost.
func TestATerminalIsAtItsDeclaredSizeBeforeTheCommandReadsIt(t *testing.T) {
	const tries = 5
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			// A command that reads the size immediately, and again after a
			// grace: the first answer is the at-creation promise, the second
			// is the eventually promise.
			const script = `stty size; sleep 1; printf 'later='; stty size`

			var early []string
			for i := range tries {
				sb := sandboxOn(t, ex, senroexec.SandboxSpec{
					StepID: "sizerace", Attempt: i + 1,
				})
				term, ok := sb.(senroexec.Terminal)
				if !ok {
					t.Skipf("%s hosts no terminal", tg.name)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
				var out strings.Builder
				resize := make(chan senroexec.WinSize)
				close(resize)
				// Open for the life of the command: a terminal has no EOF.
				stdin, w := io.Pipe()
				_, err := term.RunTerminal(ctx,
					senroexec.Cmd{Args: []string{tg.shell, "-c", script}},
					stdin, &out, senroexec.WinSize{Cols: 120, Rows: 40}, resize)
				_ = w.Close()
				cancel()
				if err != nil {
					t.Fatalf("RunTerminal: %v", err)
				}
				got := strings.ReplaceAll(out.String(), "\r\n", "\n")

				// The eventually promise holds on every executor, always.
				if !strings.Contains(got, "later=40 120") {
					t.Errorf("attempt %d never reached the declared size at all: %q", i+1, got)
				}
				first, _, _ := strings.Cut(got, "\n")
				if strings.TrimSpace(first) != "40 120" {
					early = append(early, strings.TrimSpace(first))
				}
			}

			if tg.ptySizedAfterStart {
				// Stated rather than skipped: if this platform ever starts
				// sizing at creation, the field is wrong and should go.
				t.Logf("%d of %d sessions read the size before it arrived, which this platform "+
					"permits: its exec subresource takes no initial size. Saw: %q",
					len(early), tries, early)
				return
			}
			if len(early) > 0 {
				t.Errorf("%d of %d terminal sessions were not at 40x120 when the command read the "+
					"size, and this executor can size the pty at creation. Saw: %q",
					len(early), tries, early)
			}
		})
	}
}
