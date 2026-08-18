//go:build unix

package localexec

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/ptyx"
)

// RunTerminal runs one command on a real pseudo-terminal.
//
// Unlike RunInteractive, the child gets a controlling terminal and its own
// session: job control, line editing, ^C delivered as SIGINT to its
// foreground process group, and a window size that follows the operator's.
//
// The slave MUST be closed in the parent once the child holds it: a pty
// master reports EOF only when NO process holds the slave open, so keeping
// the parent's copy would leave the output copy running forever after the
// child exits and hang the session.
//
// The final read error is discarded: reading a pty master after the last
// slave closes returns EIO on Linux, which is that device's spelling of EOF,
// not a failure. The command's exit status is the verdict, from Wait.
func (s *sandbox) RunTerminal(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, out io.Writer,
	initial senroexec.WinSize, resize <-chan senroexec.WinSize,
) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("localexec: %w: empty command", senroexec.ErrInfra)
	}

	master, slave, err := ptyx.Open()
	if err != nil {
		return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	defer func() { _ = master.Close() }()

	// Set BEFORE the child starts, so a program that reads its size once at
	// startup reads the operator's rather than "0 0".
	if initial.Cols > 0 && initial.Rows > 0 {
		if err := ptyx.SetSize(master, ptyx.WinSize{Cols: initial.Cols, Rows: initial.Rows}); err != nil {
			_ = slave.Close()
			return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
		}
	}

	cmd := exec.CommandContext(ctx, c.Args[0], c.Args[1:]...)
	cmd.Dir = s.commandDir(c)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	// The same explicit environment Run builds. TERM is added only if the
	// caller did not: no TERM makes full-screen programs fall back to dumb,
	// and overriding a declared one would ignore the operator's real
	// terminal.
	cmd.Env = withTerm(envWithDefaultPATH(c.Env))
	cmd.WaitDelay = waitDelay
	// Setsid and Setctty are what make this a terminal rather than merely a
	// tty-shaped pipe: the child leads a new session and the pty is its
	// CONTROLLING terminal, which is the thing signals are delivered through.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}

	if err := cmd.Start(); err != nil {
		_ = slave.Close()
		return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	// See the doc: the parent must not keep this open.
	_ = slave.Close()

	done := make(chan struct{})
	defer close(done)
	go pumpResize(done, master, resize)

	go func() {
		// Errors discarded for RunInteractive's reasons: the client vanished
		// or the child exited, and neither should rewrite the exit status.
		_, _ = io.Copy(master, stdin)
		// A pty has no EOF to forward: closing the master would send SIGHUP,
		// not end-of-input. So the end of input is delivered as the VEOF
		// byte, exactly what an operator's own ^D puts on the wire; a
		// command that ignores it keeps running, as on a real terminal.
		_, _ = master.Write([]byte{veof})
	}()

	// Copied on THIS goroutine so the session's output is fully drained
	// before Wait's verdict is returned.
	_, _ = io.Copy(out, master)

	return s.classifyRunError(ctx, cmd, cmd.Wait())
}

// veof is ^D, the byte a terminal's line discipline turns into end-of-input.
// 0x04 is the default VEOF on every unix, and senro never changes a pty's
// termios, so the default applies.
const veof = 0x04

// pumpResize applies every window size the client sends until the session
// ends. A failed resize is dropped rather than reported: the size is
// advisory, and ending a session over a cosmetic problem would be worse.
func pumpResize(done <-chan struct{}, master *os.File, resize <-chan senroexec.WinSize) {
	for {
		select {
		case <-done:
			return
		case ws, ok := <-resize:
			if !ok {
				return
			}
			_ = ptyx.SetSize(master, ptyx.WinSize{Cols: ws.Cols, Rows: ws.Rows})
		}
	}
}

// withTerm adds a TERM to an environment that has none. xterm-256color:
// every terminal an operator plausibly attaches from understands it, and a
// conservative value would just lose colour. A declared TERM always wins.
func withTerm(env []string) []string {
	for _, kv := range env {
		if len(kv) >= 5 && kv[:5] == "TERM=" {
			return env
		}
	}
	return append(env, "TERM=xterm-256color")
}
