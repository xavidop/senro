package tui

import (
	"context"
	"fmt"
	"io"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro/internal/source"
)

// The 's' key opens an interactive session on the focused step and hands
// it the real terminal until it ends.
//
// Through tea.Exec rather than a Cmd: a Cmd runs while the program still
// owns the terminal, and a session needs the terminal itself in ordinary
// line-buffered mode. tea.Exec suspends the renderer, restores the
// terminal, runs the ExecCommand against the real streams, and redraws
// afterwards; writing to os.Stdout from a Cmd would interleave a session's
// output with the TUI's frames.
//
// The model keeps folding throughout: the subscription goroutine is
// untouched by tea.Exec, so the run carries on and the screen is current
// the moment the session ends. That is the pairing this exists for: stop
// at a breakpoint, stand in the step, leave, clear the breakpoint.

// shellSession is a tea.ExecCommand that runs one session against the
// streams bubbletea hands it, which are the real terminal's. It holds a
// context because ExecCommand's Run takes none: the model's context, which
// is what makes 'q' or a killed program end a session.
type shellSession struct {
	ctx  context.Context
	src  source.Sheller
	step string

	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	// result is filled in by Run so the callback can report how the session
	// ended, a different question from whether Run itself failed: a shell
	// that exits 1 is a successful session.
	result source.ShellResult
}

func (s *shellSession) SetStdin(r io.Reader)  { s.stdin = r }
func (s *shellSession) SetStdout(w io.Writer) { s.stdout = w }
func (s *shellSession) SetStderr(w io.Writer) { s.stderr = w }

// Run blocks for the whole session, which is what tea.Exec expects: the
// program is paused and the terminal is the session's until this returns.
func (s *shellSession) Run() error {
	// Written to the released terminal rather than the status line: the TUI
	// is not drawing, and this is the moment an operator needs to be told
	// what kind of session they are in. cmd/senro prints the same banner.
	_, _ = fmt.Fprintf(s.stderr, "\nsenro: a session on %s. No terminal is attached: "+
		"no prompt, no line editing, no job control.\nType a command and press enter; "+
		"^D ends the session and returns to the run.\n\n", s.step)

	res, err := s.src.Shell(s.ctx, source.ShellRequest{
		Step:   s.step,
		Stdin:  s.stdin,
		Stdout: s.stdout,
		Stderr: s.stderr,
	})
	s.result = res
	return err
}

// ShellFinishedMsg reports one session's outcome back into the model, so the
// footer can say what happened rather than the screen simply reappearing.
type ShellFinishedMsg struct {
	Step   string
	Result source.ShellResult
	Err    error
}

// openShell is the 's' key. It refuses, with an explanation, in the two
// cases where there is nothing to open: no step focused (as every
// step-scoped key does, rather than letting the engine answer
// missing_step), and a Source that cannot host a session at all, the
// offline case, where the message names `senro ws pull` instead.
func (m *Model) openShell() (tea.Model, tea.Cmd) {
	if m.focused == "" {
		m.status = "shell: no step focused, press enter on a step first"
		return m, nil
	}
	sh, ok := m.src.(source.Sheller)
	if !ok {
		m.status = "shell: this run is not live, so there is no engine to host a session; " +
			"use senro ws pull to write its workspaces out"
		return m, nil
	}

	step := m.focused
	session := &shellSession{ctx: m.ctx, src: sh, step: step}
	return m, tea.Exec(session, func(err error) tea.Msg {
		return ShellFinishedMsg{Step: step, Result: session.result, Err: err}
	})
}

// handleShellFinished turns a session's outcome into the footer line an
// operator sees when the screen comes back. Three sentences for three
// genuinely different things: the transport failed, the engine refused, or
// a session ran and ended.
func (m *Model) handleShellFinished(msg ShellFinishedMsg) (tea.Model, tea.Cmd) {
	switch {
	case msg.Err != nil:
		m.status = fmt.Sprintf("shell %s: %v", msg.Step, msg.Err)
	case !msg.Result.OK:
		reason := msg.Result.Error
		if reason == "" {
			reason = "refused"
		}
		m.status = fmt.Sprintf("shell %s refused: %s", msg.Step, reason)
	case msg.Result.Error != "":
		// A session taken away rather than exiting: the run ended under it,
		// or the connection broke. Distinguished from a clean exit, or an
		// operator concludes their shell simply closed.
		m.status = fmt.Sprintf("shell %s ended: %s", msg.Step, msg.Result.Error)
	default:
		m.status = fmt.Sprintf("shell %s exited %d", msg.Step, msg.Result.ExitCode)
	}
	return m, nil
}
