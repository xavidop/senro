package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/xavidop/senro/api"
)

// Layout: a step pane on the left with expansion groups collapsed, a log
// pane on the right for the focused step, and a footer carrying run status,
// the last control result, and the key bindings.
var (
	stepsPaneStyle = lipgloss.NewStyle().Padding(0, 1)
	logPaneStyle   = lipgloss.NewStyle().Padding(0, 1)
	footerStyle    = lipgloss.NewStyle().Padding(0, 1)
	helpStyle      = lipgloss.NewStyle().Padding(0, 1)
	cursorStyle    = lipgloss.NewStyle().Bold(true)
	failedStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	successStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	streamErrStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("1"))
)

// helpText answers '?'. Any key still reserved but inert must be listed
// here rather than omitted: the help screen is where an operator who tries
// one finds out why. That list is currently empty.
//
// b/B and p/P are two keys each rather than toggles: whether a breakpoint
// is set, or the run is paused, is the ENGINE's answer, and another client
// may have changed it a moment ago. Each key is one wire operation, and
// the engine's refusal is what the footer shows.
const helpText = `senro attach: keys

  enter          focus the highlighted step: shows its log, and becomes
                 the target for r, x, b, B, R, w and pgup
  up/down, j/k   move the highlighted row
  r              retry the focused step
  R              rerun the focused step and everything downstream of it
  x              skip the focused step: it, and every step that needs it,
                 settle as skipped_manual and never run
  b              set a breakpoint on the focused step: the run stops before
                 it and waits, making whatever other progress it can
  B              clear that breakpoint, releasing the step
  w              snapshot the focused step's workspaces now, for inspection
                 with senro ws pull. Answerable for a step that has not run,
                 so pair it with b; the capture is diagnostic and enters no
                 cache key and no step's recorded output
  p              pause the whole run: the scheduler dispatches nothing new,
                 and whatever is already running is left alone to finish
  P              resume it, and the scheduler picks up where it left off
  s              open a shell on the focused step: its workspaces, read-only,
                 at the paths the step saw them, on the step's own executor.
                 No secrets are delivered, and the session has no terminal:
                 no prompt, no line editing. ^D returns here. Needs a live
                 run; a run tailed from disk has no engine to host one
  a              approve the analyzer's proposal for the focused step: the
                 engine performs the remedy it named, which in this build is
                 always retrying the step, and records who approved it
  A              reject that proposal instead: nothing is performed, and the
                 proposal is settled so it stops being offered
  c, ctrl+c      cancel the run
  q              detach, and the run keeps going
  /              filter the step list by substring (enter applies, esc cancels)
  pgup           load older log history for the focused step
  ?              toggle this help

press ? or esc to close`

// View implements tea.Model.
//
// The whole render happens under mu, not just the read of the state
// pointer: Apply mutates RunState's maps and slices IN PLACE, so the drain
// goroutine can be mutating the very map View() ranges over. Releasing the
// lock after reading the pointer would leave every read into its contents
// unprotected: a real `concurrent map read and map write` crash, not a
// hypothetical one. See
// TestModelViewIsRaceSafeWhileEventsStreamConcurrently.
func (m *Model) View() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.state

	if m.showHelp {
		return helpStyle.Render(helpText)
	}

	rows := filterRows(topLevelRows(st), m.filter)
	body := lipgloss.JoinHorizontal(lipgloss.Top, m.renderSteps(st, rows), m.renderLog())
	return lipgloss.JoinVertical(lipgloss.Left, body, m.renderFooter(st))
}

func (m *Model) renderSteps(st *api.RunState, rows []string) string {
	var b strings.Builder
	if len(rows) == 0 {
		b.WriteString("(no steps yet)\n")
	}
	for i, id := range rows {
		marker := "  "
		switch {
		case id == m.focused:
			marker = "* "
		case i == m.cursor:
			marker = "> "
		}
		line := marker + id
		if s := st.Steps[id]; s != nil {
			line += " " + stepStatusLabel(s)
		}
		if _, isGroup := st.Expansions[id]; isGroup {
			c := st.Group(id)
			line += fmt.Sprintf(" (%d units · %d failed · %d cached · %d running)",
				c.Total, c.Failed, c.Cached, c.Running)
		}
		if i == m.cursor {
			line = cursorStyle.Render(line)
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return stepsPaneStyle.Render(strings.TrimRight(b.String(), "\n"))
}

// stepStatusLabel is the same three-way rule render.Plain's printStep uses
// (terminal state [+ error] / started-but-running / nothing), so a step
// reads the same way whether a human is looking at a CI log or the TUI.
func stepStatusLabel(s *api.StepState) string {
	switch {
	case s.State != "" && s.Error != "":
		return string(s.State) + ": " + s.Error
	case s.State.Failed():
		return failedStyle.Render(string(s.State))
	case s.State == api.StateSucceeded || s.State == api.StateCached || s.State == api.StateRecovered:
		return successStyle.Render(string(s.State))
	case s.State != "":
		return string(s.State)
	case s.Paused:
		// Before the started case: a step held at a breakpoint has never
		// started, so it would otherwise render blank, identical to one
		// waiting on a dependency, leaving nothing on screen to say the run
		// is stopped rather than slow.
		return "paused"
	case !s.Started.IsZero():
		return "running"
	default:
		return ""
	}
}

// proposalLine is what the analyzer said about the focused step, shown
// above its log rather than behind a key: 'a' approves something, and an
// operator must be able to read what before pressing it. A gate whose
// subject is invisible is a prompt, not a gate.
func (m *Model) proposalLine() string {
	if s := m.summaries[m.focused]; s != "" {
		return "proposal: " + s + "  (a approve · A reject)\n"
	}
	return ""
}

func (m *Model) renderLog() string {
	if m.focused == "" {
		return logPaneStyle.Render("(press enter on a step to view its log)")
	}
	prop := m.proposalLine()
	r := m.logs[m.focused]
	if r == nil || len(r.Lines()) == 0 {
		return logPaneStyle.Render(prop + m.focused + ": (no log output yet)")
	}
	var b strings.Builder
	b.WriteString(prop)
	b.WriteString(m.focused)
	b.WriteString(":\n")
	if r.StartOffset() > 0 {
		// Scrollback exists but has not been pulled in: a pane that
		// silently starts mid-file looks identical to one showing
		// everything. See logRing.StartOffset and loadOlderLogsCmd.
		b.WriteString("── more history above: pgup ──\n")
	}
	b.WriteString(strings.Join(r.Lines(), "\n"))
	return logPaneStyle.Render(b.String())
}

func (m *Model) renderFooter(st *api.RunState) string {
	status := "running"
	switch {
	case st == nil:
	case st.Run.Done:
		status = string(st.Run.Status)
	case st.Run.Paused:
		// Before the running case: a paused run's remaining steps have no
		// Started and no State, so every row looks like one waiting on a
		// dependency. Same reasoning as stepStatusLabel's Paused case.
		status = "paused"
	}
	line := fmt.Sprintf("run: %s | enter focus · r retry · R rerun-from · x skip · b/B breakpoint · w snapshot · p/P pause · s shell · a/A proposal · c/ctrl+c cancel · q detach · / filter · ? help", status)
	if m.streamErr != nil {
		// Sticky and distinctly styled: not an ordinary status line an
		// operator can afford to miss. See Model.streamErr.
		line += " | " + streamErrStyle.Render("STREAM ERROR: "+m.streamErr.Error())
	}
	switch {
	case m.filtering:
		line += " | filter: " + m.filterInput + "_"
	case m.filter != "":
		line += " | filter: " + m.filter
	}
	if m.status != "" {
		line += " | " + m.status
	}
	return footerStyle.Render(line)
}
