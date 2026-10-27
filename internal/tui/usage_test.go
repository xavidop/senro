package tui_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/tui"
)

func TestModelStepRowShowsUsage(t *testing.T) {
	m := tui.New(&fakeSource{state: seedState("r1")})
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "setup")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "setup", 1)})
	body, _ := json.Marshal(api.StepFinishedBody{
		State: api.StateSucceeded,
		Usage: &api.ResourceUsage{CPUUser: time.Second, CPUSystem: 200 * time.Millisecond, MaxRSS: 85 << 20},
	})
	m = update(t, m, tui.EventMsg{Event: api.Event{Type: api.StepFinished, Run: "r1", Step: "setup", Attempt: 1, Payload: body}})
	if out := plain(m.View()); !strings.Contains(out, "cpu 1.2s  rss 85 MB") {
		t.Errorf("View() has no usage on the step row:\n%s", out)
	}
}

// A step row longer than the terminal is cut there, ellipsis last, instead of
// running past it; the state is before the usage, so the cut takes the usage
// first and the state column stays on screen. The steps pane is isolated by
// reading the line up to the log pane's first word.
func TestModelStepRowWithUsageIsClippedAtNarrowWidth(t *testing.T) {
	const width = 60
	id := "a-rather-long-step-name-for-a-narrow-terminal"
	m := tui.New(&fakeSource{state: seedState("r1")})
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tea.WindowSizeMsg{Width: width, Height: 20})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", id)})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", id, 1)})
	body, _ := json.Marshal(api.StepFinishedBody{
		State: api.StateSucceeded,
		Usage: &api.ResourceUsage{CPUUser: time.Second, CPUSystem: 200 * time.Millisecond, MaxRSS: 85 << 20},
	})
	m = update(t, m, tui.EventMsg{Event: api.Event{Type: api.StepFinished, Run: "r1", Step: id, Attempt: 1, Payload: body}})
	out := plain(m.View())
	var row string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "a-rather-long") {
			row = line
		}
	}
	if row == "" {
		t.Fatalf("no step row in:\n%s", out)
	}
	pane := row
	if i := strings.Index(row, "(press enter"); i >= 0 {
		pane = row[:i]
	}
	if w := lipgloss.Width(strings.TrimRight(pane, " ")); w > width {
		t.Errorf("step row is %d cells on a %d-cell terminal: %q", w, width, pane)
	}
	if !strings.Contains(pane, "succeeded") || !strings.Contains(pane, "…") {
		t.Errorf("row lost its state or has no ellipsis: %q", pane)
	}
}
