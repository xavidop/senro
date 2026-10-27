package tui_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

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
