package tui_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/tui"
)

// A service that has passed its probe reads ready on its row, and focusing it
// shows where it listens. The fixture is cut at seq 6, service.ready, so db
// has not yet finished.
func TestModelViewShowsAReadyService(t *testing.T) {
	raw, err := os.ReadFile("../../api/testdata/fixtures/service-and-hold.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	m := tui.New(&fakeSource{state: seedState("r1")})
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e api.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		if e.Seq <= 6 {
			m = update(t, m, tui.EventMsg{Event: e})
		}
	}
	if out := plain(m.View()); !regexp.MustCompile(`db\s.*ready`).MatchString(out) {
		t.Errorf("row for a ready service does not say ready:\n%s", out)
	}
	m = update(t, m, tea.KeyMsg{Type: tea.KeyEnter})
	if out := plain(m.View()); !strings.Contains(out, "db: ready at 127.0.0.1:5432 (port 5432)") {
		t.Errorf("focused header does not show the endpoint:\n%s", out)
	}
}
