package present_test

import (
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/webui/present"
)

func readyDB(t *testing.T, finish bool) *api.RunState {
	t.Helper()
	evs := []api.Event{
		{Type: api.StepCreated, Step: "db", Payload: payload(t, `{"kind":"exec","service":true}`)},
		{Type: api.StepStarted, Step: "db", Attempt: 1, TS: now},
		{Type: api.ServiceReady, Step: "db", Attempt: 1, TS: now, Payload: payload(t, `{"endpoints":[{"port":5432,"address":"127.0.0.1:5432"}]}`)},
	}
	if finish {
		evs = append(evs, api.Event{Type: api.StepFinished, Step: "db", Attempt: 1, TS: now, Payload: payload(t, `{"state":"succeeded"}`)})
	}
	return fold(t, evs...)
}

func TestStepBadgeReadyForAReadyService(t *testing.T) {
	got := present.StepBadge(readyDB(t, false).Steps["db"])
	if got != (present.Badge{Label: "ready", Tone: present.ToneRunning}) {
		t.Fatalf("badge = %+v", got)
	}
}

func TestDetailListsServiceEndpoints(t *testing.T) {
	for _, finish := range []bool{false, true} {
		fields := present.Detail(readyDB(t, finish), "db", now)
		found := false
		for _, f := range fields {
			if f.Label == "endpoints" && f.Value == "127.0.0.1:5432 (port 5432)" {
				found = true
			}
		}
		if !found {
			t.Errorf("finish=%v: no endpoints field: %+v", finish, fields)
		}
	}
}
