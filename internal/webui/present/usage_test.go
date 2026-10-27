package present_test

import (
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/webui/present"
)

func TestDetailShowsUsageOnlyWhenMeasured(t *testing.T) {
	body := payload(t, `{"state":"succeeded","usage":{"cpu_user_ns":1000000000,"cpu_system_ns":200000000,"max_rss_bytes":89128960}}`)
	st := fold(t,
		api.Event{Type: api.StepCreated, Step: "a", Payload: payload(t, `{"kind":"exec"}`)},
		api.Event{Type: api.StepFinished, Step: "a", Attempt: 1, Payload: body},
		api.Event{Type: api.StepCreated, Step: "b", Payload: payload(t, `{"kind":"exec"}`)},
		api.Event{Type: api.StepFinished, Step: "b", Attempt: 1, Payload: payload(t, `{"state":"succeeded"}`)},
	)
	vals := map[string]string{}
	for _, f := range present.Detail(st, "a", now) {
		vals[f.Label] = f.Value
	}
	if vals["cpu"] != "1.2s" || vals["memory"] != "85 MB" {
		t.Errorf("detail = %v, want cpu 1.2s and memory 85 MB", vals)
	}
	for _, f := range present.Detail(st, "b", now) {
		if f.Label == "cpu" || f.Label == "memory" {
			t.Errorf("unmeasured step has %s field", f.Label)
		}
	}
}
