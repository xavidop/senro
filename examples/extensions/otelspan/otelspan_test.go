package otelspan_test

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/examples/extensions/otelspan"
)

func emit(t *testing.T, e *otelspan.Exporter, typ api.Type, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	e.Emit(api.Event{Type: typ, Run: "r", Step: "build", Attempt: 1, TraceID: "t", TS: time.Now(), Payload: b})
}

func TestStepSpanCarriesUsageWhenMeasured(t *testing.T) {
	for _, tc := range []struct {
		name  string
		usage *api.ResourceUsage
	}{
		{"measured", &api.ResourceUsage{CPUUser: 1500, CPUSystem: 300, MaxRSS: 4096}},
		{"unmeasured", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := otelspan.New(io.Discard)
			emit(t, e, api.StepStarted, api.StepStartedBody{SpanID: "s1"})
			emit(t, e, api.StepFinished, api.StepFinishedBody{State: api.StateSucceeded, Usage: tc.usage})
			spans := e.Spans()
			if len(spans) != 1 {
				t.Fatalf("spans = %d, want 1", len(spans))
			}
			a := spans[0].Attrs
			if tc.usage == nil {
				for _, k := range []string{"senro.cpu_user_ns", "senro.cpu_system_ns", "senro.max_rss_bytes"} {
					if _, ok := a[k]; ok {
						t.Errorf("unmeasured span has %s", k)
					}
				}
				return
			}
			if a["senro.cpu_user_ns"] != "1500" || a["senro.cpu_system_ns"] != "300" || a["senro.max_rss_bytes"] != "4096" {
				t.Errorf("attrs = %v", a)
			}
		})
	}
}
