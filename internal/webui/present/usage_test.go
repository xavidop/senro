package present_test

import (
	"testing"
	"time"

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

func TestFormatRSSUnitBoundaries(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1 KB"},
		{1048166, "1 MB"},
		{1023 * 1024 * 1024, "1023 MB"},
		{1073341030, "1.0 GB"},
		{1 << 30, "1.0 GB"},
	}
	for _, c := range cases {
		if got := present.FormatRSS(&api.ResourceUsage{MaxRSS: c.n}); got != c.want {
			t.Errorf("FormatRSS(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestFormatCPUIsAlwaysSeconds(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "0.0s"},
		{300 * time.Millisecond, "0.3s"},
		{1200 * time.Millisecond, "1.2s"},
		{75400 * time.Millisecond, "75.4s"},
		{3725 * time.Second, "3725.0s"},
	}
	for _, c := range cases {
		if got := present.FormatCPU(&api.ResourceUsage{CPUUser: c.d}); got != c.want {
			t.Errorf("FormatCPU(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestFormatUsageOmitsUnmeasuredMemory(t *testing.T) {
	if got := present.FormatUsage(&api.ResourceUsage{CPUUser: 1200 * time.Millisecond}); got != "cpu 1.2s" {
		t.Errorf("FormatUsage = %q, want %q", got, "cpu 1.2s")
	}
	st := fold(t,
		api.Event{Type: api.StepCreated, Step: "a", Payload: payload(t, `{"kind":"exec"}`)},
		api.Event{Type: api.StepFinished, Step: "a", Attempt: 1, Payload: payload(t, `{"state":"succeeded","usage":{"cpu_user_ns":1200000000}}`)},
	)
	vals := map[string]string{}
	for _, f := range present.Detail(st, "a", now) {
		vals[f.Label] = f.Value
	}
	if vals["cpu"] != "1.2s" || vals["memory"] != "" {
		t.Errorf("detail = %v, want cpu 1.2s and no memory", vals)
	}
}
