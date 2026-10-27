package render_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/render"
	"github.com/xavidop/senro/internal/source"
)

func TestFormatUsage(t *testing.T) {
	cases := []struct {
		name string
		in   *api.ResourceUsage
		want string
	}{
		{"nil", nil, ""},
		{"small", &api.ResourceUsage{CPUUser: 40 * time.Millisecond, MaxRSS: 640 << 10}, "cpu 0s  rss 640 KB"},
		{"sums user and system, rounds to 100ms",
			&api.ResourceUsage{CPUUser: 900 * time.Millisecond, CPUSystem: 340 * time.Millisecond, MaxRSS: 85 << 20},
			"cpu 1.2s  rss 85 MB"},
		{"large", &api.ResourceUsage{CPUUser: 90 * time.Second, MaxRSS: 1288490189}, "cpu 1m30s  rss 1.2 GB"},
	}
	for _, c := range cases {
		if got := render.FormatUsage(c.in); got != c.want {
			t.Errorf("%s: FormatUsage = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestPlainFinishedLineCarriesUsageOnlyWhenMeasured(t *testing.T) {
	events := twoStepRun()
	events[4].Payload = mustBody(api.StepFinishedBody{
		State: api.StateSucceeded,
		Usage: &api.ResourceUsage{CPUUser: time.Second, CPUSystem: 200 * time.Millisecond, MaxRSS: 85 << 20},
	})
	fs, err := source.OpenFile(writeRun(t, events), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fs.Close() }()
	var buf bytes.Buffer
	if _, err := render.Plain(context.Background(), fs, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "setup succeeded  cpu 1.2s  rss 85 MB\n") {
		t.Errorf("setup line has no usage:\n%s", out)
	}
	if !strings.Contains(out, "build succeeded\n") {
		t.Errorf("an unmeasured step's line changed:\n%s", out)
	}
}
