package duration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/duration"
)

func finishedWithUsage(seq int, step, group string, d, user, system time.Duration) string {
	return `{"v":1,"seq":` + itoa(seq) + `,"ts":"2026-08-14T00:00:00Z","type":"step.finished",` +
		`"run":"r1","step":"` + step + `","group":"` + group + `",` +
		`"payload":{"state":"succeeded","duration_ns":` + itoa(int(d)) + `,"usage":{"cpu_user_ns":` +
		itoa(int(user)) + `,"cpu_system_ns":` + itoa(int(system)) + `,"max_rss_bytes":1024}}}`
}

// A history written before cpu existed keeps its expansions bytes and gains
// a cpu section for the units that reported usage; Durations never sees it.
func TestRecordAddsCPUToAnOldFileWithoutTouchingExpansions(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "durations.json")
	old := "{\n  \"version\": 1,\n  \"expansions\": {\n    \"test\": {\n      \"apps/api\": 3000000000,\n      \"apps/web\": 1\n    }\n  }\n}\n"
	if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	runDir := writeLedger(t,
		finishedWithUsage(1, "test[unit=apps/web]", "test", 12*time.Second, 5*time.Second, time.Second),
		finished(2, "test[unit=apps/api]", "test", "succeeded", 3*time.Second),
	)
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"version\": 1,\n  \"expansions\": {\n    \"test\": {\n      \"apps/api\": 3000000000,\n      \"apps/web\": 12000000000\n    }\n  },\n  \"cpu\": {\n    \"test\": {\n      \"apps/web\": 6000000000\n    }\n  }\n}\n"
	if string(b) != want {
		t.Errorf("file =\n%s\nwant\n%s", b, want)
	}
	got := durations(t, duration.FromFile(path), root, "test")
	if len(got) != 2 || got["apps/web"] != 12*time.Second {
		t.Errorf("Durations = %v", got)
	}
}

// Review Focus 5, the other half: an old file whose units were not
// re-observed with usage keeps its expansions bytes exactly.
func TestRecordLeavesExpansionsBytesAloneWhenNoUsageIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.json")
	old := "{\n  \"version\": 1,\n  \"expansions\": {\n    \"test\": {\n      \"a\": 1000000000\n    }\n  }\n}\n"
	if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	runDir := writeLedger(t, finished(1, "test[unit=a]", "test", "succeeded", time.Second))
	if err := duration.Record(runDir, path); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if string(b) != old {
		t.Errorf("file changed:\n%s", b)
	}
	if strings.Contains(string(b), "cpu") {
		t.Errorf("file has a cpu section:\n%s", b)
	}
}
