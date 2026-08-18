package duration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/duration"
)

// writeLedger writes a run directory containing just the events a test needs,
// which is all Record reads. Hand-written rather than produced by a real run:
// this package's job is the fold, and a fold is easiest to pin against a
// ledger whose exact contents are visible in the test.
func writeLedger(t *testing.T, lines ...string) string {
	t.Helper()
	dir := t.TempDir()
	body := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func finished(seq int, step, group, state string, d time.Duration) string {
	return `{"v":1,"seq":` + itoa(seq) + `,"ts":"2026-08-14T00:00:00Z","type":"step.finished",` +
		`"run":"r1","step":"` + step + `","group":"` + group + `",` +
		`"payload":{"state":"` + state + `","duration_ns":` + itoa(int(d)) + `}}`
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func durations(t *testing.T, h interface {
	Durations(context.Context, string, string) (map[string]time.Duration, error)
}, root, expansion string) map[string]time.Duration {
	t.Helper()
	got, err := h.Durations(context.Background(), root, expansion)
	if err != nil {
		t.Fatalf("Durations(%q): %v", expansion, err)
	}
	return got
}

// TestAMissingFileIsColdRatherThanAnError is the first run of every pipeline
// that ever uses this. Nothing has been recorded, the file is not there, and
// that is the ordinary case rather than a fault: partitioning falls back to
// balancing by count and the build goes ahead.
func TestAMissingFileIsColdRatherThanAnError(t *testing.T) {
	root := t.TempDir()
	got := durations(t, duration.FromFile(".senro/durations.json"), root, "test")
	if len(got) != 0 {
		t.Errorf("a missing history reported %v, want nothing", got)
	}
}

// TestFromFileReadsOneExpansionsDurations, and only that expansion's: "lint
// apps/web" and "test apps/web" are two different amounts of work on one
// directory, and a history that mixed them would balance neither.
func TestFromFileReadsOneExpansionsDurations(t *testing.T) {
	root := t.TempDir()
	body := `{"version":1,"expansions":{` +
		`"test":{"apps/web":12000000000,"apps/api":3000000000},` +
		`"lint":{"apps/web":500000000}}}`
	if err := os.WriteFile(filepath.Join(root, "d.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	h := duration.FromFile("d.json")

	got := durations(t, h, root, "test")
	if len(got) != 2 || got["apps/web"] != 12*time.Second || got["apps/api"] != 3*time.Second {
		t.Errorf(`Durations("test") = %v`, got)
	}
	if got := durations(t, h, root, "lint"); len(got) != 1 || got["apps/web"] != 500*time.Millisecond {
		t.Errorf(`Durations("lint") = %v`, got)
	}
	if got := durations(t, h, root, "never-recorded"); len(got) != 0 {
		t.Errorf("an expansion the file has never heard of reported %v, want nothing", got)
	}
}

// TestFromFileRefusesAFileItCannotRead: a missing history is cold, a corrupt
// one is a fault. Treating the two the same would let a typo in a committed
// file silently switch the whole fleet back to balancing by count, and the
// only symptom would be a fan-out that is slower than it used to be.
func TestFromFileRefusesAFileItCannotRead(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "d.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := duration.FromFile("d.json").Durations(context.Background(), root, "test"); err == nil {
		t.Fatal("a corrupt history file was accepted as an empty one")
	}
}

// TestFromFileRefusesAVersionItDoesNotKnow, rather than reading a future
// file's expansions map as if the meaning of the numbers in it had not
// changed.
func TestFromFileRefusesAVersionItDoesNotKnow(t *testing.T) {
	root := t.TempDir()
	body := `{"version":99,"expansions":{"test":{"apps/web":1}}}`
	if err := os.WriteFile(filepath.Join(root, "d.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := duration.FromFile("d.json").Durations(context.Background(), root, "test")
	if err == nil {
		t.Fatal("a history file from a future version was read anyway")
	}
	if !strings.Contains(err.Error(), "99") {
		t.Errorf("the error does not name the version it found: %v", err)
	}
}

// TestNoneIsAlwaysCold is the explicit opt-out, for a pipeline that wants the
// partition and has nowhere to keep a history.
func TestNoneIsAlwaysCold(t *testing.T) {
	got := durations(t, duration.None(), t.TempDir(), "test")
	if len(got) != 0 {
		t.Errorf("None() reported %v", got)
	}
	if duration.None().Describe() == "" {
		t.Error("None() describes itself as nothing at all")
	}
}

// TestRecordFoldsARunIntoAHistoryFile is the other half: the durations are
// already in the event stream, and this is what puts them somewhere the next
// build can read.
func TestRecordFoldsARunIntoAHistoryFile(t *testing.T) {
	runDir := writeLedger(t,
		finished(1, "test[unit=apps/web]", "test", "succeeded", 12*time.Second),
		finished(2, "test[unit=apps/api]", "test", "succeeded", 3*time.Second),
		finished(3, "lint[unit=apps/web]", "lint", "succeeded", 500*time.Millisecond),
	)
	root := t.TempDir()
	path := filepath.Join(root, ".senro", "durations.json")
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	h := duration.FromFile(path)
	got := durations(t, h, root, "test")
	if len(got) != 2 || got["apps/web"] != 12*time.Second || got["apps/api"] != 3*time.Second {
		t.Errorf(`recorded "test" = %v`, got)
	}
	if got := durations(t, h, root, "lint"); len(got) != 1 {
		t.Errorf(`recorded "lint" = %v`, got)
	}
}

// TestRecordSkipsAStepThatDidNotDoTheWork. A cached step finishes in
// milliseconds and a skipped one in none at all, and recording either as the
// unit's duration would tell the next partition that the slowest module in
// the tree is free, which is exactly the input that produces the unbalanced
// split this feature exists to avoid.
func TestRecordSkipsAStepThatDidNotDoTheWork(t *testing.T) {
	runDir := writeLedger(t,
		finished(1, "test[unit=apps/web]", "test", "cached", 4*time.Millisecond),
		finished(2, "test[unit=apps/api]", "test", "skipped_condition", 0),
		finished(3, "test[unit=apps/cli]", "test", "skipped_upstream_failed", 0),
		finished(4, "test[unit=apps/db]", "test", "failed", 2*time.Second),
		finished(5, "test[unit=apps/ui]", "test", "succeeded", 9*time.Second),
	)
	root := t.TempDir()
	path := filepath.Join(root, "d.json")
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	got := durations(t, duration.FromFile(path), root, "test")
	if len(got) != 1 || got["apps/ui"] != 9*time.Second {
		t.Errorf("recorded %v, want only the step that ran to completion", got)
	}
}

// TestRecordIgnoresAStepWithNoUnit covers a partitioned expansion, whose
// children are shards rather than units: there is no way to tell how much of
// a shard's ten minutes belonged to which of its five modules, and inventing
// a share would corrupt the history rather than extend it. Leaving it alone
// keeps the file stale, which is recoverable; writing a guess into it is not.
func TestRecordIgnoresAStepWithNoUnit(t *testing.T) {
	runDir := writeLedger(t,
		finished(1, "test[shard=0]", "test", "succeeded", 30*time.Second),
		finished(2, "install", "", "succeeded", time.Second),
	)
	root := t.TempDir()
	path := filepath.Join(root, "d.json")
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if got := durations(t, duration.FromFile(path), root, "test"); len(got) != 0 {
		t.Errorf("recorded %v from a run with no per-unit step in it", got)
	}
}

// TestRecordMergesRatherThanReplacing: a run narrowed by Affected touches
// three of forty modules, and a Record that replaced the file would throw
// away the other thirty-seven every time somebody pushed a one-line change.
func TestRecordMergesRatherThanReplacing(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "d.json")
	first := writeLedger(t,
		finished(1, "test[unit=apps/web]", "test", "succeeded", 12*time.Second),
		finished(2, "test[unit=apps/api]", "test", "succeeded", 3*time.Second),
	)
	if err := duration.Record(first, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	second := writeLedger(t,
		finished(1, "test[unit=apps/api]", "test", "succeeded", 4*time.Second),
	)
	if err := duration.Record(second, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	got := durations(t, duration.FromFile(path), root, "test")
	if got["apps/web"] != 12*time.Second {
		t.Errorf("the module the second run did not touch is now %v, want 12s", got["apps/web"])
	}
	if got["apps/api"] != 4*time.Second {
		t.Errorf("the module the second run did touch is %v, want the newer 4s", got["apps/api"])
	}
}

// TestRecordWritesTheSameBytesForTheSameRun. The file is meant to be
// committed, so a run that observed nothing new must produce no diff:
// otherwise every build dirties the working tree and nobody keeps it.
func TestRecordWritesTheSameBytesForTheSameRun(t *testing.T) {
	runDir := writeLedger(t,
		finished(1, "test[unit=z]", "test", "succeeded", 3*time.Second),
		finished(2, "test[unit=a]", "test", "succeeded", time.Second),
		finished(3, "test[unit=m]", "test", "succeeded", 2*time.Second),
	)
	root := t.TempDir()
	path := filepath.Join(root, "d.json")
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Errorf("recording the same run twice changed the file:\n%s\nvs\n%s", first, second)
	}
	if !strings.HasSuffix(string(first), "\n") {
		t.Error("the history file has no trailing newline")
	}
}

// TestRecordRefusesARunDirectoryWithNoLedger, rather than quietly writing an
// empty history over a good one.
func TestRecordRefusesARunDirectoryWithNoLedger(t *testing.T) {
	if err := duration.Record(t.TempDir(), filepath.Join(t.TempDir(), "d.json")); err == nil {
		t.Fatal("Record accepted a directory that holds no run")
	}
}
