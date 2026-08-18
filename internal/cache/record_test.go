package cache_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/scratch"
)

func TestRecordsRoundTripThroughARunDirectory(t *testing.T) {
	dir := t.TempDir()
	want := cache.Record{
		Step:   "build/test[unit=services/api]",
		Digest: sampleKey().Digest(),
		Key:    sampleKey(),
		Hit:    false,
		Reason: cache.ReasonKeyChanged,
		Diffs:  []cache.Diff{{Name: "input_digests", From: "a.go " + string(cas.FromBytes([]byte("a"))), To: "a.go " + string(cas.FromBytes([]byte("b")))}},
	}
	if err := cache.WriteRecord(dir, want); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	got, err := cache.ReadRecord(dir, want.Step)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if got.Step != want.Step || got.Digest != want.Digest || got.Hit != want.Hit {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
	all, err := cache.ReadRecords(dir)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(all) != 1 || all[0].Step != want.Step {
		t.Errorf("ReadRecords = %+v", all)
	}
}

// The two packages share a directory (<run>/cache) but never a file format:
// without ReadRecords' scratchRecordFile skip, scratch.json alongside a
// real step record fails the whole read with an unmarshal error.
func TestReadRecordsSkipsTheScratchCachesOwnRecordFile(t *testing.T) {
	dir := t.TempDir()
	want := cache.Record{Step: "build", Digest: sampleKey().Digest(), Key: sampleKey(), Hit: true}
	if err := cache.WriteRecord(dir, want); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := scratch.WriteRecords(dir, []scratch.Record{{Name: "deps", Key: "deps-v1", Restored: true}}); err != nil {
		t.Fatalf("scratch.WriteRecords: %v", err)
	}

	got, err := cache.ReadRecords(dir)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(got) != 1 || got[0].Step != want.Step {
		t.Errorf("ReadRecords = %+v, want exactly one record for %q", got, want.Step)
	}
}

func TestReadRecordForAnUnknownStepIsAnError(t *testing.T) {
	if _, err := cache.ReadRecord(t.TempDir(), "never-run"); err == nil {
		t.Error("ReadRecord for a step with no record returned no error")
	}
}

// FormatExplain's output shape: the miss, both key digests, the first
// differing field with both sides, and a line confirming what did not move.
func TestFormatExplainRendersAMiss(t *testing.T) {
	var buf bytes.Buffer
	err := cache.FormatExplain(&buf, cache.Record{
		Step:           "build/test",
		Digest:         cas.FromBytes([]byte("current")),
		Key:            sampleKey(),
		Hit:            false,
		Reason:         cache.ReasonKeyChanged,
		PreviousDigest: cas.FromBytes([]byte("previous")),
		Diffs: []cache.Diff{{
			Name: "input_digests",
			From: "services/api/handler.go " + string(cas.FromBytes([]byte("old"))),
			To:   "services/api/handler.go " + string(cas.FromBytes([]byte("new"))),
		}},
	})
	if err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"MISS", "build/test",
		cas.FromBytes([]byte("current")).Short(),
		cas.FromBytes([]byte("previous")).Short(),
		"input_digests", "services/api/handler.go",
		"executor_class", "unchanged",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q:\n%s", want, out)
		}
	}
}

func TestFormatExplainRendersAHit(t *testing.T) {
	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "build/test", Digest: sampleKey().Digest(), Key: sampleKey(), Hit: true,
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "HIT") {
		t.Errorf("a hit did not render as a hit:\n%s", out)
	}
	if strings.Contains(out, "MISS") {
		t.Errorf("a hit rendered as a miss:\n%s", out)
	}
}

func TestFormatExplainSaysSoWhenThereIsNoPreviousEntry(t *testing.T) {
	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "build/test", Digest: sampleKey().Digest(), Key: sampleKey(),
		Hit: false, Reason: cache.ReasonNoPreviousEntry,
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "no previous entry") {
		t.Errorf("a first run must say there was nothing to compare against:\n%s", out)
	}
	if strings.Contains(out, "unchanged") {
		t.Errorf("a first run must not claim components were unchanged:\n%s", out)
	}
}

// A value never reaches this output, because a value never reaches a key.
func TestFormatExplainCannotPrintAnEnvironmentValue(t *testing.T) {
	const token = "super-secret-value-nobody-should-see" //nolint:gosec // a test fixture, not a credential
	k := sampleKey()
	k.Env = cache.EnvComponent([]string{"BUILD_TOKEN=" + token}, []string{"BUILD_TOKEN"})
	prev := k
	prev.Env = cache.EnvComponent([]string{"BUILD_TOKEN=other"}, []string{"BUILD_TOKEN"})

	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "s", Digest: k.Digest(), Key: k, Hit: false,
		Reason: cache.ReasonKeyChanged, PreviousDigest: prev.Digest(),
		Diffs: cache.Explain(prev, k),
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	if strings.Contains(buf.String(), token) {
		t.Fatalf("cache explain printed an environment value:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "BUILD_TOKEN") {
		t.Errorf("cache explain did not name the variable that changed:\n%s", buf.String())
	}
}

// Every test above hand-crafts a single-entry Diff, which would stay green
// even if componentDiffLines could not split a real multi-file
// input_digests component. Build two keys through InputsComponent, change
// one file, and require that FormatExplain names it and says nothing about
// the four that did not change.
func TestFormatExplainNamesOnlyTheChangedFileAmongManyInputs(t *testing.T) {
	files := func(cVal string) []cache.FileDigest {
		return []cache.FileDigest{
			{Path: "a.go", Digest: cas.FromBytes([]byte("a"))},
			{Path: "b.go", Digest: cas.FromBytes([]byte("b"))},
			{Path: "c.go", Digest: cas.FromBytes([]byte(cVal))},
			{Path: "d.go", Digest: cas.FromBytes([]byte("d"))},
			{Path: "e.go", Digest: cas.FromBytes([]byte("e"))},
		}
	}
	prev := sampleKey()
	prev.InputDigests = cache.InputsComponent(files("c-before"))
	cur := prev
	cur.InputDigests = cache.InputsComponent(files("c-after"))

	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "build", Digest: cur.Digest(), Key: cur, Hit: false,
		Reason: cache.ReasonKeyChanged, PreviousDigest: prev.Digest(),
		Diffs: cache.Explain(prev, cur),
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "c.go") {
		t.Errorf("output does not name the file that changed:\n%s", out)
	}
	for _, unchanged := range []string{"a.go", "b.go", "d.go", "e.go"} {
		if strings.Contains(out, unchanged) {
			t.Errorf("output mentions %q, which did not change: a change to one of 5 files must not report on the other 4:\n%s", unchanged, out)
		}
	}
	oldDigest := cas.FromBytes([]byte("c-before"))
	newDigest := cas.FromBytes([]byte("c-after"))
	if !strings.Contains(out, oldDigest.Short()) || !strings.Contains(out, newDigest.Short()) {
		t.Errorf("output does not show both sides of the change as a shortened digest:\n%s", out)
	}
}
