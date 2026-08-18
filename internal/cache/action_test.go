package cache_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func openCache(t *testing.T) *cache.Dir {
	t.Helper()
	d, err := cache.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	return d
}

func sampleResult() *cache.Result {
	return &cache.Result{
		ExitCode:    0,
		Outputs:     []cache.FileDigest{{Path: "out/app", Digest: cas.FromBytes([]byte("app"))}},
		Workspaces:  []cache.WorkspaceDigest{{Name: "build", Digest: cas.FromBytes([]byte("ws"))}},
		Logs:        []cache.LogRef{{Stream: "stdout", Digest: cas.FromBytes([]byte("log")), Bytes: 3}},
		DurationNS:  1500,
		RunID:       "r1",
		Hermeticity: cache.HermeticityTrusted,
		SavedAt:     time.Now().UTC(),
		Bytes:       6,
	}
}

func TestSaveThenLookupReturnsTheResult(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()

	if _, ok, err := d.Lookup(ctx, "build", k); err != nil || ok {
		t.Fatalf("Lookup before Save = %v, %v; want false, nil", ok, err)
	}
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := d.Lookup(ctx, "build", k)
	if err != nil || !ok {
		t.Fatalf("Lookup after Save = %v, %v; want true, nil", ok, err)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Path != "out/app" {
		t.Errorf("result outputs = %+v", got.Outputs)
	}
	if got.Hermeticity != cache.HermeticityTrusted {
		t.Errorf("hermeticity = %q, want %q: Pure() is trusted rather than enforced, and the entry must say so",
			got.Hermeticity, cache.HermeticityTrusted)
	}
}

// The negative half, and the one that matters most: a key that differs in
// any component must not hit.
func TestADifferentKeyDoesNotHit(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	other := k
	other.InputDigests = cache.InputsComponent([]cache.FileDigest{{Path: "a.go", Digest: cas.FromBytes([]byte("edited"))}})
	if _, ok, err := d.Lookup(ctx, "build", other); err != nil || ok {
		t.Errorf("a changed input hit the cache: %v, %v", ok, err)
	}
}

// Previous needs "the most recent entry for the same step", which is the
// whole reason the store is indexed by step as well as by key.
func TestPreviousReturnsTheMostRecentEntryForAStep(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()

	first := sampleKey()
	if err := d.Save(ctx, "build", first, sampleResult()); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	second := first
	second.Platform = "darwin/arm64"
	if err := d.Save(ctx, "build", second, sampleResult()); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	e, ok, err := d.Previous(ctx, "build")
	if err != nil || !ok {
		t.Fatalf("Previous = %v, %v", ok, err)
	}
	if e.Key.Digest() != second.Digest() {
		t.Errorf("Previous returned the older entry")
	}

	if _, ok, err := d.Previous(ctx, "never-run"); err != nil || ok {
		t.Errorf("Previous for an unknown step = %v, %v; want false, nil", ok, err)
	}
}

// Step IDs contain "/" and "[]" (see internal/stepid). A store that used
// them as filenames unescaped would collide or fail.
func TestPreviousHandlesAnExpandedStepID(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	const id = "build/test[unit=services/api]"
	if err := d.Save(ctx, id, sampleKey(), sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, ok, err := d.Previous(ctx, id); err != nil || !ok {
		t.Errorf("Previous for %q = %v, %v", id, ok, err)
	}
}

func TestForgetRemovesAnEntry(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := d.Forget(ctx, k); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, ok, _ := d.Lookup(ctx, "build", k); ok {
		t.Error("Lookup still hits after Forget")
	}
	if err := d.Forget(ctx, k); err != nil {
		t.Errorf("Forget is not idempotent: %v", err)
	}
}

// An entry file that is not readable JSON is treated as absent rather than
// as an error, so one corrupt file cannot fail every run on the machine.
func TestACorruptEntryReadsAsAMiss(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := os.WriteFile(d.EntryPath(k.Digest()), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("scribble: %v", err)
	}
	_, ok, err := d.Lookup(ctx, "build", k)
	if err != nil {
		t.Errorf("a corrupt entry returned an error rather than a miss: %v", err)
	}
	if ok {
		t.Error("a corrupt entry reported a hit")
	}
}

func TestWalkVisitsEveryEntry(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k1 := sampleKey()
	k2 := k1
	k2.Platform = "darwin/arm64"
	for _, k := range []cache.Key{k1, k2} {
		if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	var n int
	if err := d.Walk(func(_ string, e cache.Entry, accessed time.Time) error {
		n++
		if accessed.IsZero() {
			t.Error("Walk reported an entry with no access time, so the GC has no clock to sort by")
		}
		if e.Result.RunID != "r1" {
			t.Errorf("Walk decoded a result with RunID %q", e.Result.RunID)
		}
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if n != 2 {
		t.Errorf("Walk saw %d entries, want 2", n)
	}
}

func TestWalkStopsOnTheCallbacksError(t *testing.T) {
	d := openCache(t)
	if err := d.Save(context.Background(), "build", sampleKey(), sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	sentinel := errors.New("stop")
	if err := d.Walk(func(string, cache.Entry, time.Time) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Errorf("Walk = %v, want the callback's error", err)
	}
}

// Reopening the same root through a brand new *cache.Dir, sharing no state
// with the writer, proves the entry reached the filesystem rather than
// living only in the writer's memory.
func TestEntriesPersistAcrossASecondOpenOfTheSameRoot(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	k := sampleKey()

	writer, err := cache.Open(root)
	if err != nil {
		t.Fatalf("cache.Open (writer): %v", err)
	}
	if err := writer.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reader, err := cache.Open(root)
	if err != nil {
		t.Fatalf("cache.Open (reader): %v", err)
	}
	got, ok, err := reader.Lookup(ctx, "build", k)
	if err != nil || !ok {
		t.Fatalf("a second, independent *cache.Dir over the same root did not see the first one's Save: %v, %v", ok, err)
	}
	if got.RunID != "r1" {
		t.Errorf("reader's Lookup returned RunID %q, want r1", got.RunID)
	}
	if _, ok, err := reader.Previous(ctx, "build"); err != nil || !ok {
		t.Errorf("reader's Previous did not see the writer's Save: %v, %v", ok, err)
	}
}

// A cache.Dir holds no CAS handle (see Lookup), so a Result can
// legitimately name digests nothing ever stored. This proves the mechanism
// the caller depends on: Lookup reports the hit exactly as saved, and
// Forget turns it into a clean miss once the caller decides the referenced
// content is gone.
func TestALookupHitCanReferenceACASObjectThatWasNeverStored(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	store, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}

	k := sampleKey()
	ghost := cas.FromBytes([]byte("never actually written to the CAS"))
	result := sampleResult()
	result.Outputs = []cache.FileDigest{{Path: "out/app", Digest: ghost}}
	if err := d.Save(ctx, "build", k, result); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if has, err := store.Has(ctx, ghost); err != nil || has {
		t.Fatalf("test fixture bug: the ghost digest is supposed to be absent from the CAS: %v, %v", has, err)
	}

	got, ok, err := d.Lookup(ctx, "build", k)
	if err != nil || !ok {
		t.Fatalf("Lookup = %v, %v; a cache.Dir has no CAS handle and must report the entry as saved", ok, err)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Digest != ghost {
		t.Fatalf("Lookup did not return the entry as saved: %+v", got)
	}

	// The caller's half: having discovered the object is gone, Forget makes
	// the next Lookup miss cleanly instead of repeating the broken hit.
	if err := d.Forget(ctx, k); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, ok, err := d.Lookup(ctx, "build", k); err != nil || ok {
		t.Errorf("Lookup after Forget = %v, %v; want a clean miss", ok, err)
	}
}

// Two runs racing to save the SAME key (two `senro run` invocations sharing
// one cache root) must never corrupt the entry or the recent-pointer, or
// leave a partial file for a reader. Exercises writeAtomic's
// temp-then-rename: a reader gets ONE result whole, never a torn mix.
func TestConcurrentSavesForTheSameKeyDoNotCorruptTheEntry(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	const n = 16

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := sampleResult()
			r.RunID = fmt.Sprintf("writer-%d", i)
			if err := d.Save(ctx, "build", k, r); err != nil {
				t.Errorf("concurrent Save %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	got, ok, err := d.Lookup(ctx, "build", k)
	if err != nil || !ok {
		t.Fatalf("Lookup after concurrent saves = %v, %v", ok, err)
	}
	if !strings.HasPrefix(got.RunID, "writer-") {
		t.Errorf("the surviving entry's RunID %q was not written by any of the concurrent Saves", got.RunID)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Path != "out/app" {
		t.Errorf("the surviving entry is not a whole, uncorrupted Result: %+v", got)
	}
}

// A field a future Result gains (or an earlier format lacked) must not turn
// a readable entry corrupt. encoding/json's leniency about missing fields
// provides that compatibility without a migration; this pins it.
func TestAnEntryWrittenByAnOlderFormatStillReadsAsAHit(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()

	old := fmt.Sprintf(`{"key":%s,"result":{"exit_code":0,"run_id":"legacy-run"}}`, mustJSON(t, k))
	p := d.EntryPath(k.Digest())
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte(old), 0o644); err != nil {
		t.Fatalf("seed a legacy-shaped entry: %v", err)
	}

	got, ok, err := d.Lookup(ctx, "build", k)
	if err != nil {
		t.Fatalf("an entry missing newer fields was read as an error: %v", err)
	}
	if !ok {
		t.Fatal("an entry missing newer fields was read as a miss, not a hit")
	}
	if got.RunID != "legacy-run" {
		t.Errorf("RunID = %q, want legacy-run", got.RunID)
	}
	// Hermeticity and SavedAt were absent from the legacy JSON above, so they
	// must decode to their zero values rather than anything invented.
	if got.Hermeticity != "" {
		t.Errorf("Hermeticity = %q, want the zero value for a field the legacy entry never set", got.Hermeticity)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(b)
}
