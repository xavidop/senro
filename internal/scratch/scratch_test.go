package scratch_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/workspace"
)

func openScratch(t *testing.T) *scratch.Dir {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	d, err := scratch.Open(filepath.Join(t.TempDir(), "scratch"), workspace.NewSnapshotter(store))
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}
	return d
}

func withFile(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return dir
}

func TestSaveThenRestoreByExactKey(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	src := withFile(t, "mod.txt", "downloaded\n")

	saved, err := d.Save(ctx, "gomod-abc", src)
	if err != nil || !saved {
		t.Fatalf("Save = %v, %v", saved, err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, "gomod-abc", nil, dest)
	if err != nil || !ok {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	if !m.Exact || m.Key != "gomod-abc" {
		t.Errorf("Match = %+v, want an exact hit on the key asked for", m)
	}
	b, err := os.ReadFile(filepath.Join(dest, "mod.txt"))
	if err != nil {
		t.Fatalf("restored content: %v", err)
	}
	if string(b) != "downloaded\n" {
		t.Errorf("restored %q", b)
	}
}

// Restore tries the exact key, then each restore key as a prefix match,
// newest first. Miss is not an error.
func TestRestoreFallsBackToARestoreKeyPrefixNewestFirst(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()

	if _, err := d.Save(ctx, "gomod-old", withFile(t, "which.txt", "old\n")); err != nil {
		t.Fatalf("Save old: %v", err)
	}
	if _, err := d.Save(ctx, "gomod-new", withFile(t, "which.txt", "new\n")); err != nil {
		t.Fatalf("Save new: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, "gomod-absent", []string{"gomod-"}, dest)
	if err != nil || !ok {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	if m.Exact {
		t.Error("a prefix fallback reported itself as an exact match")
	}
	b, err := os.ReadFile(filepath.Join(dest, "which.txt"))
	if err != nil {
		t.Fatalf("restored content: %v", err)
	}
	if string(b) != "new\n" {
		t.Errorf("the fallback restored %q, want the newest entry under the prefix", b)
	}
}

func TestRestoreTriesRestoreKeysInDeclaredOrder(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "second-1", withFile(t, "w.txt", "second\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := d.Save(ctx, "first-1", withFile(t, "w.txt", "first\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, ok, err := d.Restore(ctx, "nope", []string{"first-", "second-"}, dest); err != nil || !ok {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	b, _ := os.ReadFile(filepath.Join(dest, "w.txt"))
	if string(b) != "first\n" {
		t.Errorf("restored %q, want the first declared restore key to win", b)
	}
}

// The negative half, and the one that defines the whole mechanism: a miss is
// not an error. A pipeline whose module cache is cold must still run.
func TestARestoreMissIsNotAnError(t *testing.T) {
	d := openScratch(t)
	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(context.Background(), "cold", []string{"warm-"}, dest)
	if err != nil {
		t.Fatalf("a scratch miss returned an error: %v", err)
	}
	if ok {
		t.Errorf("a cold cache reported a hit: %+v", m)
	}
}

// Entries are immutable. Mutating one under concurrent runs is how a
// node_modules gets corrupted, so a second Save under an existing key loses
// silently.
func TestSaveUnderAnExistingKeyLosesSilently(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "k", withFile(t, "v.txt", "first\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	saved, err := d.Save(ctx, "k", withFile(t, "v.txt", "second\n"))
	if err != nil {
		t.Fatalf("the losing Save returned an error: %v", err)
	}
	if saved {
		t.Error("the second Save reported that it stored an entry")
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, _, err := d.Restore(ctx, "k", nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	b, _ := os.ReadFile(filepath.Join(dest, "v.txt"))
	if string(b) != "first\n" {
		t.Errorf("the entry was overwritten: %q", b)
	}
}

// Keys come from a user template and land in filenames.
func TestKeysWithPathCharactersAreStoredSafely(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	const key = "deps/../../escape linux+amd64"
	if _, err := d.Save(ctx, key, withFile(t, "v.txt", "x\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, ok, err := d.Restore(ctx, key, nil, dest); err != nil || !ok {
		t.Errorf("Restore of an awkward key = %v, %v", ok, err)
	}
}

func TestRestoreOfAnEntryWhoseContentIsGoneIsAMissNotAFailure(t *testing.T) {
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	root := filepath.Join(t.TempDir(), "scratch")
	d, err := scratch.Open(root, workspace.NewSnapshotter(store))
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}
	ctx := context.Background()
	if _, err := d.Save(ctx, "k", withFile(t, "v.txt", "x\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(store.Root(), "sha256")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, ok, err := d.Restore(ctx, "k", nil, filepath.Join(t.TempDir(), "dest")); err != nil {
		t.Errorf("a swept scratch entry returned an error: %v", err)
	} else if ok {
		t.Error("a swept scratch entry reported a hit")
	}
}

// A restore matching neither the exact key nor any prefix is a plain miss
// even when unrelated entries exist: green only if newestUnder checks a
// genuine prefix, not merely "the store has something in it".
func TestRestoreMissWhenEntriesExistButNoneMatchPrefixOrKey(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "other-1", withFile(t, "v.txt", "irrelevant\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, "cold-key", []string{"warm-"}, dest)
	if err != nil {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	if ok {
		t.Errorf("Restore reported a hit against an unrelated entry: %+v", m)
	}
	if _, statErr := os.Stat(dest); statErr == nil {
		t.Error("a missed restore must not create dest")
	}
}

// An entry whose content is not a well-formed digest (distinct from a swept
// CAS object, covered above) must also degrade to a miss: a best-effort
// cache that can fail a run over a corrupt record defeats its own purpose.
func TestRestoreOfAMalformedEntryRecordIsAMissNotAFailure(t *testing.T) {
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	root := filepath.Join(t.TempDir(), "scratch")
	d, err := scratch.Open(root, workspace.NewSnapshotter(store))
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}
	// Bypasses Save: a directly corrupted record, as a truncated write
	// would leave.
	if err := os.WriteFile(filepath.Join(root, "k"), []byte("not-a-digest"), 0o644); err != nil {
		t.Fatalf("seed a malformed entry: %v", err)
	}
	if _, ok, err := d.Restore(context.Background(), "k", nil, filepath.Join(t.TempDir(), "dest")); err != nil {
		t.Errorf("a malformed entry record returned an error: %v", err)
	} else if ok {
		t.Error("a malformed entry record reported a hit")
	}
}

// The concurrency half of the immutability rule under -race: exactly one of
// many racing saves wins, and (the part O_EXCL alone does not prove) the
// winner's own content survives intact, not an interleaving of writers.
func TestConcurrentSavesUnderTheSameKeyRaceSafely(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	const n = 16

	srcs := make([]string, n)
	for i := range srcs {
		srcs[i] = withFile(t, "payload.txt", fmt.Sprintf("goroutine-%d\n", i))
	}

	results := make([]bool, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait() // line every goroutine up before any of them races
			results[i], errs[i] = d.Save(ctx, "race-key", srcs[i])
		}()
	}
	start.Done()
	wg.Wait()

	winners := 0
	winnerIdx := -1
	for i, ok := range results {
		if errs[i] != nil {
			t.Errorf("goroutine %d: Save returned an error under a race: %v", i, errs[i])
		}
		if ok {
			winners++
			winnerIdx = i
		}
	}
	if winners != 1 {
		t.Fatalf("Save reported %d winners for one key under a %d-way race, want exactly 1 (results=%v)",
			winners, n, results)
	}

	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, "race-key", nil, dest)
	if err != nil || !ok {
		t.Fatalf("Restore after the race = %v, %v", ok, err)
	}
	if !m.Exact {
		t.Errorf("Restore of the raced key reported %+v, want an exact match", m)
	}
	b, err := os.ReadFile(filepath.Join(dest, "payload.txt"))
	if err != nil {
		t.Fatalf("restored content: %v", err)
	}
	want := fmt.Sprintf("goroutine-%d\n", winnerIdx)
	if string(b) != want {
		t.Errorf("restored %q, want the winning goroutine's own content %q "+
			"(entry corrupted or wrong writer served)", b, want)
	}
}

// Digests exists for internal/cache's GC: an object a scratch entry points
// at must survive an orphan sweep, or the key can never be resaved.
func TestDigestsReportsEveryStoredEntrysTarget(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "key-a", withFile(t, "a.txt", "a")); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if _, err := d.Save(ctx, "key-b", withFile(t, "b.txt", "b")); err != nil {
		t.Fatalf("Save b: %v", err)
	}

	got, err := d.Digests()
	if err != nil {
		t.Fatalf("Digests: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Digests returned %d entries, want 2: %v", len(got), got)
	}
	for _, dg := range got {
		if !dg.Valid() {
			t.Errorf("Digests returned an invalid digest: %q", dg)
		}
	}
}

// An empty or never-written scratch cache reports no digests and no error:
// a GC over a fresh root must not be told to protect what does not exist.
func TestDigestsOverAnEmptyScratchCacheIsEmpty(t *testing.T) {
	d := openScratch(t)
	got, err := d.Digests()
	if err != nil {
		t.Fatalf("Digests: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Digests over an empty cache = %v, want none", got)
	}
}
