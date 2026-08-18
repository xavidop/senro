package cache_test

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/workspace"
)

type gcFixture struct {
	store   *cas.Dir
	action  *cache.Dir
	pinsDir string
}

func newGCFixture(t *testing.T) gcFixture {
	t.Helper()
	root := t.TempDir()
	store, err := cas.Open(filepath.Join(root, "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	action, err := cache.Open(filepath.Join(root, "action"))
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	return gcFixture{store: store, action: action, pinsDir: filepath.Join(root, "pins")}
}

func (f gcFixture) put(t *testing.T, body string) cas.Digest {
	t.Helper()
	d, err := f.store.Put(context.Background(), strings.NewReader(body))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return d
}

func (f gcFixture) saveEntry(t *testing.T, step string, k cache.Key, digests ...cas.Digest) {
	t.Helper()
	r := &cache.Result{RunID: "r1", Hermeticity: cache.HermeticityTrusted, SavedAt: time.Now().UTC()}
	for i, d := range digests {
		if i == 0 {
			r.Workspaces = append(r.Workspaces, cache.WorkspaceDigest{Name: "ws", Digest: d})
			continue
		}
		r.Logs = append(r.Logs, cache.LogRef{Stream: "stdout", Digest: d})
	}
	if err := f.action.Save(context.Background(), step, k, r); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func (f gcFixture) has(t *testing.T, d cas.Digest) bool {
	t.Helper()
	ok, err := f.store.Has(context.Background(), d)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	return ok
}

func TestGCDeletesUnreferencedObjectsAndKeepsReferencedOnes(t *testing.T) {
	f := newGCFixture(t)
	live := f.put(t, "referenced by a live entry")
	logRef := f.put(t, "the log of that entry")
	orphan := f.put(t, "referenced by nothing")
	f.saveEntry(t, "build", sampleKey(), live, logRef)

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if !f.has(t, live) {
		t.Error("GC deleted an object a live entry references")
	}
	if !f.has(t, logRef) {
		t.Error("GC deleted a log an entry references, so a future hit would replay nothing")
	}
	if f.has(t, orphan) {
		t.Error("GC kept an object nothing references")
	}
	if stats.ObjectsDeleted != 1 || stats.BytesFreed <= 0 {
		t.Errorf("stats = %+v, want one object deleted and a non-zero size", stats)
	}
}

// The size budget. The oldest-accessed entry goes first, which is what LRU
// means, and everything it alone referenced goes with it.
func TestGCEvictsTheLeastRecentlyUsedEntryUnderASizeBudget(t *testing.T) {
	f := newGCFixture(t)
	oldBody := f.put(t, strings.Repeat("old", 4000))
	newBody := f.put(t, strings.Repeat("new", 4000))

	oldKey := sampleKey()
	newKey := oldKey
	newKey.Platform = "darwin/arm64"
	f.saveEntry(t, "old-step", oldKey, oldBody)
	f.saveEntry(t, "new-step", newKey, newBody)

	// Age the older entry so the LRU order is unambiguous rather than
	// dependent on filesystem timestamp resolution.
	ageEntry(t, f.action, oldKey, time.Now().Add(-48*time.Hour))

	var total int64
	if err := f.store.Walk(func(o cas.Object) error { total += o.Bytes; return nil }); err != nil {
		t.Fatalf("Walk: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir,
		MaxSize: total / 2, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if f.has(t, oldBody) {
		t.Error("GC kept the least recently used entry's content under a budget that could not hold both")
	}
	if !f.has(t, newBody) {
		t.Error("GC evicted the most recently used entry")
	}
	if stats.EntriesEvicted != 1 {
		t.Errorf("stats = %+v, want one entry evicted", stats)
	}
	if _, ok, _ := f.action.Lookup(context.Background(), "old-step", oldKey); ok {
		t.Error("the evicted entry still reports a hit, so a future run would restore content that is gone")
	}
}

// An LRU sweep must not delete the snapshot somebody is debugging. The
// unrelated orphan MUST be collected in the same sweep: without it, a GC
// that deletes nothing at all would also pass.
func TestGCKeepsAPinnedFailedRunsWorkspaceEvenWhenOverBudget(t *testing.T) {
	f := newGCFixture(t)
	debugging := f.put(t, strings.Repeat("the failed run's workspace", 500))
	unrelated := f.put(t, "nothing points at this one")

	if err := cache.WritePin(f.pinsDir, cache.Pin{
		RunID: "r-failed", Status: "failed", Finished: time.Now(), Digests: []cas.Digest{debugging},
	}); err != nil {
		t.Fatalf("WritePin: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir,
		MaxSize: 1, KeepFailed: 7 * 24 * time.Hour, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if !f.has(t, debugging) {
		t.Fatal("GC deleted the workspace of a failed run inside its retention window, which is exactly the snapshot someone is looking at")
	}
	if f.has(t, unrelated) {
		t.Error("GC kept an unrelated, unpinned object in the same sweep, so this test cannot tell a working pin from a GC that deletes nothing at all")
	}
	if stats.PinnedObjects != 1 {
		t.Errorf("stats = %+v, want one pinned object", stats)
	}
	if stats.ObjectsDeleted != 1 {
		t.Errorf("stats = %+v, want exactly the unrelated orphan deleted", stats)
	}
}

// The negative half: retention has to end, or a cache root grows forever.
// The still-referenced object MUST survive the same sweep: without it, a GC
// that deletes everything would also pass.
func TestGCCollectsAPinnedWorkspaceOnceRetentionHasElapsed(t *testing.T) {
	f := newGCFixture(t)
	stale := f.put(t, "an old failed run's workspace")
	stillNeeded := f.put(t, "a live entry's content")
	f.saveEntry(t, "build", sampleKey(), stillNeeded)

	if err := cache.WritePin(f.pinsDir, cache.Pin{
		RunID: "r-old", Status: "failed",
		Finished: time.Now().Add(-30 * 24 * time.Hour), Digests: []cas.Digest{stale},
	}); err != nil {
		t.Fatalf("WritePin: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir,
		KeepFailed: 7 * 24 * time.Hour, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if f.has(t, stale) {
		t.Error("GC kept a pinned workspace past its retention window")
	}
	if !f.has(t, stillNeeded) {
		t.Error("GC deleted an object a live entry still references while expiring an unrelated pin")
	}
	if stats.PinsExpired != 1 {
		t.Errorf("stats = %+v, want one expired pin", stats)
	}
	pins, err := cache.ReadPins(f.pinsDir)
	if err != nil {
		t.Fatalf("ReadPins: %v", err)
	}
	if len(pins) != 0 {
		t.Errorf("an expired pin file survived: %+v", pins)
	}
}

func TestGCDryRunDeletesNothingAndReportsTheSameNumbers(t *testing.T) {
	f := newGCFixture(t)
	orphan := f.put(t, "unreferenced")

	dry, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(), DryRun: true,
	})
	if err != nil {
		t.Fatalf("GC dry run: %v", err)
	}
	if !f.has(t, orphan) {
		t.Fatal("a dry run deleted an object")
	}
	wet, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if dry.ObjectsDeleted != wet.ObjectsDeleted || dry.BytesFreed != wet.BytesFreed {
		t.Errorf("dry run reported %+v, the real sweep did %+v", dry, wet)
	}
	if f.has(t, orphan) {
		t.Error("the real sweep kept the object the dry run promised to delete")
	}
}

func TestGCOnAnEmptyStoreIsNotAnError(t *testing.T) {
	f := newGCFixture(t)
	if _, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	}); err != nil {
		t.Errorf("GC over an empty store: %v", err)
	}
}

// TestGCSweepsALeakedTempFile: a Put killed before its rename leaves a temp
// file forever (cas.Dir.Walk never looks in tmp/). GC sweeps it on every
// ordinary sweep, independent of MaxSize.
func TestGCSweepsALeakedTempFile(t *testing.T) {
	f := newGCFixture(t)
	leaked := filepath.Join(f.store.Root(), "tmp", "put-crashed")
	if err := os.WriteFile(leaked, []byte("half-written"), 0o644); err != nil {
		t.Fatalf("simulate leaked temp file: %v", err)
	}
	old := time.Now().Add(-cas.TmpStaleAge)
	if err := os.Chtimes(leaked, old, old); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if stats.TmpFilesSwept != 1 {
		t.Errorf("stats.TmpFilesSwept = %d, want 1", stats.TmpFilesSwept)
	}
	if _, err := os.Stat(leaked); !os.IsNotExist(err) {
		t.Errorf("the leaked temp file is still present after GC: err=%v", err)
	}
}

// TestGCDryRunDoesNotSweepTmp: DryRun must mean "nothing on disk changed",
// so the tmp sweep is skipped too, even though a leaked temp file has no
// digest for the deletion preview to be about.
func TestGCDryRunDoesNotSweepTmp(t *testing.T) {
	f := newGCFixture(t)
	leaked := filepath.Join(f.store.Root(), "tmp", "put-crashed")
	if err := os.WriteFile(leaked, []byte("half-written"), 0o644); err != nil {
		t.Fatalf("simulate leaked temp file: %v", err)
	}
	old := time.Now().Add(-cas.TmpStaleAge)
	if err := os.Chtimes(leaked, old, old); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(), DryRun: true,
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if stats.TmpFilesSwept != 0 {
		t.Errorf("stats.TmpFilesSwept = %d under DryRun, want 0", stats.TmpFilesSwept)
	}
	if _, err := os.Stat(leaked); err != nil {
		t.Errorf("DryRun removed the leaked temp file: %v", err)
	}
}

// A corrupt entry file must not crash the sweep or take a healthy entry's
// content down with it. Dir.Walk already skips corrupt entries; this pins
// that GC inherits that rather than failing the whole sweep on one bad
// file.
func TestGCSkipsACorruptEntryWithoutLosingAnUnrelatedOne(t *testing.T) {
	f := newGCFixture(t)
	healthy := f.put(t, "a working entry's content")
	f.saveEntry(t, "build", sampleKey(), healthy)

	corruptKey := sampleKey()
	corruptKey.Platform = "windows/amd64" // any different key, so its path differs from the healthy one
	corruptPath := f.action.EntryPath(corruptKey.Digest())
	if err := os.MkdirAll(filepath.Dir(corruptPath), 0o755); err != nil {
		t.Fatalf("corrupt entry: mkdir: %v", err)
	}
	if err := os.WriteFile(corruptPath, []byte("{not valid json"), 0o644); err != nil {
		t.Fatalf("corrupt entry: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC over a corrupt entry: %v", err)
	}
	if !f.has(t, healthy) {
		t.Error("a corrupt sibling entry made GC drop an unrelated, healthy entry's content")
	}
	if stats.EntriesScanned != 1 {
		t.Errorf("stats = %+v, want the corrupt entry skipped rather than counted", stats)
	}
}

// An entry can outlive the object it references. GC must not error out over
// a reference it cannot resolve, and must still do its job for everything
// else in the same sweep.
func TestGCToleratesAnEntryWhoseObjectIsAlreadyGone(t *testing.T) {
	f := newGCFixture(t)
	ghost := cas.FromBytes([]byte("never actually stored"))
	f.saveEntry(t, "build", sampleKey(), ghost)
	orphan := f.put(t, "unreferenced, and must still go")

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC over a dangling reference: %v", err)
	}
	if f.has(t, orphan) {
		t.Error("a dangling reference in an unrelated entry stopped GC from collecting a real orphan")
	}
	if stats.ObjectsDeleted != 1 {
		t.Errorf("stats = %+v, want exactly the orphan deleted", stats)
	}
}

// scratch.Dir.Save claims a key with O_EXCL and never overwrites it, so a
// key whose target GC collected can never be resaved: a permanently dead
// key, not a slower cache. GC must treat a scratch-referenced object as
// live, like a pinned one; the unrelated orphan proves the protection is
// doing real work rather than GC keeping everything.
func TestGCKeepsAnObjectAScratchEntryStillReferences(t *testing.T) {
	f := newGCFixture(t)
	root := t.TempDir()

	// A scratch cache sharing f.store, as storage.Open wires it in
	// production: one CAS underneath both caches.
	snap := workspace.NewSnapshotter(f.store)
	sc, err := scratch.Open(filepath.Join(root, "scratch"), snap)
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "module.lock"), []byte("warm module cache"), 0o644); err != nil {
		t.Fatalf("seed scratch source: %v", err)
	}
	if ok, err := sc.Save(context.Background(), "npm-deps-v1", src); err != nil || !ok {
		t.Fatalf("scratch Save: ok=%v err=%v", ok, err)
	}
	digests, err := sc.Digests()
	if err != nil || len(digests) == 0 {
		t.Fatalf("scratch.Digests: %v (%v)", digests, err)
	}
	scratchTarget := digests[0]

	orphan := f.put(t, "nothing references this one")

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, Scratch: sc, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if !f.has(t, scratchTarget) {
		t.Fatal("GC deleted an object a scratch entry still points at; that key can now never be resaved (Save's O_EXCL sees the stale entry file as already claimed)")
	}
	if f.has(t, orphan) {
		t.Error("GC kept a true orphan in the same sweep, so this test cannot tell scratch protection from GC simply deleting nothing")
	}
	// Save's Snapshot also stores a workspace INDEX object no scratch entry
	// records (the same accepted gap as references()), a second legitimate
	// orphan, so ObjectsDeleted is 2 here, not 1. What matters is checked
	// above: the named orphan went and the scratch-referenced body did not.
	if stats.ScratchProtectedObjects != 1 {
		t.Errorf("stats = %+v, want exactly one scratch-protected object (the body; not the index Save also wrote)", stats)
	}
	if stats.ObjectsDeleted < 1 {
		t.Errorf("stats = %+v, want at least the named orphan deleted", stats)
	}
}

// TestGCDefersDeletionWhileAScratchSaveIsInFlight: a save that has claimed
// a key already Put its content, possibly minutes before writing the digest
// that would protect it (see scratch.Dir.InFlight). A GC inside that window
// must delete nothing: it cannot know which object the save will reference.
//
// The claim is simulated the way Save leaves one on disk: an empty file at
// url.PathEscape(key) under the scratch root (entryPath's construction,
// which is unexported).
func TestGCDefersDeletionWhileAScratchSaveIsInFlight(t *testing.T) {
	f := newGCFixture(t)
	root := t.TempDir()

	snap := workspace.NewSnapshotter(f.store)
	sc, err := scratch.Open(filepath.Join(root, "scratch"), snap)
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}

	// Already Put by the in-flight save, and protected by nothing yet: the
	// claim is still empty, and no pin or entry points at it.
	inFlightTarget := f.put(t, "content an in-flight scratch save just wrote to the CAS")
	claim := filepath.Join(root, "scratch", url.PathEscape("npm-deps-v2"))
	if err := os.WriteFile(claim, nil, 0o644); err != nil {
		t.Fatalf("simulate an in-flight claim: %v", err)
	}

	orphan := f.put(t, "a genuine, unrelated orphan")

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, Scratch: sc, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if !stats.DeferredForInFlightSave {
		t.Error("stats.DeferredForInFlightSave = false, want true: a claim was on disk")
	}
	if !f.has(t, inFlightTarget) {
		t.Fatal("GC deleted the object an in-flight save had already Put, before that save could ever protect it")
	}
	// A genuine orphan is not swept either: the whole deletion phase is
	// skipped, not narrowed (see GC's doc for why narrowing is unsafe).
	if !f.has(t, orphan) {
		t.Error("GC deleted an unrelated orphan even though it deferred deletion for the in-flight save")
	}
	if stats.ObjectsDeleted != 0 {
		t.Errorf("ObjectsDeleted = %d, want 0: deletion was supposed to be deferred entirely", stats.ObjectsDeleted)
	}
}

// TestGCDoesNotDeferForAStaleAbandonedClaim proves the deferral is bounded:
// a claim past scratch's staleness threshold is not "in flight" (see
// scratch.Dir.InFlight) and must not block every future sweep.
func TestGCDoesNotDeferForAStaleAbandonedClaim(t *testing.T) {
	f := newGCFixture(t)
	root := t.TempDir()

	snap := workspace.NewSnapshotter(f.store)
	sc, err := scratch.Open(filepath.Join(root, "scratch"), snap)
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}

	claim := filepath.Join(root, "scratch", url.PathEscape("npm-deps-abandoned"))
	if err := os.WriteFile(claim, nil, 0o644); err != nil {
		t.Fatalf("simulate an abandoned claim: %v", err)
	}
	// Comfortably past scratch.staleClaimAge (2h; unexported, so this test
	// cannot reference it directly) without hardcoding an exact multiple of
	// it.
	longAgo := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(claim, longAgo, longAgo); err != nil {
		t.Fatalf("backdate claim: %v", err)
	}

	orphan := f.put(t, "an ordinary orphan, nothing in flight")

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, Scratch: sc, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if stats.DeferredForInFlightSave {
		t.Error("stats.DeferredForInFlightSave = true for a stale, abandoned claim; GC can now never collect anything")
	}
	if f.has(t, orphan) {
		t.Error("GC did not delete an ordinary orphan even though nothing was actually in flight")
	}
}

// GC deletes with a plain unlink, and POSIX guarantees an unlink does not
// invalidate an already-open descriptor: an in-flight read completes with
// the full, correct bytes, while a read starting after the delete gets a
// clean "not found" the engine turns into a graceful miss (see
// hitIsReproducible/degradeToMiss in internal/engine/cache.go). Neither
// outcome is a torn read. This test proves the first half by holding a
// reader open across a sweep that deletes the very object being read.
func TestGCDeletingAnObjectDoesNotCorruptAReaderAlreadyMidStream(t *testing.T) {
	f := newGCFixture(t)
	body := strings.Repeat("still being read when gc deletes it\n", 2000)
	target := f.put(t, body)

	rc, err := f.store.Get(context.Background(), target)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()

	// Read partway through, so the descriptor is genuinely mid-stream (not
	// merely opened) before the concurrent delete runs.
	buf := make([]byte, 64)
	if _, err := io.ReadFull(rc, buf); err != nil {
		t.Fatalf("partial read before delete: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := cache.GC(context.Background(), cache.GCOptions{
			CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
		}); err != nil {
			t.Errorf("concurrent GC: %v", err)
		}
	}()
	wg.Wait()

	// GC has now fully run and unlinked the object. The reader opened before
	// that must still be able to read to completion, uncorrupted.
	rest, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read after concurrent delete: %v", err)
	}
	got := string(buf) + string(rest)
	if got != body {
		t.Fatalf("a concurrent delete corrupted an in-flight read: got %d bytes, want %d matching the original",
			len(got), len(body))
	}

	// And the object is genuinely gone for anyone who did not already have
	// it open: a fresh Get must now report ErrNotFound, not silently succeed.
	if f.has(t, target) {
		t.Error("GC reported success but did not actually delete the unreferenced object")
	}
}

// ageEntry backdates an entry file so LRU order does not depend on
// filesystem timestamp resolution.
func ageEntry(t *testing.T, d *cache.Dir, k cache.Key, when time.Time) {
	t.Helper()
	if err := osChtimes(d.EntryPath(k.Digest()), when); err != nil {
		t.Fatalf("age entry: %v", err)
	}
}

// osChtimes is os.Chtimes with both the access and modification time set to
// the same instant, so the test file's intent reads at the call site: this
// is backdating one clock, not two independent ones.
func osChtimes(p string, when time.Time) error {
	return os.Chtimes(p, when, when)
}
