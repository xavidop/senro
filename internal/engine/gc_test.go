package engine_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// Proves the run/pin/GC wiring end to end: a failed Pure() step's run
// writes a pin, and a later sweep must protect exactly that content while
// still reclaiming everything else. The second half is not decoration: a GC
// that deletes nothing would pass a survival-only test and look identical
// to a working one until the day it had to reclaim space, so an unrelated
// unpinned object must be gone after the SAME sweep.
func TestAFailedRunsPinSurvivesGCWhileAnUnrelatedOrphanInTheSameSweepDoesNot(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}

	p := purePipeline(t, "echo partial > out.txt; exit 3")
	runDir := filepath.Join(t.TempDir(), "run")
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: rec, Storage: store, RunID: "r-failed",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status == api.RunSucceeded || status == api.RunSucceededWithRecovery {
		t.Fatalf("status = %s, want a failure so the pin path under test actually runs", status)
	}

	// The engine wrote a pin for this run. Read it back the way GC will, and
	// pull the compile step's own ws.snapshot event so both its body and
	// index digest can be checked against the pin directly, not just its
	// length.
	pins, err := cache.ReadPins(filepath.Join(cacheRoot, "pins"))
	if err != nil {
		t.Fatalf("ReadPins: %v", err)
	}
	if len(pins) != 1 {
		t.Fatalf("ReadPins returned %d pins, want 1 for the one failed run", len(pins))
	}
	pin := pins[0]
	if pin.RunID != "r-failed" {
		t.Errorf("pin.RunID = %q, want %q", pin.RunID, "r-failed")
	}
	if pin.Status == string(api.RunSucceeded) {
		t.Errorf("pin.Status = %q, a succeeded run must never be pinned", pin.Status)
	}

	var compileSnap api.WSSnapshotBody
	for _, e := range rec.Events() {
		if e.Type == api.WSSnapshot && e.Step == "compile" {
			if err := e.Decode(&compileSnap); err != nil {
				t.Fatalf("decode ws.snapshot: %v", err)
			}
		}
	}
	if compileSnap.Digest == "" || compileSnap.Index == "" {
		t.Fatal("the failing step never emitted ws.snapshot; the test fixture is not exercising the failure path it claims to")
	}
	pinned := make(map[cas.Digest]bool, len(pin.Digests))
	for _, d := range pin.Digests {
		pinned[d] = true
	}
	if !pinned[cas.Digest(compileSnap.Digest)] {
		t.Error("the pin does not carry the failed step's own workspace body digest")
	}
	if !pinned[cas.Digest(compileSnap.Index)] {
		t.Error("the pin does not carry the failed step's own workspace INDEX digest, so `ws ls` would break for exactly the run someone is debugging")
	}

	// An orphan planted directly, unrelated to anything the run produced and
	// unpinned by construction: the sibling this sweep must still collect.
	orphan, err := store.CAS.Put(context.Background(), strings.NewReader("nothing to do with this run"))
	if err != nil {
		t.Fatalf("plant an unrelated orphan: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	// A fresh, real cache root open, exactly what `senro cache gc` does.
	store2, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open (gc): %v", err)
	}
	defer func() { _ = store2.Close() }()

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: store2.CAS, Action: store2.Action, Scratch: store2.Scratch,
		PinsDir: filepath.Join(cacheRoot, "pins"),
		// An aggressive budget: if the pin were not doing its job, this would
		// also be enough pressure to evict any action-cache entry, and there
		// happens to be none here, since a failed Pure() step is never
		// saved, but the budget is set as if there might be, so this test
		// does not accidentally pass only because nothing was ever under
		// pressure.
		MaxSize: 1, KeepFailed: 7 * 24 * time.Hour, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	ok, err := store2.CAS.Has(context.Background(), cas.Digest(compileSnap.Digest))
	if err != nil {
		t.Fatalf("Has(body): %v", err)
	}
	if !ok {
		t.Error("GC deleted the failed run's pinned workspace body")
	}
	ok, err = store2.CAS.Has(context.Background(), cas.Digest(compileSnap.Index))
	if err != nil {
		t.Fatalf("Has(index): %v", err)
	}
	if !ok {
		t.Error("GC deleted the failed run's pinned workspace index")
	}
	ok, err = store2.CAS.Has(context.Background(), orphan)
	if err != nil {
		t.Fatalf("Has(orphan): %v", err)
	}
	if ok {
		t.Error("GC kept an unrelated, unpinned orphan in the same sweep, so this test cannot then prove the pin did anything, since a no-op GC would pass it too")
	}
	if stats.PinnedObjects == 0 {
		t.Errorf("stats = %+v, want at least one pinned object reported", stats)
	}
}

// TestGCDefersDeletionWhileARunIsInFlight guards against a real trap: a
// run's own workspace snapshots are referenced by nothing between the
// moment a step Puts one into the CAS and either that step's own cacheSave
// or the run's end-of-run pin, written only after every step has settled.
// A `senro cache gc` with no flags (orphan sweep only) landing inside
// that window could delete an object the run had already produced, before
// anything protects it. The pin logic tested above is not
// the whole story: it only ever runs AFTER the run ends.
func TestGCDefersDeletionWhileARunIsInFlight(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf x > f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	// slow gives the concurrent GC below a wide, easy-to-hit window between
	// seed's own snapshot landing in the CAS and the run ending (and
	// writing its pin, if it even needs one), mirroring C1's own approach
	// to making an inherently racy scenario reliably reproducible.
	l.Step("slow", exec.Command("sh", "-c", "sleep 1; printf y > slow.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir := filepath.Join(t.TempDir(), "run")
	rec := sink.Recording()
	done := make(chan error, 1)
	go func() {
		_, runErr := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: "r1",
		})
		done <- runErr
	}()

	time.Sleep(300 * time.Millisecond)
	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: store.CAS, Action: store.Action, Scratch: store.Scratch,
		PinsDir: filepath.Join(cacheRoot, "pins"), InFlightDir: filepath.Join(cacheRoot, "inflight"),
		Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("mid-run GC: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	if !stats.DeferredForInFlightRun {
		t.Error("stats.DeferredForInFlightRun = false while the run was still executing")
	}
	if stats.ObjectsDeleted != 0 {
		t.Errorf("ObjectsDeleted = %d, want 0: deletion should have been deferred entirely", stats.ObjectsDeleted)
	}

	var snapDigests []cas.Digest
	for _, e := range rec.Events() {
		if e.Type == api.WSSnapshot {
			var b api.WSSnapshotBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode ws.snapshot: %v", err)
			}
			snapDigests = append(snapDigests, cas.Digest(b.Digest))
		}
	}
	if len(snapDigests) == 0 {
		t.Fatal("the run produced no ws.snapshot event; the fixture is not exercising what this test claims")
	}
	for _, d := range snapDigests {
		ok, err := store.CAS.Has(context.Background(), d)
		if err != nil {
			t.Fatalf("Has: %v", err)
		}
		if !ok {
			t.Errorf("object %s, produced mid-run, was deleted by a GC sweep that ran while the run was still in flight", d.Short())
		}
	}
}

// TestGCDoesNotDeferForACompletedRun is the negative half: once a run has
// finished (and cleared its own marker), an ordinary GC sweep must go back
// to deleting genuine orphans exactly as before: I4's fix must not leave
// every future sweep permanently deferring.
func TestGCDoesNotDeferForACompletedRun(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("noop", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	runDir := filepath.Join(t.TempDir(), "run")
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: sink.Nop(), Storage: store, RunID: "r1",
	}); err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	orphan, err := store.CAS.Put(context.Background(), strings.NewReader("a genuine orphan"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	store2, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()
	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: store2.CAS, Action: store2.Action, Scratch: store2.Scratch,
		PinsDir: filepath.Join(cacheRoot, "pins"), InFlightDir: filepath.Join(cacheRoot, "inflight"),
		Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if stats.DeferredForInFlightRun {
		t.Error("GC deferred even though the run had already finished and cleared its marker")
	}
	if ok, _ := store2.CAS.Has(context.Background(), orphan); ok {
		t.Error("GC did not collect a genuine orphan once the run had finished")
	}
}
