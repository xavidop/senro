package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// runTwice runs the same plan twice against ONE shared cache root and two
// separate run directories, which is the shape every question about a cache
// actually asks.
func runTwice(t *testing.T, p *senro.Plan) (first, second []api.Event, dirs [2]string) {
	t.Helper()
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	for i := 0; i < 2; i++ {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		runDir := filepath.Join(t.TempDir(), "run")
		dirs[i] = runDir
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir:      runDir,
			Executor: localexec.New(runDir, store.Snapshotter),
			Sink:     rec,
			Storage:  store,
			RunID:    []string{"r1", "r2"}[i],
		}); err != nil {
			t.Fatalf("engine.Run %d: %v", i+1, err)
		}
		if i == 0 {
			first = rec.Events()
		} else {
			second = rec.Events()
		}
		_ = store.Close()
	}
	return first, second, dirs
}

func purePipeline(t *testing.T, cmd string) *senro.Plan {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("compile", exec.Command("sh", "-c", cmd)).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

func TestASecondIdenticalRunHitsTheCache(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	first, second, dirs := runTwice(t, p)

	if countType(first, api.CacheMiss) != 1 || countType(first, api.CacheSaved) != 1 {
		t.Errorf("the first run should miss once and save once: miss=%d saved=%d",
			countType(first, api.CacheMiss), countType(first, api.CacheSaved))
	}
	if countType(first, api.CacheHit) != 0 {
		t.Error("the first run hit a cache that was empty")
	}
	if countType(second, api.CacheHit) != 1 {
		t.Fatalf("the second run did not hit: %d hits, %d misses",
			countType(second, api.CacheHit), countType(second, api.CacheMiss))
	}
	if countType(second, api.StepStarted) != 1 {
		t.Errorf("a cached step still emitted step.started: %d starts in the second run",
			countType(second, api.StepStarted))
	}

	// The step is cached AND its filesystem effect is back.
	if b, err := os.ReadFile(filepath.Join(dirs[1], "ws", "src", "out.txt")); err != nil {
		t.Errorf("a cache hit did not restore the output the step would have produced: %v", err)
	} else if strings.TrimSpace(string(b)) != "compiled" {
		t.Errorf("restored output = %q", b)
	}
}

func TestACachedStepFoldsToStateCached(t *testing.T) {
	_, second, _ := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	st := api.NewRunState()
	for _, e := range second {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	step, ok := st.Steps["compile"]
	if !ok {
		t.Fatal("compile is missing from the folded state")
	}
	if step.State != api.StateCached {
		t.Errorf("state = %s, want cached", step.State)
	}
	if !step.Cached {
		t.Error("the step folded without the cached flag, so a renderer would count it as an ordinary success")
	}
	if !step.Started.IsZero() {
		t.Error("a cached step has a start time, which means step.started was emitted for a step that never started")
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("a fully cached run rolled up to %s, want succeeded", st.Run.Status)
	}
}

// Restoring a hit replays the stored logs so the UI shows what would have
// happened.
func TestACacheHitReplaysTheStoredLogs(t *testing.T) {
	_, second, dirs := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))

	b, err := os.ReadFile(filepath.Join(dirs[1], "logs", "compile", "1", "stdout"))
	if err != nil {
		t.Fatalf("a cached step left no log file: %v", err)
	}
	if !strings.Contains(string(b), "compiled") {
		t.Errorf("the replayed log does not contain the cached output: %q", b)
	}
	var markers int
	for _, e := range second {
		if e.Type == api.StepLogAppended && e.Step == "compile" {
			markers++
		}
	}
	if markers == 0 {
		t.Error("no step.log.appended marker was emitted for the replayed log, so an attached client sees nothing")
	}
}

// Sequence numbers stay this run's own. Replaying a stored run's events
// verbatim would put two sequence spaces in one ledger.
func TestReplayedEventsUseThisRunsSequence(t *testing.T) {
	_, second, _ := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	var last uint64
	for _, e := range second {
		if e.Seq <= last {
			t.Fatalf("sequence regressed at %s: %d after %d", e.Type, e.Seq, last)
		}
		last = e.Seq
		if e.Run != "r2" {
			t.Errorf("event %s carries run %q, want the current run", e.Type, e.Run)
		}
	}
}

func TestCacheHitNamesTheRunThatProducedTheResult(t *testing.T) {
	_, second, _ := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	for _, e := range second {
		if e.Type != api.CacheHit {
			continue
		}
		var b api.CacheHitBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode cache.hit: %v", err)
		}
		if b.FromRun != "r1" {
			t.Errorf("cache.hit from_run = %q, want the run that produced the result", b.FromRun)
		}
		if b.Key == "" {
			t.Error("cache.hit carries no key")
		}
	}
}

// The negative half. An input change must miss, and the miss must say which
// component moved.
func TestAChangedInputMissesAndNamesTheComponent(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(body string, runID string) []api.Event {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf '"+body+"' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "cat main.go > out.txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("package main\\n", "r1")
	second := run("package other\\n", "r2")

	if countType(second, api.CacheHit) != 0 {
		t.Fatal("a changed input still hit the cache, which is a wrong build")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheMiss {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode cache.miss: %v", err)
			}
		}
	}
	if body.Reason != cache.ReasonKeyChanged {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonKeyChanged)
	}
	if body.Differing != "input_digests" && body.Differing != "workspace_digests" {
		t.Errorf("miss differing = %q, want the component that actually moved", body.Differing)
	}
}

func TestAnImpureStepIsNeverCachedAndEmitsNoCacheEvents(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	first, second, _ := runTwice(t, p)
	for i, events := range [][]api.Event{first, second} {
		for _, ty := range []api.Type{api.CacheHit, api.CacheMiss, api.CacheSaved} {
			if n := countType(events, ty); n != 0 {
				t.Errorf("run %d emitted %d %s events for an impure step", i+1, n, ty)
			}
		}
		if countType(events, api.StepStarted) != 1 {
			t.Errorf("run %d did not run the impure step", i+1)
		}
	}
}

func TestAFailedPureStepIsNotSaved(t *testing.T) {
	p := purePipeline(t, "echo partial > out.txt; exit 3")
	first, second, _ := runTwice(t, p)
	if countType(first, api.CacheSaved) != 0 {
		t.Error("a failed step saved a cache entry, so the failure would be served to every future run")
	}
	if countType(second, api.CacheHit) != 0 {
		t.Error("a failed step was served from cache")
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the second run did not re-execute the failed step: %d starts", countType(second, api.StepStarted))
	}
}

// The class fix. A GC that collected an object an entry references must not
// be able to break a build.
func TestAHitWithMissingContentDegradesToAMiss(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	// Empty the CAS while leaving every cache entry in place, which is
	// exactly what an over-eager sweep produces.
	if err := os.RemoveAll(filepath.Join(cacheRoot, "cas", "sha256")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	second := run("r2")

	if countType(second, api.CacheHit) != 0 {
		t.Error("an entry whose content was collected still reported a hit")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheMiss {
			_ = e.Decode(&body)
		}
	}
	if body.Reason != cache.ReasonEntryIncomplete {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonEntryIncomplete)
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the run did not re-execute after the degraded hit: %d starts", countType(second, api.StepStarted))
	}
}

// savedEntry loads the one action-cache entry a run saved, by walking
// cacheRoot/action/entries the same way TestACorruptCacheEntryIsTreatedAs...
// does: there is no lookup-by-step accessor on cache.Dir, only Lookup by
// key, and the whole point here is finding the entry without already
// knowing its key.
func savedEntry(t *testing.T, cacheRoot string) cache.Entry {
	t.Helper()
	var found []cache.Entry
	err := filepath.Walk(filepath.Join(cacheRoot, "action", "entries"), func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var e cache.Entry
		if err := json.Unmarshal(b, &e); err != nil {
			return err
		}
		found = append(found, e)
		return nil
	})
	if err != nil {
		t.Fatalf("walk entries: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("found %d saved entries, want exactly 1: %+v", len(found), found)
	}
	return found[0]
}

// hitIsReproducible checks three kinds of presence, in order: Workspaces,
// Outputs, Logs. These three tests check each object kind separately, one
// loop per test, so each fails on its own if its own loop is removed: a
// single combined test that emptied the WHOLE cas would only ever catch a
// missing Workspaces check, since that loop fails first and short-circuits
// the rest.

func TestAHitWithMissingWorkspaceDegradesToAMiss(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	entry := savedEntry(t, cacheRoot)
	if len(entry.Result.Workspaces) == 0 {
		t.Fatal("the saved entry has no workspace to delete; the fixture changed")
	}
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if err := store.CAS.Delete(entry.Result.Workspaces[0].Digest); err != nil {
		t.Fatalf("delete workspace object: %v", err)
	}
	_ = store.Close()

	second := run("r2")
	if countType(second, api.CacheHit) != 0 {
		t.Error("a hit whose workspace object was gone still reported a hit")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheHit {
			t.Error("cache.hit was emitted before the missing workspace was ever checked")
		}
		if e.Type == api.WSRestored {
			t.Error("ws.restored was emitted for a workspace object that does not exist")
		}
		if e.Type == api.CacheMiss {
			_ = e.Decode(&body)
		}
	}
	if body.Reason != cache.ReasonEntryIncomplete {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonEntryIncomplete)
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the run did not re-execute after the degraded hit: %d starts", countType(second, api.StepStarted))
	}
}

func TestAHitWithMissingOutputDegradesToAMiss(t *testing.T) {
	// Deliberately NOT "tee out.txt": that would give stdout and out.txt
	// identical bytes, and a content-addressed store gives identical bytes
	// the SAME digest: deleting "the output object" would then also
	// delete the log's object, and this test would pass even with the
	// Outputs loop removed, for the wrong reason (the Logs loop catching
	// it instead). Distinct content keeps the two checks independent.
	p := purePipeline(t, "echo to-stdout; echo to-outfile > out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	entry := savedEntry(t, cacheRoot)
	if len(entry.Result.Outputs) == 0 {
		t.Fatal("the saved entry has no output to delete; the fixture changed")
	}
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	// The workspace object is left alone, so a mutant that deleted the
	// Workspaces loop entirely would still catch this one via that loop,
	// which is fine, that loop is supposed to keep working. What this test
	// pins down is that Outputs is ALSO checked, which only shows up once
	// Workspaces has already passed.
	if err := store.CAS.Delete(entry.Result.Outputs[0].Digest); err != nil {
		t.Fatalf("delete output object: %v", err)
	}
	_ = store.Close()

	second := run("r2")
	if countType(second, api.CacheHit) != 0 {
		t.Error("a hit whose output object was gone still reported a hit")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheHit {
			t.Error("cache.hit was emitted before the missing output was ever checked")
		}
		if e.Type == api.CacheMiss {
			_ = e.Decode(&body)
		}
	}
	if body.Reason != cache.ReasonEntryIncomplete {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonEntryIncomplete)
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the run did not re-execute after the degraded hit: %d starts", countType(second, api.StepStarted))
	}
}

func TestAHitWithMissingLogDegradesToAMiss(t *testing.T) {
	// See TestAHitWithMissingOutputDegradesToAMiss's comment: distinct
	// content for stdout and out.txt, so deleting the log object cannot
	// also delete the output object by content-address coincidence.
	p := purePipeline(t, "echo to-stdout; echo to-outfile > out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	entry := savedEntry(t, cacheRoot)
	if len(entry.Result.Logs) == 0 {
		t.Fatal("the saved entry has no log to delete; the fixture changed")
	}
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	// Workspace AND output objects are left alone: this makes concrete the
	// hazard of a GC that collects only the logs of an otherwise-intact
	// entry, the same scenario hitIsReproducible's own doc comment (above)
	// exists to catch.
	if err := store.CAS.Delete(entry.Result.Logs[0].Digest); err != nil {
		t.Fatalf("delete log object: %v", err)
	}
	_ = store.Close()

	second := run("r2")
	if countType(second, api.CacheHit) != 0 {
		t.Error("a hit whose log object was gone still reported a hit")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheHit {
			t.Error("cache.hit was emitted before the missing log was ever checked")
		}
		if e.Type == api.CacheMiss {
			_ = e.Decode(&body)
		}
	}
	if body.Reason != cache.ReasonEntryIncomplete {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonEntryIncomplete)
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the run did not re-execute after the degraded hit: %d starts", countType(second, api.StepStarted))
	}
}

func TestTheRunRecordsItsKeyForEveryPureStep(t *testing.T) {
	_, _, dirs := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	r, err := cache.ReadRecord(filepath.Join(dirs[1], "cache"), "compile")
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if !r.Hit {
		t.Errorf("the second run's record says miss: %+v", r)
	}
	if r.Digest == "" || r.Key.Version != cache.KeyVersion {
		t.Errorf("record = %+v, want the key it looked up", r)
	}
}

// A degraded hit is a miss in every observable way, including the run's own
// record of its decision. If the record still said Hit: true after
// serveFromCache gave up, `cache explain` would tell an operator the exact
// opposite of what happened: the step ran, from scratch, right there in the
// same run.
func TestADegradedHitRecordsItselfAsAMissNotAHit(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	run := func(runID string) string {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: sink.Nop(), Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return runDir
	}

	_ = run("r1")
	if err := os.RemoveAll(filepath.Join(cacheRoot, "cas", "sha256")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	dir2 := run("r2")

	r, err := cache.ReadRecord(filepath.Join(dir2, "cache"), "compile")
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if r.Hit {
		t.Errorf("record says hit for a step that actually ran after its entry degraded: %+v", r)
	}
	if r.Reason != cache.ReasonEntryIncomplete {
		t.Errorf("record reason = %q, want %q", r.Reason, cache.ReasonEntryIncomplete)
	}
}

// A corrupt entry must read as an ordinary miss at the engine level too, not
// merely inside the cache package's own unit tests: the wiring in between
// (cacheLookup, Previous, the record it writes) must not panic, must not
// surface the corruption as a run failure, and must let the step run.
func TestACorruptCacheEntryIsTreatedAsAnOrdinaryMissNotACrash(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	runDir1 := filepath.Join(t.TempDir(), "run")
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir1, Executor: localexec.New(runDir1, store.Snapshotter),
		Sink: sink.Nop(), Storage: store, RunID: "r1",
	}); err != nil {
		t.Fatalf("engine.Run 1: %v", err)
	}
	_ = store.Close()

	// Corrupt every entry file the first run wrote: same effect as bit rot
	// or a torn write on a developer's disk, whatever key shape the plan
	// happens to produce.
	entries := filepath.Join(cacheRoot, "action", "entries")
	corrupted := 0
	if err := filepath.Walk(entries, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if writeErr := os.WriteFile(p, []byte("{not valid json"), 0o644); writeErr != nil {
			return writeErr
		}
		corrupted++
		return nil
	}); err != nil {
		t.Fatalf("corrupt entries: %v", err)
	}
	if corrupted == 0 {
		t.Fatal("no entry file found to corrupt; the first run never saved one")
	}

	store2, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()
	runDir2 := filepath.Join(t.TempDir(), "run")
	rec := sink.Recording()
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir2, Executor: localexec.New(runDir2, store2.Snapshotter),
		Sink: rec, Storage: store2, RunID: "r2",
	}); err != nil {
		t.Fatalf("engine.Run 2: %v", err)
	}
	events := rec.Events()

	if countType(events, api.CacheHit) != 0 {
		t.Error("a corrupt entry still reported a hit")
	}
	if countType(events, api.CacheMiss) != 1 {
		t.Errorf("want exactly one cache.miss for the corrupt entry, got %d", countType(events, api.CacheMiss))
	}
	if countType(events, api.StepStarted) != 2 {
		t.Errorf("the step did not run after a corrupt entry: %d starts", countType(events, api.StepStarted))
	}
}

// A save that fails mid-write must not fail the run or the step: the step
// ran correctly and produced a correct result, and a storage hiccup while
// recording that fact is slower, not broken. The next run simply misses
// again, which this test also proves.
func TestASaveThatFailsMidWriteDegradesGracefullyRatherThanFailingTheRun(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not block writes")
	}
	p := purePipeline(t, "echo compiled | tee out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	entries := filepath.Join(cacheRoot, "action", "entries")
	// Removing write permission on the action cache's entries directory
	// makes Dir.Save fail while it is trying to create the two-character
	// fanout subdirectory for a brand new digest -- after the step has
	// already run and its workspace and output have already landed safely
	// in the CAS. This isolates the save step itself, the same failure
	// shape a full disk or a read-only mount would produce.
	if err := os.Chmod(entries, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(entries, 0o755) })

	runDir := filepath.Join(t.TempDir(), "run")
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: rec, Storage: store, RunID: "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	_ = store.Close()
	events := rec.Events()

	if status != api.RunSucceeded {
		t.Errorf("a save failure changed the run's own status: got %s, want succeeded", status)
	}
	if countType(events, api.CacheSaved) != 0 {
		t.Error("a save that could not write still reported cache.saved")
	}
	var sawSaveFailedMiss bool
	for _, e := range events {
		if e.Type != api.CacheMiss {
			continue
		}
		var b api.CacheMissBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode cache.miss: %v", err)
		}
		if b.Reason == "save_failed" {
			sawSaveFailedMiss = true
		}
	}
	if !sawSaveFailedMiss {
		t.Error("no cache.miss reported the save failure")
	}
	st := api.NewRunState()
	for _, e := range events {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if step := st.Steps["compile"]; step == nil || step.State != api.StateSucceeded {
		t.Errorf("the step whose save failed did not settle as succeeded: %+v", step)
	}

	// Restore permissions and run again: nothing was actually saved, so this
	// misses too, proving the failure cost time rather than correctness.
	if err := os.Chmod(entries, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	store2, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store2.Close() }()
	runDir2 := filepath.Join(t.TempDir(), "run")
	rec2 := sink.Recording()
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir2, Executor: localexec.New(runDir2, store2.Snapshotter),
		Sink: rec2, Storage: store2, RunID: "r2",
	}); err != nil {
		t.Fatalf("engine.Run 2: %v", err)
	}
	if countType(rec2.Events(), api.CacheHit) != 0 {
		t.Error("a run whose save failed was somehow still hit by a later run")
	}
}

// TestTheSameSecretHitsAndAChangedOneMisses proves the cache's whole secret
// requirement end to end: an unchanged secret value hits and a changed one
// misses, without the value itself ever entering the key. Driven through
// senro.Run twice against one cache root.
func TestTheSameSecretHitsAndAChangedOneMisses(t *testing.T) {
	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("input"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// This step mounts no workspace, so cache.Resolve reads its declared
	// Inputs against the coordinator's own working directory (wsManager.inputRoot's
	// documented fallback for a step with no workspace), not against WorkDir.
	// Chdir makes the two agree, exactly as the "coordinator's working
	// directory... is where a repository's sources are" case assumes a real
	// invocation's cwd already would.
	t.Chdir(work)

	build := func(value string) any {
		type config struct {
			Tok secret.String `source:"fake://ci/tok#v"`
		}
		p := mamoritest.NewProvider("fake")
		p.Set("ci/tok#v", value)
		cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
		if err != nil {
			t.Fatalf("mamori.Load: %v", err)
		}
		return cfg
	}
	pipeline := func() *senro.Pipeline {
		pipe := senro.New("p")
		pipe.Workflow("w").
			Step("pure", exec.Command("cat", "in.txt")).
			WorkDir(work).
			Pure().
			Inputs(artifact.File("in.txt")).
			SecretEnv("TOK", "Tok")
		return pipe
	}
	runOnce := func(value string) []api.Event {
		dir := t.TempDir()
		if err := senro.Run(context.Background(), pipeline(),
			senro.WithDir(dir), senro.WithCacheDir(cacheDir),
			senro.WithSecrets(build(value))); err != nil {
			t.Fatalf("Run: %v", err)
		}
		return readLedger(t, dir)
	}

	const first = "value-one-aaaaaaaa"
	_ = runOnce(first)

	second := runOnce(first)
	if !hasEvent(second, api.CacheHit) {
		t.Error("the second run with the SAME secret did not hit")
	}

	third := runOnce("value-two-bbbbbbbb")
	if hasEvent(third, api.CacheHit) {
		t.Error("a run with a CHANGED secret hit; a changed credential must invalidate the step")
	}
	miss := findEvent(t, third, api.CacheMiss)
	var body api.CacheMissBody
	if err := miss.Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(body.Differing, "secrets") {
		t.Errorf("cache.miss blames %q; it must name the secrets component", body.Differing)
	}
	if strings.Contains(body.Differing, "value-one") || strings.Contains(body.Differing, "value-two") {
		t.Errorf("cache.miss carries a secret value: %q", body.Differing)
	}
}

// TestACachedLogIsRedactedAgainstTheCURRENTRunsSecrets is the class fix for
// replay. The first run's bytes were written when nothing was a secret, so
// they hold the value in the clear inside the CAS; the second run knows it is
// a secret and must not put it back into a log file or out to a client.
func TestACachedLogIsRedactedAgainstTheCURRENTRunsSecrets(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("leak: "+value+"\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// See the identical Chdir in TestTheSameSecretHitsAndAChangedOneMisses:
	// this step mounts no workspace, so its declared Inputs resolve against
	// the coordinator's cwd, not WorkDir.
	t.Chdir(work)
	pipeline := func() *senro.Pipeline {
		pipe := senro.New("p")
		pipe.Workflow("w").
			Step("pure", exec.Command("cat", "in.txt")).
			WorkDir(work).Pure().Inputs(artifact.File("in.txt"))
		return pipe
	}

	// Run one: no secrets at all, so the log the cache stores holds the value.
	firstDir := t.TempDir()
	if err := senro.Run(context.Background(), pipeline(),
		senro.WithDir(firstDir), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Run two: the same plan, but now the value is a declared secret.
	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	secondDir := t.TempDir()
	if err := senro.Run(context.Background(), pipeline(),
		senro.WithDir(secondDir), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if !hasEvent(readLedger(t, secondDir), api.CacheHit) {
		t.Fatal("the second run did not hit, so nothing was replayed and this test proves nothing")
	}

	body, err := os.ReadFile(eventlog.NewLogSet(secondDir).Path("pure", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the replayed log: %v", err)
	}
	if !bytes.Contains(body, []byte("leak: ")) {
		t.Fatalf("the replayed log has no content: %q", body)
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the replayed log carries a value that IS a secret in this run")
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Error("the replayed log was not redacted")
	}
}

// hasEvent reports whether events contains at least one of type ty.
func hasEvent(events []api.Event, ty api.Type) bool {
	for _, e := range events {
		if e.Type == ty {
			return true
		}
	}
	return false
}

// findEvent returns the first event of type ty, and fails the test outright
// if there is none: a missing event must never be read as a passing
// assertion by a caller that goes on to decode a zero-value default.
func findEvent(t *testing.T, events []api.Event, ty api.Type) api.Event {
	t.Helper()
	for _, e := range events {
		if e.Type == ty {
			return e
		}
	}
	t.Fatalf("no event of type %q in the ledger", ty)
	return api.Event{}
}
