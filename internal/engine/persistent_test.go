package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// persistFixture is one cache root reused across several runs, which is the
// only way to observe anything about a scope whose whole point is outliving
// a run. Every helper here takes an explicit run id, because two runs
// sharing a persistent workspace is exactly what several of these tests are
// about and "r1" twice would hide which one did what.
type persistFixture struct {
	t     *testing.T
	store *storage.Storage
}

func newPersistFixture(t *testing.T) *persistFixture {
	t.Helper()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	return &persistFixture{t: t, store: store}
}

// run executes p against the shared cache root and returns what happened.
// The engine error is returned rather than fataled: a refused lease is an
// engine error, and one of these tests is about exactly that.
func (f *persistFixture) run(p *senro.Plan, runID string) (api.RunStatus, []api.Event, string, error) {
	f.t.Helper()
	runDir := filepath.Join(f.t.TempDir(), "run-"+runID)
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:      runDir,
		Executor: localexec.New(runDir, f.store.Snapshotter),
		Sink:     rec,
		Storage:  f.store,
		RunID:    runID,
	})
	return status, rec.Events(), runDir, err
}

func (f *persistFixture) mustRun(p *senro.Plan, runID string) (api.RunStatus, []api.Event, string) {
	f.t.Helper()
	status, events, dir, err := f.run(p, runID)
	if err != nil {
		f.t.Fatalf("engine.Run(%s): %v", runID, err)
	}
	return status, events, dir
}

// dir is where a persistent workspace's content actually lives, taken by
// leasing it the same way a run would. Only safe to call when no run holds
// it.
func (f *persistFixture) dir(name string) string {
	f.t.Helper()
	l, err := f.store.Persist.Acquire(persist.Spec{Name: name, MaxAge: time.Hour, MaxSize: 1 << 30}, "test")
	if err != nil {
		f.t.Fatalf("Acquire(%q) to find its directory: %v", name, err)
	}
	d := l.Dir()
	l.Abandon()
	return d
}

func evictions(events []api.Event) []api.WSEvictedBody {
	var out []api.WSEvictedBody
	for _, e := range events {
		if e.Type != api.WSEvicted {
			continue
		}
		var b api.WSEvictedBody
		if err := e.Decode(&b); err != nil {
			continue
		}
		out = append(out, b)
	}
	return out
}

// The property the whole scope exists for, end to end through the engine:
// what one run leaves in a persistent workspace, the next run's step reads.
// A ScopeRun workspace under the identical pipeline would show the second
// run an empty directory.
func TestAPersistentWorkspaceCarriesContentBetweenRuns(t *testing.T) {
	f := newPersistFixture(t)
	mk := func(cmd string) *senro.Plan {
		ws := senro.Workspace("mods",
			senro.Scope(senro.ScopePersistent), senro.MaxAge(time.Hour), senro.MaxSize(1<<20))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("sh", "-c", cmd)).WorkDir("/mods").Mount(ws.At("/mods", senro.RW))
		return build(t, pipe)
	}

	if status, _, _ := f.mustRun(mk("echo expensive > dep.txt"), "r1"); status != api.RunSucceeded {
		t.Fatalf("first run status = %s, want succeeded", status)
	}
	status, _, runDir := f.mustRun(mk("cat dep.txt"), "r2")
	if status != api.RunSucceeded {
		t.Fatalf("second run status = %s: the persistent workspace did not survive", status)
	}
	b, err := os.ReadFile(filepath.Join(runDir, "logs", "s", "1", "stdout"))
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if strings.TrimSpace(string(b)) != "expensive" {
		t.Errorf("the second run read %q, want what the first run wrote", b)
	}
}

// The cache-key hazard, stated as a test, and the reason this feature could
// not be a directory that simply survives: a Pure() step mounting a
// persistent workspace has inputs that change between runs, and a key that
// does not move with the directory serves the second run a result computed
// against different bytes.
//
// The file changed between runs is deliberately NOT a declared input: that
// would move input_digests on its own and pass whether or not the
// workspace reached the key. Only workspace_digests can move here, so this
// fails the moment the opening measurement stops happening.
func TestChangingAPersistentWorkspaceBetweenRunsMovesTheCacheKey(t *testing.T) {
	f := newPersistFixture(t)
	mk := func() *senro.Plan {
		ws := senro.Workspace("mods",
			senro.Scope(senro.ScopePersistent), senro.MaxAge(time.Hour), senro.MaxSize(1<<20))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("sh", "-c", "cat dep.txt > seen.txt")).
			WorkDir("/mods").Mount(ws.At("/mods", senro.RW)).
			Pure().Inputs(artifact.File("dep.txt")).Outputs(artifact.File("seen.txt"))
		return build(t, pipe)
	}

	// A first run to populate the workspace and save an entry. sidecar.txt is
	// part of the workspace and is not a declared input of anything.
	seed := senro.Workspace("mods",
		senro.Scope(senro.ScopePersistent), senro.MaxAge(time.Hour), senro.MaxSize(1<<20))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("sh", "-c", "echo one > dep.txt; echo a > sidecar.txt; cat dep.txt > seen.txt")).
		WorkDir("/mods").Mount(seed.At("/mods", senro.RW))
	if status, _, _ := f.mustRun(build(t, pipe), "r1"); status != api.RunSucceeded {
		t.Fatalf("seed run status = %s", status)
	}

	// The same pure step twice with nothing changed: the second must hit, or
	// the miss below proves nothing.
	if status, _, _ := f.mustRun(mk(), "r2"); status != api.RunSucceeded {
		t.Fatalf("r2 status = %s", status)
	}
	_, events, _ := f.mustRun(mk(), "r3")
	if countType(events, api.CacheHit) == 0 {
		t.Fatal("an unchanged persistent workspace did not produce a cache hit, so the miss below proves nothing")
	}

	// Now change the workspace behind the pipeline's back, exactly as another
	// tool on the machine would, in a file no declared input covers.
	if err := os.WriteFile(filepath.Join(f.dir("mods"), "sidecar.txt"), []byte("b\n"), 0o644); err != nil {
		t.Fatalf("mutate the workspace: %v", err)
	}

	_, events, runDir := f.mustRun(mk(), "r4")
	if countType(events, api.CacheHit) != 0 {
		t.Fatal("a step whose persistent workspace changed between runs still hit the cache; " +
			"the result it was served was computed against different bytes")
	}
	if countType(events, api.CacheMiss) == 0 {
		t.Error("no cache.miss was recorded for the changed workspace")
	}
	// And the miss names the component that moved, so `cache explain` says
	// why rather than shrugging.
	recs, err := cache.ReadRecords(filepath.Join(runDir, "cache"))
	if err != nil {
		t.Fatalf("read cache records: %v", err)
	}
	var sawWorkspaceDiff bool
	for _, r := range recs {
		for _, d := range r.Diffs {
			if d.Name == "workspace_digests" {
				sawWorkspaceDiff = true
			}
		}
	}
	if !sawWorkspaceDiff {
		t.Error("the miss did not name workspace_digests, so the workspace's content is not what moved the key")
	}
}

// Two overlapping runs, actually overlapping: the first is held inside a
// step while the second tries to start. senro refuses the second, at second
// zero, naming the run that holds the workspace. It is not a wait (that
// means waiting for somebody else's entire pipeline, which is
// indistinguishable from a hang) and not a private copy (that is a ScopeRun
// workspace with extra steps).
func TestASecondRunSharingAPersistentWorkspaceIsRefusedWhileTheFirstHoldsIt(t *testing.T) {
	f := newPersistFixture(t)
	gate := filepath.Join(t.TempDir(), "gate")
	inStep := filepath.Join(t.TempDir(), "in-step")

	mk := func(cmd string) *senro.Plan {
		ws := senro.Workspace("mods",
			senro.Scope(senro.ScopePersistent), senro.MaxAge(time.Hour), senro.MaxSize(1<<20))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("sh", "-c", cmd)).WorkDir("/mods").Mount(ws.At("/mods", senro.RW))
		return build(t, pipe)
	}

	// The first run parks inside its step until the second has had its
	// answer, so the overlap is real rather than a hopeful sleep.
	held := mk("touch " + inStep + "; while [ ! -f " + gate + " ]; do sleep 0.02; done; echo done > dep.txt")

	var wg sync.WaitGroup
	var firstStatus api.RunStatus
	var firstErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		firstStatus, _, _, firstErr = f.run(held, "r1")
	}()
	defer func() {
		_ = os.WriteFile(gate, nil, 0o644)
		wg.Wait()
	}()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(inStep); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first run never reached its step, so nothing overlapped")
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, _, _, err := f.run(mk("true"), "r2")
	if err == nil {
		t.Fatal("a second run took the same persistent workspace while the first held it; " +
			"two runs now write one mutable tree")
	}
	var busy *persist.HeldError
	if !errors.As(err, &busy) {
		t.Fatalf("error is %T, want a *persist.HeldError: %v", err, err)
	}
	if busy.RunID != "r1" {
		t.Errorf("the refusal names run %q, want r1: an operator cannot act on \"busy\"", busy.RunID)
	}
	if !strings.Contains(err.Error(), "mods") {
		t.Errorf("the refusal does not name the workspace: %v", err)
	}

	// And the first run is undisturbed by the refusal.
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatalf("open the gate: %v", err)
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("the first run failed while the second was being refused: %v", firstErr)
	}
	if firstStatus != api.RunSucceeded {
		t.Errorf("first run status = %s, want succeeded", firstStatus)
	}

	// Once it has finished, the workspace is available again: the refusal is
	// an exclusion, not a leak.
	if status, _, _, err := f.run(mk("true"), "r3"); err != nil {
		t.Errorf("the workspace was still held after the first run finished: %v", err)
	} else if status != api.RunSucceeded {
		t.Errorf("r3 status = %s, want succeeded", status)
	}
}

// A bound that is never enforced is not a bound. This is MaxSize actually
// evicting: one run builds a tree past its bound, and the next one finds an
// empty directory and a ws.evicted saying why.
func TestAPersistentWorkspaceOverMaxSizeIsEvicted(t *testing.T) {
	f := newPersistFixture(t)
	mk := func(cmd string, maxSize int64) *senro.Plan {
		ws := senro.Workspace("mods",
			senro.Scope(senro.ScopePersistent), senro.MaxAge(time.Hour), senro.MaxSize(maxSize))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("sh", "-c", cmd)).WorkDir("/mods").Mount(ws.At("/mods", senro.RW))
		return build(t, pipe)
	}

	// 4 KiB of content against a 100 byte bound.
	_, events, _ := f.mustRun(mk("head -c 4096 /dev/zero > big.bin", 100), "r1")
	evs := evictions(events)
	if len(evs) != 1 {
		t.Fatalf("got %d ws.evicted events, want exactly 1: %+v", len(evs), evs)
	}
	ev := evs[0]
	if ev.Name != "mods" || ev.Reason != persist.ReasonMaxSize {
		t.Errorf("eviction = %+v, want mods/%s", ev, persist.ReasonMaxSize)
	}
	if ev.When != "release" {
		t.Errorf("eviction When = %q, want release: nothing may be deleted while a step is reading it", ev.When)
	}
	if ev.Bytes != 4096 || ev.MaxBytes != 100 {
		t.Errorf("eviction reports %d bytes against a %d bound, want 4096 against 100", ev.Bytes, ev.MaxBytes)
	}

	// The eviction is real on disk, and the next run starts from nothing.
	entries, err := os.ReadDir(f.dir("mods"))
	if err != nil {
		t.Fatalf("an eviction must leave an empty directory, not no directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the evicted workspace still holds %d entries", len(entries))
	}
	status, _, runDir := f.mustRun(mk("test ! -f big.bin", 100), "r2")
	if status != api.RunSucceeded {
		t.Errorf("the next run still saw the evicted content (status %s, run dir %s)", status, runDir)
	}
}

// The same for MaxAge, which is enforced at the other end: when a run leases
// a workspace nothing has used for longer than its bound.
//
// The bound is one millisecond rather than the test backdating a record,
// which keeps the test on the same code path a real run takes: nothing here
// reaches into how the age is stored. The gap it has to exceed is a whole
// engine.Run plus an explicit sleep, so the comparison is never close.
func TestAPersistentWorkspaceOlderThanMaxAgeIsEvicted(t *testing.T) {
	f := newPersistFixture(t)
	mk := func(cmd string, maxAge time.Duration) *senro.Plan {
		ws := senro.Workspace("mods",
			senro.Scope(senro.ScopePersistent), senro.MaxAge(maxAge), senro.MaxSize(1<<20))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("sh", "-c", cmd)).WorkDir("/mods").Mount(ws.At("/mods", senro.RW))
		return build(t, pipe)
	}

	// A generous bound first, so the seed run cannot itself evict anything
	// and the run below is the only candidate.
	if status, events, _ := f.mustRun(mk("echo expensive > dep.txt", time.Hour), "r1"); status != api.RunSucceeded {
		t.Fatalf("seed run status = %s", status)
	} else if len(evictions(events)) != 0 {
		t.Fatalf("the seed run evicted something: %+v", evictions(events))
	}
	time.Sleep(20 * time.Millisecond)

	status, events, _ := f.mustRun(mk("test ! -f dep.txt", time.Millisecond), "r2")
	if status != api.RunSucceeded {
		t.Errorf("status = %s: a workspace well past its MaxAge was not evicted", status)
	}
	evs := evictions(events)
	if len(evs) != 1 {
		t.Fatalf("got %d ws.evicted events, want exactly 1: %+v", len(evs), evs)
	}
	if evs[0].Reason != persist.ReasonMaxAge {
		t.Errorf("eviction reason = %q, want %q", evs[0].Reason, persist.ReasonMaxAge)
	}
	if evs[0].When != "acquire" {
		t.Errorf("eviction When = %q, want acquire", evs[0].When)
	}
	if evs[0].MaxAgeMS != 1 {
		t.Errorf("eviction MaxAgeMS = %d, want 1", evs[0].MaxAgeMS)
	}
	if evs[0].AgeMS < 1 {
		t.Errorf("eviction AgeMS = %d, want the measured age it was evicted for", evs[0].AgeMS)
	}

	// And a workspace inside its bound is left alone, so the eviction above
	// is the policy firing rather than the policy always firing.
	f.mustRun(mk("echo expensive > dep.txt", time.Hour), "r3")
	if _, events, _ := f.mustRun(mk("test -f dep.txt", time.Hour), "r4"); len(evictions(events)) != 0 {
		t.Errorf("a workspace used moments ago was evicted: %+v", evictions(events))
	}
}

// A run that starts from a populated persistent workspace records what it
// started from, so `senro ws` can name it and a reader of the ledger can see
// the state the run's first cache key was computed against.
func TestARunRecordsThePersistentWorkspaceItStartedFrom(t *testing.T) {
	f := newPersistFixture(t)
	mk := func(cmd string) *senro.Plan {
		ws := senro.Workspace("mods",
			senro.Scope(senro.ScopePersistent), senro.MaxAge(time.Hour), senro.MaxSize(1<<20))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("sh", "-c", cmd)).WorkDir("/mods").Mount(ws.At("/mods", senro.RW))
		return build(t, pipe)
	}

	f.mustRun(mk("echo expensive > dep.txt"), "r1")
	_, events, _ := f.mustRun(mk("true"), "r2")

	var opening api.WSSnapshotBody
	var found bool
	for _, e := range events {
		if e.Type != api.WSSnapshot || e.Step != "" {
			continue
		}
		if err := e.Decode(&opening); err != nil {
			t.Fatalf("decode ws.snapshot: %v", err)
		}
		found = true
		break
	}
	if !found {
		t.Fatal("no run-scoped ws.snapshot: the state the run started from is nowhere in the ledger")
	}
	if opening.Name != "mods" {
		t.Errorf("name = %q, want mods", opening.Name)
	}
	if opening.Digest == "" {
		t.Error("the opening snapshot carries no digest, so no cache key can honestly reference it")
	}
	if opening.Bytes != 10 {
		t.Errorf("bytes = %d, want 10 (the dep.txt the first run wrote)", opening.Bytes)
	}
}
