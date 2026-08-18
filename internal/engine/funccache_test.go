package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

func init() {
	senro.RegisterFunc("enginetest/cached", func(ctx senro.Ctx, p struct {
		Out string `json:"out"`
	}) error {
		ws, _ := ctx.Workspace("src")
		body, err := os.ReadFile(ws.Path("in.txt"))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "ran with %q\n", body)
		return os.WriteFile(ws.Path(p.Out), body, 0o644)
	})
}

// funcCacheRunSeq gives each runWithSeededWorkspace call its own RunID, the
// same way every other cache test in this package (runTwice, TestAChangedInputMissesAndNamesTheComponent)
// keeps two runs sharing one cache root apart.
var funcCacheRunSeq atomic.Int64

// runWithSeededWorkspace runs p against cacheDir, having first written
// content into file inside p's "src" workspace directory: these plans have
// no seed step (the func step under test is the only node), so seeding the
// directory before Run's MkdirAll (a no-op on an existing directory)
// stands in for one.
func runWithSeededWorkspace(t *testing.T, p *plan.Plan, cacheDir, file, content string) []api.Event {
	t.Helper()
	store, err := storage.Open(cacheDir)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	runDir := t.TempDir()
	wsDir := filepath.Join(runDir, "ws", "src")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed workspace dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, file), []byte(content), 0o644); err != nil {
		t.Fatalf("seed workspace file: %v", err)
	}

	rec := sink.Recording()
	runID := fmt.Sprintf("01FUNCCACHE%d", funcCacheRunSeq.Add(1))
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir:      runDir,
		Executor: localexec.New(runDir, store.Snapshotter),
		Sink:     rec,
		Storage:  store,
		RunID:    runID,
	}); err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	return rec.Events()
}

// TestAPureFuncStepIsServedFromTheCacheOnASecondRun proves a func step is a
// peer of an exec step in the cache, not merely in the scheduler: same key
// struct, same lookup, same save, same replayed logs.
func TestAPureFuncStepIsServedFromTheCacheOnASecondRun(t *testing.T) {
	cacheDir := t.TempDir()
	params := []byte(`{"out":"out.txt"}`)
	build := func() *plan.Plan {
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "transform", Kind: "func", Pure: true,
				Func:    &plan.FuncSpec{Name: "enginetest/cached", Params: params},
				Inputs:  []string{"file:in.txt"},
				Outputs: []string{"file:out.txt"},
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			}},
		}
	}
	first := runWithSeededWorkspace(t, build(), cacheDir, "in.txt", "content\n")
	if !hasEventFor(first, api.CacheMiss, "transform") {
		t.Fatal("the first run did not miss, so the second run's hit proves nothing")
	}
	if !hasEventFor(first, api.CacheSaved, "transform") {
		t.Fatal("a pure func step was not saved")
	}
	second := runWithSeededWorkspace(t, build(), cacheDir, "in.txt", "content\n")
	if !hasEventFor(second, api.CacheHit, "transform") {
		t.Error("the second run of an identical pure func step did not hit")
	}
	if st, _ := stepFinished(t, second, "transform"); st != api.StateCached {
		t.Errorf("state = %s, want cached", st)
	}
}

// TestChangingAFuncsParametersMissesTheCache is the negative half: the
// identity has to be in the key, not merely computed. Only the "tag"
// parameter differs between the runs, and that isolation is load-bearing:
// varying Outputs alongside it also moves StepShapeComponent, which let an
// earlier version pass against a mutant that dropped FuncIdentity
// entirely. Holding everything else fixed makes the miss attributable to
// FuncIdentity, confirmed directly by cache.miss's Differing field.
func TestChangingAFuncsParametersMissesTheCache(t *testing.T) {
	cacheDir := t.TempDir()
	mk := func(tag string) *plan.Plan {
		params, err := plan.CanonicalParams(struct {
			Out string `json:"out"`
			Tag string `json:"tag"`
		}{Out: "out.txt", Tag: tag})
		if err != nil {
			t.Fatalf("CanonicalParams: %v", err)
		}
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "transform", Kind: "func", Pure: true,
				Func:    &plan.FuncSpec{Name: "enginetest/twofields", Params: params},
				Inputs:  []string{"file:in.txt"},
				Outputs: []string{"file:out.txt"},
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			}},
		}
	}
	_ = runWithSeededWorkspace(t, mk("a"), cacheDir, "in.txt", "content\n")
	second := runWithSeededWorkspace(t, mk("b"), cacheDir, "in.txt", "content\n")
	if hasEventFor(second, api.CacheHit, "transform") {
		t.Fatal("a func step with different parameters was served a cached result")
	}
	miss := findEvent(t, second, api.CacheMiss)
	var body api.CacheMissBody
	if err := miss.Decode(&body); err != nil {
		t.Fatalf("decode cache.miss: %v", err)
	}
	if body.Differing != "func_identity" {
		t.Errorf("cache.miss blames %q, want %q: a change confined to the func's params must move exactly func_identity and nothing else", body.Differing, "func_identity")
	}
}

func init() {
	senro.RegisterFunc("enginetest/twofields", func(ctx senro.Ctx, p struct {
		Out string `json:"out"`
		Tag string `json:"tag"`
	}) error {
		ws, _ := ctx.Workspace("src")
		body, err := os.ReadFile(ws.Path("in.txt"))
		if err != nil {
			return err
		}
		return os.WriteFile(ws.Path(p.Out), body, 0o644)
	})
}

func init() {
	senro.RegisterFunc("enginetest/noparams", func(ctx senro.Ctx, p struct{}) error {
		ws, _ := ctx.Workspace("src")
		body, err := os.ReadFile(ws.Path("in.txt"))
		if err != nil {
			return err
		}
		return os.WriteFile(ws.Path("out.txt"), body, 0o644)
	})
}

// TestAFuncStepWithNoParamsIsCacheableToo is a negative case:
// FuncSpec.Params is optional (json:"params,omitempty"), and
// a step that declares NONE (a nil Params, not merely an empty object) must
// still be cacheable: FuncIdentityComponent must not panic or misbehave on a
// nil params value, and a second identical run must still hit.
func TestAFuncStepWithNoParamsIsCacheableToo(t *testing.T) {
	cacheDir := t.TempDir()
	build := func() *plan.Plan {
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "transform", Kind: "func", Pure: true,
				Func:    &plan.FuncSpec{Name: "enginetest/noparams"}, // Params deliberately left nil
				Inputs:  []string{"file:in.txt"},
				Outputs: []string{"file:out.txt"},
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			}},
		}
	}
	first := runWithSeededWorkspace(t, build(), cacheDir, "in.txt", "content\n")
	if st, _ := stepFinished(t, first, "transform"); st != api.StateSucceeded {
		t.Fatalf("state = %s, want succeeded; this test's premise is broken", st)
	}
	if !hasEventFor(first, api.CacheSaved, "transform") {
		t.Fatal("a func step with nil params was not saved")
	}
	second := runWithSeededWorkspace(t, build(), cacheDir, "in.txt", "content\n")
	if !hasEventFor(second, api.CacheHit, "transform") {
		t.Error("a func step declaring no params did not hit on an identical second run")
	}
}

// TestTwoParamOrderingsProduceOneFuncIdentity: CanonicalParams sorts object
// keys at every level, so two JSON encodings differing only in key order
// must produce the SAME cache key. The orderings must differ at the raw
// JSON TEXT level: encoding/json already sorts a Go map's keys, so two map
// literals would be byte-identical without CanonicalParams doing anything.
func TestTwoParamOrderingsProduceOneFuncIdentity(t *testing.T) {
	cacheDir := t.TempDir()
	build := func(params json.RawMessage) *plan.Plan {
		canon, err := plan.CanonicalParams(params)
		if err != nil {
			t.Fatalf("CanonicalParams: %v", err)
		}
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "transform", Kind: "func", Pure: true,
				Func:    &plan.FuncSpec{Name: "enginetest/twofields", Params: canon},
				Inputs:  []string{"file:in.txt"},
				Outputs: []string{"file:out.txt"},
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			}},
		}
	}
	orderA := json.RawMessage(`{"out":"out.txt","tag":"x"}`)
	orderB := json.RawMessage(`{"tag":"x","out":"out.txt"}`)

	canonA, err := plan.CanonicalParams(orderA)
	if err != nil {
		t.Fatalf("CanonicalParams: %v", err)
	}
	canonB, err := plan.CanonicalParams(orderB)
	if err != nil {
		t.Fatalf("CanonicalParams: %v", err)
	}
	if string(canonA) != string(canonB) {
		t.Fatalf("CanonicalParams is not order independent: %s vs %s; this test's premise is broken", canonA, canonB)
	}

	first := runWithSeededWorkspace(t, build(orderA), cacheDir, "in.txt", "content\n")
	if !hasEventFor(first, api.CacheSaved, "transform") {
		t.Fatal("the first run did not save, so the second run's hit proves nothing")
	}
	second := runWithSeededWorkspace(t, build(orderB), cacheDir, "in.txt", "content\n")
	if !hasEventFor(second, api.CacheHit, "transform") {
		t.Error("two key orderings of the same canonical params produced two different cache keys")
	}
}

func init() {
	senro.RegisterFunc("enginetest/cached2", func(ctx senro.Ctx, p struct {
		Out string `json:"out"`
	}) error {
		ws, _ := ctx.Workspace("src")
		body, err := os.ReadFile(ws.Path("in.txt"))
		if err != nil {
			return err
		}
		return os.WriteFile(ws.Path(p.Out), body, 0o644)
	})
}

// TestARenamedFuncMissesTheCache, end to end: renaming which registered
// function a step calls, everything else constant, must not be served the
// old name's result. enginetest/cached2 is registered in a package-level
// init, not inline: RegisterFunc panics on a duplicate name, and a test
// body runs once per -count while an init runs once per binary, so inline
// registration panicked at -count=2.
func TestARenamedFuncMissesTheCache(t *testing.T) {
	cacheDir := t.TempDir()
	params := []byte(`{"out":"out.txt"}`)
	build := func(name string) *plan.Plan {
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "transform", Kind: "func", Pure: true,
				Func:    &plan.FuncSpec{Name: name, Params: params},
				Inputs:  []string{"file:in.txt"},
				Outputs: []string{"file:out.txt"},
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			}},
		}
	}
	first := runWithSeededWorkspace(t, build("enginetest/cached"), cacheDir, "in.txt", "content\n")
	if !hasEventFor(first, api.CacheSaved, "transform") {
		t.Fatal("the first run did not save, so this test proves nothing")
	}
	second := runWithSeededWorkspace(t, build("enginetest/cached2"), cacheDir, "in.txt", "content\n")
	if hasEventFor(second, api.CacheHit, "transform") {
		t.Fatal("a step renamed to a different registered function was served the old function's cached result")
	}
	miss := findEvent(t, second, api.CacheMiss)
	var body api.CacheMissBody
	if err := miss.Decode(&body); err != nil {
		t.Fatalf("decode cache.miss: %v", err)
	}
	if body.Differing != "func_identity" {
		t.Errorf("cache.miss blames %q, want %q: a rename with everything else held constant must move exactly func_identity", body.Differing, "func_identity")
	}
}

// TestAPipelineWithNoFuncStepsProducesTodaysKeysExactly is the end-to-end
// companion to TestAKeyWithNoFuncIdentityDigestsExactlyAsItAlwaysHas in
// internal/cache/key_test.go: a plan that executes no func step at all must
// not be affected by func step support in any observable way, including
// whether a previously-cached exec step still hits.
func TestAPipelineWithNoFuncStepsProducesTodaysKeysExactly(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	first, second, _ := runTwice(t, p)
	if !hasEvent(first, api.CacheMiss) || !hasEvent(first, api.CacheSaved) {
		t.Fatal("the first run of an exec-only plan did not miss and save as it always has")
	}
	if !hasEvent(second, api.CacheHit) {
		t.Fatal("an exec-only plan's second run did not hit; FuncIdentity moved a key that carries no func step")
	}
}
