package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// c2runTwice runs pipeline build1 then build2 against ONE shared cache root:
// an entry saved under one shape, then looked up again under a changed one.
func c2runTwice(t *testing.T, build1, build2 *senro.Plan) (first, second []api.Event, dir2 string) {
	t.Helper()
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(p *senro.Plan, runID string) ([]api.Event, string) {
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
		return rec.Events(), runDir
	}
	first, _ = run(build1, "r1")
	second, dir2 = run(build2, "r2")
	return first, second, dir2
}

// TestRemovingNoSnapshotMisses guards against a real trap: an entry
// saved by a step declared .NoSnapshot() (so its Result carries zero
// workspaces) must NOT be served to the identical
// pipeline with .NoSnapshot() removed, because that
// entry cannot reproduce what the step now needs to leave behind.
func TestRemovingNoSnapshotMisses(t *testing.T) {
	build := func(t *testing.T, noSnapshot bool) *senro.Plan {
		t.Helper()
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		compile := l.Step("compile", exec.Command("sh", "-c", "wc -c < main.go > size.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		if noSnapshot {
			compile.NoSnapshot()
		}
		l.Step("consume", exec.Command("sh", "-c", "cat size.txt")).
			Needs("compile").WorkDir("/src").Mount(ws.At("/src", senro.RW))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	first, second, dir2 := c2runTwice(t, build(t, true), build(t, false))
	if countType(first, api.CacheSaved) != 1 {
		t.Fatalf("run 1 (NoSnapshot) did not save: %d saves", countType(first, api.CacheSaved))
	}
	if countType(second, api.CacheHit) != 0 {
		t.Error("run 2 (NoSnapshot removed) hit the entry run 1 saved with zero workspaces; " +
			"the author's own fix did not change the key")
	}

	st := api.NewRunState()
	for _, e := range second {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if step := st.Steps["consume"]; step == nil || step.State != api.StateSucceeded {
		t.Fatalf("consume's state = %+v, want succeeded", step)
	}
	if _, err := os.Stat(filepath.Join(dir2, "ws", "src", "size.txt")); err != nil {
		t.Fatalf("size.txt missing in run 2's workspace: %v", err)
	}
}

// TestAddingOutputsMisses checks a quieter variant of the same trap: a Pure
// step that gains an Outputs declaration must not hit the entry saved
// before the declaration existed, which would silently restore nothing.
func TestAddingOutputsMisses(t *testing.T) {
	build := func(t *testing.T, declareOutputs bool) *senro.Plan {
		t.Helper()
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		compile := l.Step("compile", exec.Command("sh", "-c", "wc -c < main.go > out.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		if declareOutputs {
			compile.Outputs(artifact.File("out.txt"))
		}
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	first, second, _ := c2runTwice(t, build(t, false), build(t, true))
	if countType(first, api.CacheSaved) != 1 {
		t.Fatalf("run 1 (no Outputs) did not save: %d saves", countType(first, api.CacheSaved))
	}
	if countType(second, api.CacheHit) != 0 {
		t.Error("run 2 (Outputs declared) hit the entry saved before the declaration existed")
	}
}

// TestChangingAMountsModeMisses checks a variant that matters even when
// nothing about content differs: an entry saved from a read-only mount
// must not answer for a read-write mount of the same workspace at the same
// digest, because the two are not the same step.
func TestChangingAMountsModeMisses(t *testing.T) {
	build := func(t *testing.T, ro bool) *senro.Plan {
		t.Helper()
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		mode := senro.RW
		if ro {
			mode = senro.RO
		}
		l.Step("compile", exec.Command("sh", "-c", "wc -c main.go")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", mode)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	first, second, _ := c2runTwice(t, build(t, true), build(t, false))
	if countType(first, api.CacheSaved) != 1 {
		t.Fatalf("run 1 (ro) did not save: %d saves", countType(first, api.CacheSaved))
	}
	if countType(second, api.CacheHit) != 0 {
		t.Error("run 2 (rw) hit the entry an ro mount saved at the same workspace digest")
	}
}

// A mount's At is deliberately NOT exercised end to end here: any pipeline
// where the command finds its input through a moved mount necessarily
// moves WorkDir or the command text too, which changes Command anyway and
// proves nothing about At. internal/cache/key_test.go carries the
// unconfounded proof that At alone moves the key.
