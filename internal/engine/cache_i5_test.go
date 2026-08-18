package engine_test

import (
	"context"
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

// TestAnExcludedDirectoryDoesNotEnterInputDigests guards against a real
// trap: a workspace's Exclude patterns must be honoured by input
// resolution the same way they are by the snapshot, or a file under an
// excluded directory could still enter input_digests if a selector matched
// it, so a build.stamp file that legitimately changes every run would make
// a Pure step miss every single time, silently, even though the
// workspace's own digest (which the exclude genuinely does keep stable)
// suggested nothing should have moved.
func TestAnExcludedDirectoryDoesNotEnterInputDigests(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(stamp, runID string) []api.Event {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun), senro.Exclude("dist/"))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c",
			"printf 'package main\\n' > main.go; printf 'notes\\n' > notes.txt; "+
				"mkdir -p dist; printf '"+stamp+"' > dist/stamp.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "wc -c main.go")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.txt"), artifact.Glob("**/*.go"))
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

	first := run("v1", "r1")
	if countType(first, api.CacheSaved) != 1 {
		t.Fatalf("run 1 did not save: %d saves", countType(first, api.CacheSaved))
	}
	// Only dist/stamp.txt's CONTENT changes between the two runs (main.go
	// is byte-identical), so a correct key, agreeing with the workspace's
	// own excluder about what dist/ is, must still hit.
	second := run("v2", "r2")
	if countType(second, api.CacheHit) != 1 {
		var body api.CacheMissBody
		for _, e := range second {
			if e.Type == api.CacheMiss {
				_ = e.Decode(&body)
			}
		}
		t.Fatalf("run 2 missed even though only an excluded file changed: reason=%q differing=%q",
			body.Reason, body.Differing)
	}
}
