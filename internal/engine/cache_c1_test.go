package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// TestACacheHitDoesNotStompAConcurrentSiblingsWorkspace guards a real
// race: a cache hit's restore (RemoveAll then Rename) must not run while a
// sibling with no Needs edge to the cached step is using the same ScopeRun
// workspace. slow and cached share no edge, so on the warm-cache run
// cached's hit restores the SAME directory slow is sitting inside. Without
// the exclusion this fails as an exit 1 (cwd or file vanished) or as
// "fork/exec /bin/sh: no such file or directory" (cwd inode unlinked).
func TestACacheHitDoesNotStompAConcurrentSiblingsWorkspace(t *testing.T) {
	build := func(t *testing.T) *senro.Plan {
		t.Helper()
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		// slow has no Needs edge to cached, so the scheduler runs them
		// together; the sleep gives a racing restore a wide, easy-to-hit
		// window rather than relying on a lucky interleaving.
		l.Step("slow", exec.Command("sh", "-c", "sleep 1; echo x > slow.txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("cached", exec.Command("sh", "-c", "wc -c < main.go > /dev/null")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(t), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}
	check := func(events []api.Event, iteration int) {
		if countType(events, api.CacheHit) != 1 {
			t.Fatalf("iteration %d: did not exercise a cache hit: %d hits, %d misses",
				iteration, countType(events, api.CacheHit), countType(events, api.CacheMiss))
		}

		st := api.NewRunState()
		for _, e := range events {
			if err := st.Apply(e); err != nil {
				t.Fatalf("iteration %d: Apply: %v", iteration, err)
			}
		}
		slow, ok := st.Steps["slow"]
		if !ok {
			t.Fatalf("iteration %d: slow is missing from the folded state", iteration)
		}
		if slow.State != api.StateSucceeded {
			t.Fatalf("iteration %d: slow's state = %s (error=%q), want succeeded: a cache hit for its "+
				"sibling cached stomped its workspace mid-run", iteration, slow.State, slow.Error)
		}

		// The stronger half: not merely that slow did not crash, but that
		// its ws.snapshot faithfully captured what it wrote rather than a
		// directory a concurrent restore was tearing down. A race that
		// corrupts the evidence instead of crashing the step is the
		// "silently discards a sibling's writes" case.
		var slowDigest cas.Digest
		for _, e := range events {
			if e.Type == api.WSSnapshot && e.Step == "slow" {
				var b api.WSSnapshotBody
				if err := e.Decode(&b); err != nil {
					t.Fatalf("iteration %d: decode ws.snapshot: %v", iteration, err)
				}
				slowDigest = cas.Digest(b.Digest)
			}
		}
		if slowDigest == "" {
			t.Fatalf("iteration %d: slow left no ws.snapshot event", iteration)
		}

		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("iteration %d: storage.Open: %v", iteration, err)
		}
		defer func() { _ = store.Close() }()
		dest := filepath.Join(t.TempDir(), "check")
		if err := store.Snapshotter.Restore(context.Background(), slowDigest, dest); err != nil {
			t.Fatalf("iteration %d: restore slow's own recorded snapshot: %v", iteration, err)
		}
		b, err := os.ReadFile(filepath.Join(dest, "slow.txt"))
		if err != nil || strings.TrimSpace(string(b)) != "x" {
			t.Fatalf("iteration %d: slow's own recorded evidence does not contain its write: content=%q err=%v",
				iteration, b, err)
		}
	}

	_ = run("r1")
	// The race does not fire on every interleaving, so the warm-cache run
	// is repeated against the one populated cache root: every iteration
	// must be clean, and the fix removes the race entirely rather than
	// narrowing its window, so a correct implementation passes each
	// deterministically.
	for i := 0; i < 5; i++ {
		check(run(fmt.Sprintf("r2-%d", i)), i)
	}
}
