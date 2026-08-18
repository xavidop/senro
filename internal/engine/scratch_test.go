package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

func TestAScratchCacheSurvivesBetweenRuns(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	build := func() *senro.Plan {
		c := senro.ScratchCache("deps", senro.Key("deps-v1"), senro.RestoreKeys("deps-"))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("install", exec.Command("sh", "-c",
			"if [ -f m/marker ]; then echo warm; else echo cold; mkdir -p m; touch m/marker; fi")).
			Mount(c.At("/m"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	run := func(runID string) (string, []api.Event) {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return runDir, rec.Events()
	}

	firstDir, _ := run("r1")
	b, err := os.ReadFile(filepath.Join(firstDir, "logs", "install", "1", "stdout"))
	if err != nil {
		t.Fatalf("read first log: %v", err)
	}
	if strings.TrimSpace(string(b)) != "cold" {
		t.Fatalf("the first run saw %q, want a cold cache", b)
	}

	secondDir, _ := run("r2")
	b, err = os.ReadFile(filepath.Join(secondDir, "logs", "install", "1", "stdout"))
	if err != nil {
		t.Fatalf("read second log: %v", err)
	}
	if strings.TrimSpace(string(b)) != "warm" {
		t.Errorf("the second run saw %q, want the scratch cache restored", b)
	}

	recs, err := scratch.ReadRecords(filepath.Join(secondDir, "cache"))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || !recs[0].Restored || recs[0].Name != "deps" {
		t.Errorf("scratch records = %+v, want one restored entry named deps", recs)
	}
}

// A scratch cache is never an input to an action cache key.
// If it were, a warm cache and a cold one would key differently and a pure
// step would never hit twice on different machines.
func TestAScratchCacheDoesNotEnterAnActionCacheKey(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	build := func() *senro.Plan {
		c := senro.ScratchCache("deps", senro.Key("deps-v1"))
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "date > /dev/null; touch /m/blob 2>/dev/null || true")).
			Needs("seed").WorkDir("/src").
			Mount(ws.At("/src", senro.RW), c.At("/m")).
			Pure().Inputs(senroGlob())
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}
	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	second := run("r2")
	if countType(second, api.CacheHit) != 1 {
		t.Errorf("the second run did not hit, so the scratch cache's contents reached the key: %d hits",
			countType(second, api.CacheHit))
	}
}

// Nothing is saved from a failed run: a half-populated module cache stored
// under a key that names a complete one is worse than no entry.
func TestAFailedRunDoesNotSaveItsScratchCache(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	c := senro.ScratchCache("deps", senro.Key("deps-v1"))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("install", exec.Command("sh", "-c", "touch /m/partial 2>/dev/null; exit 4")).Mount(c.At("/m"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	runDir := filepath.Join(t.TempDir(), "run")
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: rec, Storage: store, RunID: "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	_ = store.Close()

	recs, err := scratch.ReadRecords(filepath.Join(runDir, "cache"))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].Saved {
		t.Errorf("scratch records = %+v, want one entry that was NOT saved", recs)
	}
}

// senroGlob keeps the import of package artifact in one place in this file.
func senroGlob() artifact.Selector { return artifact.Glob("**/*.go") }

// A restore that falls back to a restore-key prefix must still save a fresh
// entry under the exact key at run end. Skipping the save whenever ANYTHING
// was restored would mean a changing scratch cache never converges: every
// future run falls back to the same stale prefix match forever.
//
// Three runs prove it: run 1 seeds "deps-a"; run 2 asks for "deps-b", falls
// back to "deps-a" (gen1), and must save a fresh "deps-b" (gen2); run 3
// asks for "deps-b" again and sees gen2 on an exact hit, or gen1 one
// generation stale if run 2 wrongly skipped its save.
func TestAScratchCacheSavesAFreshEntryAfterARestoreKeyFallback(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	build := func(key string) *senro.Plan {
		c := senro.ScratchCache("deps", senro.Key(key), senro.RestoreKeys("deps-"))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		// Print whatever marker was restored (or "none" on a cold cache),
		// then leave this run's own generation behind for the next run.
		l.Step("install", exec.Command("sh", "-c",
			"cat m/marker 2>/dev/null || echo none; mkdir -p m; printf '%s' \"$GEN\" > m/marker")).
			Env("GEN", key).
			Mount(c.At("/m"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	run := func(runID, key string) string {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(key), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(runDir, "logs", "install", "1", "stdout"))
		if err != nil {
			t.Fatalf("read log: %v", err)
		}
		return strings.TrimSpace(string(b))
	}

	if got := run("r1", "deps-a"); got != "none" {
		t.Fatalf("run 1 (cold) saw %q, want none", got)
	}
	if got := run("r2", "deps-b"); got != "deps-a" {
		t.Fatalf("run 2 fell back and saw %q, want deps-a's marker", got)
	}
	if got := run("r3", "deps-b"); got != "deps-b" {
		t.Errorf("run 3 asked for deps-b again and saw %q, want deps-b's own marker "+
			"(a restore-key fallback in run 2 must still save a fresh entry under the exact "+
			"key, or every later run keeps falling back to the stale entry forever)", got)
	}
}

// An unexpandable scratch key is a run-level failure, not a best-effort
// degradation: unlike a restore miss or a lost save race, this is the
// pipeline author's own declaration naming files that are not there, and
// the scratch cache's best-effort guarantee covers what happens to the
// CACHE, not whether the plan's own declarations are honoured. Silently
// substituting
// some other key would poison a shared cache with an entry nothing can tell
// apart from a correctly keyed one, which is worse than refusing to run.
func TestAnUnexpandableScratchKeyFailsTheRunNotJustTheCache(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	c := senro.ScratchCache("deps", senro.Key(`deps-{{ hashFiles "this-file-does-not-exist-anywhere" }}`))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("install", exec.Command("sh", "-c", "true")).Mount(c.At("/m"))
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
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: sink.Recording(), Storage: store, RunID: "r1",
	}); err == nil {
		t.Error("engine.Run with an unexpandable scratch key returned no error, " +
			"want the run refused before any step executes")
	}
}
