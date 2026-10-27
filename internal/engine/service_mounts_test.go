//go:build unix

package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/service"
)

// A service that mounts a workspace does not hold it for its whole life. On
// the warm run, cached's hit restores src (an exclusive hold) while db is up
// with src mounted; were db's shared hold kept until it stops, the restore
// would wait for db, use's mount of src would queue behind the restore, and
// db would wait for use: a deadlock. The run must finish.
func TestAServiceDoesNotHoldItsWorkspaceAgainstASiblingsRestore(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	build := func(t *testing.T, port int) *senro.Plan {
		t.Helper()
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Service("db", exec.Command(self, "-test.run=^TestHelperServe$"), service.Ports(port)).
			Env("SENRO_TEST_SERVE_PORT", strconv.Itoa(port)).
			Env("GORACE", "atexit_sleep_ms=0").
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
		l.Step("cached", exec.Command("sh", "-c", "wc -c < main.go > /dev/null")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		l.Step("use", exec.Command("sh", "-c", "sleep 0.5; cat main.go > /dev/null")).
			Needs("db").WorkDir("/src").Mount(ws.At("/src", senro.RO))
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
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status, err := engine.Run(ctx, build(t, freePort(t)), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID, MaxParallel: 4,
		})
		if err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		if ctx.Err() != nil {
			t.Fatalf("run %s did not finish within 30s: the service held src against the restore", runID)
		}
		if status != api.RunSucceeded {
			t.Fatalf("run %s = %s, want succeeded", runID, status)
		}
		return rec.Events()
	}

	_ = run("r1")
	if hits := countType(run("r2"), api.CacheHit); hits != 1 {
		t.Fatalf("the warm run had %d cache hits, want 1: the restore was not exercised", hits)
	}
}

// A service sees a private copy of its workspace as it was when it started.
// db serves src/main.go over HTTP; once it is up, cached (Pure) rewrites
// src, and on the warm run a cache hit restores src with that different
// content into the shared directory while db runs. use then asks db for the
// file and must get the ORIGINAL content, and the run must finish.
func TestAServiceSeesItsWorkspaceAsItWasWhenItStarted(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	build := func(t *testing.T, port int, out string) *senro.Plan {
		t.Helper()
		base := "http://127.0.0.1:" + strconv.Itoa(port)
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf original > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Service("db", exec.Command(self, "-test.run=^TestHelperServe$"),
			service.Ports(port), service.HTTP(port, "/healthz")).
			Env("SENRO_TEST_SERVE_PORT", strconv.Itoa(port)).
			Env("SENRO_TEST_SERVE_HTTP", "1").
			Env("SENRO_TEST_SERVE_FILE", "main.go").
			Env("GORACE", "atexit_sleep_ms=0").
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
		// gate holds cached back until db answers, without being a consumer
		// of it (a consumer's key would carry db's node, whose port differs
		// every run, and cached would never hit).
		l.Step("gate", exec.Command(self, "-test.run=^TestHelperFetch$")).
			Env("SENRO_TEST_FETCH_URL", base+"/healthz").Needs("seed")
		l.Step("cached", exec.Command("sh", "-c", "printf from-cache > main.go")).
			Needs("gate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		l.Step("use", exec.Command(self, "-test.run=^TestHelperFetch$")).
			Env("SENRO_TEST_FETCH_URL", base+"/file").Env("SENRO_TEST_FETCH_OUT", out).
			Needs("db", "cached")
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(runID string) (events []api.Event, served, shared string) {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		out := filepath.Join(t.TempDir(), "served")
		rec := sink.Recording()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status, err := engine.Run(ctx, build(t, freePort(t), out), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID, MaxParallel: 4,
		})
		if err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		if ctx.Err() != nil {
			t.Fatalf("run %s did not finish within 30s", runID)
		}
		if status != api.RunSucceeded {
			t.Fatalf("run %s = %s, want succeeded", runID, status)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("run %s: what use was served: %v", runID, err)
		}
		sh, err := os.ReadFile(filepath.Join(runDir, "ws", "src", "main.go"))
		if err != nil {
			t.Fatalf("run %s: the shared workspace: %v", runID, err)
		}
		return rec.Events(), string(b), string(sh)
	}

	if _, served, _ := run("r1"); served != "original" {
		t.Errorf("cold run: the service served %q, want the content it started with", served)
	}
	events, served, shared := run("r2")
	if hits := countType(events, api.CacheHit); hits != 1 {
		t.Fatalf("the warm run had %d cache hits, want 1: the restore was not exercised", hits)
	}
	if shared != "from-cache" {
		t.Fatalf("the shared src holds %q after the hit, want the restored content", shared)
	}
	if served != "original" {
		t.Errorf("warm run: the service served %q after the restore, want %q: it saw the shared directory change",
			served, "original")
	}
}
