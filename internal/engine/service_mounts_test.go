//go:build unix

package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// A file in the workspace the coordinator cannot read does not keep a
// service from starting: the copy skips it and says so in the service's own
// stderr log. The file sits under node_modules, a default snapshot exclude,
// so the seeding step's own snapshot never tries to read it.
func TestAServiceStartsDespiteAnUnreadableWorkspaceFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a 0000 file regardless of its mode")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c",
		"mkdir -p node_modules && printf x > node_modules/locked.bin && chmod 000 node_modules/locked.bin")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Service("db", exec.Command(self, "-test.run=^TestHelperServe$"), service.Ports(port)).
		Env("SENRO_TEST_SERVE_PORT", strconv.Itoa(port)).
		Env("GORACE", "atexit_sleep_ms=0").
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
	l.Step("use", exec.Command("true")).Needs("db")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	store := openStore(t, dir)
	// The shared workspace directory holds the 0000 file; give the test's
	// own cleanup a way to remove it.
	t.Cleanup(func() {
		_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				_ = os.Chmod(p, 0o600)
			}
			return nil
		})
	})
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, store.Snapshotter),
		Sink: sink.Nop(), Storage: store, RunID: "01UNREADABLE", MaxParallel: 4,
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status != api.RunSucceeded {
		_, fin := lastStepFinished(t, readLedger(t, dir), "db")
		t.Fatalf("Run = %s, want succeeded (db: %s %q)", status, fin.State, fin.Error)
	}
	want := "senro: skipped 1 unreadable path(s) copying this service's workspaces: src/node_modules/locked.bin\n"
	if got := stepLog(t, dir, "db", api.StreamStderr); !strings.Contains(got, want) {
		t.Errorf("db's stderr = %q, want it to contain %q", got, want)
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
		// node_modules is a default snapshot exclude: the service must see it
		// anyway, because its copy is of the directory, not of a snapshot.
		l.Step("seed", exec.Command("sh", "-c",
			"printf original > main.go && mkdir -p node_modules && printf dep > node_modules/x.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Service("db", exec.Command(self, "-test.run=^TestHelperServe$"),
			service.Ports(port), service.HTTP(port, "/healthz")).
			Env("SENRO_TEST_SERVE_PORT", strconv.Itoa(port)).
			Env("SENRO_TEST_SERVE_HTTP", "1").
			Env("SENRO_TEST_SERVE_FILE", "1").
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
			Env("SENRO_TEST_FETCH_URL", base+"/file/main.go").Env("SENRO_TEST_FETCH_OUT", out).
			Needs("db", "cached")
		l.Step("dep", exec.Command(self, "-test.run=^TestHelperFetch$")).
			Env("SENRO_TEST_FETCH_URL", base+"/file/node_modules/x.txt").
			Env("SENRO_TEST_FETCH_OUT", out+".dep").
			Needs("db")
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
		if d, err := os.ReadFile(out + ".dep"); err != nil || string(d) != "dep" {
			t.Errorf("run %s: node_modules/x.txt served as %q (%v), want \"dep\": the copy applied the snapshot excludes",
				runID, d, err)
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
