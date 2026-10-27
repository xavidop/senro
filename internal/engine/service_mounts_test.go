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
