package engine_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// TestCacheEnvSeesTheExecutorsOwnInjectedVariable: CacheEnv names a
// variable the step actually receives, and localexec injects PATH into
// every step that declares none. Built from n.Env alone, CacheEnv("PATH")
// would be a complete no-op. See localexec's
// TestEffectiveEnvAddsTheExecutorsDefaultPATH for the primitive.
func TestCacheEnvSeesTheExecutorsOwnInjectedVariable(t *testing.T) {
	build := func(t *testing.T) *senro.Plan {
		t.Helper()
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "true")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).CacheEnv("PATH")
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	cacheRoot := filepath.Join(t.TempDir(), "cache")
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	runDir := filepath.Join(t.TempDir(), "run")
	if _, err := engine.Run(context.Background(), build(t), engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: sink.Nop(), Storage: store, RunID: "r1",
	}); err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	rec, err := cache.ReadRecord(filepath.Join(runDir, "cache"), "compile")
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	// EnvComponent's grammar is length-framed ("N:PATH M:digest8\n", see its
	// own doc), not "PATH=digest8", so this checks for the name itself
	// rather than a literal "=" join.
	if !strings.Contains(rec.Key.Env, "PATH") {
		t.Fatalf("the key's env component does not name PATH even though CacheEnv(\"PATH\") was declared: %q",
			rec.Key.Env)
	}
}
