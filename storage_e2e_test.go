package senro_test

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/eventlog"
)

// e2e runs one pipeline through the public entry point and returns the run's
// events. Every test below shares one cache root across several calls,
// because that is the only shape in which a cache question can be asked.
func e2e(t *testing.T, p *senro.Plan, cacheDir, runID string) (string, []api.Event) {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "run")
	// p is already a resolved *senro.Plan, not a *senro.Pipeline, so this
	// goes through RunPlan rather than Run: see run.go's own doc for why the
	// two entry points exist.
	err := senro.RunPlan(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID(runID), senro.WithCacheDir(cacheDir))
	var runErr *senro.RunError
	if err != nil && !errors.As(err, &runErr) {
		t.Fatalf("senro.RunPlan: %v", err)
	}
	events, readErr := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	if readErr != nil && len(events) == 0 {
		t.Fatalf("read ledger: %v", readErr)
	}
	return runDir, events
}

func count(events []api.Event, ty api.Type) int {
	var n int
	for _, e := range events {
		if e.Type == ty {
			n++
		}
	}
	return n
}

// startedCount is how many step.started events name step: several tests
// need to know whether ONE particular step executed, not whether anything
// did.
func startedCount(events []api.Event, step string) int {
	var n int
	for _, e := range events {
		if e.Type == api.StepStarted && e.Step == step {
			n++
		}
	}
	return n
}

// THE test this whole plan exists for.
//
// tar records mtimes, `go build` touches files, and an unnormalized tar
// produces a different digest on every run, which silently destroys every
// cache key downstream of a workspace. The pipeline below is
// exactly that shape: `generate` rewrites main.go with byte-identical
// content and a fresh mtime on every run, and `compile` is a Pure() step
// downstream of the workspace it wrote into.
//
// If the snapshot digest carried an mtime, the second run would miss, and
// nothing anywhere would report an error. The assertion is the hit.
func TestRewritingAFileWithTheSameContentStillHitsTheCache(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")

	build := func() *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		// The generator writes the same bytes every run and touches the file
		// while doing it, which is what a compiler does to files it did not
		// change.
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("size.txt"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	_, first := e2e(t, build(), cacheDir, "r1")
	if count(first, api.CacheMiss) != 1 || count(first, api.CacheSaved) != 1 {
		t.Fatalf("the first run should miss once and save once: miss=%d saved=%d",
			count(first, api.CacheMiss), count(first, api.CacheSaved))
	}

	secondDir, second := e2e(t, build(), cacheDir, "r2")
	if count(second, api.CacheHit) != 1 {
		var why string
		for _, e := range second {
			if e.Type == api.CacheMiss {
				var b api.CacheMissBody
				_ = e.Decode(&b)
				why = b.Reason + "/" + b.Differing
			}
		}
		t.Fatalf("the second run did not hit (%s).\n"+
			"An unnormalized workspace tar digests differently on every run and silently "+
			"destroys every cache key downstream of a workspace, which is exactly this", why)
	}
	if b, err := os.ReadFile(filepath.Join(secondDir, "ws", "src", "size.txt")); err != nil {
		t.Errorf("the hit did not restore the declared output: %v", err)
	} else if !strings.Contains(string(b), "main.go") {
		t.Errorf("restored output = %q", b)
	}
	// Guards against a cache.hit emitted alongside a real re-execution: a
	// hit is only a hit if "compile" never actually ran the second time.
	if n := startedCount(second, "compile"); n != 0 {
		t.Errorf("compile has %d step.started events on the hit run, want 0 — "+
			"a step whose cache.hit is real never starts", n)
	}
}

// The negative half. Without it the test above passes for a cache that hits
// unconditionally, which is a wrong build rather than a slow one.
func TestChangingTheContentMissesAndNamesTheComponent(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")

	build := func(body string) *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf '"+body+"' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("size.txt"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	_, _ = e2e(t, build("package main\\n"), cacheDir, "r1")
	_, second := e2e(t, build("package main\\n\\nvar x = 1\\n"), cacheDir, "r2")

	if count(second, api.CacheHit) != 0 {
		t.Fatal("a changed source hit the cache, which is a wrong build")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheMiss {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode cache.miss: %v", err)
			}
		}
	}
	if body.Differing == "" {
		t.Error("the miss names no differing component, so `cache explain` has nothing to report")
	}
}

// Secrets must never appear in cache keys, events, or logs. This is where
// that bites hardest: a key is derived from a step's inputs and environment,
// and a cache entry outlives the run directory.
func TestNoEnvironmentValueReachesTheCacheOrTheLedger(t *testing.T) {
	const token = "s3cr3t-canary-value-do-not-store" //nolint:gosec // a test fixture, not a credential
	cacheDir := filepath.Join(t.TempDir(), "cache")

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt")).
		Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Env("BUILD_TOKEN", token).
		Pure().Inputs(artifact.Glob("**/*.go")).CacheEnv("BUILD_TOKEN")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir, _ := e2e(t, p, cacheDir, "r1")

	// The cache root, which persists across runs and is shared by every
	// pipeline on the machine.
	var sawName bool
	err = filepath.WalkDir(cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), token) {
			t.Errorf("the value appears in the cache at %s", path)
		}
		if strings.Contains(string(b), "BUILD_TOKEN") {
			sawName = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk cache: %v", err)
	}
	// The positive half, and it is what makes the negative half mean
	// anything: the walk really does read the file the key lives in, so a
	// value would have been found if one were there.
	if !sawName {
		t.Fatal("the walk never saw BUILD_TOKEN at all, so it proves nothing about the value")
	}

	// The ledger, and every log file.
	ledger, err := os.ReadFile(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if strings.Contains(string(ledger), token) {
		t.Error("the value appears in events.jsonl")
	}
	err = filepath.WalkDir(filepath.Join(runDir, "logs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(b), token) {
			t.Errorf("the value appears in a log file at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk logs: %v", err)
	}
}

// TestNoArgumentValueReachesTheCache is the Env sibling test's counterpart
// for argv, which CommandComponent folds into every cache key with no
// allowlist to filter it: the same two walks, a token planted in an
// ARGUMENT rather than in Env. Unlike its sibling it does NOT check
// events.jsonl; the comment at the end of the body says why.
func TestNoArgumentValueReachesTheCache(t *testing.T) {
	const token = "s3cr3t-argv-canary-do-not-store" //nolint:gosec // a test fixture, not a credential
	cacheDir := filepath.Join(t.TempDir(), "cache")

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	// The token rides as an ORDINARY argument, exactly the shape a step
	// passing a credential on the command line (a flag, a URL) would use,
	// not in Env, and not as argv[0] (the executable), which
	// CommandComponent's own doc treats as shape rather than data.
	l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt", token)).
		Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir, _ := e2e(t, p, cacheDir, "r1")

	var sawKind bool
	err = filepath.WalkDir(cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), token) {
			t.Errorf("the argument value appears in the cache at %s", path)
		}
		// "exec" is CommandComponent's kind literal: proof this walk really
		// does read the file the command component lives in, the same
		// non-vacuity check TestNoEnvironmentValueReachesTheCacheOrTheLedger
		// makes for BUILD_TOKEN.
		if strings.Contains(string(b), "exec") {
			sawKind = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk cache: %v", err)
	}
	if !sawKind {
		t.Fatal("the walk never saw the command component at all, so it proves nothing about the argument")
	}

	err = filepath.WalkDir(filepath.Join(runDir, "cache"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), token) {
			t.Errorf("the argument value appears in the run's own cache record at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk run cache: %v", err)
	}

	// events.jsonl is deliberately NOT checked here: api.StepStartedBody.Cmd
	// is published API that intentionally carries a step's real argv into
	// step.started, the way any CI system's live log shows the command it
	// runs. This test is about the cache specifically.
}

// TestWithLocalClassOverridesTheReportedExecutorClass proves a declared
// class (the shape local.Host() produces) reaches the executor Run
// constructs, via the class step.started reports, the same field
// cache.Key.ExecutorClass is built from. The local executor cannot compute a
// toolchain fingerprint on its own.
func TestWithLocalClassOverridesTheReportedExecutorClass(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("noop", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir := filepath.Join(t.TempDir(), "run")
	const want = "local/darwin/arm64/go1.26"
	if err := senro.RunPlan(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID("r1"),
		senro.WithCacheDir(filepath.Join(t.TempDir(), "cache")),
		senro.WithLocalClass(want),
	); err != nil {
		t.Fatalf("RunPlan: %v", err)
	}

	events, err := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	var found bool
	for _, e := range events {
		if e.Type != api.StepStarted {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode step.started: %v", err)
		}
		found = true
		if b.ExecutorClass != want {
			t.Errorf("ExecutorClass = %q, want %q", b.ExecutorClass, want)
		}
	}
	if !found {
		t.Fatal("no step.started event; this test proves nothing about the class it carried")
	}
}

// The trust boundary, stated as two assertions rather than as a comment.
func TestOnlyAnAllowlistedEnvironmentVariableInvalidates(t *testing.T) {
	build := func(declared, value string) *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		sb := l.Step("compile", exec.Command("sh", "-c", "true")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Env("VAR", value).
			Pure().Inputs(artifact.Glob("**/*.go"))
		if declared != "" {
			sb.CacheEnv(declared)
		}
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	t.Run("allowlisted value change misses", func(t *testing.T) {
		cacheDir := filepath.Join(t.TempDir(), "cache")
		_, _ = e2e(t, build("VAR", "one"), cacheDir, "r1")
		_, second := e2e(t, build("VAR", "two"), cacheDir, "r2")
		if count(second, api.CacheHit) != 0 {
			t.Error("an allowlisted variable changed and the step still hit, so the cache served the wrong build")
		}
	})

	t.Run("undeclared value change does not miss", func(t *testing.T) {
		cacheDir := filepath.Join(t.TempDir(), "cache")
		_, _ = e2e(t, build("", "one"), cacheDir, "r1")
		_, second := e2e(t, build("", "two"), cacheDir, "r2")
		if count(second, api.CacheHit) != 1 {
			t.Error("an undeclared variable changed and the step missed; nothing outside CacheEnv enters a key, " +
				"which is what stops every machine-specific variable making every key unique")
		}
	})
}

// A fully cached run is still a run that says what happened, and the fold is
// what every renderer reads.
func TestAFullyCachedRunFoldsToSucceededWithCachedSteps(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	build := func() *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "true")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}
	_, _ = e2e(t, build(), cacheDir, "r1")
	_, second := e2e(t, build(), cacheDir, "r2")

	st := api.NewRunState()
	for _, e := range second {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", st.Run.Status)
	}
	if st.Steps["compile"].State != api.StateCached || !st.Steps["compile"].Cached {
		t.Errorf("compile = %+v, want cached in both the state and the flag", st.Steps["compile"])
	}
	// The state alone does not prove the step was skipped: a step.finished
	// carrying state cached would fold the same way whether or not a
	// step.started for it preceded it. The absence of that event is the
	// only evidence, anywhere in this test, that "compile" did not actually
	// run the second time.
	if n := startedCount(second, "compile"); n != 0 {
		t.Errorf("compile has %d step.started events on the cached run, want 0", n)
	}

	// The published fixture corpus reads these; a cached run must survive a
	// round trip through JSON like every other event.
	for _, e := range second {
		if _, err := json.Marshal(e); err != nil {
			t.Fatalf("marshal %s: %v", e.Type, err)
		}
	}
}
