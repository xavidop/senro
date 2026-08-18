package senro_test

// senro.Run falls back to storage.DefaultRoot() (the real user cache
// directory) without WithCacheDir, and most tests here call Run that way on
// purpose, so an unguarded suite would pollute a real machine's cache.
// TestMain isolates the WHOLE binary, once, so any test present or added
// later inherits the isolation; the two Test functions below are the proof
// that it holds, not just a comment asserting it.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/storage"
)

// realCacheRoot and realRunsDir are computed once, in TestMain, before
// anything overrides SENRO_CACHE_DIR or changes the working directory: what
// storage.DefaultRoot() and the "runs/<id>" fallback would actually resolve
// to on this machine. Left empty when TestMain could not determine one, in
// which case the proof tests skip rather than assert against an empty path.
var (
	realCacheRoot string
	realRunsDir   string
)

// TestMain isolates every test in this binary from the real cache directory
// and working directory, before a single Test function runs. Deliberately
// not a per-test t.Setenv: a fix repeated in every new test is exactly what
// a new test forgets, and nothing can run before TestMain and slip past it.
func TestMain(m *testing.M) {
	if root, err := storage.DefaultRoot(); err == nil {
		realCacheRoot = root
	} else {
		// No user cache dir on this host (rare): isolation below still
		// applies via SENRO_CACHE_DIR, so the suite stays safe; only the
		// proof tests, which need a real path to check, skip.
		fmt.Fprintf(os.Stderr, "senro_test: TestMain: storage.DefaultRoot: %v\n", err)
	}
	if wd, err := os.Getwd(); err == nil {
		realRunsDir = filepath.Join(wd, "runs")
	} else {
		fmt.Fprintf(os.Stderr, "senro_test: TestMain: Getwd: %v\n", err)
	}

	scratch, err := os.MkdirTemp("", "senro-test-cache")
	if err != nil {
		fmt.Fprintf(os.Stderr, "senro_test: TestMain: MkdirTemp: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("SENRO_CACHE_DIR", scratch); err != nil {
		fmt.Fprintf(os.Stderr, "senro_test: TestMain: Setenv: %v\n", err)
		os.Exit(1)
	}

	// And the shared cache's own variables: senro.Run reads
	// SENRO_REMOTE_CACHE without WithRemoteCache, so a developer who exports
	// it would otherwise have this suite writing into their team's real
	// bucket. See internal/remotecache.ClearEnv.
	remotecache.ClearEnv()

	// The shared MinIO container (see RequireMinIO) outlives every
	// individual test, so no t.Cleanup can stop it: the first test to finish
	// would take the server away from the rest. TestMain must, or the
	// container survives the test binary and leaked containers eventually
	// stop the machine starting new ones.
	code := m.Run()
	dockertest.StopSharedMinIO()
	_ = os.RemoveAll(scratch)
	os.Exit(code)
}

// TestSuiteNeverTouchesTheRealCacheDir is the assertion behind TestMain's
// isolation, not just a description of it. It calls senro.Run exactly the
// way every other public-API test in this file does: no WithCacheDir at
// all. A single-line change that made TestMain's os.Setenv a no-op, or
// that made storage.DefaultRoot() ignore SENRO_CACHE_DIR, would leave this
// failing with a nonzero entry-count delta; nothing here would pass by
// coincidence.
func TestSuiteNeverTouchesTheRealCacheDir(t *testing.T) {
	if realCacheRoot == "" {
		t.Skip("no user cache dir on this host")
	}
	before := countEntries(t, realCacheRoot)

	pipe := senro.New("isolation-proof")
	l := pipe.Workflow("main")
	l.Step("noop", exec.Command("true"))
	if err := senro.Run(context.Background(), pipe, senro.WithDir(t.TempDir())); err != nil {
		t.Fatalf("Run: %v", err)
	}

	after := countEntries(t, realCacheRoot)
	if after != before {
		t.Fatalf("real cache dir %s went from %d entries to %d: a Run with no WithCacheDir escaped TestMain's isolation", realCacheRoot, before, after)
	}
}

// TestNoDirOptionLeavesTheRealRepoDirectoryUntouched is the same proof for
// the OTHER default senro.Run falls back to: "runs/<id>" relative to the
// working directory, when neither WithDir nor WithAttach is given (see
// Run's own doc). Every test in this file that exercises that fallback
// isolates its own working directory first with t.Chdir(t.TempDir()); this
// test does the same, then checks that doing so actually kept the real
// repository checkout's own runs/ directory untouched, rather than taking
// "every test already does this" on faith.
func TestNoDirOptionLeavesTheRealRepoDirectoryUntouched(t *testing.T) {
	if realRunsDir == "" {
		t.Skip("could not determine the real working directory")
	}
	before := countEntries(t, realRunsDir)

	t.Chdir(t.TempDir())
	pipe := senro.New("isolation-proof-dir")
	l := pipe.Workflow("main")
	l.Step("noop", exec.Command("true"))
	// Deliberately no WithDir: senro.Run's own documented fallback
	// (runs/<id> under the working directory) is what this proves stays
	// inside the isolated temp cwd above, never the real repo checkout.
	if err := senro.Run(context.Background(), pipe); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat("runs"); err != nil {
		t.Fatalf("Run did not create runs/<id> under the isolated working directory as documented: %v", err)
	}

	after := countEntries(t, realRunsDir)
	if after != before {
		t.Fatalf("the real repo's runs/ directory went from %d entries to %d", before, after)
	}
}

func countEntries(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	return len(entries)
}
