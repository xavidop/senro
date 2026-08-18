package source_test

// senro.Run falls back to storage.DefaultRoot() (the real user cache dir)
// when a caller passes no WithCacheDir, and runRealAttachedPipeline calls
// it that way. That call site was only incidentally safe:
// isolateAttachRegistry sets HOME for an unrelated reason. TestMain below
// makes the isolation deliberate. See the root package's
// cachedir_isolation_test.go.

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/storage"
)

var realCacheRoot string

// TestMain isolates every test in this binary (white-box and black-box
// files share one compiled test binary) from the real cache directory.
// Runs once per binary; see the root package's TestMain.
func TestMain(m *testing.M) {
	if root, err := storage.DefaultRoot(); err == nil {
		realCacheRoot = root
	} else {
		fmt.Fprintf(os.Stderr, "source_test: TestMain: storage.DefaultRoot: %v\n", err)
	}

	scratch, err := os.MkdirTemp("", "senro-source-test-cache")
	if err != nil {
		fmt.Fprintf(os.Stderr, "source_test: TestMain: MkdirTemp: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("SENRO_CACHE_DIR", scratch); err != nil {
		fmt.Fprintf(os.Stderr, "source_test: TestMain: Setenv: %v\n", err)
		os.Exit(1)
	}

	// The shared cache's variables too: senro.Run reads SENRO_REMOTE_CACHE
	// when no WithRemoteCache is passed, and a developer exporting it would
	// have this suite writing into their team's real bucket. See
	// internal/remotecache.ClearEnv.
	remotecache.ClearEnv()

	code := m.Run()
	_ = os.RemoveAll(scratch)
	os.Exit(code)
}

// TestSuiteNeverTouchesTheRealCacheDir calls senro.Run with no WithCacheDir
// and no isolateAttachRegistry, deliberately unlike runRealAttachedPipeline:
// it proves TestMain's isolation on its own, independent of the attach
// registry's unrelated HOME override.
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
