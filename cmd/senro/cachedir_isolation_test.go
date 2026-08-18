package main

// See the root package's cachedir_isolation_test.go for the full story.
// The risk here is one step removed: cmdRun execs a fixture as a REAL
// child whose main.go calls senro.Run with no WithCacheDir, and
// exec.Command inherits this process's environment. Without TestMain the
// child's cache-dir resolution would depend on isolateRegistry's HOME
// override happening to redirect os.UserCacheDir too. TestMain makes the
// isolation explicit for the parent, the build and the fixture alike.

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/storage"
)

var realCacheRoot string

// TestMain isolates every test in this binary from the real cache
// directory. See the root package's TestMain for why this runs once per
// binary rather than per test.
func TestMain(m *testing.M) {
	if root, err := storage.DefaultRoot(); err == nil {
		realCacheRoot = root
	} else {
		fmt.Fprintf(os.Stderr, "main_test: TestMain: storage.DefaultRoot: %v\n", err)
	}

	scratch, err := os.MkdirTemp("", "senro-cmd-test-cache")
	if err != nil {
		fmt.Fprintf(os.Stderr, "main_test: TestMain: MkdirTemp: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("SENRO_CACHE_DIR", scratch); err != nil {
		fmt.Fprintf(os.Stderr, "main_test: TestMain: Setenv: %v\n", err)
		os.Exit(1)
	}

	// And the shared cache's variables, which WithCacheDir does not cover:
	// senro.Run reads SENRO_REMOTE_CACHE when given no WithRemoteCache, so
	// a developer who exports it would have this suite writing into their
	// team's real bucket.
	remotecache.ClearEnv()

	// Through dockertest.RunMain rather than m.Run: `senro logs fetch`'s own
	// end-to-end test needs a live object store, dockertest starts one per
	// test binary, and nothing else in the binary would ever stop it.
	code := dockertest.RunMain(m)
	_ = os.RemoveAll(scratch)
	os.Exit(code)
}

// TestSuiteNeverTouchesTheRealCacheDir runs the same success-fixture flow
// TestCmdRunSuccessFixtureExitsZero does, but to prove the isolation
// rather than the exit code.
func TestSuiteNeverTouchesTheRealCacheDir(t *testing.T) {
	if realCacheRoot == "" {
		t.Skip("no user cache dir on this host")
	}
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	before := countEntries(t, realCacheRoot)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./testdata/fixtures/success", "--ui=none"}, &stdout, &stderr, false)
	if code != exitSuccess {
		t.Fatalf("cmdRun success fixture exit = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
	}

	after := countEntries(t, realCacheRoot)
	if after != before {
		t.Fatalf("real cache dir %s went from %d entries to %d: the fixture subprocess escaped TestMain's isolation", realCacheRoot, before, after)
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
