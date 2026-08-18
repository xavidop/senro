package storage_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/storage"
)

func TestOpenCreatesTheStoreLayout(t *testing.T) {
	root := filepath.Join(t.TempDir(), "senro-cache")
	s, err := storage.Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.CAS == nil {
		t.Fatal("Open returned a Storage with no CAS")
	}
	if s.Snapshotter == nil {
		t.Error("Open returned a Storage with no Snapshotter, so the engine has nothing to snapshot with")
	}
	if s.Action == nil {
		t.Error("Open returned a Storage with no action cache")
	}
	if s.Scratch == nil {
		t.Error("Open returned a Storage with no scratch cache")
	}
	for _, sub := range []string{"cas", "action", "scratch", "pins"} {
		if fi, err := os.Stat(filepath.Join(root, sub)); err != nil || !fi.IsDir() {
			t.Errorf("Open did not create %s: %v", sub, err)
		}
	}
}

func TestOpenIsIdempotentOverAnExistingRoot(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 2; i++ {
		s, err := storage.Open(root)
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		_ = s.Close()
	}
}

func TestDefaultRootPrefersTheEnvironmentOverride(t *testing.T) {
	want := t.TempDir()
	t.Setenv("SENRO_CACHE_DIR", want)
	got, err := storage.DefaultRoot()
	if err != nil {
		t.Fatalf("DefaultRoot: %v", err)
	}
	if got != want {
		t.Errorf("DefaultRoot = %q, want %q", got, want)
	}
}

func TestDefaultRootFallsBackToTheUserCacheDir(t *testing.T) {
	t.Setenv("SENRO_CACHE_DIR", "")
	got, err := storage.DefaultRoot()
	if err != nil {
		t.Fatalf("DefaultRoot: %v", err)
	}
	base, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no user cache dir on this host: %v", err)
	}
	if want := filepath.Join(base, "senro"); got != want {
		t.Errorf("DefaultRoot = %q, want %q", got, want)
	}
}

// TestObjectsAndActionCacheAreTheLocalOnesWhenThereIsNoRemote. Every call
// site in the engine goes through these two rather than through CAS and
// Action directly, so a run with no remote configured has to find exactly the
// local stores behind them.
func TestObjectsAndActionCacheAreTheLocalOnesWhenThereIsNoRemote(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.Objects != cas.Store(s.CAS) {
		t.Error("Objects is not the local CAS for a run with no remote, so an unconfigured " +
			"run has picked up a tier it never asked for")
	}
	if s.ActionCache != cache.ActionCache(s.Action) {
		t.Error("ActionCache is not the local action cache for a run with no remote")
	}
	if s.Remote != nil {
		t.Error("a run with no remote configured has one")
	}
}

// TestWithRemotePutsTheTierInFrontWithoutMovingTheLocalStores. The GC and the
// cache commands work against the local halves and must keep seeing them.
func TestWithRemotePutsTheTierInFrontWithoutMovingTheLocalStores(t *testing.T) {
	pathStyle := true
	r, err := remotecache.Open(remotecache.Config{
		Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "b",
		AccessKeyID: "AKIA", SecretAccessKey: "s",
		PathStyle: &pathStyle, ReportWriter: io.Discard,
	})
	if err != nil {
		t.Fatalf("remotecache.Open: %v", err)
	}

	root := t.TempDir()
	s, err := storage.Open(root, storage.WithRemote(r))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.Remote != r {
		t.Error("Open did not keep the remote it was given")
	}
	if s.Objects == cas.Store(s.CAS) {
		t.Error("Objects is the bare local CAS even though a remote was configured")
	}
	if s.ActionCache == cache.ActionCache(s.Action) {
		t.Error("ActionCache is the bare local action cache even though a remote was configured")
	}
	// The local halves are still the local halves: cache.GC and the `senro
	// cache` commands hold these directly.
	if s.CAS.Root() != filepath.Join(root, "cas") {
		t.Errorf("the local CAS moved to %s", s.CAS.Root())
	}
	if s.Action.Root() != filepath.Join(root, "action") {
		t.Errorf("the local action cache moved to %s", s.Action.Root())
	}
}
