package remotecache_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/storage"
)

// sharedScratch opens two independent local caches pointed at ONE bucket,
// which is what a fleet of CI runners is: each has its own empty disk and
// nothing but the shared store between them.
func sharedScratch(t *testing.T, namespace string) (a, b *storage.Storage) {
	t.Helper()
	m := dockertest.RequireMinIO(t)

	open := func() *storage.Storage {
		r, err := remotecache.Open(remotecache.Config{
			Endpoint:        m.Endpoint,
			Region:          m.Region,
			Bucket:          m.Bucket,
			AccessKeyID:     m.AccessKey,
			SecretAccessKey: m.SecretKey,
			PathStyle:       boolPtr(true),
			Scratch:         true,
		})
		if err != nil {
			t.Fatalf("remotecache.Open: %v", err)
		}
		s, err := storage.Open(t.TempDir(),
			storage.WithRemote(r), storage.WithScratchNamespace(namespace))
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	return open(), open()
}

func boolPtr(b bool) *bool { return &b }

// TestAScratchCacheFilledOnOneMachineRestoresOnAnother is the whole point of
// the feature: two cold runners, one bucket, and the second one does not
// redo the download the first one did.
func TestAScratchCacheFilledOnOneMachineRestoresOnAnother(t *testing.T) {
	t.Parallel()
	a, b := sharedScratch(t, "acme-ci")
	ctx := t.Context()

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mod.txt"), []byte("a module"), 0o644); err != nil {
		t.Fatal(err)
	}
	if saved, err := a.ScratchCache.Save(ctx, "gomod-v1", src); err != nil || !saved {
		t.Fatalf("Save = %v, %v", saved, err)
	}

	dest := t.TempDir()
	m, ok, err := b.ScratchCache.Restore(ctx, "gomod-v1", nil, dest)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !ok {
		t.Fatal("the second machine missed a cache the first one shared")
	}
	if !m.Exact {
		t.Errorf("the exact key came back as a fallback: %+v", m)
	}
	got, err := os.ReadFile(filepath.Join(dest, "mod.txt"))
	if err != nil {
		t.Fatalf("the restored tree has no content: %v", err)
	}
	if string(got) != "a module" {
		t.Errorf("restored %q, want %q", got, "a module")
	}
}

// The prefix fallback is why this needs a listing at all: a lock-file edit
// moves the key, and the run still starts from the last tree instead of from
// nothing.
func TestARestoreKeyPrefixFallsBackAcrossMachines(t *testing.T) {
	t.Parallel()
	a, b := sharedScratch(t, "acme-ci")
	ctx := t.Context()

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mod.txt"), []byte("older"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScratchCache.Save(ctx, "gomod-OLD", src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// The key a lock-file edit produces: nothing has ever been stored under
	// it, so only the prefix can answer.
	dest := t.TempDir()
	m, ok, err := b.ScratchCache.Restore(ctx, "gomod-NEW", []string{"gomod-"}, dest)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !ok {
		t.Fatal("the prefix fallback found nothing across the bucket")
	}
	if m.Exact {
		t.Error("a fallback reported itself as an exact hit")
	}
	if m.Key != "gomod-OLD" {
		t.Errorf("fell back to %q, want gomod-OLD", m.Key)
	}
}

// A different pipeline name is a different namespace, which is what stops
// one project's RestoreKeys("gomod-") from matching another's entries: a
// scratch key renders from lock-file content alone and names no repository.
func TestAnotherPipelineNameDoesNotSeeTheEntries(t *testing.T) {
	t.Parallel()
	a, _ := sharedScratch(t, "acme-ci")
	_, other := sharedScratch(t, "widgets-ci")
	ctx := t.Context()

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mod.txt"), []byte("acme's"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScratchCache.Save(ctx, "gomod-v1", src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	dest := t.TempDir()
	if _, ok, err := other.ScratchCache.Restore(ctx, "gomod-v1", []string{"gomod-"}, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	} else if ok {
		t.Fatal("a different pipeline restored this one's scratch cache, so the namespace does not hold")
	}
}

// With no namespace there is nothing to keep one project's entries apart
// from another's, so the tier is not installed at all rather than writing to
// a shared prefix. This is senro.RunPlan, which has no pipeline to name.
func TestWithoutANamespaceScratchStaysLocal(t *testing.T) {
	t.Parallel()
	a, b := sharedScratch(t, "")
	ctx := t.Context()

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mod.txt"), []byte("local only"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScratchCache.Save(ctx, "gomod-v1", src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	dest := t.TempDir()
	if _, ok, err := b.ScratchCache.Restore(ctx, "gomod-v1", nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	} else if ok {
		t.Fatal("an unnamespaced entry crossed machines, so it was published to a shared prefix")
	}
}

// Turning sharing OFF must leave the previous behaviour exactly as it was: a
// scratch cache stays on the machine that filled it.
func TestWithoutOptingInScratchDoesNotTravel(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	open := func() *storage.Storage {
		r, err := remotecache.Open(remotecache.Config{
			Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
			AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
			PathStyle: boolPtr(true),
			// Scratch deliberately left false.
		})
		if err != nil {
			t.Fatalf("remotecache.Open: %v", err)
		}
		s, err := storage.Open(t.TempDir(),
			storage.WithRemote(r), storage.WithScratchNamespace("acme-ci"))
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a, b := open(), open()

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mod.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScratchCache.Save(ctx, "gomod-v1", src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	dest := t.TempDir()
	if _, ok, err := b.ScratchCache.Restore(ctx, "gomod-v1", nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	} else if ok {
		t.Fatal("a scratch cache travelled without SENRO_REMOTE_SCRATCH, changing the default")
	}
}

// TestListScratchSeesWhatWasStored is what `senro cache scratch` renders: a
// scratch cache explains nothing during a run, so without a listing the only
// way to ask "is anything in the bucket" is a bucket browser.
func TestListScratchSeesWhatWasStored(t *testing.T) {
	t.Parallel()
	a, b := sharedScratch(t, "acme-ci")
	ctx := t.Context()

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mod.txt"), []byte("a module"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"gomod-v1", "npm-v1"} {
		if _, err := a.ScratchCache.Save(ctx, k, src); err != nil {
			t.Fatalf("Save %s: %v", k, err)
		}
	}

	// Listed from the OTHER machine, which is the case that matters: this is
	// a question about the bucket, not about the local disk.
	got, err := b.Remote.ListScratch(ctx, "acme-ci", "", 0)
	if err != nil {
		t.Fatalf("ListScratch: %v", err)
	}
	keys := map[string]bool{}
	for _, e := range got {
		keys[e.Key] = true
		if e.Namespace != "acme-ci" {
			t.Errorf("entry %q reported namespace %q", e.Key, e.Namespace)
		}
		if e.Stored.IsZero() {
			t.Errorf("entry %q has no stored time, so it cannot be ordered", e.Key)
		}
	}
	if !keys["gomod-v1"] || !keys["npm-v1"] {
		t.Errorf("listing missed an entry: %+v", got)
	}

	// A key prefix narrows it the same way RestoreKeys does.
	only, err := b.Remote.ListScratch(ctx, "acme-ci", "gomod-", 0)
	if err != nil {
		t.Fatalf("ListScratch: %v", err)
	}
	if len(only) != 1 || only[0].Key != "gomod-v1" {
		t.Errorf("prefix listing = %+v, want just gomod-v1", only)
	}
}
