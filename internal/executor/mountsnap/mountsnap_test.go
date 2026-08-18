package mountsnap_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/workspace"
)

func snapshotter(t *testing.T) *workspace.Snapshotter {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	return workspace.NewSnapshotter(store)
}

// The mandatory excludes must apply even when a pipeline forgot them, and a
// workspace's .senroignore must apply identically wherever "part of this
// workspace" is decided: a reimplementation would produce a valid but
// different digest, and the only symptom would be a cache that never hits.
func TestSnapshotAppliesTheMandatoryExcludesAndTheIgnoreFile(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("keep.go", "package main\n")
	write(".git/HEAD", "ref: refs/heads/main\n")
	write("node_modules/left-pad/index.js", "module.exports = 1\n")
	write("dist/app.js", "console.log(1)\n")
	write(".senroignore", "dist/\n")

	got, err := mountsnap.Snapshot(context.Background(), snapshotter(t), executor.Mount{
		Name: "src", Path: root, At: "/repo",
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// keep.go and .senroignore: two entries. .git/ and node_modules/ fall to
	// the mandatory defaults, dist/ to the .senroignore. Files counts
	// directories too, so every write above sits directly at root to keep
	// the count obvious.
	if got.Files != 2 {
		t.Fatalf("snapshot has %d entries, want 2 (keep.go and .senroignore)", got.Files)
	}
	if got.Digest == "" || got.Index == "" {
		t.Fatalf("snapshot = %+v, want both digests", got)
	}
}

func TestSnapshotWidensTheDefaultsForAPreserveSymlinksWorkspace(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "node_modules", "ui"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "node_modules", "ui", "index.js"), []byte("1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snap := snapshotter(t)

	plain, err := mountsnap.Snapshot(context.Background(), snap, executor.Mount{Name: "m", Path: root, At: "/m"})
	if err != nil {
		t.Fatal(err)
	}
	widened, err := mountsnap.Snapshot(context.Background(), snap, executor.Mount{
		Name: "m", Path: root, At: "/m", PreserveSymlinks: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plain.Files != 0 {
		t.Errorf("the default excludes let %d node_modules file(s) through", plain.Files)
	}
	// node_modules, node_modules/ui, node_modules/ui/index.js: Files counts
	// directories as well as files.
	if widened.Files != 3 {
		t.Errorf("PreserveSymlinks kept %d entries, want 3", widened.Files)
	}
	if plain.Digest == widened.Digest {
		t.Error("the two snapshots share a digest, so PreserveSymlinks changed nothing")
	}
}

// A SCRATCH cache excludes nothing at all. node_modules is usually the whole
// point of one, and internal/scratch saves and restores the directory whole,
// so a workspace's mandatory defaults applied here would send a remote step a
// cache with its contents missing and then store that hollow tree under a key
// nothing can rewrite.
func TestAScratchMountExcludesNothingAtAll(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{
		"node_modules/left-pad/index.js", ".git/HEAD", ".senroignore", "keep.txt",
	} {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("dist/\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	snap := snapshotter(t)
	asWorkspace, err := mountsnap.Snapshot(context.Background(), snap, executor.Mount{
		Name: "deps", Path: root, At: "/deps",
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	asScratch, err := mountsnap.Snapshot(context.Background(), snap, executor.Mount{
		Name: "deps", Path: root, At: "/deps", Scratch: true,
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// .senroignore, keep.txt, .git, .git/HEAD, node_modules,
	// node_modules/left-pad, node_modules/left-pad/index.js: Files counts
	// directories too.
	if asScratch.Files != 7 {
		t.Errorf("a scratch mount carried %d entries, want all 7: something was excluded", asScratch.Files)
	}
	if asWorkspace.Digest == asScratch.Digest {
		t.Error("a scratch mount and a workspace over the same tree digest the same, so the " +
			"workspace excludes were not lifted")
	}
}

// The nil guard must fail loudly: the zero Snapshot is a valid digest for
// "nothing" and would poison the next step's cache key with a stable, wrong
// value.
func TestSnapshotWithNoSnapshotterIsAnInfrastructureError(t *testing.T) {
	_, err := mountsnap.Snapshot(context.Background(), nil, executor.Mount{Name: "m", Path: t.TempDir(), At: "/m"})
	if err == nil {
		t.Fatal("Snapshot with a nil Snapshotter returned no error")
	}
}
