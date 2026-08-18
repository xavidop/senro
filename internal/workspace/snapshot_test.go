package workspace_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

func snapshotter(t *testing.T) (*workspace.Snapshotter, *cas.Dir) {
	t.Helper()
	store, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	return workspace.NewSnapshotter(store), store
}

// The spine again, this time through the CAS, and through two Snapshotters
// over two separate stores so nothing shared can be memoizing the answer.
func TestTwoSnapshottersOverTheSameTreeAgreeOnTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"go.sum":          "h1:abc\n",
		"cmd/app/main.go": "package main\n",
		"empty/":          "/",
	})
	ctx := context.Background()

	a, _ := snapshotter(t)
	b, _ := snapshotter(t)

	sa, err := a.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot a: %v", err)
	}
	sb, err := b.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot b: %v", err)
	}
	if sa.Digest != sb.Digest {
		t.Errorf("two independent snapshotters disagreed: %s vs %s", sa.Digest, sb.Digest)
	}
	if sa.Index != sb.Index {
		t.Errorf("two independent snapshotters produced different indexes: %s vs %s", sa.Index, sb.Index)
	}
	// tree()'s "cmd/app/main.go" implicitly creates the "cmd" and "cmd/app"
	// directories via MkdirAll, in addition to the explicit "empty/". That is
	// three directories (cmd, cmd/app, empty) plus two files (go.sum,
	// cmd/app/main.go): five entries, not four.
	if sa.Files != 5 {
		t.Errorf("Files = %d, want 5 (three dirs: cmd, cmd/app, empty; two files: go.sum, cmd/app/main.go)", sa.Files)
	}
	if sa.Bytes <= 0 {
		t.Errorf("Bytes = %d, want the total size of the regular files", sa.Bytes)
	}
}

func TestTouchingAnMtimeDoesNotChangeTheSnapshotDigest(t *testing.T) {
	root := tree(t, map[string]string{"a.go": "package a\n", "b/c.go": "package c\n"})
	ctx := context.Background()
	s, _ := snapshotter(t)

	before, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	future := time.Now().Add(72 * time.Hour)
	for _, p := range []string{"a.go", "b/c.go", "b"} {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(p)), future, future); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}
	after, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot again: %v", err)
	}
	if before.Digest != after.Digest {
		t.Fatalf("touching mtimes moved the workspace digest: %s then %s", before.Digest, after.Digest)
	}
}

func TestSnapshotOfARestoreReproducesTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt":   "a\n",
		"d/b.txt": "b\n",
		"run.sh":  "#!/bin/sh\n",
		"l":       "->a.txt",
	})
	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	ctx := context.Background()
	s, _ := snapshotter(t)

	orig, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err := s.Restore(ctx, orig.Digest, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	again, err := s.Snapshot(ctx, dest, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot of the restore: %v", err)
	}
	if again.Digest != orig.Digest {
		t.Errorf("snapshot(restore(d)) = %s, want %s", again.Digest, orig.Digest)
	}
}

// TestPreserveSymlinksKeepsANestedNodeModulesDirectorySoATargetSurvives is
// the justification for senro.PreserveSymlinks, proven through the CAS: the
// symlink entry always survives, but without DefaultExcludesFor(true)'s
// wider excludes its target (a nested directory named "node_modules") does
// not, and a restore produces a broken link.
func TestPreserveSymlinksKeepsANestedNodeModulesDirectorySoATargetSurvives(t *testing.T) {
	root := tree(t, map[string]string{
		"pkg/node_modules/left-pad/index.js": "module.exports = leftPad\n",
		"left-pad":                           "->pkg/node_modules/left-pad",
	})
	ctx := context.Background()

	for _, tc := range []struct {
		name           string
		preserve       bool
		wantTargetLive bool
	}{
		{"default excludes strip the nested node_modules directory", false, false},
		{"PreserveSymlinks keeps it", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := snapshotter(t)
			ex := workspace.NewExcluder(workspace.DefaultExcludesFor(tc.preserve)...)
			snap, err := s.Snapshot(ctx, root, ex)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			dest := t.TempDir()
			if err := s.Restore(ctx, snap.Digest, dest); err != nil {
				t.Fatalf("Restore: %v", err)
			}

			if fi, lerr := os.Lstat(filepath.Join(dest, "left-pad")); lerr != nil || fi.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("the symlink entry itself did not survive the round trip: %v", lerr)
			}
			target := filepath.Join(dest, "pkg", "node_modules", "left-pad", "index.js")
			_, statErr := os.Stat(target)
			live := statErr == nil
			if live != tc.wantTargetLive {
				t.Errorf("symlink target present after restore = %v, want %v (stat: %v)", live, tc.wantTargetLive, statErr)
			}
		})
	}
}

// TestSnapshotDigestIsStableWithPreserveSymlinksEitherWay is the digest half
// of the same guarantee, the same property
// TestTwoSnapshottersOverTheSameTreeAgreeOnTheDigest already pins for the
// ordinary excludes: whichever way PreserveSymlinks is set, two independent
// snapshots of the same tree still agree: the option changes WHICH excludes
// apply, not whether snapshotting stays deterministic.
func TestSnapshotDigestIsStableWithPreserveSymlinksEitherWay(t *testing.T) {
	root := tree(t, map[string]string{
		"pkg/node_modules/left-pad/index.js": "module.exports = leftPad\n",
		"left-pad":                           "->pkg/node_modules/left-pad",
	})
	ctx := context.Background()

	for _, preserve := range []bool{false, true} {
		ex := workspace.NewExcluder(workspace.DefaultExcludesFor(preserve)...)
		a, _ := snapshotter(t)
		b, _ := snapshotter(t)
		sa, err := a.Snapshot(ctx, root, ex)
		if err != nil {
			t.Fatalf("Snapshot a (preserve=%v): %v", preserve, err)
		}
		sb, err := b.Snapshot(ctx, root, ex)
		if err != nil {
			t.Fatalf("Snapshot b (preserve=%v): %v", preserve, err)
		}
		if sa.Digest != sb.Digest {
			t.Errorf("preserve=%v: two independent snapshotters disagreed: %s vs %s", preserve, sa.Digest, sb.Digest)
		}
	}
}

// Restore replaces, it does not merge. A leftover file from a previous step
// would make the directory hash to something other than the digest that
// named it, and every key computed from it downstream would be wrong.
func TestRestoreReplacesWhateverWasThere(t *testing.T) {
	root := tree(t, map[string]string{"wanted.txt": "wanted\n"})
	ctx := context.Background()
	s, _ := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if err := os.MkdirAll(filepath.Join(dest, "stale"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "stale", "junk.txt"), []byte("junk\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Restore(ctx, snap.Digest, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "stale")); err == nil {
		t.Error("Restore left a directory from before it behind, so the destination no longer matches its digest")
	}
	if _, err := os.Stat(filepath.Join(dest, "wanted.txt")); err != nil {
		t.Errorf("Restore did not write the snapshot's own content: %v", err)
	}
}

// The negative cases. A restore that fails must say why and must not leave a
// half-populated directory that a later snapshot would happily digest.
func TestRestoreFailsLoudlyOnAMissingDigest(t *testing.T) {
	s, _ := snapshotter(t)
	dest := filepath.Join(t.TempDir(), "dest")
	err := s.Restore(context.Background(), cas.FromBytes([]byte("never stored")), dest)
	if !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Restore of an unknown digest = %v, want ErrNotFound", err)
	}
}

func TestRestoreFailsLoudlyOnACorruptedBody(t *testing.T) {
	root := tree(t, map[string]string{"a.txt": "a\n"})
	ctx := context.Background()
	s, store := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := os.WriteFile(store.Path(snap.Digest), []byte("not a zstd frame"), 0o644); err != nil {
		t.Fatalf("scribble: %v", err)
	}
	err = s.Restore(ctx, snap.Digest, filepath.Join(t.TempDir(), "dest"))
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Restore of a corrupted body = %v, want ErrCorrupt", err)
	}
}

func TestRestoreOfAMalformedDigestIsRefusedBeforeAnythingIsDeleted(t *testing.T) {
	s, _ := snapshotter(t)
	dest := t.TempDir()
	keep := filepath.Join(dest, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Restore(context.Background(), cas.Digest("not-a-digest"), dest); err == nil {
		t.Fatal("Restore accepted a malformed digest")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("Restore deleted the destination before discovering it could not fill it")
	}
}

func TestLoadIndexReadsTheFileListWithoutTheBody(t *testing.T) {
	root := tree(t, map[string]string{"a.txt": "hello", "d/": "/"})
	ctx := context.Background()
	s, _ := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	ix, err := s.LoadIndex(ctx, snap.Index)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	if len(ix.Entries) != 2 {
		t.Fatalf("index has %d entries, want 2", len(ix.Entries))
	}
	for _, e := range ix.Entries {
		if e.Path == "a.txt" && e.Digest != cas.FromBytes([]byte("hello")) {
			t.Errorf("a.txt digest = %s, want the sha256 of its content", e.Digest)
		}
	}
}

// A snapshot of a tree that cannot be walked must not report success with an
// empty digest, which would be a perfectly stable content address for
// "nothing" and would poison every key downstream.
func TestSnapshotOfAMissingRootIsAnError(t *testing.T) {
	s, _ := snapshotter(t)
	_, err := s.Snapshot(context.Background(), filepath.Join(t.TempDir(), "absent"), workspace.NewExcluder())
	if err == nil {
		t.Fatal("Snapshot of a missing directory returned no error")
	}
}

// RestoreTree is Restore plus the index of what it actually put on disk;
// see its doc for why `senro ws pull` needs that for a cache-hit workspace
// with no recorded index anywhere.
func TestRestoreTreeReturnsTheIndexOfWhatItWrote(t *testing.T) {
	root := tree(t, map[string]string{
		"main.go":   "package main\n",
		"sub/a.txt": "aaa",
		"link":      "->main.go",
	})
	ctx := context.Background()
	s, _ := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	ix, err := s.RestoreTree(ctx, snap.Digest, dest)
	if err != nil {
		t.Fatalf("RestoreTree: %v", err)
	}
	if len(ix.Entries) != snap.Files {
		t.Errorf("RestoreTree reported %d entries, the snapshot recorded %d", len(ix.Entries), snap.Files)
	}
	if ix.Bytes() != snap.Bytes {
		t.Errorf("RestoreTree reported %d bytes, the snapshot recorded %d", ix.Bytes(), snap.Bytes)
	}
	if _, err := os.Stat(filepath.Join(dest, "main.go")); err != nil {
		t.Errorf("RestoreTree did not write the snapshot's own content: %v", err)
	}
}
