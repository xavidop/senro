package workspace_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// fakeStore is a minimal cas.Store whose Get answers ANY digest with a
// valid, empty tar. cas.Dir refuses a malformed digest itself, so against
// it Restore's own `!d.Valid()` guard is indistinguishable from dead code,
// and a fake answering garbage would fail in ReadTar for an unrelated
// reason. Answering with content ReadTar accepts means the only thing that
// can make Restore fail against this store is the guard itself, held to the
// interface's contract rather than to the one implementation that makes it
// unnecessary.
type fakeStore struct{}

func (fakeStore) Put(_ context.Context, r io.Reader) (cas.Digest, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return cas.FromBytes(b), nil
}

func (fakeStore) Get(_ context.Context, _ cas.Digest) (io.ReadCloser, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.Close(); err != nil {
		panic(err) // never fails for zero entries; a fake is allowed to panic on its own bug
	}
	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

func (fakeStore) Has(_ context.Context, _ cas.Digest) (bool, error) { return true, nil }

// The test that distinguishes Restore's own digest check from cas.Dir's:
// fakeStore.Get never refuses anything, so removing Restore's `!d.Valid()`
// guard leaves the snapshot_test.go variant passing (real cas.Dir rejects
// the digest itself) and only this test failing.
func TestRestoreValidatesTheDigestEvenAgainstAStoreThatDoesNot(t *testing.T) {
	s := workspace.NewSnapshotter(fakeStore{})
	dest := t.TempDir()
	keep := filepath.Join(dest, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Restore(context.Background(), cas.Digest("not-a-digest"), dest); err == nil {
		t.Fatal("Restore accepted a malformed digest from a Store that would have answered it")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("Restore deleted the destination before its own Valid() check ran")
	}
}

// TestSnapshotOfARestoreReproducesTheDigest (snapshot_test.go) covers
// subdirectories, a symlink and an executable file, but not an empty
// directory: every one of its paths has content under it. An empty
// directory is exactly the shape a naive restore drops, since there is no
// file entry to recreate it by side effect the way a regular file's parent
// gets os.MkdirAll'd into existence. This test adds that missing shape.
func TestSnapshotRestoreRoundTripCoversSubdirsSymlinksExecutablesAndEmptyDirectories(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt":       "a\n",
		"sub/b.txt":   "b\n",
		"run.sh":      "#!/bin/sh\necho hi\n",
		"link":        "->a.txt",
		"empty/":      "/",
		"sub/deeper/": "/",
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

	// Structural checks, not just digest equality: a mutation applied
	// identically on both the snapshot and the re-snapshot would pass a
	// digest-only check. These pin the filesystem shape restore produced.
	if fi, err := os.Stat(filepath.Join(dest, "empty")); err != nil || !fi.IsDir() {
		t.Errorf("Restore dropped the empty top-level directory: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(dest, "sub", "deeper")); err != nil || !fi.IsDir() {
		t.Errorf("Restore dropped the empty nested directory: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(dest, "link")); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("Restore did not recreate the symlink: %v", err)
	} else if target, err := os.Readlink(filepath.Join(dest, "link")); err != nil || target != "a.txt" {
		t.Errorf("Restore's symlink target = %q, %v, want a.txt", target, err)
	}
	if fi, err := os.Stat(filepath.Join(dest, "run.sh")); err != nil {
		t.Errorf("Restore did not write run.sh: %v", err)
	} else if fi.Mode().Perm()&0o111 == 0 {
		t.Errorf("Restore lost the executable bit on run.sh: mode %v", fi.Mode())
	}

	again, err := s.Snapshot(ctx, dest, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot of the restore: %v", err)
	}
	if again.Digest != orig.Digest {
		t.Errorf("snapshot(restore(d)) = %s, want %s", again.Digest, orig.Digest)
	}
	if again.Index != orig.Index {
		t.Errorf("snapshot(restore(d)) index = %s, want %s", again.Index, orig.Index)
	}
}

// A workspace with nothing in it is a real state, not an error. Its digest
// must be stable and round-trip; no other test in this package exercises an
// entryless root.
func TestSnapshotOfAnEmptyDirectoryIsStableAndRestorable(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	s, _ := snapshotter(t)

	first, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if first.Files != 0 {
		t.Errorf("Files = %d, want 0 for an empty root", first.Files)
	}
	if first.Bytes != 0 {
		t.Errorf("Bytes = %d, want 0 for an empty root", first.Bytes)
	}
	if !first.Digest.Valid() {
		t.Fatalf("Digest = %q, not a well-formed digest", first.Digest)
	}

	second, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot again: %v", err)
	}
	if second.Digest != first.Digest {
		t.Errorf("two snapshots of the same empty directory disagreed: %s vs %s", first.Digest, second.Digest)
	}

	dest := filepath.Join(t.TempDir(), "restored-empty")
	if err := s.Restore(ctx, first.Digest, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dest, err)
	}
	if len(entries) != 0 {
		t.Errorf("restored empty snapshot into %s produced %d entries, want 0", dest, len(entries))
	}

	third, err := s.Snapshot(ctx, dest, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot of the restore: %v", err)
	}
	if third.Digest != first.Digest {
		t.Errorf("snapshot(restore(empty)) = %s, want %s", third.Digest, first.Digest)
	}
}

// TestRestoreFailsLoudlyOnAMissingDigest (snapshot_test.go) uses a
// destination that does not exist yet, so it cannot tell whether a failed
// restore cleared an existing one. This checks the destination survives,
// against a pre-populated directory.
func TestRestoreOfAMissingDigestLeavesAnExistingDestinationAlone(t *testing.T) {
	s, _ := snapshotter(t)
	dest := t.TempDir()
	keep := filepath.Join(dest, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := s.Restore(context.Background(), cas.FromBytes([]byte("never stored")), dest)
	if !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Restore of an unknown digest = %v, want ErrNotFound", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("Restore deleted an existing destination before discovering the object did not exist")
	}
}

// Same property for corruption rather than absence: a corrupt object must
// be detected BEFORE anything already at the destination is thrown away,
// not merely before the function returns.
func TestRestoreOfACorruptedBodyLeavesAnExistingDestinationAlone(t *testing.T) {
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

	dest := t.TempDir()
	keep := filepath.Join(dest, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = s.Restore(ctx, snap.Digest, dest)
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Restore of a corrupted body = %v, want ErrCorrupt", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("Restore deleted an existing destination before discovering the object was corrupt")
	}
}

// cas.Dir's Get only verifies the digest once the reader reaches real EOF,
// and archive/tar stops reading at the tar's own end marker, which can come
// earlier. Without Restore's separate drain, a well-formed tar substituted
// under a different digest would extract successfully: ReadTar cannot
// notice the mismatch, only the drain can. This test copies one snapshot's
// on-disk bytes onto another's path and requires Restore to fail with
// ErrCorrupt rather than materialize the wrong tree.
func TestRestoreDetectsAnObjectSubstitutedForADifferentDigest(t *testing.T) {
	ctx := context.Background()
	s, store := snapshotter(t)

	root1 := tree(t, map[string]string{"a.txt": "content of tree one\n"})
	root2 := tree(t, map[string]string{"b.txt": "different content entirely, tree two\n"})

	snap1, err := s.Snapshot(ctx, root1, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot 1: %v", err)
	}
	snap2, err := s.Snapshot(ctx, root2, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if snap1.Digest == snap2.Digest {
		t.Fatal("the two trees produced the same digest, this test needs them to differ")
	}

	other, err := os.ReadFile(store.Path(snap2.Digest))
	if err != nil {
		t.Fatalf("read stored object for snap2: %v", err)
	}
	if err := os.WriteFile(store.Path(snap1.Digest), other, 0o644); err != nil {
		t.Fatalf("substitute snap2's bytes under snap1's digest: %v", err)
	}

	dest := t.TempDir()
	keep := filepath.Join(dest, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = s.Restore(ctx, snap1.Digest, dest)
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Restore of a substituted object = %v, want ErrCorrupt", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "b.txt")); err == nil {
		t.Error("Restore materialized the substituted tree's content under the wrong digest")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("Restore deleted an existing destination before discovering the object was substituted")
	}
}

// Snapshot streams WriteTar into the store through an io.Pipe in its own
// goroutine, and a WriteTar failure partway through must reach the caller
// as an error, not a goroutine blocked on a pipe nobody reads. This
// exercises that plumbing; the WriteTar-level half lives in
// tar_negative_test.go.
func TestSnapshotPropagatesAWriteTarFailureRatherThanHanging(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not block reads")
	}
	root := tree(t, map[string]string{"secret.txt": "shh\n"})
	full := filepath.Join(root, "secret.txt")
	if err := os.Chmod(full, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(full, 0o644) })

	s, _ := snapshotter(t)
	_, err := s.Snapshot(context.Background(), root, workspace.NewExcluder())
	if err == nil {
		t.Fatal("Snapshot silently produced a digest over a file it could not read")
	}
}
