package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/xavidop/senro/internal/cas"
)

// Snapshot is one workspace captured at one moment.
type Snapshot struct {
	// Digest is the workspace's identity: the CAS address of the normalized,
	// uncompressed tar. This is the value that enters a cache key, travels in
	// ws.snapshot, and is the only argument Restore needs.
	Digest cas.Digest `json:"digest"`
	// Index is the CAS address of the canonical file index. Separate from the
	// body so ws ls can list a snapshot without downloading it.
	Index cas.Digest `json:"index"`
	// Bytes is the total size of the regular files, uncompressed.
	Bytes int64 `json:"bytes"`
	// Files is the number of entries, directories and symlinks included.
	Files int `json:"files"`
}

// Snapshotter turns directories into snapshots and back.
type Snapshotter struct{ store cas.Store }

// NewSnapshotter returns a Snapshotter backed by store.
func NewSnapshotter(store cas.Store) *Snapshotter { return &Snapshotter{store: store} }

// Snapshot captures root and stores both the body and the index.
//
// The tar is streamed into the store through a pipe rather than buffered or
// spooled to a temp file: a workspace is exactly the thing that can be
// gigabytes, and a workspace above roughly 2 GiB is meant to warn rather
// than be refused outright.
func (s *Snapshotter) Snapshot(ctx context.Context, root string, ex *Excluder) (Snapshot, error) {
	if fi, err := os.Stat(root); err != nil {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: %w", root, err)
	} else if !fi.IsDir() {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: not a directory", root)
	}

	pr, pw := io.Pipe()
	type written struct {
		ix  Index
		err error
	}
	done := make(chan written, 1)
	go func() {
		ix, err := WriteTar(pw, root, ex)
		// CloseWithError(nil) is Close, so the reader sees EOF only once
		// WriteTar has finished, and sees the writer's error otherwise.
		_ = pw.CloseWithError(err)
		done <- written{ix: ix, err: err}
	}()

	d, putErr := s.store.Put(ctx, pr)
	// Unblocks the writer if Put gave up early. On the ordinary path the
	// pipe is already at EOF and this is a no-op.
	_ = pr.Close()
	w := <-done

	if w.err != nil {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: %w", root, w.err)
	}
	if putErr != nil {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: %w", root, putErr)
	}

	b, err := w.ix.Marshal()
	if err != nil {
		return Snapshot{}, err
	}
	id, err := cas.PutBytes(ctx, s.store, b)
	if err != nil {
		return Snapshot{}, fmt.Errorf("workspace: store index for %s: %w", root, err)
	}
	return Snapshot{Digest: d, Index: id, Bytes: w.ix.Bytes(), Files: len(w.ix.Entries)}, nil
}

// Restore materializes d into dest, REPLACING whatever is there.
//
// Replacement, not merge: a file left over from a previous step would make
// dest hash to something other than d, and every later cache key would be
// wrong.
//
// dest is only touched after the whole object has been read, extracted into
// a staging directory beside it, and verified. Get returning without error
// does not mean the body is not corrupt (cas.Dir's zstd reader validates
// lazily), so extracting in place after a RemoveAll would leave a directory
// that is neither the old content nor the new snapshot. Staging covers
// every failure mode alike: missing object, corrupt header or tail,
// truncated tar, or an entry ReadTar refuses (ErrUnsafePath).
func (s *Snapshotter) Restore(ctx context.Context, d cas.Digest, dest string) error {
	_, err := s.RestoreTree(ctx, d, dest)
	return err
}

// RestoreTree is Restore plus the index of what it put on disk, with every
// guarantee in Restore's doc. It exists because `senro ws pull` must report
// file and byte counts for a workspace restored from a cache hit, and
// cache.Result stores only a body digest; the extraction already walks
// every entry, so answering from it costs nothing and cannot disagree with
// what was written.
func (s *Snapshotter) RestoreTree(ctx context.Context, d cas.Digest, dest string) (Index, error) {
	if !d.Valid() {
		return Index{}, fmt.Errorf("workspace: restore into %s: %q is not a digest", dest, string(d))
	}
	rc, err := s.store.Get(ctx, d)
	if err != nil {
		return Index{}, fmt.Errorf("workspace: restore %s: %w", d.Short(), err)
	}
	defer func() { _ = rc.Close() }()

	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return Index{}, fmt.Errorf("workspace: create %s: %w", parent, err)
	}
	staging, err := os.MkdirTemp(parent, ".senro-restore-*")
	if err != nil {
		return Index{}, fmt.Errorf("workspace: stage restore of %s: %w", d.Short(), err)
	}
	// No-op once the rename below has succeeded, since nothing remains at
	// the staging path by then. On every failure path this is what stops a
	// half-extracted tree leaking in next to dest.
	defer func() { _ = os.RemoveAll(staging) }()

	ix, err := ReadTar(rc, staging)
	if err != nil {
		return Index{}, fmt.Errorf("workspace: restore %s into %s: %w", d.Short(), dest, err)
	}
	// The body is only verified once it has been read to EOF (see
	// cas.Dir.Get), and ReadTar stops at the tar's end-of-archive marker,
	// which can precede the end of the object. Draining is what makes a
	// corrupted tail an error rather than a silently short restore, and it
	// runs before dest is touched, same as every other failure mode above.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return Index{}, fmt.Errorf("workspace: restore %s into %s: %w", d.Short(), dest, err)
	}

	// Everything under staging is now known-good. This is the only point
	// that touches dest, and it is reached only once there is a complete
	// replacement ready to put there.
	if err := os.RemoveAll(dest); err != nil {
		return Index{}, fmt.Errorf("workspace: clear %s: %w", dest, err)
	}
	if err := os.Rename(staging, dest); err != nil {
		return Index{}, fmt.Errorf("workspace: place restore of %s at %s: %w", d.Short(), dest, err)
	}
	return ix, nil
}

// LoadIndex reads a snapshot's file list. The argument is Snapshot.Index,
// not Snapshot.Digest: the two are separate objects on purpose.
func (s *Snapshotter) LoadIndex(ctx context.Context, index cas.Digest) (Index, error) {
	b, err := cas.GetBytes(ctx, s.store, index)
	if err != nil {
		return Index{}, fmt.Errorf("workspace: load index %s: %w", index.Short(), err)
	}
	return UnmarshalIndex(b)
}
