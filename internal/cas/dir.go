package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Dir is the local-directory backend: <root>/sha256/<aa>/<bb>/<hex>, with
// <root>/tmp for in-flight writes. Two levels of fanout keep any one
// directory from holding the whole store, which matters on filesystems whose
// directory lookup degrades with entry count.
type Dir struct {
	root string
	tmp  string
}

var _ Store = (*Dir)(nil)

// Open prepares root as a store, creating it if it is not there.
func Open(root string) (*Dir, error) {
	tmp := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("cas: open %s: %w", root, err)
	}
	return &Dir{root: root, tmp: tmp}, nil
}

// Root is the directory this store lives in.
func (s *Dir) Root() string { return s.root }

// Path is where d is stored. It returns "" for a digest that is not
// well-formed, and every caller checks Valid before using the result.
func (s *Dir) Path(d Digest) string {
	if !d.Valid() {
		return ""
	}
	h := d.Hex()
	return filepath.Join(s.root, "sha256", h[0:2], h[2:4], h)
}

// Put stores everything r yields and returns its digest.
//
// The digest is computed over the plaintext while the compressed form is
// written to a temp file, and the temp file is renamed into place only once
// the whole object is on disk. A reader can therefore never observe a
// partial object under its final name, which is the property that lets a
// concurrent run share this store safely.
func (s *Dir) Put(ctx context.Context, r io.Reader) (Digest, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(s.tmp, "put-")
	if err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	// No-ops on the success path; on every failure path they stop a partial
	// object leaking into the temp directory.
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	h := sha256.New()
	enc, err := NewEncoder(tmp)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(io.MultiWriter(enc, h), r); err != nil {
		_ = enc.Close()
		return "", fmt.Errorf("cas: %w", err)
	}
	if err := enc.Close(); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}

	d := Digest(Prefix + hex.EncodeToString(h.Sum(nil)))
	p := s.Path(d)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if _, err := os.Stat(p); err == nil {
		// Already stored: immutable content means the same bytes. Touch it
		// so the GC's clock sees the write as an access.
		_ = s.touch(p)
		return d, nil
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	return d, nil
}

// Get returns d's content. The returned reader verifies as it goes and fails
// with ErrCorrupt at EOF if what it decoded does not hash to d, so a caller
// that reads to completion cannot be handed the wrong bytes. A caller that
// stops early gets no such guarantee, which is why Verify exists separately.
func (s *Dir) Get(ctx context.Context, d Digest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := s.Path(d)
	if p == "" {
		return nil, fmt.Errorf("cas: %w: %q is not a digest", ErrNotFound, string(d))
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("cas: %w: %s", ErrNotFound, d.Short())
		}
		return nil, fmt.Errorf("cas: %w", err)
	}
	_ = s.touch(p)
	// The same decode-and-verify every backend uses: two implementations
	// would be two chances to differ subtly. See DecodeVerify.
	return DecodeVerify(f, d)
}

// Has reports whether d is stored. It stats and does not decode, so it
// cannot detect corruption: Has yes then Get ErrCorrupt is a legitimate
// sequence every caller must survive. Verify is the deep check.
func (s *Dir) Has(_ context.Context, d Digest) (bool, error) {
	p := s.Path(d)
	if p == "" {
		return false, nil
	}
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("cas: %w", err)
}

// Verify reads d in full and checks it. This is what a fsck or a GC uses.
func (s *Dir) Verify(ctx context.Context, d Digest) error {
	rc, err := s.Get(ctx, d)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return err
	}
	return nil
}

// Delete removes an object. Missing is not an error: a GC racing another GC
// must not fail.
func (s *Dir) Delete(d Digest) error {
	p := s.Path(d)
	if p == "" {
		return nil
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cas: %w", err)
	}
	return nil
}

// Object is one stored object as the GC sees it.
type Object struct {
	Digest Digest
	// Bytes is the size on disk, compressed. The GC works against a disk
	// budget, so the on-disk figure is the one that matters to it.
	Bytes int64
	// Accessed is this store's access clock: the file's mtime, not atime.
	// Immutable content leaves mtime free to carry it, while atime is
	// unreliable under relatime and noatime mounts. Put and Get both
	// advance it; see touch.
	Accessed time.Time
}

// Walk calls fn for every stored object. Files under tmp are skipped: they
// are in-flight writes with no address yet.
func (s *Dir) Walk(fn func(Object) error) error {
	base := filepath.Join(s.root, "sha256")
	err := filepath.WalkDir(base, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && p == base {
				return fs.SkipAll
			}
			return err
		}
		if e.IsDir() {
			return nil
		}
		d := Digest(Prefix + e.Name())
		if !d.Valid() {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // deleted underneath us by a concurrent GC
			}
			return err
		}
		return fn(Object{Digest: d, Bytes: info.Size(), Accessed: info.ModTime()})
	})
	if err != nil {
		return fmt.Errorf("cas: walk: %w", err)
	}
	return nil
}

// TmpStaleAge bounds how long a file under <root>/tmp is trusted to be a
// Put still writing rather than a killed process's leftover (the same
// problem scratch.staleClaimAge solves). Generous well beyond any object's
// write time, so a genuinely in-progress Put never has its temp file swept
// out from under it.
const TmpStaleAge = 24 * time.Hour

// SweepTmp removes every file under tmp/ at least TmpStaleAge old,
// returning how many. Walk never looks in tmp/, so nothing else reclaims
// one. A crashed Put's temp file has no digest yet, so nothing can point at
// it: deleting one old enough is always safe, unlike an object under
// sha256/, which needs cache.GC's reference counting.
func (s *Dir) SweepTmp(now time.Time) (int, error) {
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("cas: sweep tmp: %w", err)
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // removed underneath us, by a concurrent sweep
			}
			return removed, fmt.Errorf("cas: sweep tmp: %w", err)
		}
		if now.Sub(info.ModTime()) < TmpStaleAge {
			continue
		}
		if err := os.Remove(filepath.Join(s.tmp, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removed, fmt.Errorf("cas: sweep tmp: %w", err)
		}
		removed++
	}
	return removed, nil
}

// touch advances the access clock. See Object.Accessed.
func (s *Dir) touch(p string) error {
	now := time.Now()
	return os.Chtimes(p, now, now)
}
