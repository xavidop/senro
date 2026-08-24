// Package scratch is senro's best-effort cache: a mutable directory such as
// a module cache, restored by key with prefix fallbacks.
//
// Deliberately not the action cache (internal/cache): conflating the two is
// a common mistake. A wrong hit there is a wrong build; a stale hit here
// only costs time. A miss here is not an error, entries are immutable so a
// concurrent save loses silently, and a scratch cache is NEVER an input to
// an action cache key. If a scratch cache can change a step's output, the
// step is not pure and should not say it is.
package scratch

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// staleClaimAge is how long an empty entry file is trusted to mean "a save
// is in progress" rather than "the creating process was killed". Set well
// beyond how long a snapshot can take, so a live save never loses its
// claim, while a SIGKILLed one recovers in bounded time instead of never;
// nothing else in the system clears an empty placeholder.
const staleClaimAge = 2 * time.Hour

// Match is which entry a restore actually used.
type Match struct {
	Key    string
	Digest cas.Digest
	Exact  bool
}

// Cache is the best-effort cache interface.
type Cache interface {
	Restore(ctx context.Context, key string, restoreKeys []string, dest string) (Match, bool, error)
	Save(ctx context.Context, key string, src string) (bool, error)
}

// Dir is the local-directory scratch cache. Each entry is one small file
// named for its key and holding a workspace digest, so the content itself
// lives in the CAS and is shared with everything else stored there.
type Dir struct {
	root string
	snap *workspace.Snapshotter
}

var _ Cache = (*Dir)(nil)

// Open prepares root.
func Open(root string, snap *workspace.Snapshotter) (*Dir, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("scratch: open %s: %w", root, err)
	}
	return &Dir{root: root, snap: snap}, nil
}

// entryPath is where a key is stored. Keys come from a user-authored
// template and routinely contain "/" and spaces, so they are percent-encoded
// into one path segment rather than trusted as filenames.
func (d *Dir) entryPath(key string) string {
	return filepath.Join(d.root, url.PathEscape(key))
}

// Restore materializes the newest entry matching key, or matching one of
// restoreKeys as a prefix, into dest.
//
// A miss returns (Match{}, false, nil), never an error: a cold cache is the
// ordinary state of a fresh machine. An entry whose content a sweep
// collected is treated the same way.
func (d *Dir) Restore(ctx context.Context, key string, restoreKeys []string, dest string) (Match, bool, error) {
	if dg, ok, err := d.read(key); err != nil {
		return Match{}, false, err
	} else if ok {
		m := Match{Key: key, Digest: dg, Exact: true}
		return d.materialize(ctx, m, dest)
	}
	for _, prefix := range restoreKeys {
		m, ok, err := d.newestUnder(prefix)
		if err != nil {
			return Match{}, false, err
		}
		if !ok {
			continue
		}
		return d.materialize(ctx, m, dest)
	}
	return Match{}, false, nil
}

func (d *Dir) materialize(ctx context.Context, m Match, dest string) (Match, bool, error) {
	if err := d.snap.Restore(ctx, m.Digest, dest); err != nil {
		if errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrCorrupt) {
			// The entry outlived its content: a miss, not an error.
			return Match{}, false, nil
		}
		return Match{}, false, fmt.Errorf("scratch: restore %q: %w", m.Key, err)
	}
	return m, true, nil
}

// newestUnder finds the most recently saved entry whose key starts with
// prefix, by the entry file's mtime (written once, so its creation time).
func (d *Dir) newestUnder(prefix string) (Match, bool, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Match{}, false, nil
		}
		return Match{}, false, fmt.Errorf("scratch: %w", err)
	}
	type candidate struct {
		key  string
		when time.Time
	}
	var found []candidate
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		key, err := url.PathUnescape(e.Name())
		if err != nil || !strings.HasPrefix(key, prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		found = append(found, candidate{key: key, when: info.ModTime()})
	}
	if len(found) == 0 {
		return Match{}, false, nil
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].when.Equal(found[j].when) {
			// Deterministic tiebreak for entries in one timestamp tick.
			return found[i].key > found[j].key
		}
		return found[i].when.After(found[j].when)
	})
	dg, ok, err := d.read(found[0].key)
	if err != nil || !ok {
		return Match{}, false, err
	}
	return Match{Key: found[0].key, Digest: dg}, true, nil
}

func (d *Dir) read(key string) (cas.Digest, bool, error) {
	b, err := os.ReadFile(d.entryPath(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("scratch: %w", err)
	}
	dg := cas.Digest(strings.TrimSpace(string(b)))
	if !dg.Valid() {
		return "", false, nil
	}
	return dg, true, nil
}

// Lookup reports the content this key points at, without materializing it.
//
// For a tier in front of this one (see remotecache.TieredScratch): after a
// local Save it is how the tier learns the digest to publish remotely, and
// it answers from the entry file alone, so it costs no snapshot.
func (d *Dir) Lookup(key string) (cas.Digest, bool, error) { return d.read(key) }

// Adopt records that key holds content ALREADY in the CAS, which is what a
// tier does with a remote hit so the next run on this machine answers from
// disk.
//
// Immutable exactly as Save is: it claims with O_EXCL and a losing caller
// gets (false, nil). Deliberately no snapshot, because there is nothing to
// measure; the digest was verified when the CAS wrote it.
func (d *Dir) Adopt(key string, dg cas.Digest) (bool, error) {
	if !dg.Valid() {
		return false, fmt.Errorf("scratch: adopt %q: %q is not a digest", key, dg)
	}
	p := d.entryPath(key)
	f, err := d.claim(p)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("scratch: adopt %q: %w", key, err)
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(p)
		}
	}()
	if _, err := f.Write([]byte(dg)); err != nil {
		return false, fmt.Errorf("scratch: adopt %q: %w", key, err)
	}
	if err := f.Sync(); err != nil {
		return false, fmt.Errorf("scratch: adopt %q: %w", key, err)
	}
	ok = true
	return true, nil
}

// Digests reports the content address every stored entry points at, for
// internal/cache's GC: an entry whose target a sweep collects can never be
// resaved under the same key, since Save claims with O_EXCL and never
// overwrites a surviving pointer file. An unreadable or malformed entry is
// skipped rather than failing the call: one bad file must not stop a caller
// from protecting every other entry's content.
func (d *Dir) Digests() ([]cas.Digest, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("scratch: %w", err)
	}
	var out []cas.Digest
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d.root, e.Name()))
		if err != nil {
			continue
		}
		dg := cas.Digest(strings.TrimSpace(string(b)))
		if dg.Valid() {
			out = append(out, dg)
		}
	}
	return out, nil
}

// Save snapshots src and stores it under key, unless key already exists,
// reporting whether it stored anything. Losing the race is silent success:
// entries are immutable because mutating one under concurrent runs is how a
// node_modules gets corrupted.
func (d *Dir) Save(ctx context.Context, key, src string) (bool, error) {
	p := d.entryPath(key)
	f, err := d.claim(p)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	// The placeholder is what claims the key; removed on every failure path
	// (claim's doc covers the crash this cannot).
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(p)
		}
	}()

	snap, err := d.snap.Snapshot(ctx, src, workspace.NewExcluder())
	if err != nil {
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	if _, err := f.Write([]byte(snap.Digest)); err != nil {
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	if err := f.Sync(); err != nil {
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	ok = true
	return true, nil
}

// claim creates p as an empty placeholder with O_CREATE|O_EXCL, except that
// a losing attempt gets ONE chance to reclaim a stale claim: an empty entry
// older than staleClaimAge is what a SIGKILLed save leaves behind, and
// reclaiming turns "permanently dead-ended" into "recovers after
// staleClaimAge". An ordinary concurrent Save still loses as before: its
// rival's claim is either non-empty or not yet stale, so this falls through
// to the original fs.ErrExist.
func (d *Dir) claim(p string) (*os.File, error) {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err == nil {
		return f, nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	info, statErr := os.Stat(p)
	if statErr != nil || !staleEmptyEntry(info) {
		return nil, err
	}
	if rmErr := os.Remove(p); rmErr != nil {
		// Lost a second race (another Save reclaiming, or already gone):
		// still "lost the race", via the ORIGINAL ErrExist.
		return nil, err
	}
	return os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
}

// staleEmptyEntry reports whether info describes an empty file too old to
// be an in-flight save's claim. Shared between claim (may this Save reclaim
// it) and InFlight (must GC hold off): an abandoned claim is neither one to
// refuse to reclaim nor one to keep blocking a sweep for.
func staleEmptyEntry(info os.FileInfo) bool {
	return info.Size() == 0 && time.Since(info.ModTime()) >= staleClaimAge
}

// InFlight reports whether any entry is an empty, not-yet-stale
// placeholder: a Save that claimed a key but has not written its digest.
//
// Digests() cannot see such a save, so the CAS object it has Put but not
// yet pointed an entry at has nothing protecting it from a GC sweep in that
// window; GC checks this and holds off entirely (see gc.go). A stale empty
// entry does NOT count: it is abandoned, and treating it as live would let
// one crashed key block every future sweep forever.
func (d *Dir) InFlight() (bool, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("scratch: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Size() == 0 && !staleEmptyEntry(info) {
			return true, nil
		}
	}
	return false, nil
}
