package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/stepid"
)

// HermeticityTrusted records that Pure() was trusted rather than enforced
// (there is no network sandboxing here). Written now so entries produced
// under real enforcement are distinguishable without a migration.
const HermeticityTrusted = "trusted"

// Miss reasons, as they appear in cache.miss events and in `cache explain`.
const (
	ReasonNoPreviousEntry = "no_previous_entry"
	ReasonKeyChanged      = "key_changed"
	// ReasonEntryIncomplete means an entry was found but a GC collected
	// content it referenced. A miss, not a failure: a cache sweep must not
	// be able to break a build.
	ReasonEntryIncomplete = "entry_incomplete"
)

// LogRef points at one stream of a cached step's output, stored in the CAS
// so a hit can replay what the step would have printed.
type LogRef struct {
	Stream string     `json:"stream"`
	Digest cas.Digest `json:"digest"`
	Bytes  int64      `json:"bytes"`
}

// Result is everything a hit has to reproduce.
type Result struct {
	ExitCode   int               `json:"exit_code"`
	Outputs    []FileDigest      `json:"outputs,omitempty"`
	Workspaces []WorkspaceDigest `json:"workspaces,omitempty"`
	Logs       []LogRef          `json:"logs,omitempty"`
	DurationNS int64             `json:"duration_ns"`
	// RunID is the run that produced this result: the one execution that
	// actually happened.
	RunID       string    `json:"run_id"`
	Hermeticity string    `json:"hermeticity"`
	SavedAt     time.Time `json:"saved_at"`
	// Bytes is the total uncompressed size stored for this entry: what
	// cache.saved reports and the GC budgets against.
	Bytes int64 `json:"bytes"`
}

// Entry is a key and its result, stored together. Storing the key rather
// than only its digest is what makes `cache explain` a diff instead of a
// re-derivation.
//
// Storing the components leaks the SHAPE of a build and never a secret: the
// env component holds digests, not values; the secrets component holds the
// provider:key:version:digest8 identity form; and the command component
// hashes every argument after the executable, never storing one as text
// (see CommandComponent). TestNoArgumentValueReachesTheCache in
// storage_e2e_test.go enforces this.
type Entry struct {
	Key    Key    `json:"key"`
	Result Result `json:"result"`
}

// ActionCache is the correctness-critical cache: a hit skips the step.
//
// The step ID is part of every operation because Previous needs "the most
// recent entry for the same step", which is unfindable in a store indexed
// by key digest alone.
type ActionCache interface {
	Lookup(ctx context.Context, step string, k Key) (*Result, bool, error)
	Save(ctx context.Context, step string, k Key, r *Result) error
	Previous(ctx context.Context, step string) (*Entry, bool, error)
	// Forget removes an entry whose referenced content a GC has collected,
	// so the next run misses cleanly instead of refinding the broken entry.
	Forget(ctx context.Context, k Key) error
}

// Dir is the local-directory action cache: <root>/entries/<aa>/<hex> for the
// entries, <root>/recent/<encoded-step> naming each step's latest key.
type Dir struct {
	root    string
	entries string
	recent  string
}

var _ ActionCache = (*Dir)(nil)

// Open prepares root.
func Open(root string) (*Dir, error) {
	d := &Dir{
		root:    root,
		entries: filepath.Join(root, "entries"),
		recent:  filepath.Join(root, "recent"),
	}
	for _, p := range []string{d.entries, d.recent} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			return nil, fmt.Errorf("cache: open %s: %w", root, err)
		}
	}
	return d, nil
}

// Root is the directory this cache lives in.
func (d *Dir) Root() string { return d.root }

// EntryPath is where the entry for a key digest is stored.
func (d *Dir) EntryPath(k cas.Digest) string {
	if !k.Valid() {
		return ""
	}
	h := k.Hex()
	return filepath.Join(d.entries, h[0:2], h)
}

// Lookup returns the stored result for k, if there is one.
//
// A corrupt or unreadable entry reads as a MISS, not an error: one damaged
// file must not fail every run on a machine. The damaged file is left for a
// GC to reclaim, so Lookup stays a read.
//
// Lookup does not check that referenced objects are still in the CAS: Dir
// deliberately has no CAS handle. That check, and the Forget it justifies,
// is the caller's job (see ReasonEntryIncomplete and Forget).
//
// step is intentionally unused: the key is step-identity-free, so two
// different steps with identical components correctly share one entry.
// Keying by step would be memoization by name, a weaker cache. step stays a
// parameter because Previous is genuinely step-scoped.
func (d *Dir) Lookup(_ context.Context, _ string, k Key) (*Result, bool, error) {
	p := d.EntryPath(k.Digest())
	if p == "" {
		return nil, false, nil
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: %w", err)
	}
	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, false, nil
	}
	now := time.Now()
	// mtime is this store's access clock, as in the CAS: entries are
	// immutable, and atime is unreliable under relatime.
	_ = os.Chtimes(p, now, now)
	return &e.Result, true, nil
}

// Save writes the entry and records it as the step's most recent. The entry
// is filed purely by k.Digest(); step only names the "recent" pointer (see
// Lookup on step-identity-free addressing).
func (d *Dir) Save(_ context.Context, step string, k Key, r *Result) error {
	if r == nil {
		return fmt.Errorf("cache: refusing to save a nil result for step %q", step)
	}
	dg := k.Digest()
	b, err := json.MarshalIndent(Entry{Key: k, Result: *r}, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: marshal entry for step %q: %w", step, err)
	}
	p := d.EntryPath(dg)
	if err := writeAtomic(p, b); err != nil {
		return fmt.Errorf("cache: save entry for step %q: %w", step, err)
	}
	// Recent pointer second: dying between the two writes leaves a valid
	// hit and only costs `cache explain` one generation of history.
	if err := writeAtomic(filepath.Join(d.recent, stepid.Encode(step)), []byte(dg)); err != nil {
		return fmt.Errorf("cache: record most recent entry for step %q: %w", step, err)
	}
	return nil
}

// Previous returns the most recent entry saved for a step.
func (d *Dir) Previous(_ context.Context, step string) (*Entry, bool, error) {
	b, err := os.ReadFile(filepath.Join(d.recent, stepid.Encode(step)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: %w", err)
	}
	eb, err := os.ReadFile(d.EntryPath(cas.Digest(b)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// The pointer outlived the entry (a GC leaves this). Not an
			// error: there is no previous entry.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: %w", err)
	}
	var e Entry
	if err := json.Unmarshal(eb, &e); err != nil {
		return nil, false, nil
	}
	return &e, true, nil
}

// Forget removes an entry, leaving the recent pointer to fall through to
// "no previous entry" on its own.
func (d *Dir) Forget(_ context.Context, k Key) error {
	p := d.EntryPath(k.Digest())
	if p == "" {
		return nil
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache: %w", err)
	}
	return nil
}

// Walk calls fn for every stored entry. Unreadable files are skipped for the
// same reason Lookup treats them as a miss.
func (d *Dir) Walk(fn func(path string, e Entry, accessed time.Time) error) error {
	err := filepath.WalkDir(d.entries, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && p == d.entries {
				return fs.SkipAll
			}
			return err
		}
		if de.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		var e Entry
		if err := json.Unmarshal(b, &e); err != nil {
			return nil
		}
		info, err := de.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		return fn(p, e, info.ModTime())
	})
	if err != nil && !errors.Is(err, fs.SkipAll) {
		return err
	}
	return nil
}

// writeAtomic writes b to p through a temp file and a rename, so a reader
// never sees a partial entry.
func writeAtomic(p string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(b); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}
