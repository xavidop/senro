package remotecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

// Entries is the shared action cache: the correctness-critical one, where
// a hit SKIPS THE STEP. Written once over docs: what to believe and what a
// failure means do not depend on the backend. The two layouts are:
//
//	<prefix>action/entries/<aa>/<hex>       the entry, as JSON
//	<prefix>action/recent/<encoded step>    that step's latest key digest
//
//	senro-v1-action-sha256-<hex>            the entry, as JSON
//	senro-v1-recent-sha256-<hex>            that step's latest key digest
//
// The entry document is byte-for-byte what the local backend writes, so
// all three are readable by the same code and diffable by a person.
type Entries struct {
	docs     docs
	readOnly bool
	deg      *degrader
}

var _ cache.ActionCache = (*Entries)(nil)

// EntryKey is the name the entry for a key digest is stored under: a key in a
// bucket, a tag in a repository.
func (e *Entries) EntryKey(k cas.Digest) string { return e.docs.entryName(k) }

// RecentKey is the name a step's most recent key digest is recorded under.
func (e *Entries) RecentKey(step string) string { return e.docs.recentName(step) }

// Lookup returns the stored result for k, if the shared cache holds one.
//
// A miss, an unreadable entry and a store that could not answer are all
// (nil, false, nil): an error return would let a cache problem fail a
// build, which is backwards (the local Lookup rules the same way). The
// degrader is what makes "could not answer" heard.
//
// The entry is checked against the key it was asked for before it is
// believed: the most safety-critical check in the package, since a hit
// SKIPS THE STEP. An entry served under the wrong key produces a build
// that quietly did not do what it was told, on every machine sharing the
// cache.
func (e *Entries) Lookup(ctx context.Context, _ string, k cache.Key) (*cache.Result, bool, error) {
	want := k.Digest()
	entry, ok := e.entryAt(ctx, "lookup", e.EntryKey(want), want)
	if !ok {
		return nil, false, nil
	}
	return &entry.Result, true, nil
}

// Save writes the entry and records it as the step's most recent, in that
// order (matching the local backend): an entry with no pointer is still a
// valid hit, while a pointer with no entry is a dangling read Previous
// tolerates anyway.
//
// Saving a key that already has an entry REPLACES it: the one mutable
// thing this half of the cache does, and the reason docs exists.
// Concurrent saves of one key both succeed and the later write stays; see
// ociDocs for the registry case.
func (e *Entries) Save(ctx context.Context, step string, k cache.Key, r *cache.Result) error {
	if r == nil {
		return fmt.Errorf("remote cache: refusing to save a nil result for step %q", step)
	}
	if e.readOnly {
		return nil
	}
	dg := k.Digest()
	// MarshalIndent, matching the local backend byte for byte, so entries
	// diff cleanly and read straight out of the store.
	b, err := json.MarshalIndent(cache.Entry{Key: k, Result: *r}, "", "  ")
	if err != nil {
		return fmt.Errorf("remote cache: marshal entry for step %q: %w", step, err)
	}
	if err := e.docs.put(ctx, e.EntryKey(dg), b); err != nil {
		return err
	}
	return e.docs.put(ctx, e.RecentKey(step), []byte(dg))
}

// Previous returns the most recent entry saved for a step, which is what
// `senro cache explain` diffs a miss against.
func (e *Entries) Previous(ctx context.Context, step string) (*cache.Entry, bool, error) {
	b, err := e.docs.get(ctx, e.RecentKey(step), maxPointerBytes)
	if err != nil {
		if !errors.Is(err, cas.ErrNotFound) {
			e.deg.classify("previous", err)
		}
		return nil, false, nil
	}
	want := cas.Digest(b)
	if !want.Valid() {
		e.deg.notice("previous", fmt.Errorf(
			"remote cache: %w: the recent pointer for step %q is not a digest", cas.ErrCorrupt, step))
		return nil, false, nil
	}
	entry, ok := e.entryAt(ctx, "previous", e.EntryKey(want), want)
	if !ok {
		// A pointer that outlived its entry is what a sweep leaves behind. Not
		// an error: there simply is no previous entry.
		return nil, false, nil
	}
	return entry, true, nil
}

// Forget does nothing to the shared cache, deliberately. A hit referencing
// missing content is a statement about the machine that noticed, not the
// shared cache; one pruned machine would otherwise delete an entry every
// other machine can still reproduce. The local half does forget, so this
// machine misses cleanly and re-saves. It also lets the cache run on
// credentials with no delete permission, the only policy worth having on a
// registry: pull and push are all senro asks for.
func (e *Entries) Forget(context.Context, cache.Key) error { return nil }

// entryAt reads one entry and refuses to believe it unless it is filed under
// the digest it claims. See Lookup's doc for why this check exists.
func (e *Entries) entryAt(ctx context.Context, op, name string, want cas.Digest) (*cache.Entry, bool) {
	if name == "" {
		return nil, false
	}
	b, err := e.docs.get(ctx, name, maxEntryBytes)
	if err != nil {
		if !errors.Is(err, cas.ErrNotFound) {
			e.deg.classify(op, err)
		}
		return nil, false
	}
	var entry cache.Entry
	if err := json.Unmarshal(b, &entry); err != nil {
		e.deg.notice(op, fmt.Errorf("remote cache: %w: entry %s does not parse: %v",
			cas.ErrCorrupt, want.Short(), err))
		return nil, false
	}
	if got := entry.Key.Digest(); got != want {
		e.deg.notice(op, fmt.Errorf(
			"remote cache: %w: the entry stored at %s holds the key for %s, so it was not served",
			cas.ErrCorrupt, want.Short(), got.Short()))
		return nil, false
	}
	return &entry, true
}
