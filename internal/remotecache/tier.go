package remotecache

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

// RemoteObjects is the remote half of an object tier, with two
// implementations: the bucket (Objects) and the registry (OCIObjects).
// What a remote failure means for a run is identical for both, so it is
// written once below.
//
// UploadEncoded is on it rather than left to cas.Store because it is why
// the tier is fast: both remotes take the locally encoded file as it is
// instead of re-encoding a multi-gigabyte workspace.
type RemoteObjects interface {
	cas.Store
	UploadEncoded(ctx context.Context, d cas.Digest, path string) error
	// Name is where the object is kept: a key in a bucket, a tag in a
	// repository. It is what an error message quotes and what a test plants
	// bytes at, and it is "" for a digest that is not well-formed.
	Name(d cas.Digest) string
}

// TieredObjects is the local content-addressed store with a remote one
// behind it: the type that enforces the rule the feature rests on. Every
// method treats a remote failure as "there is no remote cache", records it
// through the degrader, and returns what the local store alone would have.
type TieredObjects struct {
	local  *cas.Dir
	remote RemoteObjects
	deg    *degrader
}

var _ cas.Store = (*TieredObjects)(nil)

// Local is the store on this machine's disk.
func (t *TieredObjects) Local() *cas.Dir { return t.local }

// Put stores the object locally and then uploads it. Local first, and the
// local result is what the caller gets; a failed upload is a degradation,
// never an error, so a step's output is never lost to an unreachable
// cache.
func (t *TieredObjects) Put(ctx context.Context, r io.Reader) (cas.Digest, error) {
	d, err := t.local.Put(ctx, r)
	if err != nil {
		return "", err
	}
	t.upload(ctx, d)
	return d, nil
}

// upload copies a locally stored object into the bucket, best effort.
func (t *TieredObjects) upload(ctx context.Context, d cas.Digest) {
	if !t.deg.live() {
		return
	}
	path := t.local.Path(d)
	if path == "" {
		return
	}
	// Ask before sending: most of what a run stores is already shared, and
	// a HEAD is a few hundred bytes against a tarball's hundreds of
	// megabytes. Two machines racing both get told no and both send: fine.
	switch ok, err := t.remote.Has(ctx, d); {
	case err != nil:
		t.deg.classify("head", err)
		return
	case ok:
		return
	}
	if err := t.remote.UploadEncoded(ctx, d, path); err != nil {
		t.deg.classify("put", err)
	}
}

// Get returns the object, from disk if it is there and from the bucket if
// not. A remote fetch is written through the local store, which is also
// what verifies it: bytes that are not what the digest promised fail
// before anything lands on disk.
func (t *TieredObjects) Get(ctx context.Context, d cas.Digest) (io.ReadCloser, error) {
	rc, err := t.local.Get(ctx, d)
	if err == nil {
		return rc, nil
	}
	if !errors.Is(err, cas.ErrNotFound) {
		// A local store failing for another reason (permissions, a full
		// disk) is a real error; a network fetch would hide it.
		return nil, err
	}
	if !t.deg.live() {
		return nil, err
	}
	if fillErr := t.fill(ctx, d); fillErr != nil {
		// The local miss is the more useful error: it says "not cached",
		// which is what the caller acts on. The remote's failure has
		// already been reported.
		return nil, err
	}
	return t.local.Get(ctx, d)
}

// fill downloads one object into the local store.
func (t *TieredObjects) fill(ctx context.Context, d cas.Digest) error {
	body, err := t.remote.Get(ctx, d)
	if err != nil {
		t.deg.classify("get", err)
		return err
	}
	defer func() { _ = body.Close() }()

	// The local Put re-derives the digest, so a mismatch is caught twice
	// (the verifying reader, then the comparison below). Put removes its
	// temp file on any error, so a failed body leaves nothing behind.
	got, err := t.local.Put(ctx, body)
	if err != nil {
		t.deg.classify("get", err)
		return err
	}
	if got != d {
		// Unreachable while the verifying reader is correct, which is why
		// it is checked: the last line between a poisoned bucket and a
		// poisoned build.
		err := fmt.Errorf("remote cache: %w: %s arrived as %s", cas.ErrCorrupt, d.Short(), got.Short())
		t.deg.notice("get", err)
		_ = t.local.Delete(got)
		return err
	}
	return nil
}

// Has reports whether either tier holds the object. A remote that cannot
// answer reports false, not an error: the engine calls this to decide
// whether a hit can be reproduced, and an error would fail the step. False
// means "run the step", which is always safe.
func (t *TieredObjects) Has(ctx context.Context, d cas.Digest) (bool, error) {
	ok, err := t.local.Has(ctx, d)
	if ok || err != nil {
		return ok, err
	}
	if !t.deg.live() {
		return false, nil
	}
	ok, err = t.remote.Has(ctx, d)
	if err != nil {
		t.deg.classify("head", err)
		return false, nil
	}
	return ok, nil
}

// TieredEntries is the local action cache with the remote one behind it.
type TieredEntries struct {
	local  cache.ActionCache
	remote *Entries
	deg    *degrader
}

var _ cache.ActionCache = (*TieredEntries)(nil)

// Lookup consults this machine first and the bucket second. A remote hit
// is written through to the local cache, best effort: failing to warm the
// local cache is no reason to discard a hit already in hand.
func (t *TieredEntries) Lookup(
	ctx context.Context, step string, k cache.Key,
) (*cache.Result, bool, error) {
	res, ok, err := t.local.Lookup(ctx, step, k)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return res, true, nil
	}
	if !t.deg.live() {
		return nil, false, nil
	}
	res, ok, err = t.remote.Lookup(ctx, step, k)
	if err != nil || !ok {
		// Entries.Lookup already reported anything worth reporting and
		// ruled a lookup failure a miss.
		return nil, false, nil
	}
	_ = t.local.Save(ctx, step, k, res)
	return res, true, nil
}

// Save writes the entry locally and then shares it. The local write must
// succeed; failing to publish is only a degradation, since a build failed
// by an unreachable cache is worse than a build nobody else can reuse.
func (t *TieredEntries) Save(ctx context.Context, step string, k cache.Key, r *cache.Result) error {
	if err := t.local.Save(ctx, step, k, r); err != nil {
		return err
	}
	if !t.deg.live() {
		return nil
	}
	if err := t.remote.Save(ctx, step, k, r); err != nil {
		t.deg.classify("save", err)
	}
	return nil
}

// Previous returns this step's most recent entry, preferring this machine's
// own history: `senro cache explain` is answering "what changed since I last
// built this", and the local answer is the one the person asking means.
func (t *TieredEntries) Previous(ctx context.Context, step string) (*cache.Entry, bool, error) {
	e, ok, err := t.local.Previous(ctx, step)
	if err != nil || ok {
		return e, ok, err
	}
	if !t.deg.live() {
		return nil, false, nil
	}
	e, ok, err = t.remote.Previous(ctx, step)
	if err != nil {
		return nil, false, nil
	}
	return e, ok, nil
}

// Forget removes the entry from this machine only. See Entries.Forget for why
// the shared copy is left alone.
func (t *TieredEntries) Forget(ctx context.Context, k cache.Key) error {
	return t.local.Forget(ctx, k)
}
