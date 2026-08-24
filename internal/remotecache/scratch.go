package remotecache

import (
	"context"
	"errors"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/s3"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/workspace"
)

// maxScratchEntry bounds one entry document. An entry holds a digest and
// nothing else, so this is orders of magnitude more than it can need; a
// store answering with more than this is answering with something else.
const maxScratchEntry = 4 << 10

// maxScratchScan bounds how much of a prefix a fallback reads. A scratch key
// churns on every lock-file edit, so a long-lived prefix accumulates entries
// without limit, and the fallback only ever wants the newest one. Ten pages
// is ten thousand entries: far past any real project, and a ceiling on what
// one cache miss can cost in round trips.
const maxScratchScan = 10

// scratchDocs is the remote half of a scratch tier: entry documents in the
// bucket, one per key.
//
//	<prefix>scratch/<namespace>/<percent-encoded key>
//
// The namespace is what keeps one project's prefix fallback out of
// another's entries. A scratch key carries no repository in it (see
// scratch.ExpandKey, which renders from lock-file content alone), so two
// repositories that both declare RestoreKeys("gomod-") against one bucket
// would otherwise restore each other's module caches. Deriving it from the
// pipeline's own name costs no configuration and is wrong only if two
// projects share a name AND a bucket prefix, which the URL's own
// "s3://bucket/<prefix>" fixes.
//
// S3 only, and the type exists only when the backend is a bucket: the
// fallback is a prefix listing, and a registry cannot list by prefix.
type scratchDocs struct {
	client *s3.Client
	prefix string
}

func (d *scratchDocs) entryName(key string) string {
	// Percent-encoded into one path segment, exactly as the local directory
	// backend names its files: a scratch key is user-authored and routinely
	// holds "/" and spaces.
	return d.prefix + url.PathEscape(key)
}

// prefixName is entryName for a RestoreKeys prefix, and it must agree with
// it byte for byte or a fallback would list a set the exact lookup could
// never be in. Safe because PathEscape maps each byte independently, so
// escaping a prefix yields a prefix of the escaped key.
func (d *scratchDocs) prefixName(p string) string { return d.prefix + url.PathEscape(p) }

func (d *scratchDocs) get(ctx context.Context, key string) (cas.Digest, bool, error) {
	body, err := d.client.Get(ctx, d.entryName(key))
	if err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	defer func() { _ = body.Close() }()
	b, err := io.ReadAll(io.LimitReader(body, maxScratchEntry))
	if err != nil {
		return "", false, err
	}
	dg := cas.Digest(strings.TrimSpace(string(b)))
	if !dg.Valid() {
		// A malformed entry is a miss, not an error: this is the
		// best-effort cache, and refusing the run over a bad pointer file
		// would be the one failure mode it promises never to have.
		return "", false, nil
	}
	return dg, true, nil
}

// newestUnder is the remote prefix fallback, and the reason this tier needs
// a listing at all.
//
// Ordered by the STORE's LastModified rather than a local mtime. The
// on-disk backend picks the newest match by its entry file's own mtime,
// which is an ordering only one machine can compute; every client of one
// bucket reads the same LastModified, so a fleet agrees on which entry is
// newest instead of each machine believing whatever its own clock last
// wrote.
func (d *scratchDocs) newestUnder(ctx context.Context, prefix string) (string, cas.Digest, bool, error) {
	want := d.prefixName(prefix)
	var best s3.Object
	for token, page := "", 0; page < maxScratchScan; page++ {
		p, err := d.client.List(ctx, want, token)
		if err != nil {
			return "", "", false, err
		}
		for _, o := range p.Objects {
			if best.Key == "" || o.LastModified.After(best.LastModified) {
				best = o
			}
		}
		if p.Token == "" {
			break
		}
		token = p.Token
	}
	if best.Key == "" {
		return "", "", false, nil
	}
	key, err := url.PathUnescape(strings.TrimPrefix(best.Key, d.prefix))
	if err != nil {
		return "", "", false, nil
	}
	dg, ok, err := d.get(ctx, key)
	if err != nil || !ok {
		return "", "", false, err
	}
	return key, dg, true, nil
}

func (d *scratchDocs) put(ctx context.Context, key string, dg cas.Digest) error {
	name := d.entryName(key)
	// Immutable, like every other scratch entry: ask before writing, and
	// treat a key that already exists as done. Two machines racing both see
	// no entry and both write, which is harmless in a way overwriting a
	// DIFFERENT tree would not be, because the loser's bytes are identical
	// only if the key means what it says. See scratch.Dir.Save.
	switch _, ok, err := d.client.Head(ctx, name); {
	case err != nil:
		return err
	case ok:
		return nil
	}
	return d.client.PutBytes(ctx, name, []byte(dg))
}

// ScratchEntry is one stored entry as the bucket holds it, for a person
// looking rather than for a run: the key as it was declared, and the two
// facts that explain why a fallback picked it.
type ScratchEntry struct {
	Namespace string
	Key       string
	Size      int64
	Stored    time.Time
}

// ListScratch reports the scratch entries in the bucket, newest first.
//
// namespace is a pipeline's name, or "" for every pipeline sharing this
// store; keyPrefix narrows to keys starting with it, the same way
// RestoreKeys does. For `senro cache scratch`, which is the one way to see
// what a shared cache actually holds without a run.
//
// Reports (nil, nil) when this remote does not share scratch caches, so a
// caller can tell "nothing stored" from "not configured" by asking
// SharesScratch.
func (r *Remote) ListScratch(
	ctx context.Context, namespace, keyPrefix string, limit int,
) ([]ScratchEntry, error) {
	if r.scratch == nil {
		return nil, nil
	}
	want := r.scratch.prefix
	if namespace != "" {
		want += url.PathEscape(namespace) + "/" + url.PathEscape(keyPrefix)
	}
	var out []ScratchEntry
	for token, page := "", 0; page < maxScratchScan; page++ {
		p, err := r.scratch.client.List(ctx, want, token)
		if err != nil {
			return nil, err
		}
		for _, o := range p.Objects {
			ns, key, ok := r.scratch.split(o.Key)
			if !ok || (namespace == "" && !strings.HasPrefix(key, keyPrefix)) {
				continue
			}
			out = append(out, ScratchEntry{Namespace: ns, Key: key, Size: o.Size, Stored: o.LastModified})
		}
		if p.Token == "" {
			break
		}
		token = p.Token
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stored.After(out[j].Stored) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// split turns a stored object name back into the namespace and key it was
// written from, and reports false for anything that is not one of ours.
func (d *scratchDocs) split(name string) (namespace, key string, ok bool) {
	rest, found := strings.CutPrefix(name, d.prefix)
	if !found {
		return "", "", false
	}
	ns, k, found := strings.Cut(rest, "/")
	if !found {
		return "", "", false
	}
	nsDec, err := url.PathUnescape(ns)
	if err != nil {
		return "", "", false
	}
	keyDec, err := url.PathUnescape(k)
	if err != nil {
		return "", "", false
	}
	return nsDec, keyDec, true
}

// TieredScratch is the local scratch cache with a bucket behind it.
//
// Every remote failure is a degradation and never an error: this is the
// best-effort cache, whose whole contract is that a miss costs time and
// nothing else, so an unreachable bucket must leave a run behaving exactly
// as it would with no remote configured.
type TieredScratch struct {
	local    *scratch.Dir
	remote   *scratchDocs
	snap     *workspace.Snapshotter
	deg      *degrader
	readOnly bool
}

var _ scratch.Cache = (*TieredScratch)(nil)

// Restore answers from disk when it can, and from the bucket when it
// cannot, writing what it finds into the local backend so the next run on
// this machine needs no network.
func (t *TieredScratch) Restore(
	ctx context.Context, key string, restoreKeys []string, dest string,
) (scratch.Match, bool, error) {
	m, ok, err := t.local.Restore(ctx, key, restoreKeys, dest)
	if err != nil || ok {
		return m, ok, err
	}
	if !t.deg.live() {
		return scratch.Match{}, false, nil
	}

	// Exact key first, then each declared prefix in order: the same
	// precedence the local backend applies, so turning the remote on cannot
	// change WHICH entry a hit means, only whether there is one.
	if dg, found, rerr := t.remote.get(ctx, key); rerr != nil {
		t.deg.classify("get", rerr)
		return scratch.Match{}, false, nil
	} else if found {
		return t.fill(ctx, scratch.Match{Key: key, Digest: dg, Exact: true}, dest)
	}
	for _, p := range restoreKeys {
		hit, dg, found, rerr := t.remote.newestUnder(ctx, p)
		if rerr != nil {
			t.deg.classify("list", rerr)
			return scratch.Match{}, false, nil
		}
		if found {
			return t.fill(ctx, scratch.Match{Key: hit, Digest: dg}, dest)
		}
	}
	return scratch.Match{}, false, nil
}

// fill materializes a remote entry into dest and records it locally.
//
// Adopt is best effort on purpose: the caller already has the bytes, and
// failing the restore because the local pointer could not be written would
// turn a hit into a miss for no gain.
func (t *TieredScratch) fill(
	ctx context.Context, m scratch.Match, dest string,
) (scratch.Match, bool, error) {
	if err := t.snap.Restore(ctx, m.Digest, dest); err != nil {
		// Includes the entry outliving its content, which a sweep on the
		// far side makes ordinary: a miss, not an error.
		t.deg.classify("get", err)
		return scratch.Match{}, false, nil
	}
	if _, err := t.local.Adopt(m.Key, m.Digest); err != nil {
		t.deg.classify("get", err)
	}
	return m, true, nil
}

// Save stores locally, then publishes the pointer. The local result is what
// the caller gets: a bucket that refuses the entry must not make a run
// believe it cached nothing.
func (t *TieredScratch) Save(ctx context.Context, key, src string) (bool, error) {
	saved, err := t.local.Save(ctx, key, src)
	if err != nil {
		return saved, err
	}
	if t.readOnly || !t.deg.live() {
		return saved, nil
	}
	// Published even when this run did not store it: losing the local race
	// means another run on THIS machine wrote the same key, and the bucket
	// may still not have it. The content itself is already remote, because
	// a shared scratch cache snapshots through the tiered object store.
	dg, ok, lerr := t.local.Lookup(key)
	if lerr != nil || !ok {
		return saved, nil
	}
	if perr := t.remote.put(ctx, key, dg); perr != nil {
		t.deg.classify("put", perr)
	}
	return saved, nil
}

// TierScratch returns the scratch cache the engine should use: local first,
// the bucket behind it. It returns local unchanged when sharing is off, when
// the backend is a registry, or when namespace is empty, so a caller needs
// no branch of its own.
//
// An empty namespace is the senro.RunPlan case, which has no pipeline to
// take a name from. Local-only rather than unnamespaced: entries written to
// a shared prefix nothing distinguishes are exactly the collision the
// namespace exists to prevent, and they would be immutable once written.
func (r *Remote) TierScratch(
	local *scratch.Dir, snap *workspace.Snapshotter, namespace string,
) scratch.Cache {
	if r.scratch == nil || namespace == "" {
		return local
	}
	ns := &scratchDocs{
		client: r.scratch.client,
		prefix: r.scratch.prefix + url.PathEscape(namespace) + "/",
	}
	return &TieredScratch{
		local: local, remote: ns, snap: snap, deg: r.deg, readOnly: r.scratchReadOnly,
	}
}
