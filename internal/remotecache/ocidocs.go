package remotecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/oci"
)

// ociDocs keeps each small mutable document in a tag: the one mutable name
// a registry has, so not the obvious bridge but the only one.
//
// A tag is at most 128 characters of [A-Za-z0-9_.-], starting alphanumeric
// or underscore. A cache key digest fits almost verbatim; a step id does
// not come close, so it is hashed:
//
//	senro-v1-action-sha256-<64 hex>   an action-cache entry, named by the
//	                                  KEY DIGEST ITSELF (the colon written
//	                                  as a dash); nothing hashed twice.
//	senro-v1-recent-sha256-<64 hex>   a step's most recent key, named by
//	                                  the SHA-256 OF THE STEP ID, which is
//	                                  arbitrary text a tag cannot hold.
//	senro-v1-log-sha256-<64 hex>      one archived stream, by the SHA-256
//	                                  of run, step, attempt and stream.
//	senro-v1-run-sha256-<64 hex>      a run's ledger, by the SHA-256 of
//	                                  the run id.
//
// The hashing costs LEGIBILITY: `crane ls` shows a wall of hex, and a tag
// cannot be worked backwards into its step (the S3 layout keeps the step
// id in the key, which is why debugging by listing is easier there). The
// hash is NOT a privacy measure: a secret's value never reaches a name or
// a cache key in the first place.
//
// It does not cost correctness: a tag is a name anybody with push access
// can write, so every document records its own name inside the manifest,
// the bytes are hashed against the manifest's digest, and an entry is
// checked a third time against its key by the shared code.
//
// Concurrent writers: last writer wins, correctly. Each entry is a result
// some build genuinely produced for that key; the loser's blob is left
// unreferenced for the registry's garbage collector.
type ociDocs struct {
	client *oci.Client
	config *ociConfigBlob
}

var _ docs = (*ociDocs)(nil)

const (
	// The tag namespaces. Separate from the object namespace
	// ("senro-v1-sha256-...") so that a repository holding both can be told
	// apart at a glance, and so a future layout can live beside this one.
	ociKindEntry  = "action"
	ociKindRecent = "recent"
	ociKindStream = "log"
	ociKindLedger = "run"

	// ociDocumentArtifactType marks these manifests as senro's own, so a
	// registry's tooling shows them as an artifact rather than as an image
	// somebody might try to run.
	ociDocumentArtifactType = "application/vnd.senro.cache.document.v1"
	// ociDocumentMediaType names the layer bytes: JSON for an entry, a bare
	// digest for a pointer. Opaque on purpose: the tag already says which
	// kind it is, and claiming JSON would lie about half of them.
	ociDocumentMediaType = "application/octet-stream"
	// ociNameAnnotation records the document's own name inside the manifest.
	// The tag already is that name; having it in the document too is what lets
	// a manifest served under the wrong tag be caught.
	ociNameAnnotation = "dev.senro.document.name"
)

// ociTag builds one document's tag. Twenty-three characters of namespace plus
// sixty-four of hex, comfortably inside the specification's 128.
func ociTag(kind, hex string) string {
	return "senro-" + layout + "-" + kind + "-sha256-" + hex
}

func (d *ociDocs) entryName(key cas.Digest) string {
	if !key.Valid() {
		return ""
	}
	return ociTag(ociKindEntry, key.Hex())
}

func (d *ociDocs) recentName(step string) string {
	return ociTag(ociKindRecent, hashString(step))
}

// streamName hashes the four things that identify a stream, joined on NUL.
//
// NUL rather than a slash or a dash because a step id may contain anything a
// person can type: joining on a character the id could also contain would make
// (run, "a/1", 1, "stdout") and (run, "a", 1, "1/stdout") the same name, which
// is a collision waiting for somebody to name a step badly.
func (d *ociDocs) streamName(runID, step string, attempt int, stream string) string {
	return ociTag(ociKindStream, hashString(
		runID+"\x00"+step+"\x00"+strconv.Itoa(attempt)+"\x00"+stream))
}

func (d *ociDocs) ledgerName(runID string) string {
	return ociTag(ociKindLedger, hashString(runID))
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// get reads one document back, and refuses everything it cannot prove.
func (d *ociDocs) get(ctx context.Context, name string, max int64) ([]byte, error) {
	if name == "" {
		return nil, fmt.Errorf("remote cache: %w: no document was named", cas.ErrNotFound)
	}
	raw, err := d.client.GetManifest(ctx, name)
	if err != nil {
		return nil, ociMiss(err, name)
	}
	m, err := parseDocumentManifest(raw, name)
	if err != nil {
		return nil, err
	}
	// Checked before the blob is fetched as well as after it is read:
	// refusing on the manifest's declared size costs one comparison
	// instead of a download.
	if m.Layers[0].Size > max {
		return nil, fmt.Errorf("remote cache: %w: what is stored at %s is larger than it can be",
			cas.ErrCorrupt, name)
	}
	body, err := d.client.GetBlob(ctx, m.Layers[0].Digest)
	if err != nil {
		// A manifest whose blob is gone is a miss, not a fault: what a
		// registry's GC leaves behind after a tag was overwritten.
		return nil, ociMiss(err, name)
	}
	defer func() { _ = body.Close() }()
	b, err := readBounded(body, name, max)
	if err != nil {
		return nil, err
	}
	// The bytes are checked against the digest the manifest named. A
	// conformant registry cannot fail this; a proxy or storage backend
	// can, and a verifying cache does not depend on good behaviour.
	if got := cas.Prefix + hashBytes(b); got != m.Layers[0].Digest {
		return nil, fmt.Errorf(
			"remote cache: %w: the document at %s arrived as %s and its manifest names %s",
			cas.ErrCorrupt, name, cas.Digest(got).Short(), cas.Digest(m.Layers[0].Digest).Short())
	}
	return b, nil
}

// put stores one document: its blob, and the manifest tagged with its name.
func (d *ociDocs) put(ctx context.Context, name string, b []byte) error {
	if name == "" {
		return errors.New("remote cache: refusing to store a document with no name")
	}
	blob := cas.Prefix + hashBytes(b)
	// Ask before sending: two machines writing the identical document share
	// one blob, and the second has nothing to upload.
	if _, ok, err := d.client.HasBlob(ctx, blob); err != nil {
		return err
	} else if !ok {
		if err := d.client.PutBlob(ctx, blob, newBytesSeeker(b), int64(len(b))); err != nil {
			return err
		}
	}
	if err := d.config.ensure(ctx, d.client); err != nil {
		return err
	}
	manifest, err := documentManifest(name, blob, int64(len(b)))
	if err != nil {
		return err
	}
	return d.client.PutManifest(ctx, name, oci.MediaTypeImageManifest, manifest)
}

// documentManifest renders the manifest for one document.
func documentManifest(name, blob string, size int64) ([]byte, error) {
	b, err := json.Marshal(ociManifest{
		SchemaVersion: 2,
		MediaType:     oci.MediaTypeImageManifest,
		ArtifactType:  ociDocumentArtifactType,
		Config: ociDescriptor{
			MediaType: ociEmptyMediaType, Digest: ociEmptyDigest, Size: ociEmptySize,
		},
		Layers: []ociDescriptor{{
			MediaType: ociDocumentMediaType, Digest: blob, Size: size,
		}},
		Annotations: map[string]string{ociNameAnnotation: name},
	})
	if err != nil {
		return nil, fmt.Errorf("remote cache: rendering the manifest for %s: %w", name, err)
	}
	return b, nil
}

// parseDocumentManifest reads a manifest back and refuses one that is not
// about the document it was asked for: the tag is a name somebody else
// could have written, so what came back has to say for itself which
// document it is. The same check the object store and the action cache
// make: three independent statements of identity.
func parseDocumentManifest(raw []byte, name string) (*ociManifest, error) {
	var m ociManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("remote cache: %w: the manifest at %s does not parse: %v",
			cas.ErrCorrupt, name, err)
	}
	if len(m.Layers) != 1 {
		return nil, fmt.Errorf(
			"remote cache: %w: the manifest at %s has %d layers, and a senro document has one",
			cas.ErrCorrupt, name, len(m.Layers))
	}
	if got := m.Annotations[ociNameAnnotation]; got != name {
		return nil, fmt.Errorf(
			"remote cache: %w: the manifest tagged %s says it is %q, so it was not served",
			cas.ErrCorrupt, name, got)
	}
	if !cas.Digest(m.Layers[0].Digest).Valid() {
		return nil, fmt.Errorf("remote cache: %w: the manifest at %s names no usable layer",
			cas.ErrCorrupt, name)
	}
	if m.Layers[0].Size < 0 {
		return nil, fmt.Errorf("remote cache: %w: the manifest at %s names a layer of %d bytes",
			cas.ErrCorrupt, name, m.Layers[0].Size)
	}
	return &m, nil
}

// ociMiss turns a registry's "not there" into the cache's own, and leaves
// everything else alone. A refused connection and a rejected token are not
// misses: the registry never answered the question.
func ociMiss(err error, what string) error {
	if errors.Is(err, oci.ErrNotFound) {
		return fmt.Errorf("remote cache: %w: %s", cas.ErrNotFound, what)
	}
	return err
}

// ociConfigBlob is the empty configuration blob every senro manifest
// references, pushed at most once per opened remote: a manifest naming a
// blob the registry does not hold is refused, so this must precede the
// first manifest. Shared between the object and document stores; the lock
// is held across the push so concurrent first writes do not each send it.
type ociConfigBlob struct {
	mu   sync.Mutex
	done bool
}

func (c *ociConfigBlob) ensure(ctx context.Context, client *oci.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return nil
	}
	empty := []byte(ociEmptyBody)
	if err := client.PutBlob(ctx, ociEmptyDigest, newBytesSeeker(empty), int64(len(empty))); err != nil {
		return err
	}
	c.done = true
	return nil
}
