package remotecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/oci"
)

// OCIObjects is the content-addressed store held in a registry repository.
//
// A registry addresses a blob by the digest of the bytes it holds; senro
// addresses an object by the digest of its PLAINTEXT while storing it
// compressed (internal/cas), so a blob cannot be found by asking directly.
// The bridge is a tag: each object is a tiny OCI artifact, tagged with the
// plaintext digest, whose single layer is the encoded blob:
//
//	tag  senro-v1-sha256-<hex of the plaintext digest>
//	 └── manifest (application/vnd.oci.image.manifest.v1+json)
//	      └── layer  the object, in the encoding cas.NewEncoder produces
//
// Storing plaintext instead would remove the manifest, and was rejected:
// it would send workspace snapshots uncompressed over the uplink and force
// every upload to decode a multi-gigabyte object. The manifest also keeps
// the blob referenced: a registry's garbage collector deletes loose blobs,
// and a retention policy needs something to act on.
type OCIObjects struct {
	client   *oci.Client
	readOnly bool
	// config pushes the empty configuration blob every manifest references,
	// once, and is shared with the document store because both name the same
	// two bytes.
	config *ociConfigBlob
}

var _ cas.Store = (*OCIObjects)(nil)

const (
	// ociArtifactType marks these manifests as senro's own, so a registry's
	// tooling shows them as an artifact rather than as an image somebody
	// might try to run, and so anything else sharing the repository is
	// distinguishable at a glance.
	ociArtifactType = "application/vnd.senro.cache.object.v1"
	// ociLayerMediaType names what the layer actually is: one senro object in
	// the encoding cas.NewEncoder produces.
	ociLayerMediaType = "application/vnd.senro.cache.object.v1+zstd"
	// ociEmptyMediaType and the three constants below are the OCI empty
	// descriptor: the two-byte document `{}`, which the specification defines
	// for exactly this case, an artifact with no configuration of its own.
	ociEmptyMediaType = "application/vnd.oci.empty.v1+json"
	ociEmptyDigest    = "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
	ociEmptyBody      = "{}"
	ociEmptySize      = 2
	// ociDigestAnnotation records the plaintext digest inside the manifest.
	// The tag already encodes it; having it in the document too means a
	// manifest read back says what it is a manifest OF, which is what lets a
	// mis-filed one be caught before its blob is fetched.
	ociDigestAnnotation = "dev.senro.object.digest"
)

// OCITag is the tag one object is stored under. Returns "" for a malformed
// digest (digests arrive from logs, plans and command lines, untrusted).
// The layout version is in the tag because a tag is the only namespace a
// repository has, and a later layout must live beside this one.
func OCITag(d cas.Digest) string {
	if !d.Valid() {
		return ""
	}
	return "senro-" + layout + "-sha256-" + d.Hex()
}

// Name is the tag this object is stored under, which for a registry is
// OCITag. See RemoteObjects.Name.
func (o *OCIObjects) Name(d cas.Digest) string { return OCITag(d) }

// Get returns the object's plaintext, verified: the returned reader fails
// with cas.ErrCorrupt if what it decoded is not what d promised. A
// registry only verifies that a blob matches its own digest, which says
// nothing about whether the manifest pointing at it names the right one.
func (o *OCIObjects) Get(ctx context.Context, d cas.Digest) (io.ReadCloser, error) {
	tag := OCITag(d)
	if tag == "" {
		return nil, fmt.Errorf("remote cache: %w: %q is not a digest", cas.ErrNotFound, string(d))
	}
	raw, err := o.client.GetManifest(ctx, tag)
	if err != nil {
		return nil, o.miss(err, d)
	}
	m, err := parseObjectManifest(raw, d)
	if err != nil {
		return nil, err
	}
	body, err := o.client.GetBlob(ctx, m.Layers[0].Digest)
	if err != nil {
		// A manifest whose blob is gone is a miss, not a fault: what the
		// registry's GC leaves if it ran between the two requests.
		return nil, o.miss(err, d)
	}
	return cas.DecodeVerify(body, d)
}

// Has reports whether the object is in the registry. One request, on the
// manifest: asking about the blob would mean knowing the encoded digest,
// which is what the manifest exists to record. Like the local Has, it
// cannot detect corruption; Has saying yes then Get saying ErrCorrupt is a
// legitimate sequence.
func (o *OCIObjects) Has(ctx context.Context, d cas.Digest) (bool, error) {
	tag := OCITag(d)
	if tag == "" {
		return false, nil
	}
	ok, err := o.client.HasManifest(ctx, tag)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Put stores everything r yields and returns its digest.
//
// The body is spooled to a temp file: a blob is uploaded under a digest
// known before the bytes are sent, and a workspace tarball does not belong
// in memory. The tier above uploads straight from the local store and
// never comes through here; this path is for a caller using the registry
// on its own.
//
// Concurrent Puts of the same content are safe: identical bytes to the
// same blob digest, then byte-identical manifests to the same tag.
func (o *OCIObjects) Put(ctx context.Context, r io.Reader) (cas.Digest, error) {
	if o.readOnly {
		// Not an error: read-only is deliberate and the tier has already
		// stored the object locally; see Objects.Put.
		return o.digestOnly(r)
	}

	spool, err := os.CreateTemp("", "senro-registry-put-")
	if err != nil {
		return "", fmt.Errorf("remote cache: %w", err)
	}
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
	}()

	plain := sha256.New()
	stored := sha256.New()
	enc, err := cas.NewEncoder(io.MultiWriter(spool, stored))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(io.MultiWriter(enc, plain), r); err != nil {
		_ = enc.Close()
		return "", fmt.Errorf("remote cache: %w", err)
	}
	if err := enc.Close(); err != nil {
		return "", fmt.Errorf("remote cache: %w", err)
	}
	size, err := spool.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", fmt.Errorf("remote cache: %w", err)
	}

	d := cas.Digest(cas.Prefix + hex.EncodeToString(plain.Sum(nil)))
	blob := cas.Prefix + hex.EncodeToString(stored.Sum(nil))
	if err := o.store(ctx, d, blob, spool, size); err != nil {
		return "", err
	}
	return d, nil
}

// digestOnly consumes r and reports what its digest would have been, storing
// nothing. It is what Put does in read-only mode.
func (o *OCIObjects) digestOnly(r io.Reader) (cas.Digest, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("remote cache: %w", err)
	}
	return cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil))), nil
}

// UploadEncoded stores a local object's file verbatim under its digest.
// path must be bytes in cas.NewEncoder's encoding; re-encoding would burn
// CPU to change nothing on the largest objects this cache moves.
//
// The file is read once to derive the blob digest the registry requires up
// front (the one cost a registry has that a bucket does not) and once more
// to send it. Safe to read concurrently: content is immutable and
// completed files are renamed into place.
func (o *OCIObjects) UploadEncoded(ctx context.Context, d cas.Digest, path string) error {
	if o.readOnly {
		return nil
	}
	if !d.Valid() {
		return fmt.Errorf("remote cache: %q is not a digest", string(d))
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	blob := cas.Prefix + hex.EncodeToString(h.Sum(nil))
	return o.store(ctx, d, blob, f, fi.Size())
}

// store pushes one object: its blob, then the manifest that names it. The
// blob is checked for first: the bytes are the expensive half and the
// check is one request. Not a race to win: both pushers send the same
// bytes to the same digest.
func (o *OCIObjects) store(
	ctx context.Context, d cas.Digest, blob string, body io.ReadSeeker, size int64,
) error {
	if _, ok, err := o.client.HasBlob(ctx, blob); err != nil {
		return err
	} else if !ok {
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("remote cache: rewinding %s: %w", d.Short(), err)
		}
		if err := o.client.PutBlob(ctx, blob, body, size); err != nil {
			return err
		}
	}
	if err := o.config.ensure(ctx, o.client); err != nil {
		return err
	}
	manifest, err := objectManifest(d, blob, size)
	if err != nil {
		return err
	}
	return o.client.PutManifest(ctx, OCITag(d), oci.MediaTypeImageManifest, manifest)
}

// miss turns a registry's "not there" into the cache's own, and leaves
// everything else alone.
func (o *OCIObjects) miss(err error, d cas.Digest) error {
	return ociMiss(err, d.Short())
}

// ociDescriptor is one entry of a manifest.
type ociDescriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ociManifest is the whole document senro writes for one object.
type ociManifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	ArtifactType  string            `json:"artifactType"`
	Config        ociDescriptor     `json:"config"`
	Layers        []ociDescriptor   `json:"layers"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// objectManifest renders the manifest for one object. Deterministic, and
// that is load-bearing: two machines storing the same object must write
// byte-identical manifests, so no timestamp, no identifier, no field that
// depends on anything but the object itself.
func objectManifest(d cas.Digest, blob string, size int64) ([]byte, error) {
	b, err := json.Marshal(ociManifest{
		SchemaVersion: 2,
		MediaType:     oci.MediaTypeImageManifest,
		ArtifactType:  ociArtifactType,
		Config: ociDescriptor{
			MediaType: ociEmptyMediaType, Digest: ociEmptyDigest, Size: ociEmptySize,
		},
		Layers: []ociDescriptor{{
			MediaType: ociLayerMediaType, Digest: blob, Size: size,
		}},
		Annotations: map[string]string{ociDigestAnnotation: string(d)},
	})
	if err != nil {
		return nil, fmt.Errorf("remote cache: rendering the manifest for %s: %w", d.Short(), err)
	}
	return b, nil
}

// parseObjectManifest reads a manifest back and refuses one that is not
// about the object it was asked for: the tag is a name somebody else could
// have written. Catching it here saves pulling a whole workspace tarball;
// the reader would catch it anyway, which is the point: two independent
// checks, neither optional.
func parseObjectManifest(raw []byte, want cas.Digest) (*ociManifest, error) {
	var m ociManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("remote cache: %w: the manifest for %s does not parse: %v",
			cas.ErrCorrupt, want.Short(), err)
	}
	if len(m.Layers) != 1 {
		return nil, fmt.Errorf(
			"remote cache: %w: the manifest for %s has %d layers, and a senro object has one",
			cas.ErrCorrupt, want.Short(), len(m.Layers))
	}
	if got := m.Annotations[ociDigestAnnotation]; got != string(want) {
		return nil, fmt.Errorf(
			"remote cache: %w: the manifest tagged for %s says it holds %s, so it was not served",
			cas.ErrCorrupt, want.Short(), cas.Digest(got).Short())
	}
	if !cas.Digest(m.Layers[0].Digest).Valid() {
		return nil, fmt.Errorf("remote cache: %w: the manifest for %s names no usable layer",
			cas.ErrCorrupt, want.Short())
	}
	return &m, nil
}

// newBytesSeeker is a reader over a byte slice that can be rewound for a
// retry.
func newBytesSeeker(b []byte) io.ReadSeeker { return &bytesSeeker{b: b} }

type bytesSeeker struct {
	b []byte
	i int64
}

func (r *bytesSeeker) Read(p []byte) (int, error) {
	if r.i >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += int64(n)
	return n, nil
}

func (r *bytesSeeker) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.i + offset
	case io.SeekEnd:
		abs = int64(len(r.b)) + offset
	default:
		return 0, errors.New("remote cache: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("remote cache: negative position")
	}
	r.i = abs
	return abs, nil
}
