package remotecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/s3"
)

// Objects is the content-addressed store held in the bucket.
//
// Keys mirror the local directory backend's layout, hex fanout and all:
// <prefix>cas/sha256/<aa>/<bb>/<hex>. Object stores do not need the
// fanout, but a listing that reads the same on both sides beats two saved
// characters.
//
// The stored bytes are exactly what the local backend writes (the
// cas.NewEncoder encoding), which lets the tier upload an object straight
// from disk without re-encoding a multi-gigabyte workspace.
type Objects struct {
	client   *s3.Client
	prefix   string
	readOnly bool
}

var _ cas.Store = (*Objects)(nil)

// Name is the key a digest is stored under in the bucket. It returns "" for a
// digest that is not well-formed: a digest reaches this package from event
// logs, plans and command-line arguments, and none of those are trusted.
func (o *Objects) Name(d cas.Digest) string {
	if !d.Valid() {
		return ""
	}
	h := d.Hex()
	return o.prefix + "sha256/" + h[0:2] + "/" + h[2:4] + "/" + h
}

// Get returns the object's plaintext, verified: the returned reader fails
// with cas.ErrCorrupt if what it decoded is not what d promised, through
// the same code the local backend uses.
func (o *Objects) Get(ctx context.Context, d cas.Digest) (io.ReadCloser, error) {
	key := o.Name(d)
	if key == "" {
		return nil, fmt.Errorf("remote cache: %w: %q is not a digest", cas.ErrNotFound, string(d))
	}
	body, err := o.client.Get(ctx, key)
	if err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			return nil, fmt.Errorf("remote cache: %w: %s", cas.ErrNotFound, d.Short())
		}
		return nil, err
	}
	return cas.DecodeVerify(body, d)
}

// Has reports whether the object is in the bucket. Like the local Has, it
// cannot detect corruption (metadata only, no bytes); Has saying yes then
// Get saying ErrCorrupt is a legitimate sequence every caller survives.
func (o *Objects) Has(ctx context.Context, d cas.Digest) (bool, error) {
	key := o.Name(d)
	if key == "" {
		return false, nil
	}
	_, ok, err := o.client.Head(ctx, key)
	return ok, err
}

// Put stores everything r yields and returns its digest.
//
// The body is spooled to a temp file: the request is signed over the exact
// bytes it will send, and a workspace tarball does not belong in memory.
// The tier above uploads straight from the local store and never comes
// through here; this path is for a caller using the remote on its own.
//
// Concurrent Puts of the same content are safe: same key, same meaning, an
// atomic PUT, and the reader verifies whatever it gets.
func (o *Objects) Put(ctx context.Context, r io.Reader) (cas.Digest, error) {
	if o.readOnly {
		// Not an error: read-only is a deliberate configuration and the
		// tier has already stored the object locally; a failure here would
		// turn a setting into a degradation notice per object.
		return o.digestOnly(r)
	}

	spool, err := os.CreateTemp("", "senro-remote-put-")
	if err != nil {
		return "", fmt.Errorf("remote cache: %w", err)
	}
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
	}()

	h := sha256.New()
	enc, err := cas.NewEncoder(spool)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(io.MultiWriter(enc, h), r); err != nil {
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

	d := cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil)))
	if err := o.client.Put(ctx, o.Name(d), spool, size); err != nil {
		return "", err
	}
	return d, nil
}

// digestOnly consumes r and reports what its digest would have been, storing
// nothing. It is what Put does in read-only mode.
func (o *Objects) digestOnly(r io.Reader) (cas.Digest, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("remote cache: %w", err)
	}
	return cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil))), nil
}

// UploadEncoded stores a local object's file verbatim under its digest.
// path must be bytes in cas.NewEncoder's encoding (what the local backend
// writes): re-encoding would burn CPU to change nothing, on the largest
// objects this cache moves. Safe to read concurrently: content is
// immutable and the local backend renames completed files into place, so a
// path that exists is a complete object.
func (o *Objects) UploadEncoded(ctx context.Context, d cas.Digest, path string) error {
	if o.readOnly {
		return nil
	}
	key := o.Name(d)
	if key == "" {
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
	return o.client.Put(ctx, key, f, fi.Size())
}
