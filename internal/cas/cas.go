// Package cas is senro's content-addressed store: bytes in, a digest out,
// and the same bytes back for that digest anywhere the store is reachable.
//
// A digest is taken over the PLAINTEXT a caller hands in. How a backend
// encodes those bytes on its own storage, compressed or not, is the
// backend's business and never enters the digest. That is not a detail: a
// workspace digest feeds the next step's cache key, so if an encoder
// version could change a digest, upgrading a compression library would
// silently invalidate every cache key in the fleet, the same kind of quiet
// failure an unnormalized tar mtime would cause.
package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// Prefix is the algorithm marker every digest carries. There is one algorithm;
// the prefix exists so a second one can be added without re-parsing anything
// already written to disk or to an event log.
const Prefix = "sha256:"

// Digest is a content address: Prefix followed by 64 lowercase hex digits.
type Digest string

var (
	// ErrNotFound means the store has no object at that address, or the
	// address was not a well-formed digest at all. The two are deliberately
	// one error: a caller asking for a malformed digest is asking for
	// something the store does not have, and distinguishing them would tempt
	// a caller into building a path from an address it has not validated.
	ErrNotFound = errors.New("cas: object not found")

	// ErrCorrupt means the object could not be produced as promised: the
	// stored bytes did not decode, or they decoded to content whose digest is
	// not the one requested. Both are reported as corruption rather than as
	// an I/O error, because from a caller's side the store failed to keep the
	// only promise it makes.
	ErrCorrupt = errors.New("cas: object does not match its digest")
)

// FromBytes is the digest of b.
func FromBytes(b []byte) Digest {
	sum := sha256.Sum256(b)
	return Digest(Prefix + hex.EncodeToString(sum[:]))
}

// Valid reports whether d is a well-formed digest. Every function that turns
// a digest into a filesystem path calls this first: a digest reaches this
// package from event logs, plans and CLI arguments, all of which are
// untrusted input, and "sha256:../../etc/passwd" must never become a path.
func (d Digest) Valid() bool {
	s := string(d)
	if len(s) != len(Prefix)+64 || s[:len(Prefix)] != Prefix {
		return false
	}
	for _, c := range s[len(Prefix):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Hex is the digest without its algorithm prefix.
func (d Digest) Hex() string {
	if len(d) <= len(Prefix) {
		return ""
	}
	return string(d[len(Prefix):])
}

// Short is the first eight hex digits, for error messages, `cache explain`
// output, and a secret's identity form in a cache key. Never use it as an
// address.
func (d Digest) Short() string {
	h := d.Hex()
	if len(h) < 8 {
		return h
	}
	return h[:8]
}

// Store is the content-addressed store interface. Implementations ship in two
// places: Dir, on local disk, and internal/remotecache, which keeps a Dir in
// front of a shared store. The shared one is either an S3-compatible object
// store or an OCI registry; both go behind the same tier, so the rule that a
// shared store which is down never fails a run is written once.
type Store interface {
	Put(ctx context.Context, r io.Reader) (Digest, error)
	Get(ctx context.Context, d Digest) (io.ReadCloser, error)
	Has(ctx context.Context, d Digest) (bool, error)
}

// PutBytes stores b. For the small JSON objects this repo stores (indexes,
// cache entries) streaming buys nothing and a byte slice reads better.
func PutBytes(ctx context.Context, s Store, b []byte) (Digest, error) {
	return s.Put(ctx, bytesReader(b))
}

// GetBytes reads an object whole. Only for objects a caller already knows
// are small: a workspace tarball goes through Get and stays streamed.
func GetBytes(ctx context.Context, s Store, d Digest) ([]byte, error) {
	rc, err := s.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("cas: read %s: %w", d.Short(), err)
	}
	return b, nil
}

func bytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
