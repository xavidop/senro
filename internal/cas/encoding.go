package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"
)

// NewEncoder wraps w in the encoding every backend stores objects through.
// The encoding never enters a digest (see the package doc), but all
// backends must agree on ONE: local and remote stores move objects between
// each other verbatim, and differing encodings would produce objects nobody
// can decode. The caller must Close the returned writer to flush the final
// frame; an unclosed writer produces the truncation DecodeVerify refuses.
func NewEncoder(w io.Writer) (io.WriteCloser, error) {
	// Concurrency 1: a run encodes many objects at once, and a pool of
	// encoder goroutines per object multiplies the build's thread count by
	// its parallelism for no gain at these sizes.
	enc, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("cas: %w", err)
	}
	return enc, nil
}

// DecodeVerify turns a stored object's raw bytes into the plaintext they
// encode, refusing anything that is not what want promised.
//
// The single implementation of the only guarantee this package makes,
// shared by every backend: a truncated download, a proxy's error page, or
// an overwritten object are ordinary occurrences, and serving any of them
// would poison every downstream build. Verification happens at EOF, the
// earliest moment the whole content is known: a caller that reads to
// completion cannot be handed the wrong bytes; one that stops early has
// checked nothing. Everything in this repository reads objects whole.
//
// The returned reader closes raw, so a caller has exactly one thing to
// close.
func DecodeVerify(raw io.ReadCloser, want Digest) (io.ReadCloser, error) {
	if !want.Valid() {
		_ = raw.Close()
		return nil, fmt.Errorf("cas: %w: %q is not a digest", ErrNotFound, string(want))
	}
	dec, err := zstd.NewReader(raw)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("cas: %w: %s: %v", ErrCorrupt, want.Short(), err)
	}
	return &verifyReader{
		body: dec.IOReadCloser(),
		dec:  dec,
		raw:  raw,
		want: want,
		h:    sha256.New(),
	}, nil
}

// verifyReader decodes an object and checks its digest at EOF.
type verifyReader struct {
	body io.ReadCloser
	dec  *zstd.Decoder
	raw  io.Closer
	want Digest
	h    hash.Hash
	done bool
}

func (r *verifyReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if n > 0 {
		_, _ = r.h.Write(p[:n])
	}
	switch {
	case errors.Is(err, io.EOF):
		if r.done {
			return n, io.EOF
		}
		r.done = true
		got := Digest(Prefix + hex.EncodeToString(r.h.Sum(nil)))
		if got != r.want {
			return n, fmt.Errorf("cas: %w: %s decoded to %s", ErrCorrupt, r.want.Short(), got.Short())
		}
		return n, io.EOF
	case err != nil:
		// A decode or read failure on a stored object is corruption, not an
		// I/O error: the store failed its only promise, and distinguishing
		// the two only tempts a caller into retrying.
		return n, fmt.Errorf("cas: %w: %s: %v", ErrCorrupt, r.want.Short(), err)
	}
	return n, nil
}

func (r *verifyReader) Close() error {
	r.dec.Close()
	return r.raw.Close()
}
