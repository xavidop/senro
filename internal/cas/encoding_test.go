package cas_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
)

// encoded is what a backend stores for these plaintext bytes.
func encoded(t *testing.T, plain []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := cas.NewEncoder(&buf)
	if err != nil {
		t.Fatalf("cas.NewEncoder: %v", err)
	}
	if _, err := enc.Write(plain); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("closing the encoder: %v", err)
	}
	return buf.Bytes()
}

func TestDecodeVerifyReturnsThePlaintextForAMatchingDigest(t *testing.T) {
	plain := []byte("what a step wrote\n")
	d := cas.FromBytes(plain)

	rc, err := cas.DecodeVerify(io.NopCloser(bytes.NewReader(encoded(t, plain))), d)
	if err != nil {
		t.Fatalf("DecodeVerify: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("read back %q, want %q", got, plain)
	}
}

// TestDecodeVerifyRefusesBytesThatAreNotWhatTheDigestPromised is the whole
// reason this exists: a store may hand back the wrong object, and the digest
// is the only thing that can tell.
func TestDecodeVerifyRefusesBytesThatAreNotWhatTheDigestPromised(t *testing.T) {
	plain := []byte("what a step wrote\n")
	other := cas.FromBytes([]byte("something else entirely"))

	rc, err := cas.DecodeVerify(io.NopCloser(bytes.NewReader(encoded(t, plain))), other)
	if err != nil {
		t.Fatalf("DecodeVerify: %v", err)
	}
	defer func() { _ = rc.Close() }()

	_, err = io.ReadAll(rc)
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading an object under the wrong digest = %v, want ErrCorrupt", err)
	}
}

// TestDecodeVerifyRefusesATruncatedObject. A remote store returning a short
// body is an ordinary occurrence, and the decoder has to notice.
func TestDecodeVerifyRefusesATruncatedObject(t *testing.T) {
	plain := bytes.Repeat([]byte("a workspace tarball would be much bigger than this\n"), 200)
	d := cas.FromBytes(plain)
	full := encoded(t, plain)

	for _, keep := range []int{0, 1, len(full) / 3, len(full) - 1} {
		rc, err := cas.DecodeVerify(io.NopCloser(bytes.NewReader(full[:keep])), d)
		if err != nil {
			// Failing this early is also a refusal, which is the point.
			if !errors.Is(err, cas.ErrCorrupt) {
				t.Errorf("DecodeVerify of %d/%d bytes = %v, want ErrCorrupt", keep, len(full), err)
			}
			continue
		}
		_, err = io.ReadAll(rc)
		_ = rc.Close()
		if !errors.Is(err, cas.ErrCorrupt) {
			t.Errorf("reading %d/%d bytes = %v, want ErrCorrupt", keep, len(full), err)
		}
	}
}

// TestDecodeVerifyRefusesBytesThatAreNotEncodedAtAll covers a store holding
// something that was never one of these objects: a stray file, an HTML error
// page a proxy substituted, a half-written upload from another tool.
func TestDecodeVerifyRefusesBytesThatAreNotEncodedAtAll(t *testing.T) {
	d := cas.FromBytes([]byte("anything"))
	body := strings.NewReader("<html><body>503 Service Unavailable</body></html>")

	rc, err := cas.DecodeVerify(io.NopCloser(body), d)
	if err != nil {
		if !errors.Is(err, cas.ErrCorrupt) {
			t.Fatalf("DecodeVerify of unencoded bytes = %v, want ErrCorrupt", err)
		}
		return
	}
	defer func() { _ = rc.Close() }()
	if _, err := io.ReadAll(rc); !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading unencoded bytes = %v, want ErrCorrupt", err)
	}
}

// TestDecodeVerifyRefusesADigestThatIsNotOne. A digest reaches this package
// from event logs, plans and CLI arguments, none of which are trusted.
func TestDecodeVerifyRefusesADigestThatIsNotOne(t *testing.T) {
	for _, d := range []cas.Digest{"", "sha256:", "sha256:../../etc/passwd", "deadbeef"} {
		if _, err := cas.DecodeVerify(io.NopCloser(strings.NewReader("x")), d); err == nil {
			t.Errorf("DecodeVerify accepted %q as a digest", string(d))
		}
	}
}

// TestDecodeVerifyClosesWhatItWasGiven: the raw body is a network connection
// for the remote backend, and leaking one per object exhausts a pool.
func TestDecodeVerifyClosesWhatItWasGiven(t *testing.T) {
	plain := []byte("small")
	raw := &countingCloser{Reader: bytes.NewReader(encoded(t, plain))}

	rc, err := cas.DecodeVerify(raw, cas.FromBytes(plain))
	if err != nil {
		t.Fatalf("DecodeVerify: %v", err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("reading: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if raw.closed != 1 {
		t.Errorf("the underlying body was closed %d times, want exactly 1", raw.closed)
	}
}

type countingCloser struct {
	io.Reader
	closed int
}

func (c *countingCloser) Close() error {
	c.closed++
	return nil
}
