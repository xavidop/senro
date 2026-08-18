package cas_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
)

func TestFromBytesIsPlainSHA256(t *testing.T) {
	b := []byte("the quick brown fox")
	sum := sha256.Sum256(b)
	want := cas.Digest("sha256:" + hex.EncodeToString(sum[:]))
	if got := cas.FromBytes(b); got != want {
		t.Errorf("FromBytes = %q, want %q", got, want)
	}
}

// Valid is the guard in front of every path that turns a digest into a
// filename. A digest that arrives from an event log, a plan, or a CLI
// argument is untrusted input, and "sha256:../../etc/passwd" must not
// become a path.
func TestValidRejectsAnythingThatIsNotADigest(t *testing.T) {
	sum := sha256.Sum256(nil)
	good := cas.Digest("sha256:" + hex.EncodeToString(sum[:]))
	if !good.Valid() {
		t.Errorf("%q should be valid", good)
	}
	for _, bad := range []cas.Digest{
		"",
		"sha256:",
		"sha256:../../etc/passwd",
		cas.Digest("sha256:" + strings.Repeat("z", 64)),
		cas.Digest("sha256:" + strings.Repeat("a", 63)),
		cas.Digest("sha256:" + strings.Repeat("a", 65)),
		cas.Digest("sha1:" + strings.Repeat("a", 40)),
		cas.Digest(strings.ToUpper("sha256:" + hex.EncodeToString(sum[:]))),
	} {
		if bad.Valid() {
			t.Errorf("%q should not be valid", bad)
		}
	}
}

func TestShortIsEightHexDigits(t *testing.T) {
	d := cas.FromBytes([]byte("x"))
	if got := d.Short(); len(got) != 8 || !strings.HasPrefix(d.Hex(), got) {
		t.Errorf("Short() = %q, want the first 8 hex digits of %q", got, d.Hex())
	}
}

func TestPutBytesAndGetBytesRoundTrip(t *testing.T) {
	s, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	want := []byte("payload")
	d, err := cas.PutBytes(ctx, s, want)
	if err != nil {
		t.Fatalf("PutBytes: %v", err)
	}
	got, err := cas.GetBytes(ctx, s, d)
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round trip = %q, want %q", got, want)
	}
}
