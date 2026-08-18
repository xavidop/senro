package redact_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestEveryNamedEncodingIsCaught walks Variants' encodings one at a time,
// encoding each with the standard library so a bug in senro's encoder is
// caught by disagreeing with the library, not by agreeing with itself.
func TestEveryNamedEncodingIsCaught(t *testing.T) {
	// Deliberately awkward: slash and plus differ across the base64
	// alphabets, space and ampersand exercise both URL escapers, quote and
	// backslash exercise JSON and shell, dollar and backtick double-quoted
	// shell.
	raw := []byte(`tok/en+val ue&"x\y$z` + "`q`")
	s := redact.New(redact.Value{Label: "tok", Value: raw})

	jsonBody := func(v []byte) string {
		b, err := json.Marshal(string(v))
		if err != nil {
			t.Fatalf("marshalling the fixture: %v", err)
		}
		return string(b[1 : len(b)-1])
	}

	cases := []struct {
		name    string
		encoded string
	}{
		{"raw", string(raw)},
		{"base64 std padded", base64.StdEncoding.EncodeToString(raw)},
		{"base64 std unpadded", base64.RawStdEncoding.EncodeToString(raw)},
		{"base64 url padded", base64.URLEncoding.EncodeToString(raw)},
		{"base64 url unpadded", base64.RawURLEncoding.EncodeToString(raw)},
		{"url query escaped", url.QueryEscape(string(raw))},
		{"url path escaped", url.PathEscape(string(raw))},
		{"json string escaped", jsonBody(raw)},
		{"shell single quoted", strings.ReplaceAll(string(raw), `'`, `'\''`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line := "output: " + tc.encoded + " done"
			out, n := s.Redact([]byte(line))
			if n == 0 {
				t.Fatalf("no replacement in %q; this encoding is not registered", line)
			}
			if bytes.Contains(out, []byte(tc.encoded)) {
				t.Errorf("the %s form survived: %q", tc.name, out)
			}
			// Canary: without this, a bug returning an empty slice would
			// pass the containment check for every case.
			if !bytes.Contains(out, []byte("output: ")) || !bytes.Contains(out, []byte(" done")) {
				t.Fatalf("the surrounding text is gone from %q; this assertion "+
					"was not actually looking at the redacted line", out)
			}
		})
	}
}

// TestShellDoubleQuoteEscapingIsCaught is separate because there is no
// standard-library encoder to disagree with; the expected form is by hand.
func TestShellDoubleQuoteEscapingIsCaught(t *testing.T) {
	raw := []byte(`a"b\c$d` + "`e`")
	s := redact.New(redact.Value{Label: "tok", Value: raw})
	encoded := `a\"b\\c\$d` + "\\`e\\`"

	out, n := s.Redact([]byte("echo " + encoded))
	if n == 0 {
		t.Fatalf("the double-quoted shell form is not registered: %q", encoded)
	}
	if bytes.Contains(out, []byte(encoded)) {
		t.Errorf("it survived: %q", out)
	}
	if !bytes.Contains(out, []byte("echo ")) {
		t.Fatalf("the surrounding text is gone from %q", out)
	}
}

// TestVariantsAreDeduplicated keeps the automaton small: a plain
// alphanumeric token is byte-identical under URL, JSON and both shell
// forms, so only the four base64 forms remain distinct.
func TestVariantsAreDeduplicated(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("abcdefghijkl")})
	if got := s.Len(); got > 6 {
		t.Errorf("Len() = %d for a plain alphanumeric token; the encodings that "+
			"cannot differ from the raw form are not being deduplicated", got)
	}
	if got := s.Len(); got < 2 {
		t.Errorf("Len() = %d; the base64 forms must still be registered", got)
	}
}

// TestAnEncodingShorterThanMinLengthIsNotRegistered guards against a future
// encoder that shrinks its input registering a tiny pattern that would
// redact everything.
func TestAnEncodingShorterThanMinLengthIsNotRegistered(t *testing.T) {
	for _, v := range redact.Variants([]byte("abcdefgh")) {
		if len(v) > 0 && len(v) < redact.MinLength {
			t.Errorf("Variants produced %q, shorter than MinLength=%d", v, redact.MinLength)
		}
	}
}

// TestAVariantCollidingWithOrdinaryOutputIsRedactedAnyway: Redact matches
// bytes, not provenance, so an unrelated line sharing a registered form's
// exact bytes is redacted too. Over-redaction is the safe failure
// direction: never a leak, sometimes noisy.
func TestAVariantCollidingWithOrdinaryOutputIsRedactedAnyway(t *testing.T) {
	const secretValue = "MyS3cretPass!Value"
	lookalike := base64.StdEncoding.EncodeToString([]byte(secretValue))
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secretValue)})

	// The line never mentions the secret; it happens to print the same bytes
	// its base64 form would produce.
	line := "artifact checksum=" + lookalike + " size=42"
	out, n := s.Redact([]byte(line))
	if n == 0 {
		t.Fatalf("no replacement in %q; the collision was not caught", line)
	}
	if bytes.Contains(out, []byte(lookalike)) {
		t.Errorf("the colliding bytes survived: %q", out)
	}
	if !bytes.Contains(out, []byte("artifact checksum=")) || !bytes.Contains(out, []byte(" size=42")) {
		t.Fatalf("the surrounding text is gone from %q; this assertion was not "+
			"actually looking at the redacted line", out)
	}
}

// TestASecretThatIsAlreadyValidBase64RegistersAndRedactsBothForms: a secret
// whose raw bytes look like base64 (a Basic-Auth value) must redact in raw
// AND base64-of-itself form, and an unrelated base64-shaped string must
// survive; the shape alone is not the trigger.
func TestASecretThatIsAlreadyValidBase64RegistersAndRedactsBothForms(t *testing.T) {
	const secretValue = "QWxhZGRpbjpvcGVuc2VzYW1l" // itself valid base64
	doubleEncoded := base64.StdEncoding.EncodeToString([]byte(secretValue))
	if doubleEncoded == secretValue {
		t.Fatal("fixture is broken: double-encoding must differ from the raw value")
	}
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secretValue)})

	rawLine := "auth: Basic " + secretValue + " accepted"
	out, n := s.Redact([]byte(rawLine))
	if n == 0 {
		t.Fatalf("no replacement in %q; the raw already-base64 value is not registered", rawLine)
	}
	if bytes.Contains(out, []byte(secretValue)) {
		t.Errorf("the raw value survived: %q", out)
	}
	if !bytes.Contains(out, []byte("auth: Basic ")) || !bytes.Contains(out, []byte(" accepted")) {
		t.Fatalf("the surrounding text is gone from %q", out)
	}

	doubleLine := "auth: Basic " + doubleEncoded + " accepted"
	out, n = s.Redact([]byte(doubleLine))
	if n == 0 {
		t.Fatalf("no replacement in %q; the base64-of-base64 form is not registered", doubleLine)
	}
	if bytes.Contains(out, []byte(doubleEncoded)) {
		t.Errorf("the double-encoded value survived: %q", out)
	}
	if !bytes.Contains(out, []byte("auth: Basic ")) || !bytes.Contains(out, []byte(" accepted")) {
		t.Fatalf("the surrounding text is gone from %q", out)
	}

	// An unrelated base64-shaped string, never registered, must survive.
	const unrelated = "dW5yZWxhdGVkOnZhbHVlaGVyZQ=="
	out, n = s.Redact([]byte("auth: Basic " + unrelated + " accepted"))
	if n != 0 {
		t.Errorf("Redact reported %d replacements for an unrelated base64-shaped "+
			"string; only registered bytes should match", n)
	}
	if !bytes.Contains(out, []byte(unrelated)) {
		t.Error("the unrelated value was redacted; it was never registered")
	}
}

// TestVariantMatchesAtBufferBoundaries repeats
// TestRedactCatchesAValueWhereverItSits for a generated variant pattern, so
// boundary handling is pinned for the longer patterns too.
func TestVariantMatchesAtBufferBoundaries(t *testing.T) {
	const secretValue = "s3cr3t-boundary-value"
	encoded := base64.StdEncoding.EncodeToString([]byte(secretValue))
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secretValue)})

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"alone", encoded, redact.Placeholder},
		{"at the start", encoded + " trailing", redact.Placeholder + " trailing"},
		{"at the end", "leading " + encoded, "leading " + redact.Placeholder},
		{"back to back", encoded + encoded, redact.Placeholder + redact.Placeholder},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, n := s.Redact([]byte(tc.in))
			if n == 0 {
				t.Fatalf("no replacement in %q", tc.in)
			}
			if string(out) != tc.want {
				t.Errorf("Redact(%q) = %q, want %q", tc.in, out, tc.want)
			}
			if bytes.Contains(out, []byte(encoded)) {
				t.Errorf("the encoded value survived in %q", out)
			}
		})
	}
}

// TestCollidingVariantsAcrossTwoSecretsStillRedactBoth: when two secrets
// share an encoded form, New registers it once, labelled with whichever it
// saw first. A label tie-break for Match, not a redaction gap.
func TestCollidingVariantsAcrossTwoSecretsStillRedactBoth(t *testing.T) {
	const secretA = "collide-value-one"
	// secretB's raw value equals secretA's base64 form, so their variant
	// sets intersect and New's dedup collapses the shared bytes.
	secretBRaw := base64.StdEncoding.EncodeToString([]byte(secretA))
	s := redact.New(
		redact.Value{Label: "A", Value: []byte(secretA)},
		redact.Value{Label: "B", Value: []byte(secretBRaw)},
	)

	// The shared bytes must still be redacted.
	out, n := s.Redact([]byte("value=" + secretA + " end"))
	if n == 0 {
		t.Fatalf("the shared bytes were not redacted at all")
	}
	if bytes.Contains(out, []byte(secretA)) {
		t.Errorf("the shared bytes survived: %q", out)
	}
	if !bytes.Contains(out, []byte("value=")) || !bytes.Contains(out, []byte(" end")) {
		t.Fatalf("the surrounding text is gone from %q", out)
	}

	// secretB's own raw value is unaffected by the collision.
	out, n = s.Redact([]byte("value=" + secretBRaw + " end"))
	if n == 0 {
		t.Fatalf("secretB's own raw value was not redacted")
	}
	if bytes.Contains(out, []byte(secretBRaw)) {
		t.Errorf("secretB's raw value survived: %q", out)
	}
}
