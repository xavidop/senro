package redact_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/internal/redact"
)

// TestANilSetRedactsNothingAndAllocatesNothing pins the no-secrets fast
// path: every method on a nil *Set behaves as an identity rather than
// panicking, because the engine holds one unconditionally.
func TestANilSetRedactsNothingAndAllocatesNothing(t *testing.T) {
	var s *redact.Set

	in := []byte("nothing secret here")
	out, n := s.Redact(in)
	if n != 0 {
		t.Errorf("Redact on a nil set reported %d replacements, want 0", n)
	}
	if &out[0] != &in[0] {
		t.Error("Redact on a nil set returned a copy; it must return the input slice itself")
	}
	if label, ok := s.Match(in); ok {
		t.Errorf("Match on a nil set reported a match labelled %q", label)
	}
	if got := s.Len(); got != 0 {
		t.Errorf("Len on a nil set = %d, want 0", got)
	}
	if got := s.Skipped(); got != nil {
		t.Errorf("Skipped on a nil set = %v, want nil", got)
	}
}

// TestNewReturnsNilWhenThereIsNothingToRegister: New returns nil, not an
// empty set, so a secret-free run actually takes the fast path above.
func TestNewReturnsNilWhenThereIsNothingToRegister(t *testing.T) {
	if s := redact.New(); s != nil {
		t.Error("New() with no values returned a non-nil set")
	}
	if s := redact.New(redact.Value{Label: "empty", Value: nil}); s != nil {
		t.Error("New() with an empty value returned a non-nil set")
	}
}

// TestNewSkipsAndReportsAValueShorterThanMinLength: skipping is right,
// skipping silently is not; Skipped is what the run-start check turns into
// a refusal.
func TestNewSkipsAndReportsAValueShorterThanMinLength(t *testing.T) {
	s := redact.New(
		redact.Value{Label: "pin", Value: []byte("1234")},
		redact.Value{Label: "token", Value: []byte("abcdef0123456789")},
	)
	if s == nil {
		t.Fatal("New returned nil despite one registrable value")
	}
	skipped := s.Skipped()
	if len(skipped) != 1 || skipped[0] != "pin" {
		t.Fatalf("Skipped() = %v, want [pin]", skipped)
	}
	out, n := s.Redact([]byte("pin 1234 token abcdef0123456789"))
	if n != 1 {
		t.Fatalf("Redact reported %d replacements, want 1 (the long value only)", n)
	}
	if !bytes.Contains(out, []byte("1234")) {
		t.Error("the short value was redacted; MinLength must have skipped it")
	}
	if bytes.Contains(out, []byte("abcdef0123456789")) {
		t.Error("the long value survived redaction")
	}
	if !strings.Contains(string(out), redact.Placeholder) {
		t.Error("no placeholder in the output")
	}
}

// TestNewReportsSkippedEvenWhenNothingElseWasRegistrable: every value too
// short, nothing registrable. The run-start refusal is built entirely on
// Skipped(), so returning nil here would let such a run start silently
// unprotected instead of being refused.
func TestNewReportsSkippedEvenWhenNothingElseWasRegistrable(t *testing.T) {
	s := redact.New(redact.Value{Label: "pin", Value: []byte("1234")})
	if s == nil {
		t.Fatal("New returned nil, discarding Skipped(); a caller cannot refuse " +
			"a run it has no way to see was left unprotected")
	}
	skipped := s.Skipped()
	if len(skipped) != 1 || skipped[0] != "pin" {
		t.Fatalf("Skipped() = %v, want [pin]", skipped)
	}
	// Zero patterns registered: nothing to match, input untouched.
	out, n := s.Redact([]byte("pin 1234"))
	if n != 0 || string(out) != "pin 1234" {
		t.Errorf("Redact = (%q, %d), want the input untouched with 0 replacements", out, n)
	}
	if label, ok := s.Match([]byte("pin 1234")); ok {
		t.Errorf("Match reported %q on a set with zero registered patterns", label)
	}
}

// TestRedactCatchesAValueWhereverItSits covers the boundaries: start of
// buffer, end, twice in one buffer, and adjacent to itself.
func TestRedactCatchesAValueWhereverItSits(t *testing.T) {
	const secretValue = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secretValue)})

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"alone", secretValue, redact.Placeholder},
		{"at the start", secretValue + " trailing", redact.Placeholder + " trailing"},
		{"at the end", "leading " + secretValue, "leading " + redact.Placeholder},
		{"in the middle", "a " + secretValue + " b", "a " + redact.Placeholder + " b"},
		{"twice", secretValue + "|" + secretValue, redact.Placeholder + "|" + redact.Placeholder},
		{"back to back", secretValue + secretValue, redact.Placeholder + redact.Placeholder},
		{"absent", "nothing to see", "nothing to see"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, _ := s.Redact([]byte(tc.in))
			if string(out) != tc.want {
				t.Errorf("Redact(%q) = %q, want %q", tc.in, out, tc.want)
			}
			if bytes.Contains(out, []byte(secretValue)) {
				t.Errorf("the value survived in %q", out)
			}
		})
	}
}

// TestMatchNamesTheSecretItFound: the label comes back so an error can name
// a field without printing a value.
func TestMatchNamesTheSecretItFound(t *testing.T) {
	s := redact.New(
		redact.Value{Label: "NPMToken", Value: []byte("npm-aaaaaaaaaa")},
		redact.Value{Label: "RegistryToken", Value: []byte("reg-bbbbbbbbbb")},
	)
	label, ok := s.MatchString("--auth=reg-bbbbbbbbbb")
	if !ok {
		t.Fatal("Match missed a value that is plainly present")
	}
	if label != "RegistryToken" {
		t.Errorf("Match labelled it %q, want RegistryToken", label)
	}
	if _, ok := s.MatchString("--auth=harmless"); ok {
		t.Error("Match reported a match in a string containing no value")
	}
}

// TestOverlappingPatternsLeaveAFragment pins the exact boundary of the
// package's guarantee: one secret a substring of another, replacing the
// shorter destroys the longer's completeness (the guarantee) but leaves its
// tail visible (not covered).
func TestOverlappingPatternsLeaveAFragment(t *testing.T) {
	s := redact.New(
		redact.Value{Label: "short", Value: []byte("abcdef")},
		redact.Value{Label: "long", Value: []byte("abcdefghij")},
	)
	out, n := s.Redact([]byte("abcdefghij"))
	if n != 1 {
		t.Fatalf("reported %d replacements, want 1", n)
	}
	if bytes.Contains(out, []byte("abcdef")) {
		t.Error("the shorter value survived complete")
	}
	if bytes.Contains(out, []byte("abcdefghij")) {
		t.Error("the longer value survived complete")
	}
	if string(out) != redact.Placeholder+"ghij" {
		t.Errorf("Redact = %q; the documented behaviour is %q, and the docs "+
			"must change with this assertion if the algorithm changes",
			out, redact.Placeholder+"ghij")
	}
}

// TestPlaceholderMatchesMamori keeps senro's placeholder identical to
// mamori's, so a log reader never sees two spellings.
func TestPlaceholderMatchesMamori(t *testing.T) {
	if redact.Placeholder != secret.Redacted {
		t.Errorf("redact.Placeholder = %q, mamori secret.Redacted = %q; keep them identical",
			redact.Placeholder, secret.Redacted)
	}
}

// TestRedactDoesNotSpanSeparateCalls pins the honest edge of Set.Redact: it
// carries no state between calls, so a value split across two calls is not
// caught here. Writer is what closes that gap. If Redact ever grows
// cross-call memory, its doc and the package doc must change with it.
func TestRedactDoesNotSpanSeparateCalls(t *testing.T) {
	const secretValue = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secretValue)})

	half := len(secretValue) / 2
	first, second := []byte(secretValue[:half]), []byte(secretValue[half:])

	out1, n1 := s.Redact(first)
	if n1 != 0 {
		t.Errorf("Redact(%q) reported %d replacements, want 0: the pattern is not complete within one call", first, n1)
	}
	if string(out1) != secretValue[:half] {
		t.Errorf("Redact(%q) = %q, want the untouched input back", first, out1)
	}

	out2, n2 := s.Redact(second)
	if n2 != 0 {
		t.Errorf("Redact(%q) reported %d replacements, want 0: a Set.Redact call starts fresh at the root", second, n2)
	}
	if string(out2) != secretValue[half:] {
		t.Errorf("Redact(%q) = %q, want the untouched input back", second, out2)
	}
}

// BenchmarkRedactScalesWithSecretCount demonstrates the property
// Aho-Corasick is chosen for: matching is linear in the input regardless of
// secret count, where per-secret bytes.Replace is O(input * secrets).
// Sink.Emit sits on this path and must never block. ns/op should stay
// roughly flat as secretCount grows:
//
//	go test ./internal/redact/ -bench BenchmarkRedactScalesWithSecretCount -benchtime 200x -run '^$'
func BenchmarkRedactScalesWithSecretCount(b *testing.B) {
	haystack := buildBenchmarkHaystack(64 * 1024) // 64 KiB, deterministic, no matches
	for _, secretCount := range []int{10, 100, 1000, 10000} {
		s := redact.New(benchmarkSecrets(secretCount)...)
		b.Run(fmt.Sprintf("%dsecrets", secretCount), func(b *testing.B) {
			b.SetBytes(int64(len(haystack)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Redact(haystack)
			}
		})
	}
}

// benchmarkSecrets returns n distinct registrable values, none appearing in
// the haystack, so the benchmark measures steady-state per-byte cost.
func benchmarkSecrets(n int) []redact.Value {
	vals := make([]redact.Value, n)
	for i := range vals {
		vals[i] = redact.Value{
			Label: fmt.Sprintf("secret-%d", i),
			Value: []byte(fmt.Sprintf("zzTopSecretValue%08dzz", i)),
		}
	}
	return vals
}

// buildBenchmarkHaystack returns n bytes of deterministic, secret-free text
// standing in for ordinary child-process log output.
func buildBenchmarkHaystack(n int) []byte {
	const line = "2026-08-10T12:00:00Z step=build msg=\"compiling package, nothing sensitive here\"\n"
	out := make([]byte, 0, n+len(line))
	for len(out) < n {
		out = append(out, line...)
	}
	return out[:n]
}
