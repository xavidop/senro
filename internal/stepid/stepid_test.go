package stepid_test

import (
	"testing"

	"github.com/xavidop/senro/internal/stepid"
)

func TestFormatSortsKeys(t *testing.T) {
	// Sorted keys are what make expansion child IDs deterministic. An expander
	// returning map iteration order must still produce a stable ID.
	got := stepid.Format("build/test", map[string]string{"os": "linux", "unit": "api"})
	want := "build/test[os=linux,unit=api]"
	if got != want {
		t.Errorf("Format = %q, want %q", got, want)
	}
}

func TestFormatNoKeys(t *testing.T) {
	if got := stepid.Format("build/test", nil); got != "build/test" {
		t.Errorf("Format = %q, want bare ID", got)
	}
}

func TestParseAddress(t *testing.T) {
	cases := []struct {
		in      string
		id      string
		attempt int
	}{
		{"build/test", "build/test", 0},
		{"build/test@2", "build/test", 2},
		{"build/test[unit=api]", "build/test[unit=api]", 0},
		{"build/test[unit=api]@3", "build/test[unit=api]", 3},
	}
	for _, tc := range cases {
		id, attempt, err := stepid.ParseAddress(tc.in)
		if err != nil {
			t.Fatalf("ParseAddress(%q): %v", tc.in, err)
		}
		if id != tc.id || attempt != tc.attempt {
			t.Errorf("ParseAddress(%q) = (%q, %d), want (%q, %d)", tc.in, id, attempt, tc.id, tc.attempt)
		}
	}
}

func TestParseAddressRejectsBadAttempt(t *testing.T) {
	// An attempt of 0 or negative is not addressable; attempts are 1-based.
	for _, in := range []string{"build/test@0", "build/test@-1", "build/test@x", "build/test@"} {
		if _, _, err := stepid.ParseAddress(in); err == nil {
			t.Errorf("ParseAddress(%q) should error", in)
		}
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	// Step IDs contain / and [] and cannot be path segments directly. Encoding
	// must be reversible and must keep the result readable when debugging a run
	// from disk.
	for _, id := range []string{
		"build/test",
		"build/test[unit=services/api]",
		"deploy/discover/apply-cm4[a=1,b=2]",
	} {
		enc := stepid.Encode(id)
		if enc == "" {
			t.Fatalf("Encode(%q) empty", id)
		}
		for _, bad := range []rune{'/', '[', ']'} {
			for _, r := range enc {
				if r == bad {
					t.Errorf("Encode(%q) = %q still contains %q", id, enc, string(bad))
				}
			}
		}
		back, err := stepid.Decode(enc)
		if err != nil {
			t.Fatalf("Decode(%q): %v", enc, err)
		}
		if back != id {
			t.Errorf("round-trip: %q -> %q -> %q", id, enc, back)
		}
	}
}

// TestKeysRoundTripsFormat is the property Keys exists for: the id grammar is
// written by Format and read back by whatever has only an id to work from
// (recording a duration against the unit whose step produced it, for one).
func TestKeysRoundTripsFormat(t *testing.T) {
	in := map[string]string{"unit": "apps/web", "os": "linux"}
	base, keys, ok := stepid.Keys(stepid.Format("verify/lint", in))
	if !ok {
		t.Fatal("Keys refused an id Format built")
	}
	if base != "verify/lint" {
		t.Errorf("base = %q", base)
	}
	if len(keys) != 2 || keys["unit"] != "apps/web" || keys["os"] != "linux" {
		t.Errorf("keys = %v, want %v", keys, in)
	}
}

func TestKeysOnABareID(t *testing.T) {
	base, keys, ok := stepid.Keys("build")
	if !ok || base != "build" || len(keys) != 0 {
		t.Errorf("Keys(%q) = (%q, %v, %v), want a bare id with no keys", "build", base, keys, ok)
	}
}

// TestKeysRefusesAMalformedID: a caller uses the ok result to decide whether
// an id names a unit at all, so guessing at a broken one would attribute work
// to the wrong unit.
func TestKeysRefusesAMalformedID(t *testing.T) {
	for _, in := range []string{"a[", "a]", "a[b]", "a[=v]", "a[k=v", "a[k=v]x", "a[]"} {
		if _, _, ok := stepid.Keys(in); ok {
			t.Errorf("Keys(%q) accepted a malformed id", in)
		}
	}
}
