package api_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
)

// TestCheckVersionEqualIsSilent is the baseline: a client and engine built
// from the exact same api package must negotiate with neither an error nor
// a warning: the ordinary case, exercised on every attach.
func TestCheckVersionEqualIsSilent(t *testing.T) {
	warn, err := api.CheckVersion(api.Version, api.VersionMinor, api.Version, api.VersionMinor)
	if err != nil {
		t.Fatalf("CheckVersion() err = %v, want nil", err)
	}
	if warn != "" {
		t.Fatalf("CheckVersion() warn = %q, want empty", warn)
	}
}

// TestCheckVersionMinorMismatchWarns checks that a minor mismatch warns
// rather than failing: both directions (client ahead, engine ahead) must
// proceed (err == nil) while still surfacing something a caller can show.
func TestCheckVersionMinorMismatchWarns(t *testing.T) {
	t.Run("client ahead", func(t *testing.T) {
		warn, err := api.CheckVersion(1, 2, 1, 0)
		if err != nil {
			t.Fatalf("CheckVersion() err = %v, want nil (minor mismatch must not fail)", err)
		}
		if warn == "" {
			t.Fatal("CheckVersion() warn = \"\", want a non-empty warning naming the minor mismatch")
		}
	})
	t.Run("engine ahead", func(t *testing.T) {
		warn, err := api.CheckVersion(1, 0, 1, 3)
		if err != nil {
			t.Fatalf("CheckVersion() err = %v, want nil (minor mismatch must not fail)", err)
		}
		if warn == "" {
			t.Fatal("CheckVersion() warn = \"\", want a non-empty warning naming the minor mismatch")
		}
	})
}

// TestCheckVersionMajorMismatchRefuses checks that a major mismatch is
// refused, in BOTH directions, with a distinguishable error a caller can
// act on: not a decode error, not a generic failure.
func TestCheckVersionMajorMismatchRefuses(t *testing.T) {
	t.Run("stale CLI against a newer engine", func(t *testing.T) {
		// The client compiled against protocol v1; the engine speaks v2. The
		// client must be told to upgrade, not shown a JSON decode error.
		_, err := api.CheckVersion(1, 0, 2, 0)
		if err == nil {
			t.Fatal("CheckVersion() err = nil, want a refusal for a major version mismatch")
		}
		var mismatch *api.VersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("CheckVersion() err = %v (%T), want *api.VersionMismatchError", err, err)
		}
		if mismatch.ClientMajor != 1 || mismatch.ServerMajor != 2 {
			t.Fatalf("VersionMismatchError = %+v, want ClientMajor=1 ServerMajor=2", mismatch)
		}
		if !strings.Contains(err.Error(), "upgrade") {
			t.Fatalf("error message %q does not mention upgrading the CLI", err.Error())
		}
	})

	t.Run("new CLI against an old engine", func(t *testing.T) {
		// The other direction: a client built ahead of the engine it is
		// attaching to. Still refused, still a *VersionMismatchError, still
		// naming both sides so a caller can build an accurate message.
		_, err := api.CheckVersion(2, 0, 1, 0)
		if err == nil {
			t.Fatal("CheckVersion() err = nil, want a refusal for a major version mismatch")
		}
		var mismatch *api.VersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("CheckVersion() err = %v (%T), want *api.VersionMismatchError", err, err)
		}
		if mismatch.ClientMajor != 2 || mismatch.ServerMajor != 1 {
			t.Fatalf("VersionMismatchError = %+v, want ClientMajor=2 ServerMajor=1", mismatch)
		}
	})
}

// TestVersionMismatchErrorIsMachineReadable pins the mutation an
// error-string-only implementation would pass by accident: a caller must be
// able to recover the two version numbers programmatically, not just read
// them out of prose. A single-line change that formats a plain error with
// fmt.Errorf (dropping the struct) would fail this test but could still
// pass a test that only checked err != nil or substring-matched the message.
func TestVersionMismatchErrorIsMachineReadable(t *testing.T) {
	_, err := api.CheckVersion(1, 5, 3, 1)
	var mismatch *api.VersionMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v (%T), want *api.VersionMismatchError", err, err)
	}
	if mismatch.ClientMajor != 1 || mismatch.ClientMinor != 5 || mismatch.ServerMajor != 3 || mismatch.ServerMinor != 1 {
		t.Fatalf("VersionMismatchError = %+v, want {1 5 3 1}", mismatch)
	}
}
