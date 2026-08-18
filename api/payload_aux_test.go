package api_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
)

// A secret event must never carry a value. Only identity: name, source URI,
// and provider version. This is checked structurally so a future field
// addition cannot quietly introduce one.
func TestSecretResolvedCarriesNoValue(t *testing.T) {
	in := api.SecretResolvedBody{
		Name:    "registry_token",
		Source:  "aws-sm://prod/ci#registry_token",
		Version: "AWSCURRENT",
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Allowlist, not denylist. A denylist of guessed names silently passes a
	// future field called Token or APIKey; enumerating what IS permitted means
	// any new field on this struct fails until someone justifies it.
	allowed := map[string]bool{"name": true, "source": true, "version": true}
	for k := range m {
		if !allowed[k] {
			t.Errorf("secret.resolved carries unexpected field %q — "+
				"identity only, never a value", k)
		}
	}
}

// cache.miss must say what differed, or the cache gets a reputation for being
// broken whether or not it is.
func TestCacheMissNamesTheDifferingComponent(t *testing.T) {
	in := api.CacheMissBody{
		Key:       "4f1c",
		Reason:    "input_changed",
		Differing: "inputDigests",
	}
	b, _ := json.Marshal(in)

	var out api.CacheMissBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Differing != "inputDigests" {
		t.Errorf("Differing = %q", out.Differing)
	}
}

// Every accepted control op is attributed, so the audit trail is complete and
// other attached clients can see who did what.
func TestControlAppliedIsAttributed(t *testing.T) {
	in := api.ControlAppliedBody{
		Op:       "step.retry",
		ClientID: "c7",
		Args:     map[string]string{"step": "build/test"},
	}
	b, _ := json.Marshal(in)

	var out api.ControlAppliedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ClientID == "" {
		t.Error("control.applied must carry the originating client identity")
	}
}

// Digest and Index are separate CAS addresses (the workspace body and its
// file list, stored apart so `ws ls` never pulls the body). A round trip
// that only checked Digest could not tell an implementation that dropped or
// aliased Index apart from one that carries both correctly, so this checks
// both explicitly, and that the two differ, the way two distinct objects
// should.
func TestWSSnapshotRoundTrip(t *testing.T) {
	in := api.WSSnapshotBody{
		Name:   "build",
		Digest: "sha256:ab12",
		Index:  "sha256:cd34",
		Bytes:  4096,
		Files:  12,
	}
	b, _ := json.Marshal(in)

	var out api.WSSnapshotBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.HasPrefix(out.Digest, "sha256:") || out.Files != 12 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
	if !strings.HasPrefix(out.Index, "sha256:") {
		t.Errorf("Index = %q, want a sha256: address", out.Index)
	}
	if out.Index == out.Digest {
		t.Errorf("Index and Digest round-tripped to the same value %q, they must address separate objects", out.Index)
	}
}

// Index is additive within the major version: a decoder that has never
// heard of it must still read every other field of an older event
// correctly, and one that produces events without it (an older engine) must
// not fail a newer client that expects it to be present but empty.
func TestWSSnapshotBodyWithoutIndexStillDecodes(t *testing.T) {
	const old = `{"name":"build","digest":"sha256:ab12","bytes":4096,"files":12}`
	var out api.WSSnapshotBody
	if err := json.Unmarshal([]byte(old), &out); err != nil {
		t.Fatalf("unmarshal pre-Index payload: %v", err)
	}
	if out.Index != "" {
		t.Errorf("Index = %q decoding a payload that never had one, want empty", out.Index)
	}
	if out.Digest != "sha256:ab12" || out.Files != 12 {
		t.Errorf("decoding a pre-Index payload lost other fields: %+v", out)
	}
}

// binary.staged was reserved and emitted by nothing for the whole of v0. It
// has an emit site now, so it belongs in the DECLARED set: the difference is
// not cosmetic, since DeclaredTypes is what the published schema's own
// examples list is checked against, and a type in one and not the other is
// the drift api/event.go's declaredTypes doc records happening once already.
func TestBinaryStagedIsDeclaredRatherThanReserved(t *testing.T) {
	if !api.BinaryStaged.Known() {
		t.Fatal("binary.staged is not a known type")
	}
	var found bool
	for _, tp := range api.DeclaredTypes() {
		if tp == api.BinaryStaged {
			found = true
		}
	}
	if !found {
		t.Error("binary.staged is not in DeclaredTypes(); it has an emit site now")
	}
}

// The staging record is a fact about a host and a file, and must stay that:
// it is persisted, streamed to every client and pasted into bug reports.
func TestBinaryStagedCarriesIdentityAndNothingElse(t *testing.T) {
	in := api.BinaryStagedBody{
		Digest:   "sha256:aa",
		Platform: "linux/arm64",
		Strategy: "cross-build",
		Target:   "build-07.internal",
		Path:     "/home/ci/.senro/bin/senro-sha256-aa",
		Bytes:    41_000_000,
		Reused:   true,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Allowlist rather than denylist, exactly as the secret bodies above: a
	// guessed list of forbidden names silently passes a future field nobody
	// thought to guess.
	allowed := map[string]bool{
		"digest": true, "platform": true, "strategy": true, "target": true,
		"path": true, "bytes": true, "reused": true, "duration_ns": true,
	}
	for k := range m {
		if !allowed[k] {
			t.Errorf("binary.staged carries unexpected field %q", k)
		}
	}

	var out api.BinaryStagedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if out != in {
		t.Errorf("round-tripped as %+v, want %+v", out, in)
	}
}

// reused and duration_ns are omitempty, so a staging that transferred bytes
// says so by their absence rather than by a false a reader has to interpret.
func TestBinaryStagedOmitsReusedWhenItActuallyTransferred(t *testing.T) {
	b, err := json.Marshal(api.BinaryStagedBody{
		Digest: "sha256:aa", Platform: "linux/amd64", Strategy: "identity",
		Target: "h", Path: "/p", Bytes: 1,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), "reused") {
		t.Errorf("marshalled as %s, want no reused field", b)
	}
}
