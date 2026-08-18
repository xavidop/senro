package oci

import (
	"net/http"
	"testing"
)

// A 4xx is normally final: "your request is wrong and will be wrong next
// time". These three codes on a manifest PUT are the one exception, because
// each says the registry does not yet hold a blob the manifest names and the
// writer putting it there is another machine doing the same work.
//
// The scoping is the point of this test. A blanket 400 retry would make an
// actually malformed manifest fail three times slowly instead of once, and
// would retry a bad blob upload for no reason.
func TestOnlyAManifestWriteRetriesARacedWriter(t *testing.T) {
	manifest := request{method: http.MethodPut, what: "manifest senro-v1-action-sha256-abc"}
	blob := request{method: http.MethodPut, what: "blob sha256:abc"}
	get := request{method: http.MethodGet, what: "manifest senro-v1-action-sha256-abc"}

	for _, tc := range []struct {
		name string
		req  request
		code string
		want bool
	}{
		{"a manifest racing a blob commit", manifest, "DIGEST_INVALID", true},
		{"a manifest naming an absent blob", manifest, "BLOB_UNKNOWN", true},
		{"a manifest naming an absent blob, the other spelling", manifest, "MANIFEST_BLOB_UNKNOWN", true},

		{"a manifest that is simply invalid", manifest, "MANIFEST_INVALID", false},
		{"a manifest refused by name", manifest, "NAME_UNKNOWN", false},
		{"a manifest refused outright", manifest, "DENIED", false},

		// The same codes on anything that is not a manifest write. A blob
		// upload has its own whole-upload retry and must not also be retried
		// here, and a read that got one of these is reporting a fact.
		{"a blob upload", blob, "DIGEST_INVALID", false},
		{"a manifest read", get, "DIGEST_INVALID", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := racedWithAnotherWriter(tc.req, tc.code); got != tc.want {
				t.Errorf("racedWithAnotherWriter(%s %q, %q) = %v, want %v",
					tc.req.method, tc.req.what, tc.code, got, tc.want)
			}
		})
	}
}
