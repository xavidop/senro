package workspace_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// The index is stored in the CAS, so its bytes are its address. Marshalling
// must therefore be canonical: the same index must produce the same bytes on
// every machine and every Go release, or ws ls and the digest that names it
// come apart.
func TestIndexMarshalIsCanonicalAndSorted(t *testing.T) {
	ix := workspace.Index{
		Version: workspace.IndexVersion,
		Entries: []workspace.Entry{
			{Path: "b.txt", Mode: 0o644, Size: 1, Digest: cas.FromBytes([]byte("b"))},
			{Path: "a.txt", Mode: 0o644, Size: 1, Digest: cas.FromBytes([]byte("a"))},
			{Path: "a", Mode: 0o755},
		},
	}
	b1, err := ix.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	b2, err := ix.Marshal()
	if err != nil {
		t.Fatalf("Marshal again: %v", err)
	}
	if string(b1) != string(b2) {
		t.Error("two Marshal calls over the same index produced different bytes")
	}

	got := string(b1)
	ia, ib := strings.Index(got, `"a.txt"`), strings.Index(got, `"b.txt"`)
	if ia < 0 || ib < 0 || ia > ib {
		t.Errorf("Marshal did not sort entries by path:\n%s", got)
	}
	if strings.Contains(got, `&`) || strings.Contains(got, `<`) {
		t.Errorf("Marshal HTML-escaped a path, which makes the bytes depend on content:\n%s", got)
	}
}

func TestIndexRoundTrips(t *testing.T) {
	want := workspace.Index{
		Version: workspace.IndexVersion,
		Entries: []workspace.Entry{
			{Path: "dir", Mode: 0o755},
			{Path: "dir/file", Mode: 0o644, Size: 3, Digest: cas.FromBytes([]byte("abc"))},
			{Path: "link", Mode: 0o777, Link: "dir/file"},
		},
	}
	b, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := workspace.UnmarshalIndex(b)
	if err != nil {
		t.Fatalf("UnmarshalIndex: %v", err)
	}
	if len(got.Entries) != len(want.Entries) || got.Version != want.Version {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	for i := range want.Entries {
		if got.Entries[i] != want.Entries[i] {
			t.Errorf("entry %d = %+v, want %+v", i, got.Entries[i], want.Entries[i])
		}
	}
}

func TestUnmarshalIndexRejectsAFutureVersion(t *testing.T) {
	_, err := workspace.UnmarshalIndex([]byte(`{"version":999,"entries":[]}`))
	if err == nil {
		t.Error("an index from a future version was accepted; a reader that guesses at an unknown layout is worse than one that refuses")
	}
}
