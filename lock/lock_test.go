package lock_test

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/lock"
)

const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "senro.lock")
	in := lock.File{Version: lock.Version, Images: map[string]string{
		"golang:1.26":      digest,
		"ghcr.io/acme/t:1": digest,
	}}
	if err := lock.Write(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := lock.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if out.Version != in.Version || len(out.Images) != 2 || out.Images["golang:1.26"] != digest {
		t.Fatalf("round trip changed the file: %+v", out)
	}
}

func TestWriteIsDeterministic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := lock.File{Version: lock.Version, Images: map[string]string{
		"b:1": digest, "a:1": digest, "c:1": digest,
	}}
	var got [][]byte
	for _, name := range []string{"one", "two"} {
		p := filepath.Join(dir, name)
		if err := lock.Write(p, f); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, b)
	}
	if !bytes.Equal(got[0], got[1]) {
		t.Fatal("two writes of the same file differ")
	}
	s := string(got[0])
	if !strings.HasSuffix(s, "}\n") || !strings.Contains(s, "\n  \"") {
		t.Errorf("want two-space indent and trailing newline, got %q", s)
	}
	if strings.Index(s, `"a:1"`) > strings.Index(s, `"b:1"`) || strings.Index(s, `"b:1"`) > strings.Index(s, `"c:1"`) {
		t.Errorf("keys are not sorted: %s", s)
	}
}

func TestReadMissingIsNotExist(t *testing.T) {
	t.Parallel()
	_, err := lock.Read(filepath.Join(t.TempDir(), "nope"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("got %v, want fs.ErrNotExist", err)
	}
}

func TestReadWrongVersionNamesBoth(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "senro.lock")
	if err := os.WriteFile(p, []byte(`{"version": 99, "images": {}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := lock.Read(p)
	if err == nil || !strings.Contains(err.Error(), "99") || !strings.Contains(err.Error(), "1") {
		t.Fatalf("error must name both versions, got %v", err)
	}
}

func TestPinned(t *testing.T) {
	t.Parallel()
	f := lock.File{Version: lock.Version, Images: map[string]string{
		"golang:1.26":          digest,
		"localhost:5000/x:y":   digest,
		"ghcr.io/acme/tool:v1": digest,
	}}
	for ref, want := range map[string]string{
		"golang:1.26":          "golang@" + digest,
		"localhost:5000/x:y":   "localhost:5000/x@" + digest,
		"ghcr.io/acme/tool:v1": "ghcr.io/acme/tool@" + digest,
	} {
		got, ok := f.Pinned(ref)
		if !ok || got != want {
			t.Errorf("Pinned(%q) = %q, %v; want %q", ref, got, ok, want)
		}
	}
	if got, ok := f.Pinned("alpine:3"); ok || got != "" {
		t.Errorf("an unlocked ref was pinned: %q", got)
	}
}

func TestIsDigestRef(t *testing.T) {
	t.Parallel()
	if !lock.IsDigestRef("golang@" + digest) {
		t.Error("digest ref not recognised")
	}
	if lock.IsDigestRef("golang:1.26") {
		t.Error("tag ref taken for a digest ref")
	}
}
