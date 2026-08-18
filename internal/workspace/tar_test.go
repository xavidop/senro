package workspace_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// tree writes files into a fresh directory. Paths use forward slashes; a
// value ending in "/" is a directory, and a value of the form "->target" is
// a symlink.
func tree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		full := filepath.Join(root, filepath.FromSlash(p))
		body := files[p]
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", p, err)
		}
		switch {
		case body == "/":
			if err := os.MkdirAll(full, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", p, err)
			}
		case len(body) > 2 && body[:2] == "->":
			if err := os.Symlink(body[2:], full); err != nil {
				t.Fatalf("symlink %s: %v", p, err)
			}
		default:
			if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
				t.Fatalf("write %s: %v", p, err)
			}
		}
	}
	return root
}

func tarOf(t *testing.T, root string, ex *workspace.Excluder) ([]byte, workspace.Index) {
	t.Helper()
	var buf bytes.Buffer
	ix, err := workspace.WriteTar(&buf, root, ex)
	if err != nil {
		t.Fatalf("WriteTar: %v", err)
	}
	return buf.Bytes(), ix
}

// THE test: an unnormalized tar produces a different digest on every run,
// which silently destroys every cache key downstream of a workspace. Two
// separate WriteTar operations over the same tree must produce
// byte-identical output, and therefore byte-identical digests.
func TestSnapshotDigestIsIdenticalAcrossTwoSeparateOperations(t *testing.T) {
	root := tree(t, map[string]string{
		"go.sum":           "h1:abc\n",
		"cmd/app/main.go":  "package main\n",
		"internal/lib.go":  "package internal\n",
		"internal/nested/": "/",
		"link":             "->go.sum",
	})

	first, ixA := tarOf(t, root, workspace.NewExcluder())
	second, ixB := tarOf(t, root, workspace.NewExcluder())

	if !bytes.Equal(first, second) {
		t.Fatalf("two WriteTar operations over the same tree produced different bytes (%d vs %d)",
			len(first), len(second))
	}
	if cas.FromBytes(first) != cas.FromBytes(second) {
		t.Fatal("byte-equal tars produced different digests, which is impossible and means the comparison above is wrong")
	}
	if len(ixA.Entries) != len(ixB.Entries) {
		t.Errorf("the two indexes disagree on entry count: %d vs %d", len(ixA.Entries), len(ixB.Entries))
	}
}

// The other half of THE test, and the one that names the actual mechanism.
// `go build` rewrites files it did not change. If mtime reached the tar, this
// is the assertion that would fail, and every cache key downstream of a
// workspace would silently stop hitting.
func TestTouchingAnMtimeDoesNotChangeTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"go.sum":          "h1:abc\n",
		"cmd/app/main.go": "package main\n",
	})

	before, _ := tarOf(t, root, workspace.NewExcluder())

	future := time.Now().Add(48 * time.Hour)
	for _, p := range []string{"go.sum", "cmd/app/main.go", "cmd/app", "cmd"} {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(p)), future, future); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}

	after, _ := tarOf(t, root, workspace.NewExcluder())

	if !bytes.Equal(before, after) {
		t.Fatalf("touching mtimes changed the tar: %s then %s.\n"+
			"An unnormalized tar digest that moves when only a file's mtime changes is the "+
			"single most likely way to ship a cache that appears to work and never hits",
			cas.FromBytes(before), cas.FromBytes(after))
	}
}

// The negative half. Without this, a WriteTar that emitted a constant would
// pass both tests above.
func TestChangingContentDoesChangeTheDigest(t *testing.T) {
	root := tree(t, map[string]string{"a.txt": "one\n"})
	before, _ := tarOf(t, root, workspace.NewExcluder())

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("two\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	after, _ := tarOf(t, root, workspace.NewExcluder())

	if bytes.Equal(before, after) {
		t.Fatal("changing a file's content did not change the tar, so the digest is not a content address")
	}
}

func TestChangingAnExecutableBitDoesChangeTheDigest(t *testing.T) {
	root := tree(t, map[string]string{"run.sh": "#!/bin/sh\n"})
	before, _ := tarOf(t, root, workspace.NewExcluder())

	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	after, _ := tarOf(t, root, workspace.NewExcluder())

	if bytes.Equal(before, after) {
		t.Fatal("making a file executable did not change the digest, so a restored workspace could lose the bit undetected")
	}
}

// The check that would actually catch a regression. The two digest tests
// above still pass if somebody drops the normalization and the tree happens
// to be written fast enough that every mtime lands in the same second. This
// one reads the tar and asserts the normalization directly.
func TestEveryTarHeaderIsNormalizedAndNamesAreSorted(t *testing.T) {
	root := tree(t, map[string]string{
		"z.txt":       "z\n",
		"a.txt":       "a\n",
		"m/":          "/",
		"m/inner.txt": "inner\n",
		"run.sh":      "#!/bin/sh\n",
		"link":        "->a.txt",
	})
	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	body, _ := tarOf(t, root, workspace.NewExcluder())

	var names []string
	tr := tar.NewReader(bytes.NewReader(body))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		names = append(names, hdr.Name)

		if !hdr.ModTime.Equal(workspace.Epoch) {
			t.Errorf("%s: ModTime = %v, want the fixed epoch %v", hdr.Name, hdr.ModTime, workspace.Epoch)
		}
		if !hdr.AccessTime.IsZero() {
			t.Errorf("%s: AccessTime = %v, want the zero time so no PAX atime record is written",
				hdr.Name, hdr.AccessTime)
		}
		if !hdr.ChangeTime.IsZero() {
			t.Errorf("%s: ChangeTime = %v, want the zero time so no PAX ctime record is written",
				hdr.Name, hdr.ChangeTime)
		}
		if hdr.Uid != 0 || hdr.Gid != 0 {
			t.Errorf("%s: uid/gid = %d/%d, want 0/0", hdr.Name, hdr.Uid, hdr.Gid)
		}
		if hdr.Uname != "" || hdr.Gname != "" {
			t.Errorf("%s: uname/gname = %q/%q, want empty", hdr.Name, hdr.Uname, hdr.Gname)
		}
		if len(hdr.PAXRecords) != 0 {
			t.Errorf("%s: PAX records %v, want none", hdr.Name, hdr.PAXRecords)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if hdr.Mode != 0o755 {
				t.Errorf("%s: dir mode %o, want 755", hdr.Name, hdr.Mode)
			}
		case tar.TypeSymlink:
			// mode is not meaningful for a symlink in tar; nothing to assert.
		default:
			if hdr.Mode != 0o644 && hdr.Mode != 0o755 {
				t.Errorf("%s: file mode %o, want 644 or 755", hdr.Name, hdr.Mode)
			}
		}
	}

	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for i := range names {
		if names[i] != sorted[i] {
			t.Fatalf("tar entries are not in lexicographic order:\n got %v\nwant %v", names, sorted)
		}
	}
}

func TestWriteTarBuildsAnIndexWithPerFileDigests(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt": "hello",
		"d/":    "/",
		"l":     "->a.txt",
	})
	_, ix := tarOf(t, root, workspace.NewExcluder())

	byPath := map[string]workspace.Entry{}
	for _, e := range ix.Entries {
		byPath[e.Path] = e
	}
	if e := byPath["a.txt"]; e.Digest != cas.FromBytes([]byte("hello")) || e.Size != 5 {
		t.Errorf("a.txt entry = %+v, want size 5 and the sha256 of its content", e)
	}
	if e := byPath["d"]; e.Digest != "" || e.Mode != 0o755 {
		t.Errorf("directory entry = %+v, want no digest and mode 755", e)
	}
	if e := byPath["l"]; e.Link != "a.txt" || e.Digest != "" {
		t.Errorf("symlink entry = %+v, want Link=a.txt and no digest", e)
	}
	if ix.Version != workspace.IndexVersion {
		t.Errorf("index version = %d, want %d", ix.Version, workspace.IndexVersion)
	}
}

func TestWriteTarHonoursTheExcluder(t *testing.T) {
	root := tree(t, map[string]string{
		"keep.go":             "package a\n",
		"skip.tmp":            "junk\n",
		".git/config":         "[core]\n",
		"node_modules/x/i.js": "1\n",
	})
	ex := workspace.NewExcluder(append([]string{"**/*.tmp"}, workspace.DefaultExcludes...)...)
	_, ix := tarOf(t, root, ex)

	for _, e := range ix.Entries {
		switch e.Path {
		case "skip.tmp", ".git", ".git/config", "node_modules", "node_modules/x", "node_modules/x/i.js":
			t.Errorf("excluded path %q reached the index", e.Path)
		}
	}
	var found bool
	for _, e := range ix.Entries {
		if e.Path == "keep.go" {
			found = true
		}
	}
	if !found {
		t.Error("the excluder dropped a file it was not asked to drop")
	}
}

func TestWriteTarSkipsIrregularFilesRatherThanFailing(t *testing.T) {
	root := tree(t, map[string]string{"ok.txt": "x\n"})
	fifo := filepath.Join(root, "pipe")
	if err := mkfifo(fifo); err != nil {
		t.Skipf("cannot create a fifo here: %v", err)
	}
	_, ix := tarOf(t, root, workspace.NewExcluder())
	for _, e := range ix.Entries {
		if e.Path == "pipe" {
			t.Error("a fifo reached the index; only regular files, directories and symlinks are portable")
		}
	}
}

// Restore is the other half. Restoring a snapshot and snapshotting the result
// must reproduce the digest exactly, which is the only end-to-end statement
// that both halves agree on the same normalization.
func TestReadTarThenWriteTarReproducesTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt":   "a\n",
		"d/b.txt": "b\n",
		"run.sh":  "#!/bin/sh\n",
		"l":       "->a.txt",
	})
	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	body, _ := tarOf(t, root, workspace.NewExcluder())

	dest := t.TempDir()
	if _, err := workspace.ReadTar(bytes.NewReader(body), dest); err != nil {
		t.Fatalf("ReadTar: %v", err)
	}
	again, _ := tarOf(t, dest, workspace.NewExcluder())

	if !bytes.Equal(body, again) {
		t.Fatalf("snapshot(restore(snapshot(tree))) changed the digest: %s then %s",
			cas.FromBytes(body), cas.FromBytes(again))
	}
}

// Untrusted input. A workspace tarball comes from a previous run and, once one exists,
// from a shared cache backend, so a path that escapes the destination is a
// remote write primitive.
func TestReadTarRejectsPathsThatEscapeTheDestination(t *testing.T) {
	for _, tc := range []struct {
		name string
		hdr  tar.Header
	}{
		{"parent traversal", tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644}},
		{"absolute path", tar.Header{Name: "/etc/escape", Typeflag: tar.TypeReg, Mode: 0o644}},
		{"nested traversal", tar.Header{Name: "a/../../escape", Typeflag: tar.TypeReg, Mode: 0o644}},
		{"symlink out", tar.Header{Name: "l", Typeflag: tar.TypeSymlink, Linkname: "../../etc/passwd"}},
		{"absolute symlink", tar.Header{Name: "l", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			hdr := tc.hdr
			hdr.ModTime = workspace.Epoch
			if err := tw.WriteHeader(&hdr); err != nil {
				t.Fatalf("WriteHeader: %v", err)
			}
			if err := tw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			dest := t.TempDir()
			_, err := workspace.ReadTar(bytes.NewReader(buf.Bytes()), dest)
			if !errors.Is(err, workspace.ErrUnsafePath) {
				t.Errorf("ReadTar over %s = %v, want ErrUnsafePath", tc.name, err)
			}
		})
	}
}
