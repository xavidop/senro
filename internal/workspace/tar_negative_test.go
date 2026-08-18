package workspace_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/workspace"
)

// tar_test.go covers the digest-stability spine. This file covers five
// additional shapes: an empty directory, a symlink (already in
// tar_test.go), a file with no read permission, a name needing escaping,
// and a deeply nested tree. A sixth test below is a regression test for a
// real ordering bug in collect().

// An empty directory carries no content of its own, but it is still part of
// a tree's shape: a build step that expects a directory to exist (even
// empty) must see it after a restore.
func TestWriteTarPreservesAnEmptyDirectory(t *testing.T) {
	root := tree(t, map[string]string{
		"empty/": "/",
	})
	body, ix := tarOf(t, root, workspace.NewExcluder())

	var found bool
	for _, e := range ix.Entries {
		if e.Path == "empty" {
			found = true
			if e.Mode != 0o755 {
				t.Errorf("empty dir entry mode = %o, want 755", e.Mode)
			}
			if e.Digest != "" {
				t.Errorf("empty dir entry has a digest %q, want none", e.Digest)
			}
		}
	}
	if !found {
		t.Fatal("an empty directory did not reach the index")
	}

	dest := t.TempDir()
	if _, err := workspace.ReadTar(bytes.NewReader(body), dest); err != nil {
		t.Fatalf("ReadTar: %v", err)
	}
	fi, err := os.Stat(filepath.Join(dest, "empty"))
	if err != nil {
		t.Fatalf("restored tree is missing the empty directory: %v", err)
	}
	if !fi.IsDir() {
		t.Fatal("restored 'empty' is not a directory")
	}
}

// A file WriteTar cannot read is a hard error, not a silent omission. A tar
// that quietly drops content it promised to carry produces a workspace
// digest for a tree that is not the one on disk, which is a worse failure
// than WriteTar simply refusing.
func TestWriteTarFailsRatherThanSilentlyDroppingAnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not block reads")
	}
	root := tree(t, map[string]string{"secret.txt": "shh\n"})
	full := filepath.Join(root, "secret.txt")
	if err := os.Chmod(full, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(full, 0o644) })

	var buf bytes.Buffer
	if _, err := workspace.WriteTar(&buf, root, workspace.NewExcluder()); err == nil {
		t.Fatal("WriteTar silently produced a tar over a file it could not read")
	}
}

// A name long enough to need a PAX extended header, with characters (spaces,
// non-ASCII, parentheses) that need escaping in other contexts, must still
// round-trip exactly and digest identically across two runs. The escaping
// itself is deterministic given the same name, so it does not reintroduce
// the machine-dependence this whole package exists to remove.
func TestWriteTarHandlesANameThatNeedsPaxEscaping(t *testing.T) {
	long := strings.Repeat("this-directory-name-is-long/", 5) + "the file (needs éscaping).txt"
	root := tree(t, map[string]string{long: "content\n"})

	first, ixA := tarOf(t, root, workspace.NewExcluder())
	second, _ := tarOf(t, root, workspace.NewExcluder())
	if !bytes.Equal(first, second) {
		t.Fatal("a name long or unusual enough to need PAX escaping still moved the digest between two identical snapshots")
	}

	var found bool
	for _, e := range ixA.Entries {
		if e.Path == long {
			found = true
		}
	}
	if !found {
		t.Fatalf("the long/unusual name did not survive into the index: %+v", ixA.Entries)
	}

	dest := t.TempDir()
	if _, err := workspace.ReadTar(bytes.NewReader(first), dest); err != nil {
		t.Fatalf("ReadTar: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, filepath.FromSlash(long))); err != nil {
		t.Fatalf("restored tree is missing the long/unusual name: %v", err)
	}
}

// A deep tree exercises the same normalization at scale: every directory on
// the path to the leaf gets its own header, and the digest still has to be
// stable across two runs and survive a round trip.
func TestWriteTarHandlesADeeplyNestedTree(t *testing.T) {
	const depth = 30
	segs := make([]string, depth)
	for i := range segs {
		segs[i] = fmt.Sprintf("level%02d", i)
	}
	deep := strings.Join(segs, "/") + "/leaf.txt"
	root := tree(t, map[string]string{deep: "leaf\n"})

	first, ixA := tarOf(t, root, workspace.NewExcluder())
	second, _ := tarOf(t, root, workspace.NewExcluder())
	if !bytes.Equal(first, second) {
		t.Fatal("a deeply nested tree produced a different digest across two snapshots")
	}
	if len(ixA.Entries) != depth+1 {
		t.Fatalf("index has %d entries, want %d (one per directory level plus the leaf file)", len(ixA.Entries), depth+1)
	}

	dest := t.TempDir()
	if _, err := workspace.ReadTar(bytes.NewReader(first), dest); err != nil {
		t.Fatalf("ReadTar: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, filepath.FromSlash(deep))); err != nil {
		t.Fatalf("restored tree is missing the deeply nested leaf: %v", err)
	}
}

// Regression test: collect() must sort by the name an entry gets IN THE TAR
// (a directory carries a trailing "/"), not its bare relative path. '.' is
// 0x2E and '/' is 0x2F, so "m.util" sorts between bare "m" and
// "m/inner.txt" but BEFORE tar-name "m/": sorting bare paths and appending
// the slash afterward writes this tree out of lexicographic order.
func TestTarNamesSortCorrectlyWhenADirectoryHasADotNamedSibling(t *testing.T) {
	root := tree(t, map[string]string{
		"m/inner.txt": "inner\n",
		"m.util":      "sibling\n",
	})
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
	}

	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for i := range names {
		if names[i] != sorted[i] {
			t.Fatalf("tar entries are not in lexicographic order when a directory has a dot-named sibling:\n got  %v\nwant %v",
				names, sorted)
		}
	}
}
