package workspace_test

import (
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// reg builds a regular-file entry the way WriteTar would: a normalized mode,
// a size, and a digest over the content.
func reg(path string, mode uint32, content string) workspace.Entry {
	return workspace.Entry{
		Path:   path,
		Mode:   mode,
		Size:   int64(len(content)),
		Digest: cas.FromBytes([]byte(content)),
	}
}

// dir and link build the other two entry shapes a snapshot can hold. Both
// carry the fixed modes normalize() assigns, because nothing else ever
// reaches an index.
func dirEntry(path string) workspace.Entry {
	return workspace.Entry{Path: path, Mode: 0o755}
}

func linkEntry(path, target string) workspace.Entry {
	return workspace.Entry{Path: path, Mode: 0o777, Link: target}
}

func index(entries ...workspace.Entry) workspace.Index {
	return workspace.Index{Version: workspace.IndexVersion, Entries: entries}
}

// byPath turns a change list into a lookup, so a test asserts on the change
// for one path rather than on a slice position that would move whenever an
// unrelated case is added.
func byPath(t *testing.T, changes []workspace.Change) map[string]workspace.Change {
	t.Helper()
	out := make(map[string]workspace.Change, len(changes))
	for _, c := range changes {
		if _, dup := out[c.Path]; dup {
			t.Fatalf("Diff reported %q twice: %v", c.Path, changes)
		}
		out[c.Path] = c
	}
	return out
}

// This is the whole reason the index exists as an object separate from the
// tarball: added, removed and content-changed are all answerable from two
// indexes, with no body downloaded on either side.
func TestDiffReportsAddedRemovedAndContentChanges(t *testing.T) {
	a := index(
		reg("keep.txt", 0o644, "same"),
		reg("main.go", 0o644, "package main\n"),
		reg("gone.txt", 0o644, "bye"),
	)
	b := index(
		reg("keep.txt", 0o644, "same"),
		reg("main.go", 0o644, "package main // changed\n"),
		reg("out.txt", 0o644, "compiled\n"),
	)

	got := byPath(t, workspace.Diff(a, b))
	if len(got) != 3 {
		t.Fatalf("want exactly three changes (one added, one removed, one modified), got %v", got)
	}
	if s := got["out.txt"].Status; s != workspace.Added {
		t.Errorf("out.txt: status = %q, want %q", s, workspace.Added)
	}
	if s := got["gone.txt"].Status; s != workspace.Removed {
		t.Errorf("gone.txt: status = %q, want %q", s, workspace.Removed)
	}
	if s := got["main.go"].Status; s != workspace.Modified {
		t.Errorf("main.go: status = %q, want %q", s, workspace.Modified)
	}
	if _, unchangedReported := got["keep.txt"]; unchangedReported {
		t.Errorf("a path identical on both sides must not appear in the change list: %v", got)
	}
	// Both sides are carried so a caller can print sizes and digests without
	// going back to either index.
	if got["main.go"].A.Digest == "" || got["main.go"].B.Digest == "" {
		t.Errorf("a modified change carries neither side's entry: %+v", got["main.go"])
	}
	if got["out.txt"].A.Path != "" {
		t.Errorf("an added change must have no A side: %+v", got["out.txt"])
	}
	if got["gone.txt"].B.Path != "" {
		t.Errorf("a removed change must have no B side: %+v", got["gone.txt"])
	}
}

// chmod +x is a real change to the tree and one of the easiest to miss by
// eye, since the content digest does not move. The executable bit is also
// the ONLY permission a snapshot carries, so if this is not reported the
// mode a snapshot records is unobservable through diff entirely.
func TestDiffReportsAModeChangeOnIdenticalContent(t *testing.T) {
	a := index(reg("build.sh", 0o644, "#!/bin/sh\n"))
	b := index(reg("build.sh", 0o755, "#!/bin/sh\n"))

	got := byPath(t, workspace.Diff(a, b))
	c, ok := got["build.sh"]
	if !ok {
		t.Fatalf("a file that became executable was reported as unchanged: %v", got)
	}
	if c.Status != workspace.ModeChanged {
		t.Errorf("status = %q, want %q (the content is byte-identical)", c.Status, workspace.ModeChanged)
	}
}

// A path whose KIND changed is not a content change: "the bytes differ" is
// the wrong thing to tell someone whose file is now a directory.
func TestDiffReportsAKindChangeSeparatelyFromAContentChange(t *testing.T) {
	a := index(reg("thing", 0o644, "hello"), reg("other", 0o644, "hello"))
	b := index(linkEntry("thing", "elsewhere"), dirEntry("other"))

	got := byPath(t, workspace.Diff(a, b))
	for _, p := range []string{"thing", "other"} {
		if got[p].Status != workspace.KindChanged {
			t.Errorf("%s: status = %q, want %q", p, got[p].Status, workspace.KindChanged)
		}
	}
}

// A symlink's target is content as far as a workspace is concerned: it is
// the only thing the entry carries, and repointing it changes what the tree
// resolves to.
func TestDiffReportsARepointedSymlinkAsModified(t *testing.T) {
	a := index(linkEntry("current", "releases/v1"))
	b := index(linkEntry("current", "releases/v2"))

	got := byPath(t, workspace.Diff(a, b))
	c, ok := got["current"]
	if !ok {
		t.Fatalf("a repointed symlink was reported as unchanged: %v", got)
	}
	if c.Status != workspace.Modified {
		t.Errorf("status = %q, want %q", c.Status, workspace.Modified)
	}
	if c.A.Link != "releases/v1" || c.B.Link != "releases/v2" {
		t.Errorf("both targets must be carried for a caller to print them: %+v", c)
	}
}

func TestDiffOfAnIndexWithItselfReportsNothing(t *testing.T) {
	ix := index(reg("a.txt", 0o644, "a"), dirEntry("d"), linkEntry("l", "a.txt"))
	if got := workspace.Diff(ix, ix); len(got) != 0 {
		t.Errorf("an index differs from itself: %v", got)
	}
}

// Marshal sorts on the way out, but an Index handed to Diff by any other
// route need not be sorted, and a diff whose line order depends on the order
// entries happened to be built in is not readable output.
func TestDiffIsOrderedByPathRegardlessOfInputOrder(t *testing.T) {
	a := index(reg("z.txt", 0o644, "z"), reg("a.txt", 0o644, "a"))
	b := index(reg("m.txt", 0o644, "m"), reg("a.txt", 0o644, "changed"))

	got := workspace.Diff(a, b)
	var paths []string
	for _, c := range got {
		paths = append(paths, c.Path)
	}
	want := []string{"a.txt", "m.txt", "z.txt"}
	if len(paths) != len(want) {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("paths = %v, want %v", paths, want)
		}
	}
}

// Kind is what tells a directory apart from an empty regular file in an
// index: a directory has no digest, an empty file has the digest of no
// bytes. Getting this backwards would report every empty file as a kind
// change against itself.
func TestEntryKindDistinguishesFileDirectoryAndSymlink(t *testing.T) {
	cases := []struct {
		name  string
		entry workspace.Entry
		want  workspace.Kind
	}{
		{"regular file", reg("f", 0o644, "x"), workspace.KindFile},
		{"empty regular file", reg("empty", 0o644, ""), workspace.KindFile},
		{"directory", dirEntry("d"), workspace.KindDir},
		{"symlink", linkEntry("l", "d"), workspace.KindSymlink},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.entry.Kind(); got != tc.want {
				t.Errorf("Kind() = %q, want %q", got, tc.want)
			}
		})
	}
}
