package cache_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func seed(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	return root
}

func TestResolveHashesGlobsAndFiles(t *testing.T) {
	root := seed(t, map[string]string{
		"main.go":    "package main\n",
		"pkg/a/a.go": "package a\n",
		"go.sum":     "h1:abc\n",
		"README.md":  "hi\n",
	})
	got, err := cache.Resolve(root, []string{
		artifact.Glob("**/*.go").Serial(),
		artifact.File("go.sum").Serial(),
	}, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := map[string]cas.Digest{
		"main.go":    cas.FromBytes([]byte("package main\n")),
		"pkg/a/a.go": cas.FromBytes([]byte("package a\n")),
		"go.sum":     cas.FromBytes([]byte("h1:abc\n")),
	}
	if len(got) != len(want) {
		t.Fatalf("Resolve returned %d files, want %d: %+v", len(got), len(want), got)
	}
	for _, f := range got {
		if want[f.Path] != f.Digest {
			t.Errorf("%s = %s, want %s", f.Path, f.Digest, want[f.Path])
		}
	}
}

func TestResolveIsSortedAndDeduplicated(t *testing.T) {
	root := seed(t, map[string]string{"b.go": "b\n", "a.go": "a\n"})
	got, err := cache.Resolve(root, []string{
		artifact.Glob("**/*.go").Serial(),
		artifact.File("a.go").Serial(),
	}, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Resolve returned %d files, want 2 after dedup: %+v", len(got), got)
	}
	if got[0].Path != "a.go" || got[1].Path != "b.go" {
		t.Errorf("Resolve is not sorted: %+v", got)
	}
}

// A declared input that selects nothing is a typo, and its consequence is a
// key that cannot change when the sources do. Loud beats silent.
func TestResolveRefusesASelectorThatMatchesNothing(t *testing.T) {
	root := seed(t, map[string]string{"a.go": "a\n"})
	for _, sel := range []string{
		artifact.Glob("**/*.rs").Serial(),
		artifact.File("absent.txt").Serial(),
	} {
		_, err := cache.Resolve(root, []string{sel}, nil)
		if err == nil {
			t.Errorf("Resolve(%q) matched nothing and returned no error", sel)
			continue
		}
		if !strings.Contains(err.Error(), sel) {
			t.Errorf("the error does not name the selector: %v", err)
		}
	}
}

func TestResolveSkipsDirectoriesAndDefaultExclusions(t *testing.T) {
	root := seed(t, map[string]string{
		"a.go":              "a\n",
		".git/objects/x.go": "junk\n",
		"node_modules/m.go": "junk\n",
	})
	got, err := cache.Resolve(root, []string{artifact.Glob("**/*.go").Serial()}, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 1 || got[0].Path != "a.go" {
		t.Errorf("Resolve = %+v, want only a.go: .git and node_modules must not enter a cache key", got)
	}
}

func TestResolveRefusesASelectorThatEscapesTheRoot(t *testing.T) {
	root := seed(t, map[string]string{"a.go": "a\n"})
	if _, err := cache.Resolve(root, []string{artifact.File("../outside.txt").Serial()}, nil); err == nil {
		t.Error("a file selector pointing outside the input root was accepted")
	}
}

// SafeRelative is a security boundary. A raw HasPrefix(rel, "..") test
// would also reject a file merely named with two leading dots ("..keep");
// this pins that such a file is accepted while every actual escape shape
// (leading, trailing and interior "..") is rejected.
func TestResolveAcceptsALeadingDotDotFilenameButRejectsEveryEscapeShape(t *testing.T) {
	root := seed(t, map[string]string{"..keep": "kept\n"})
	if _, err := cache.Resolve(root, []string{artifact.File("..keep").Serial()}, nil); err != nil {
		t.Errorf("a filename that merely starts with \"..\" was rejected as an escape: %v", err)
	}
	for _, escape := range []string{"../outside.txt", "sub/../../outside.txt", "sub/../.."} {
		if err := cache.SafeRelative(escape); err == nil {
			t.Errorf("SafeRelative(%q) did not reject an escaping path", escape)
		}
	}
}
