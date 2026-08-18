package workspace_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/workspace"
)

func TestExcluderMatchesGlobsIncludingDoubleStar(t *testing.T) {
	ex := workspace.NewExcluder("**/*.tmp", "build/", "top.log", "a/?/c")
	for _, tc := range []struct {
		path  string
		isDir bool
		want  bool
	}{
		{"x.tmp", false, true},
		{"deep/nested/x.tmp", false, true},
		{"x.tmpx", false, false},
		{"build", true, true},
		{"build/out", false, true},
		{"rebuild", true, false},
		{"top.log", false, true},
		{"sub/top.log", false, false},
		{"a/b/c", false, true},
		{"a/bb/c", false, false},
		{"src/main.go", false, false},
	} {
		if got := ex.Match(tc.path, tc.isDir); got != tc.want {
			t.Errorf("Match(%q, isDir=%v) = %v, want %v", tc.path, tc.isDir, got, tc.want)
		}
	}
}

// The negative case that matters most: an excluder with no patterns must
// exclude nothing at all. An excluder that quietly matched everything would
// produce an empty, stable, wrong workspace digest, which is the failure
// mode this whole package exists to prevent, only louder.
func TestAnEmptyExcluderMatchesNothing(t *testing.T) {
	ex := workspace.NewExcluder()
	for _, p := range []string{"a", "a/b", ".git", "anything at all"} {
		if ex.Match(p, false) {
			t.Errorf("an empty excluder matched %q", p)
		}
	}
}

func TestDefaultExcludesCoverTheDirectoriesTheDesignNames(t *testing.T) {
	ex := workspace.NewExcluder(workspace.DefaultExcludes...)
	for _, tc := range []struct {
		path  string
		isDir bool
	}{
		{".git", true},
		{".git/config", false},
		{"node_modules", true},
		{"node_modules/left-pad/index.js", false},
		{"pkg/node_modules", true},
	} {
		if !ex.Match(tc.path, tc.isDir) {
			t.Errorf("DefaultExcludes did not match %q", tc.path)
		}
	}
	if ex.Match("src/main.go", false) {
		t.Error("DefaultExcludes matched an ordinary source file")
	}
}

// TestDefaultExcludesForPreservesNodeModulesOnlyWhenAsked is
// senro.PreserveSymlinks's mechanism: DefaultExcludesFor(true) keeps the
// nested "node_modules" directories a pnpm symlink tree depends on, and
// DefaultExcludesFor(false) must be byte-for-byte DefaultExcludes so every
// other workspace behaves exactly as before.
func TestDefaultExcludesForPreservesNodeModulesOnlyWhenAsked(t *testing.T) {
	narrow := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	if !narrow.Match("node_modules", true) {
		t.Error("DefaultExcludesFor(false) does not exclude node_modules, want it to (the ordinary default)")
	}
	if !narrow.Match(".git", true) {
		t.Error("DefaultExcludesFor(false) does not exclude .git")
	}

	wide := workspace.NewExcluder(workspace.DefaultExcludesFor(true)...)
	if wide.Match("node_modules", true) {
		t.Error("DefaultExcludesFor(true) still excludes node_modules; a preserved workspace must keep it, " +
			"since that is exactly where a symlink's own target lives")
	}
	if !wide.Match(".git", true) {
		t.Error("DefaultExcludesFor(true) stopped excluding .git, which PreserveSymlinks has nothing to do with")
	}
}

// TestDefaultExcludesForFalseIsExactlyDefaultExcludes pins that the narrow
// case is not merely equivalent but IDENTICAL to the slice every existing
// caller already uses, so ordinary workspaces are unaffected by
// PreserveSymlinks.
func TestDefaultExcludesForFalseIsExactlyDefaultExcludes(t *testing.T) {
	got := workspace.DefaultExcludesFor(false)
	if len(got) != len(workspace.DefaultExcludes) {
		t.Fatalf("DefaultExcludesFor(false) = %v, want exactly DefaultExcludes %v", got, workspace.DefaultExcludes)
	}
	for i := range got {
		if got[i] != workspace.DefaultExcludes[i] {
			t.Errorf("DefaultExcludesFor(false)[%d] = %q, want %q", i, got[i], workspace.DefaultExcludes[i])
		}
	}
}

// MatchGlob exists so internal/cache's input selection and workspace
// exclusion agree on what a pattern means. This checks the claim is true
// rather than aspirational: an Excluder built from the same patterns must
// give the same verdict as MatchGlob at several depths, including the
// bare-pattern case where the two could most easily diverge.
func TestMatchGlobAgreesWithExcluderMatchOnEveryPattern(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		rel     string
	}{
		{"top.log", "top.log"},
		{"top.log", "sub/top.log"}, // bare pattern: must NOT reach into a subdirectory
		{"**/*.tmp", "deep/nested/x.tmp"},
		{"a/?/c", "a/b/c"},
		{"src/main.go", "src/main.go"},
		{"src/main.go", "other/src/main.go"},
	} {
		want := workspace.NewExcluder(tc.pattern).Match(tc.rel, false)
		got := workspace.MatchGlob(tc.pattern, tc.rel)
		if got != want {
			t.Errorf("MatchGlob(%q, %q) = %v, but Excluder.Match disagreed with %v",
				tc.pattern, tc.rel, got, want)
		}
	}
}

func TestLoadIgnoreFileReadsPatternsAndSkipsCommentsAndBlanks(t *testing.T) {
	root := t.TempDir()
	body := "# a comment\n\n  *.tmp  \nbuild/\n"
	if err := os.WriteFile(filepath.Join(root, workspace.IgnoreFile), []byte(body), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := workspace.LoadIgnoreFile(root)
	if err != nil {
		t.Fatalf("LoadIgnoreFile: %v", err)
	}
	want := []string{"*.tmp", "build/"}
	if len(got) != len(want) {
		t.Fatalf("LoadIgnoreFile = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pattern %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLoadIgnoreFileIsNotAnErrorWhenThereIsNoFile(t *testing.T) {
	got, err := workspace.LoadIgnoreFile(t.TempDir())
	if err != nil {
		t.Fatalf("LoadIgnoreFile on a workspace with no ignore file: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("LoadIgnoreFile invented %d patterns", len(got))
	}
}

// A negation pattern is the one piece of .gitignore syntax that changes what
// an earlier pattern means, and half-supporting it is how a file silently
// enters or leaves a snapshot. senro refuses it by name rather than ignoring
// the "!" and matching the rest of the line as a literal.
func TestLoadIgnoreFileRejectsNegationRatherThanMisreadingIt(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, workspace.IgnoreFile), []byte("!keep.txt\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := workspace.LoadIgnoreFile(root); err == nil {
		t.Error("a negation pattern was accepted; negation is not implemented and must be refused by name")
	}
}
