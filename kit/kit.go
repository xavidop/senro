// Package kit holds what every preset step shares: the shape of a match, where
// a report is written, and how a repository is walked to find matches.
//
// A kit is a preset step for one tool (go test, pytest, ESLint, Playwright)
// that knows where in a repository it applies. Each one answers the same two
// questions, "where do I apply" and "where do I leave my report", so the
// answers live here once instead of being copied into four packages.
//
// This package and everything under kit/internal use the standard library
// only. A kit imports them; they never import a kit or the rest of senro.
package kit

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Match is one place a kit applies: the kit's name, the directory it applies
// to relative to the repository root with forward slashes ("." for the root),
// and a short sentence on why it matched, such as the file that gave it away.
// Reason is for people reading `senro init` output; nothing branches on it.
type Match struct {
	Kit, Dir, Reason string
}

// ReportDir is the directory, relative to a step's working directory, where
// kits write their JUnit reports. It is hidden so a report never looks like
// source, and it is skipped by Walk for the same reason.
const ReportDir = ".senro"

// ReportPath is where the kit called kitName writes its JUnit report,
// relative to the step's working directory. The path is slash-joined on every
// platform because it is handed to a tool as an argument, not opened by us.
func ReportPath(kitName string) string {
	return ReportDir + "/" + kitName + ".xml"
}

// skipped names the directories no kit should look inside: dependencies,
// vendored code, fixtures and build output. A match found in one of them
// would be a project nobody owns.
var skipped = map[string]bool{
	"node_modules":  true,
	"vendor":        true,
	"testdata":      true,
	"venv":          true,
	"env":           true,
	"build":         true,
	"dist":          true,
	"target":        true,
	"site-packages": true,
}

func skipDir(name string) bool {
	return skipped[name] || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// Walk calls fn for root (rel ".") and for every directory below it that is
// not skipped, in lexical order. rel is slash-separated whatever the
// platform. Returning fs.SkipDir from fn skips that directory's subtree, and
// any other error stops the walk and is returned.
//
// Directories named node_modules, vendor, testdata, venv, env, build, dist,
// target or site-packages are skipped, as is any directory whose name starts
// with "." or "_". The root is always visited, even if its own name would
// have been skipped: the caller chose it.
//
// A directory that cannot be read, such as one without read permission, is
// still passed to fn but its contents are skipped rather than ending the
// walk: one locked directory should not hide every other match in the
// repository. A root that does not exist, or is not a directory, is an error
// naming it, and fn is not called.
func Walk(root string, fn func(dir, rel string) error) error {
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("kit: walk %s: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("kit: walk %s: not a directory", root)
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// WalkDir reports a failed ReadDir by calling again with the
			// directory's entry and the error; the first call already
			// visited it.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if path == root {
			return fn(path, ".")
		}
		if skipDir(d.Name()) {
			return fs.SkipDir
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		return fn(path, filepath.ToSlash(rel))
	})
}

// FirstFile returns the first of names that exists in dir as a regular file,
// in the order given, and true. It returns "" and false when none does. The
// name is returned bare, not joined to dir, so it can go straight into a
// Match's Reason.
func FirstFile(dir string, names ...string) (string, bool) {
	for _, n := range names {
		if info, err := os.Stat(filepath.Join(dir, n)); err == nil && info.Mode().IsRegular() {
			return n, true
		}
	}
	return "", false
}

// UnitDir returns the working directory a step for a unit at dir should
// declare: "" for "" and ".", which both mean the repository root and need no
// WorkDir, and dir itself otherwise.
func UnitDir(dir string) string {
	if dir == "." {
		return ""
	}
	return dir
}

// SortMatches orders ms by kit name, then directory, so the same repository
// always yields the same list.
func SortMatches(ms []Match) {
	sort.Slice(ms, func(i, j int) bool {
		if ms[i].Kit != ms[j].Kit {
			return ms[i].Kit < ms[j].Kit
		}
		return ms[i].Dir < ms[j].Dir
	})
}
