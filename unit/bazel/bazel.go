// Package bazel discovers the packages of a Bazel workspace.
//
// It finds units and stops there:
//
//	verify.Expand("test", bazel.Packages()).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("bazel", "test", u.Name+"/..."))
//		})
//
// A unit is one Bazel package: its ID and Dir are the directory relative to
// the root, and its Name is the package label ("//apps/web", "//" for the
// root), which is what every bazel command takes.
//
// # It reads the tree, and runs nothing
//
// No bazel is needed and none is run: discovery is a pruned walk for BUILD
// and BUILD.bazel plus one small read of .bazelignore. Shelling out to
// `bazel query` was rejected, although it would answer the affected-set
// question exactly: it is a build, not a query (a JVM, every BUILD file
// evaluated, and under bzlmod repository rules RUN during module
// resolution, which is arbitrary code executing during planning); its
// answer depends on the machine, and "skips cleanly when bazel is absent"
// means the same pipeline computes different sets on CI and a laptop with
// nothing saying which happened; and it could not be tested against
// checked-in fixtures. If your CI already runs bazel and you want its query
// to drive the fan-out, that is a graph of about forty lines in your own
// repository; see https://senro.dev/docs/unit-graphs/.
//
// # It does NOT implement the affected-set interface, deliberately
//
// senro.ExpandBuilder.Affected over this graph is REFUSED at build time,
// as over unit/glob and unit/pyproject. A BUILD file is a Starlark program,
// not a manifest, and its edges routinely do not appear in it: a macro
// computes deps itself, glob() and select() decide from the filesystem and
// configuration, a dep can be a variable or an alias, and edges also come
// from toolchains, implicit rule dependencies and the .bzl files
// themselves. Any one of those is a missing edge; a missing edge is a
// package left out of an affected set; and that is a green build for a
// tree that does not build. An approximation right most of the time is
// worse than none, because nobody can tell the times apart.
//
// So fan out over this graph and run every unit, which is what a Bazel
// repository usually wants anyway: bazel does its own incrementality inside
// each invocation.
//
// # What is in, and what is out
//
// A package whose BUILD file declares no targets, or that only a
// never-taken select() branch reaches, is still a unit: deciding otherwise
// means evaluating the file. Deliberately LEFT OUT are the three
// directories Bazel itself would refuse to build from this root: one
// listed in .bazelignore, one inside a nested repository (see
// repoBoundaries), and one with no BUILD file, which is not a package at
// all.
package bazel

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/internal/workspace"
)

// Unit is the value a template receives, re-exported so a pipeline never has
// to name an internal package.
type Unit = unit.Unit

// The two names a package's own file can have. Bazel accepts either and
// prefers BUILD.bazel when both are present; for this graph they mean the
// same thing, which is that the directory is a package.
var buildFiles = []string{"BUILD.bazel", "BUILD"}

// repoBoundaries are the files that mark a directory as the root of a
// repository, in Bazel's own spelling.
//
// At the ROOT they say "this is a Bazel workspace". BELOW the root they
// mark a DIFFERENT repository whose packages are not this one's, so the
// walk stops: descending would produce units for labels this workspace
// cannot name.
//
// This is the one place the graph could drop a package it should have
// kept: a MODULE.bazel somewhere Bazel does not treat as a boundary
// silently excludes the directory under it. The mitigation is that bazel
// itself would refuse the label, so the missing step is one that would
// have failed anyway.
var repoBoundaries = []string{
	"MODULE.bazel",
	"REPO.bazel",
	"WORKSPACE",
	"WORKSPACE.bazel",
	"WORKSPACE.bzlmod",
}

// ignoreFile is Bazel's own list of directories that are not part of the
// build. Read from the workspace root only, which is where Bazel reads it.
const ignoreFile = ".bazelignore"

// Graph is a bazel unit graph. Build one with Packages.
//
// It implements unit.Graph and NOT unit.Affector; the package doc says why at
// length. There is no assertion here that it does not, because a negative
// assertion is not a thing Go can write; there is a test instead.
//
// One Graph memoizes one walk per root. See gowork's Graph for why the memo
// has no expiry.
type Graph struct {
	mu    sync.Mutex
	cache map[string][]Unit
}

// Packages makes one unit per Bazel package: one directory holding a BUILD or
// BUILD.bazel file, which is Bazel's own definition of the word.
//
// Deliberately not one unit per TARGET. A target set cannot be read off the
// tree: a macro expands to targets whose names it computes, and enumerating
// them means either evaluating Starlark or running bazel. A package is a
// directory, which is a fact, and `bazel test //apps/web/...` covers every
// target in one.
func Packages() *Graph { return &Graph{} }

var _ unit.Graph = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string { return "bazel packages" }

// Units reports every Bazel package under root, sorted by ID.
//
// root must be a workspace root: a directory holding one of the repository
// boundary files (MODULE.bazel for bzlmod, WORKSPACE for the older setup).
// A tree with none of them is an error rather than an empty graph, because
// every label this graph produces is relative to a root, and a root that is
// really a subdirectory would produce labels that resolve to nothing.
//
// A workspace with no BUILD file anywhere is an error too: an expansion that
// silently produced no steps is indistinguishable from one whose root was
// wrong.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("bazel: %w", err)
	}
	// Before the memo, not after it: a build that answered from a dead
	// context would depend on which call happened to warm the cache.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if us, ok := g.cache[abs]; ok {
		return append([]Unit(nil), us...), nil
	}
	us, err := g.discover(ctx, abs)
	if err != nil {
		return nil, err
	}
	if g.cache == nil {
		g.cache = make(map[string][]Unit, 1)
	}
	g.cache[abs] = us
	return append([]Unit(nil), us...), nil
}

func (g *Graph) discover(ctx context.Context, root string) ([]Unit, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("bazel: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("bazel: %s: %s is not a directory", g.Describe(), root)
	}
	if !isRepoRoot(root) {
		return nil, fmt.Errorf("bazel: %s: %s holds none of %s, so it is not a Bazel workspace "+
			"root; point the expansion at one, or use a different unit graph",
			g.Describe(), root, strings.Join(repoBoundaries, ", "))
	}
	ignored, err := readIgnoreFile(root)
	if err != nil {
		return nil, fmt.Errorf("bazel: %s: %w", g.Describe(), err)
	}

	dirs, err := findPackages(ctx, root, ignored)
	if err != nil {
		return nil, err
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("bazel: %s: no %s anywhere under %s, so there is nothing to "+
			"discover; point the expansion at a Bazel workspace or use a different unit graph",
			g.Describe(), strings.Join(buildFiles, " or "), root)
	}
	sort.Strings(dirs)
	units := make([]Unit, 0, len(dirs))
	for _, d := range dirs {
		units = append(units, Unit{ID: d, Name: label(d), Dir: d})
	}
	return units, nil
}

// findPackages walks root for the directories that are Bazel packages.
//
// Pruned three ways, and the walk itself never follows a symlink, which is
// what keeps bazel-bin, bazel-out and the other convenience symlinks at a
// workspace root out of this without having to guess at their names: they are
// links into the output base, and filepath.WalkDir reports a link as a
// non-directory and does not descend.
func findPackages(ctx context.Context, root string, ignored []string) ([]string, error) {
	ex := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			if hasBuildFile(p) {
				out = append(out, rel)
			}
			return nil
		}
		if ex.Match(rel, true) {
			return fs.SkipDir
		}
		// A directory .bazelignore names, and everything under it: Bazel does
		// not look inside one at all.
		if unit.Nearest(ignored, rel) != "" {
			return fs.SkipDir
		}
		// A repository of its own. Its packages are its, not this one's, and
		// the directory holding the boundary file is not this workspace's
		// package either even when it also holds a BUILD file.
		if isRepoRoot(p) {
			return fs.SkipDir
		}
		if hasBuildFile(p) {
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// label is the Bazel package label for a root-relative directory: "//" for
// the root package and "//apps/web" for the rest. It is Unit.Name, because it
// is what every bazel command takes.
func label(dir string) string {
	if dir == "." {
		return "//"
	}
	return "//" + dir
}

func hasBuildFile(dir string) bool {
	for _, name := range buildFiles {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err == nil && !fi.IsDir() {
			return true
		}
	}
	return false
}

// isRepoRoot reports whether dir holds one of Bazel's repository boundary
// files. See repoBoundaries for what that means at the root and below it.
func isRepoRoot(dir string) bool {
	for _, name := range repoBoundaries {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err == nil && !fi.IsDir() {
			return true
		}
	}
	return false
}

// readIgnoreFile reads .bazelignore: one directory path per line, relative to
// the workspace root, with "#" comments and blank lines skipped. No globs;
// Bazel does not support them there, and pretending otherwise would drop
// directories nobody asked to drop.
//
// A missing file is not an error, which is the ordinary case. A path that
// climbs out of the root is dropped rather than honoured, because there is
// nothing outside the root for the walk to skip.
//
// Returned longest-first, ready for unit.Nearest, so an entry matches a
// directory and everything under it and never a directory whose NAME merely
// starts with the same letters.
func readIgnoreFile(root string) ([]string, error) {
	body, err := os.ReadFile(filepath.Join(root, ignoreFile)) // #nosec G304 -- a fixed name under the caller's own root
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", ignoreFile, err)
	}
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if rel, ok := unit.CleanRel(path.Clean(filepath.ToSlash(line))); ok {
			out = append(out, rel)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", ignoreFile, err)
	}
	return unit.LongestFirst(out), nil
}
