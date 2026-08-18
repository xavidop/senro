// Package glob discovers units by matching paths, with no dependency graph.
//
// It is the unit graph for a tree whose layout says everything there is to
// say: one step per directory matching a pattern, and no more. A path does
// not say what it imports, so an expansion over it always covers every unit.
//
// senro.ExpandBuilder.Affected over a glob graph is REFUSED at plan time,
// with an error naming the graph, instead of quietly returning every unit:
// an expansion that ran everything would look exactly like one that had
// computed a real affected set, and a CI that cannot tell those apart will
// eventually trust a green build that skipped the unit a change broke. For
// an affected set, use gowork, cargo, jswork, maven or gradle (all under
// github.com/xavidop/senro/unit); unit/pyproject and unit/bazel decline for
// the same reason this package does.
//
// Patterns use senro's own syntax: "*" and "?" match within a path segment,
// "**" spans segments, and matching is against the slash-separated path
// relative to the root, on every platform.
package glob

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/internal/workspace"
)

// Unit is the value a template receives, re-exported so a pipeline never has
// to name an internal package.
type Unit = unit.Unit

type graph struct {
	pattern string
	dirs    bool
}

// Dirs makes one unit of every DIRECTORY matching pattern:
//
//	glob.Dirs("apps/*")
func Dirs(pattern string) unit.Graph { return graph{pattern: pattern, dirs: true} }

// Files makes one unit of the DIRECTORY CONTAINING every file matching
// pattern, which is how a repository usually marks a unit:
//
//	glob.Files("services/*/go.mod")
//	glob.Files("**/package.json")
//
// Two matches in one directory produce one unit, not two.
func Files(pattern string) unit.Graph { return graph{pattern: pattern} }

func (g graph) Describe() string {
	if g.dirs {
		return "glob dirs " + g.pattern
	}
	return "glob files " + g.pattern
}

// Units walks root once, pruning the mandatory excludes.
//
// The walk is pruned rather than filtered: descending into node_modules in a
// monorepo is seconds of syscalls for results that are then thrown away, and
// "**/package.json" over an installed tree would otherwise produce one unit
// per dependency, turning a bad glob into tens of thousands of nodes.
func (g graph) Units(ctx context.Context, root string) ([]unit.Unit, error) {
	if fi, err := os.Stat(root); err != nil {
		return nil, fmt.Errorf("glob: %s: %w", g.Describe(), err)
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("glob: %s: %s is not a directory", g.Describe(), root)
	}
	ex := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	seen := make(map[string]bool)

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if ex.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() != g.dirs {
			return nil
		}
		if !workspace.MatchGlob(g.pattern, rel) {
			return nil
		}
		dir := rel
		if !g.dirs {
			dir = path.Dir(rel)
		}
		seen[dir] = true
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("glob: %s: %w", g.Describe(), err)
	}

	out := make([]unit.Unit, 0, len(seen))
	for dir := range seen {
		out = append(out, unit.Unit{ID: dir, Name: dir, Dir: dir})
	}
	// Sorted, always: an expander that returns a nondeterministic order is a
	// bug, and map iteration is exactly that.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
