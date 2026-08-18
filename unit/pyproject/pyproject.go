// Package pyproject discovers the distributions of a Python monorepo.
//
// It finds units and stops there:
//
//	verify.Expand("test", pyproject.Packages()).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("pytest")).WorkDir(u.Dir)
//		})
//
// A unit is one distribution: its ID and Dir are the directory relative to the
// root, and its Name is the distribution name, which is what `uv run
// --package` and `pip install -e` want.
//
// # It does NOT implement the affected-set interface, deliberately
//
// senro.ExpandBuilder.Affected over this graph is REFUSED at build time,
// the way it is over unit/glob. A judgement about Python, not an unfinished
// feature: the manifests do carry dependency declarations, but in Python a
// declaration is not what makes an import work.
//
//   - `uv sync` installs EVERY workspace member into one virtual
//     environment (`pip install -e` does the same by hand), so a package
//     can `import acme_core` with nothing about acme-core in its own
//     pyproject.toml and it works for years, undetected: no node_modules
//     to isolate it, no compiler to refuse it.
//   - A src layout on PYTHONPATH, a conftest.py, a pytest plugin loaded by
//     entry point and an import inside a function are four more ways a
//     real dependency exists with nothing static to read.
//   - `[project] dynamic = ["dependencies"]` says outright that the
//     dependencies are computed by the build backend at build time.
//   - The four tools in common use disagree about where an
//     intra-repository dependency is even written: [tool.uv.sources],
//     [tool.poetry.dependencies], [project] dependencies, or a setup.py
//     argument only running Python can read.
//
// Any one of those is a missing edge; a missing edge is a distribution
// left out of an affected set; and that is a green build for a tree that
// does not work. Refusing to answer is recoverable; answering wrongly is
// not.
//
// So fan out over this graph and run every unit. If your monorepo
// genuinely declares and enforces every intra-repository dependency, that
// fact lives in your repository and not in this package, and you can write
// a graph that knows it: see https://senro.dev/docs/unit-graphs/.
//
// # It reads manifests, and runs nothing
//
// No uv, poetry, hatch or python is needed, and none is run. Discovery is
// a pruned walk and a parse per manifest, honouring the context.
package pyproject

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/xavidop/senro/internal/toml"
	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/internal/workspace"
)

// Unit is the value a template receives, re-exported so a pipeline never has
// to name an internal package.
type Unit = unit.Unit

const (
	pyprojectFile = "pyproject.toml"
	setupPy       = "setup.py"
	setupCfg      = "setup.cfg"
)

// Graph is a pyproject unit graph. Build one with Packages.
//
// It implements unit.Graph and NOT unit.Affector; the package doc says why at
// length. There is no assertion here that it does not, because a negative
// assertion is not a thing Go can write; there is a test instead.
type Graph struct {
	mu    sync.Mutex
	cache map[string][]Unit
}

// Packages makes one unit per distribution.
func Packages() *Graph { return &Graph{} }

var _ unit.Graph = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string { return "pyproject packages" }

// Units reports every distribution under root, sorted by ID.
//
// A distribution is a directory holding a pyproject.toml with a name in it, a
// setup.py or a setup.cfg. A pyproject.toml with NO name (a virtual uv
// workspace root, or a file that only configures ruff and mypy for the whole
// repository) is not a distribution and is not a unit: it configures a build
// rather than being one.
//
// A directory that [tool.uv.workspace] excludes is left out, which is the one
// explicit statement these manifests make that a directory is not part of the
// build.
//
// A tree with no distribution anywhere is an error rather than an empty graph,
// and so is a manifest that will not parse: a graph read as smaller than it is
// is a fan-out missing steps.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("pyproject: %w", err)
	}
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

// candidate is one directory that looks like a distribution.
type candidate struct {
	dir string
	// name is the distribution name, or "" when nothing static declares one.
	name string
	// excluded lists what a [tool.uv.workspace] here puts out of the build.
	excluded []string
}

func (g *Graph) discover(ctx context.Context, root string) ([]Unit, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("pyproject: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("pyproject: %s: %s is not a directory", g.Describe(), root)
	}
	found, err := findDistributions(ctx, root, g.Describe())
	if err != nil {
		return nil, err
	}

	var excluded []string
	for _, c := range found {
		excluded = append(excluded, c.excluded...)
	}
	excluded = unit.LongestFirst(excluded)

	units := make([]Unit, 0, len(found))
	for _, c := range found {
		if c.name == "" {
			continue // configuration, not a distribution
		}
		if c.dir != "." && unit.Nearest(excluded, c.dir) != "" {
			continue
		}
		units = append(units, Unit{ID: c.dir, Name: c.name, Dir: c.dir})
	}
	if len(units) == 0 {
		return nil, fmt.Errorf("pyproject: %s: no %s, %s or %s declaring a distribution "+
			"anywhere under %s, so there is nothing to discover; point the expansion at a "+
			"Python monorepo or use a different unit graph",
			g.Describe(), pyprojectFile, setupPy, setupCfg, root)
	}
	sort.Slice(units, func(i, j int) bool { return units[i].ID < units[j].ID })
	return units, nil
}

// findDistributions walks root for the three files that mark one.
//
// Pruned the way every walk in senro is, plus the directories a Python tree
// grows that hold a manifest per installed dependency: a virtual environment
// under any of its usual names, and the build and cache directories. Walking
// into a .venv would turn a four-package monorepo into a plan of hundreds.
func findDistributions(ctx context.Context, root, describe string) ([]*candidate, error) {
	ex := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	var out []*candidate
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
		rel = filepath.ToSlash(rel)
		if rel != "." && ex.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if rel != "." && ignoredDir(path.Base(rel)) {
			return fs.SkipDir
		}
		c, err := inspect(p, rel)
		if err != nil {
			return fmt.Errorf("pyproject: %s: %w", describe, err)
		}
		if c != nil {
			out = append(out, c)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dir < out[j].dir })
	return out, nil
}

// inspect reads whichever of the three markers a directory holds.
func inspect(full, rel string) (*candidate, error) {
	c := &candidate{dir: rel}
	if body, err := os.ReadFile(filepath.Join(full, pyprojectFile)); err == nil { // #nosec G304 -- a directory this walk found under the root
		tbl, perr := toml.Parse(body)
		if perr != nil {
			return nil, fmt.Errorf("%s: %w", path.Join(rel, pyprojectFile), perr)
		}
		c.name = distributionName(tbl)
		for _, e := range tbl.StrList("tool", "uv", "workspace", "exclude") {
			if d, ok := unit.CleanRel(path.Join(rel, e)); ok {
				c.excluded = append(c.excluded, d)
			}
		}
		return c, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	hasSetupPy := exists(filepath.Join(full, setupPy))
	cfgName, hasSetupCfg, err := setupCfgName(filepath.Join(full, setupCfg))
	if err != nil {
		return nil, err
	}
	if !hasSetupPy && !hasSetupCfg {
		return nil, nil
	}
	// setup.cfg declares the name as data; setup.py declares it as an argument
	// to a function call, and reading THAT would mean running the program. The
	// directory is the honest fallback, and it is still a usable Name: it is
	// what the step's own working directory is called.
	c.name = cfgName
	if c.name == "" {
		c.name = path.Base(rel)
	}
	return c, nil
}

// distributionName is the name from whichever of the two places a
// pyproject.toml declares one: PEP 621's [project], and Poetry's own table,
// which predates it and is still what a Poetry 1.x project writes.
func distributionName(tbl toml.Table) string {
	if n := tbl.Str("project", "name"); n != "" {
		return n
	}
	return tbl.Str("tool", "poetry", "name")
}

// setupCfgName reads [metadata] name out of a setup.cfg.
//
// A narrow INI read rather than a configuration library: the file is
// sections in square brackets and "key = value" lines, and only one of them
// is wanted. A continuation line (a value indented under its key, which
// install_requires always is) is skipped rather than misread as a key.
func setupCfgName(p string) (name string, found bool, err error) {
	body, rerr := os.ReadFile(p) // #nosec G304 -- a fixed name under a directory this walk found
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return "", false, nil
		}
		return "", false, rerr
	}
	section := ""
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(strings.TrimSpace(line), "#") ||
			strings.HasPrefix(strings.TrimSpace(line), ";") {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			continue // a continuation of the previous value
		}
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			section = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			continue
		}
		if section != "metadata" {
			continue
		}
		k, v, ok := strings.Cut(trimmed, "=")
		if !ok || strings.TrimSpace(k) != "name" {
			continue
		}
		return strings.TrimSpace(v), true, nil
	}
	return "", true, nil
}

func exists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}

// ignoredDir is a directory that holds a manifest per installed dependency, a
// build artifact, or a fixture, and never a distribution this graph should
// build.
func ignoredDir(base string) bool {
	switch base {
	case "venv", "env", "build", "dist", "site-packages", "node_modules", "testdata":
		return true
	}
	if strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_") {
		return true
	}
	return strings.HasSuffix(base, ".egg-info")
}
