// Package cargo discovers the crates of a Rust workspace, and knows which one
// depends on which.
//
// It implements the affected-set interface, so an expansion over it runs only
// the crates a change reaches:
//
//	verify.Expand("test", cargo.Crates()).
//		Affected(change.FromTrigger(ev)).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("cargo", "test", "-p", u.Name))
//		})
//
// A unit is one crate: its ID and Dir are the crate directory relative to the
// root, and its Name is the crate name from [package], which is what
// `cargo test -p` wants.
//
// # It reads manifests, and runs nothing
//
// No cargo binary is needed, and none is run. `cargo metadata --no-deps`
// would be Cargo's own answer, and it resolves the [patch] and [replace]
// tables this does not read, but it requires cargo on whatever builds the
// plan. The manifests hold the member list, the crate names and the path
// dependencies, which is the whole graph; what they cannot settle is
// over-approximated rather than fetched (see Owns and ReverseDeps).
//
// # What is a unit
//
// Every Cargo.toml under the root that has a [package] name, minus any
// directory a [workspace] table above it puts in `exclude`.
//
// Not the `members` list, deliberately: a path dependency inside the
// workspace becomes a member whether or not `members` names it, so "absent
// from members" would DROP a crate that is genuinely part of the build.
// `exclude` is honoured as the one deliberate statement that a directory is
// not part of the build. The cost is that a crate no workspace reaches
// still gets a step: over-runs, never under-runs.
//
// A tree with no Cargo.toml at all is an error rather than an empty graph:
// an expansion that silently produced no steps is indistinguishable from
// one whose root was wrong. A malformed Cargo.toml is an error too: a
// manifest half-read is a missing dependency edge, and a missing edge
// skips the crate a change broke.
//
// Every remaining ambiguity resolves towards running more crates; see Owns
// and ReverseDeps.
package cargo

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

// manifestName is the file that marks a crate and, with a [workspace] table,
// a workspace root.
const manifestName = "Cargo.toml"

// Graph is a cargo unit graph. Build one with Crates.
//
// One Graph memoizes one reading per root, so the three calls an affected
// set makes (Units, Owns, ReverseDeps) walk the tree once between them. No
// expiry: a Graph is built when a pipeline is declared and consumed when it
// is built, one process and one tree. Building the same pipeline twice
// across an edit wants a fresh Graph.
type Graph struct {
	mu    sync.Mutex
	cache map[string]*listing
}

// Crates makes one unit per crate.
func Crates() *Graph { return &Graph{} }

// Graph answers every question an affected set needs. Asserted here so a
// signature that drifts is a compile error in this package rather than a
// plan-time error in somebody's monorepo.
var _ unit.Affector = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string { return "cargo crates" }

// Units reports every crate under root, sorted by ID.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return append([]Unit(nil), l.units...), nil
}

// Owns reports which crate each of files belongs to, in three rules:
//
//  1. A file DIRECTLY in the workspace root belongs to every crate:
//     Cargo.toml, Cargo.lock, rust-toolchain.toml and a shared deny.toml
//     all live exactly there and change what every crate builds.
//  2. Otherwise the nearest crate directory at or above the file owns it,
//     which also gives a crate nested inside another crate's directory its
//     own files.
//  3. Otherwise NO crate owns it, and unit.Affected reads that as "this
//     could have changed anything" and runs everything.
//
// Paths are slash-separated, relative to root, and NEVER stat'ed, so a file
// a change deleted is answered for exactly like one it added. A path that
// escapes the root, or an absolute one, is owned by nothing, which again
// means everything runs.
func (g *Graph) Owns(ctx context.Context, root string, files []string) ([][]string, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return l.owner.OwnersOf(files), nil
}

// ReverseDeps reports the crates that DIRECTLY depend on each crate, sorted.
//
// An edge is drawn from every dependency table a manifest has:
// [dependencies], [dev-dependencies], [build-dependencies], and each of
// those under a [target.<cfg>] table. A dev-dependency is an edge like any
// other: the crate only a test connects is exactly the pair a graph most
// often disconnects by accident. A target-specific dependency is an edge
// because this graph has no idea which target the step will build for.
//
// A dependency resolves to a crate two ways, and EITHER draws the edge:
//
//   - `path = ` resolved against the manifest's own directory (or, for an
//     inherited `dep.workspace = true`, the [workspace] table's). Exact.
//   - the dependency's crate name (its `package = ` rename, otherwise the
//     key) matching a crate in this tree. The over-approximation, here for
//     [patch] and [replace] redirects this graph does not read: a name
//     collision with a real crates.io dependency over-runs, while the
//     opposite mistake would silently skip a patched crate's dependents.
//
// Direct edges only. The transitive closure is unit.Affected's.
func (g *Graph) ReverseDeps(ctx context.Context, root string) (map[string][]string, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(l.rdeps))
	for k, v := range l.rdeps {
		out[k] = append([]string(nil), v...)
	}
	return out, nil
}

// listing is one reading of one root and everything derived from it.
type listing struct {
	units []Unit
	owner *unit.PathOwner
	rdeps map[string][]string
}

func (g *Graph) load(ctx context.Context, root string) (*listing, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("cargo: %w", err)
	}
	// Before the memo, not after it: which of the three calls an affected set
	// makes happens to warm the cache is an implementation detail, and a
	// build that answered two of them from a dead context would depend on it.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if l, ok := g.cache[abs]; ok {
		return l, nil
	}
	l, err := g.discover(ctx, abs)
	if err != nil {
		return nil, err
	}
	if g.cache == nil {
		g.cache = make(map[string]*listing, 1)
	}
	g.cache[abs] = l
	return l, nil
}

// manifest is one parsed Cargo.toml and where it sits.
type manifest struct {
	dir   string // relative to root, slash-separated, "." for the root
	table toml.Table
	err   error // a parse failure, reported only if this manifest turns out to matter
}

func (g *Graph) discover(ctx context.Context, root string) (*listing, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("cargo: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("cargo: %s: %s is not a directory", g.Describe(), root)
	}
	found, err := findManifests(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("cargo: %s: %w", g.Describe(), err)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("cargo: %s: no %s anywhere under %s, so there is nothing to "+
			"discover; point the expansion at a Rust workspace or use a different unit graph",
			g.Describe(), manifestName, root)
	}

	// Excluded directories come from every [workspace] table found, which is
	// why the manifests are all read before any of them is judged.
	var excluded []string
	for _, m := range found {
		if m.err != nil {
			continue
		}
		ws := m.table.Sub("workspace")
		if ws == nil {
			continue
		}
		for _, e := range ws.StrList("exclude") {
			if d, ok := unit.CleanRel(path.Join(m.dir, e)); ok {
				excluded = append(excluded, d)
			}
		}
	}
	excluded = unit.LongestFirst(excluded)

	// A manifest that would not have been part of the build anyway is not
	// worth failing over: `exclude` is where a workspace puts a fixture crate
	// that is deliberately not buildable. Everything else that failed to
	// parse is an error, because a manifest half-read is an edge missing.
	kept := make([]*manifest, 0, len(found))
	for _, m := range found {
		if m.dir != "." && unit.Nearest(excluded, m.dir) != "" {
			continue
		}
		if m.err != nil {
			return nil, fmt.Errorf("cargo: %s: %s: %w", g.Describe(),
				path.Join(m.dir, manifestName), m.err)
		}
		kept = append(kept, m)
	}
	return build(kept)
}

// build turns the parsed manifests into the graph.
func build(manifests []*manifest) (*listing, error) {
	// Workspace roots, for resolving an inherited dependency's path against
	// the directory of the table it is inherited FROM.
	wsDirs := make([]string, 0, 4)
	wsAt := make(map[string]toml.Table, 4)
	for _, m := range manifests {
		if ws := m.table.Sub("workspace"); ws != nil {
			wsDirs = append(wsDirs, m.dir)
			wsAt[m.dir] = ws
		}
	}
	wsDirs = unit.LongestFirst(wsDirs)

	units := make([]Unit, 0, len(manifests))
	byDir := make(map[string]string, len(manifests))  // crate dir  -> unit ID
	byName := make(map[string]string, len(manifests)) // crate name -> unit ID
	crates := make([]*manifest, 0, len(manifests))
	for _, m := range manifests {
		name := m.table.Str("package", "name")
		if name == "" {
			continue // a workspace root that is not itself a package
		}
		units = append(units, Unit{ID: m.dir, Name: name, Dir: m.dir})
		byDir[m.dir] = m.dir
		// First crate to claim a name keeps it. Two crates cannot share a
		// name in one workspace, and if a tree holds two anyway the edge goes
		// to one of them; the alternative, dropping both, would lose an edge.
		if _, seen := byName[name]; !seen {
			byName[name] = m.dir
		}
		crates = append(crates, m)
	}
	sort.Slice(units, func(i, j int) bool { return units[i].ID < units[j].ID })

	rev := make(map[string]map[string]bool)
	for _, m := range crates {
		for _, d := range dependencies(m, wsDirs, wsAt) {
			for _, to := range []string{byDir[d.dir], byName[d.name]} {
				if to == "" || to == m.dir {
					continue
				}
				if rev[to] == nil {
					rev[to] = make(map[string]bool)
				}
				rev[to][m.dir] = true
			}
		}
	}
	l := &listing{units: units, owner: unit.NewPathOwner(units), rdeps: make(map[string][]string, len(rev))}
	for to, set := range rev {
		l.rdeps[to] = unit.SortedKeys(set)
	}
	return l, nil
}

// dep is one dependency resolved as far as the manifests allow: a directory
// when a path said so, a crate name always.
type dep struct {
	dir  string
	name string
}

// depTableNames are the three kinds of dependency, all of which are edges.
var depTableNames = []string{"dependencies", "dev-dependencies", "build-dependencies"}

// dependencies reads every dependency of one manifest, ordinary, dev, build
// and target-specific alike.
func dependencies(m *manifest, wsDirs []string, wsAt map[string]toml.Table) []dep {
	var out []dep
	collect := func(t toml.Table) {
		if t == nil {
			return
		}
		for _, key := range t.Keys() {
			out = append(out, resolve(m, wsDirs, wsAt, t, key))
		}
	}
	for _, n := range depTableNames {
		collect(m.table.Sub(n))
	}
	// [target.'cfg(unix)'.dependencies] and friends. The cfg expression is
	// not evaluated: this graph does not know what target the step will build
	// for, and guessing would drop an edge on the platform it guessed wrong.
	if tgt := m.table.Sub("target"); tgt != nil {
		for _, cfg := range tgt.Keys() {
			sub := tgt.Sub(cfg)
			for _, n := range depTableNames {
				collect(sub.Sub(n))
			}
		}
	}
	return out
}

// resolve turns one dependency entry into a directory and a crate name.
func resolve(m *manifest, wsDirs []string, wsAt map[string]toml.Table, t toml.Table, key string) dep {
	d := dep{name: key}
	spec := t.Sub(key)
	if spec == nil {
		// `serde = "1.0"`: a version and nothing else. Only the name is left,
		// which is the over-approximating half of the rule in ReverseDeps.
		return d
	}
	if pkg := spec.Str("package"); pkg != "" {
		d.name = pkg
	}
	if p := spec.Str("path"); p != "" {
		if rel, ok := unit.CleanRel(path.Join(m.dir, p)); ok {
			d.dir = rel
		}
		return d
	}
	if !spec.Bool("workspace") {
		return d
	}
	// Inherited: the path, if there is one, lives in the [workspace]
	// table nearest above this crate, and is relative to THAT directory.
	wd := unit.Nearest(wsDirs, m.dir)
	ws := wsAt[wd]
	if ws == nil {
		return d
	}
	inherited := ws.Sub("dependencies", key)
	if inherited == nil {
		return d
	}
	if pkg := inherited.Str("package"); pkg != "" {
		d.name = pkg
	}
	if p := inherited.Str("path"); p != "" {
		if rel, ok := unit.CleanRel(path.Join(wd, p)); ok {
			d.dir = rel
		}
	}
	return d
}

// findManifests walks root for Cargo.toml, parsing each one it finds.
//
// Pruned the way every walk in senro is, plus target/ and vendor/, which
// hold dependencies' own manifests: reading either would turn a monorepo of
// six crates into a graph of six hundred. A real crate in a directory named
// target or vendor is missed; the trade is documented rather than guessed.
func findManifests(ctx context.Context, root string) ([]*manifest, error) {
	ex := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	var out []*manifest
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
		if d.IsDir() && rel != "." && ignoredDir(path.Base(rel)) {
			return fs.SkipDir
		}
		if d.IsDir() || d.Name() != manifestName {
			return nil
		}
		m := &manifest{dir: path.Dir(rel)}
		body, readErr := os.ReadFile(p) // #nosec G304 -- a path this walk found under the root
		if readErr != nil {
			m.err = readErr
		} else if m.table, m.err = toml.Parse(body); m.err != nil {
			m.table = nil
		}
		out = append(out, m)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dir < out[j].dir })
	return out, nil
}

func ignoredDir(base string) bool {
	switch base {
	case "target", "vendor", "testdata":
		return true
	}
	return strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".")
}
