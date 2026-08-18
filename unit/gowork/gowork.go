// Package gowork discovers Go modules and packages, and knows which one
// imports which.
//
// It is one of the five graphs an affected set can be computed from; cargo,
// jswork, maven and gradle are the others. glob, pyproject and bazel find
// units and stop there: they cannot say who breaks when a package changes,
// so an expansion over one of them is refused rather than quietly covering
// everything. This one asks the Go toolchain, which already holds the answer:
//
//	verify.Expand("test", gowork.Packages()).
//		Affected(change.FromTrigger(ev)).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
//		})
//
// # Two granularities
//
// Packages makes one unit per Go package, which is the finest the toolchain
// reports and the one that skips the most work. Modules makes one unit per
// go.mod, which is what a repository of independently released services
// usually wants: one step per service, not one per package inside it.
//
// Both come from ONE listing. Modules is Packages collapsed onto module
// directories, edges included, so the two can never disagree about which unit
// depends on which.
//
// # What it runs
//
// `go list -deps -test -e -json` once per module found under the root, with
// the module's own directory as the working directory. -deps so a package
// in another workspace module is reachable even when nothing in this
// module's pattern names it; -test so a test-only import is an edge like
// any other, since the package pair only a test connects is exactly the one
// a graph most easily disconnects. The same edges are also read from
// TestImports and XTestImports, so dropping either source alone stays
// correct; a test fails when both are dropped.
//
// A `go` binary has to be on PATH. A workspace with no go.mod anywhere
// under the root is an error rather than an empty graph: an expansion that
// silently produced no steps is indistinguishable from one whose pattern
// was wrong.
//
// Every ambiguity resolves towards running more units (see Owns): skipping
// a unit a change actually broke turns a green build into a lie.
package gowork

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
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

// Graph is a gowork unit graph. Build one with Packages or Modules.
//
// One Graph memoizes one listing per root, so the three calls an affected
// set makes (Units, Owns, ReverseDeps) shell out to the toolchain once
// between them. The memo has no expiry: a Graph is built when a pipeline is
// declared and consumed when it is built, one process and one tree. A
// long-lived host building the same pipeline twice across an edit wants a
// fresh Graph, which is one more call to Packages or Modules.
type Graph struct {
	modules bool

	mu    sync.Mutex
	cache map[string]*listing
}

// Packages makes one unit per Go package: the finest granularity the
// toolchain reports, and the one that skips the most work on a small change.
func Packages() *Graph { return &Graph{} }

// Modules makes one unit per go.mod, which is usually what a repository of
// separately released services wants. A module's unit Name is its module
// path; a package's is its import path.
func Modules() *Graph { return &Graph{modules: true} }

// Graph answers every question a Graph can be asked, which is what lets an
// expansion over it compute an affected set. Asserted here so a signature
// that drifts is a compile error in this package rather than a plan-time
// error in somebody's monorepo.
var _ unit.Affector = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string {
	if g.modules {
		return "gowork modules"
	}
	return "gowork packages"
}

// Units reports every unit under root, sorted by ID.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return append([]Unit(nil), l.units...), nil
}

// ReverseDeps reports the units that DIRECTLY depend on each unit, sorted.
//
// A dependency the toolchain reports outside the root (the standard library,
// anything in the module cache) produces no edge: it is not a unit, and
// nothing here can rebuild it.
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

// Owns reports which units each of files belongs to, in five rules, tried in
// order. Every one of them that could go either way goes towards attributing
// MORE, because an unattributed change is a skipped build.
//
//  1. go.work and go.work.sum belong to every unit. They decide which module
//     path resolves to which directory, so a change to one can change what
//     anything in the workspace compiles against.
//  2. A file the Go toolchain does not compile, sitting DIRECTLY in a
//     module's root directory, belongs to every unit of that module: go.mod,
//     go.sum, and more dangerously a Makefile, .golangci.yml or generator
//     config all change what every package of the module builds. A .go (or
//     .s, .c, .h, ...) file at the same level is compiled INTO the root
//     package and takes rule 3 instead, so ordinary code at a module's root
//     does not drag its subpackages in.
//  3. Otherwise the file belongs to the nearest unit at or above its own
//     directory, without leaving its module. That is what puts a testdata
//     fixture, a doc and a generated file on the package next to them.
//  4. A file inside a module that rule 3 found no unit for belongs to every
//     unit of that module. This is a directory of documentation beside a
//     module whose root holds no Go files.
//  5. A file in no module at all belongs to NO unit, and Owns says so with an
//     empty entry. A Makefile, a CI workflow or a linter config above every
//     module can change what all of them build, and unit.Affected reads the
//     empty answer as exactly that: run everything.
//
// Paths are slash-separated and relative to root. They are never stat'ed: a
// change that DELETED a file is answered for from the path alone, which is
// what makes a deletion (the change whose dependents most need rebuilding)
// behave like any other edit. A path that escapes the root, or an absolute
// one, is owned by nothing, which again means everything runs.
func (g *Graph) Owns(ctx context.Context, root string, files []string) ([][]string, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	out := make([][]string, len(files))
	for i, f := range files {
		out[i] = l.owners(f)
	}
	return out, nil
}

// listing is one `go list` sweep of one root, and everything derived from it.
type listing struct {
	units []Unit
	// all is every unit ID, in units order, for the go.work rule.
	all []string
	// unitDir maps a unit's directory (relative, slash-separated, "." for the
	// root) to its ID. For Packages the two are equal; for Modules a package
	// directory maps to the module's ID, which is what makes rule 3 in Owns
	// work at both granularities.
	unitDir map[string]string
	// modDirs are the module directories, relative and slash-separated,
	// longest first so the nearest one wins.
	modDirs []string
	// modUnits maps a module directory to every unit ID inside it, sorted.
	modUnits map[string][]string
	// rdeps maps a unit ID to the units that directly depend on it, sorted.
	rdeps map[string][]string
}

func (g *Graph) load(ctx context.Context, root string) (*listing, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("gowork: %w", err)
	}
	// Before the memo, not after it. A cancelled context has to be reported
	// whether or not this Graph happens to have listed this root already:
	// which of the three calls an affected set makes is the one that warms
	// the cache is an implementation detail, and a build that answered two of
	// them from a dead context would depend on it.
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

// pkg is the slice of `go list -json` this needs. Named fields rather than
// the whole record: a -deps -test listing of a real workspace is thousands of
// packages, and the fields left out are the bulky ones (every source file of
// every dependency).
type pkg struct {
	ImportPath   string
	Dir          string
	Standard     bool
	Imports      []string
	TestImports  []string
	XTestImports []string
	Module       *struct {
		Path string
		Dir  string
	}
	Error *struct {
		Err string
	}
}

func (g *Graph) discover(ctx context.Context, root string) (*listing, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("gowork: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("gowork: %s: %s is not a directory", g.Describe(), root)
	}
	modDirs, err := findModules(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("gowork: %s: %w", g.Describe(), err)
	}
	if len(modDirs) == 0 {
		return nil, fmt.Errorf("gowork: %s: no go.mod anywhere under %s, so there is nothing to "+
			"discover; point the expansion at a Go workspace or use a different unit graph",
			g.Describe(), root)
	}

	// pkgs is keyed by import path (test variants included, which carry the
	// test-only imports) so an import can be resolved to a directory.
	pkgs := make(map[string]*pkg)
	for _, md := range modDirs {
		got, err := goList(ctx, filepath.Join(root, filepath.FromSlash(md)))
		if err != nil {
			return nil, fmt.Errorf("gowork: %s: %w", g.Describe(), err)
		}
		for _, p := range got {
			if prev, ok := pkgs[p.ImportPath]; ok && prev.Dir != "" {
				continue
			}
			pkgs[p.ImportPath] = p
		}
	}
	return g.build(root, modDirs, pkgs)
}

// build turns a package listing into the graph, at whichever granularity this
// Graph was made for. One function for both, so the two can never disagree
// about an edge.
func (g *Graph) build(root string, modDirs []string, pkgs map[string]*pkg) (*listing, error) {
	// dirOf resolves an import path to a directory relative to root, or ""
	// for anything outside it (the standard library, the module cache).
	dirOf := make(map[string]string, len(pkgs))
	// modOf maps a relative package directory to its module directory.
	modOf := make(map[string]string, len(pkgs))
	for ip, p := range pkgs {
		if p.Standard || p.Dir == "" {
			continue
		}
		rel, ok := relTo(root, p.Dir)
		if !ok {
			continue
		}
		dirOf[ip] = rel
		if p.Module != nil && p.Module.Dir != "" {
			if mrel, ok := relTo(root, p.Module.Dir); ok {
				modOf[rel] = mrel
			}
		}
	}

	// unitOf maps a package directory to the ID of the unit it belongs to:
	// itself for Packages, its module for Modules.
	unitOf := make(map[string]string, len(dirOf))
	names := make(map[string]string, len(dirOf))
	dirs := make(map[string]string, len(dirOf))
	for ip, rel := range dirOf {
		id := rel
		name := ip
		dir := rel
		if g.modules {
			m, ok := modOf[rel]
			if !ok {
				// A package inside root whose module is outside it cannot be
				// collapsed onto a module unit. It is not this graph's to
				// build; dropping it from the unit set is right, and it still
				// resolves as a dependency below via unitOf's absence.
				continue
			}
			id, dir = m, m
			name = modulePathOf(pkgs, ip)
		}
		unitOf[rel] = id
		// The first import path to claim an ID wins only for a package unit,
		// where there is exactly one. For a module unit, prefer the module
		// path, which modulePathOf already returned.
		if _, seen := names[id]; !seen || g.modules {
			names[id] = name
		}
		dirs[id] = dir
	}
	// A test variant ("example.com/x [example.com/x.test]") shares its base
	// package's directory, so it is already collapsed by the map above. Its
	// NAME would be the bracketed path, which is not a name anything should
	// show, so prefer the plain import path for a package unit.
	if !g.modules {
		for ip, rel := range dirOf {
			if !strings.ContainsAny(ip, " [") {
				names[rel] = ip
			}
		}
	}

	ids := make([]string, 0, len(dirs))
	for id := range dirs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	l := &listing{
		unitDir:  make(map[string]string, len(unitOf)),
		modUnits: make(map[string][]string, len(modDirs)),
		rdeps:    make(map[string][]string),
	}
	for _, id := range ids {
		l.units = append(l.units, Unit{ID: id, Name: names[id], Dir: dirs[id]})
		l.all = append(l.all, id)
	}
	for rel, id := range unitOf {
		l.unitDir[rel] = id
	}

	// Module directories, longest first, so "a/b" beats "a" for a file under
	// a nested module.
	l.modDirs = unit.LongestFirst(modDirs)
	inMod := make(map[string]map[string]bool, len(modDirs))
	for rel, id := range unitOf {
		md := modOf[rel]
		if md == "" {
			md = unit.Nearest(l.modDirs, rel)
		}
		if md == "" {
			continue
		}
		if inMod[md] == nil {
			inMod[md] = make(map[string]bool)
		}
		inMod[md][id] = true
	}
	for md, set := range inMod {
		l.modUnits[md] = unit.SortedKeys(set)
	}

	// Edges. Direct imports only: unit.Affected computes the closure, once,
	// over whatever this says, so a bug in the closure cannot hide behind a
	// pre-flattened edge set.
	rev := make(map[string]map[string]bool)
	for ip, p := range pkgs {
		from, ok := unitOf[dirOf[ip]]
		if !ok {
			continue
		}
		for _, imp := range allImports(p) {
			to, ok := unitOf[dirOf[imp]]
			if !ok || to == from {
				continue
			}
			if rev[to] == nil {
				rev[to] = make(map[string]bool)
			}
			rev[to][from] = true
		}
	}
	for to, set := range rev {
		l.rdeps[to] = unit.SortedKeys(set)
	}
	return l, nil
}

// allImports is every direct import of a package, ordinary and test.
//
// TestImports and XTestImports as well as Imports, even though a -test
// listing also reports the test variants whose own Imports carry the same
// edges: the two sources agree, the union costs nothing, and a graph that
// depended on the variant being present would break quietly the day a
// toolchain stopped emitting one.
func allImports(p *pkg) []string {
	out := make([]string, 0, len(p.Imports)+len(p.TestImports)+len(p.XTestImports))
	out = append(out, p.Imports...)
	out = append(out, p.TestImports...)
	out = append(out, p.XTestImports...)
	return out
}

func modulePathOf(pkgs map[string]*pkg, ip string) string {
	if p := pkgs[ip]; p != nil && p.Module != nil {
		return p.Module.Path
	}
	return ip
}

const (
	goMod = "go.mod"
	// go.sum needs no name of its own: rule 2 in Owns catches it, and every
	// other tool config beside it, by being an uncompiled file at a module's
	// root rather than by being on a list somebody has to remember to grow.
	goWork    = "go.work"
	goWorkSum = "go.work.sum"
)

// owners implements the five rules Owns documents.
func (l *listing) owners(file string) []string {
	rel, ok := unit.CleanRel(file)
	if !ok {
		return nil // rule 5: not ours, so everything runs
	}
	base := path.Base(rel)
	if base == goWork || base == goWorkSum {
		return l.all // rule 1
	}
	md := unit.Nearest(l.modDirs, rel)
	if md == "" {
		return nil // rule 5
	}
	// Rule 2: not compiled, and directly in the module's own root directory.
	if path.Dir(rel) == md && !compiledSource(base) {
		return l.modUnits[md]
	}
	// Rule 3: the nearest unit at or above the file's own directory, stopping
	// at the module it belongs to.
	for d := path.Dir(rel); ; d = path.Dir(d) {
		if id, ok := l.unitDir[d]; ok {
			return []string{id}
		}
		if d == md || d == "." || d == "/" {
			break
		}
	}
	return l.modUnits[md] // rule 4
}

// compiledSource reports whether the Go toolchain compiles a file with this
// name INTO a package, which is go/build's own list of source extensions.
//
// It decides rule 2 in Owns, and it is deliberately the narrow half of the
// decision: a name not on this list is treated as configuration and
// attributed to a whole module, which runs more, and a name on it is
// attributed to one package, which runs less. Getting a real source
// extension wrong therefore costs CI minutes and never a skipped build.
func compiledSource(base string) bool {
	switch strings.ToLower(path.Ext(base)) {
	case ".go", ".s", ".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx",
		".m", ".f", ".for", ".f90", ".sx", ".syso":
		return true
	}
	return false
}

// relTo is dir relative to root, slash-separated, or ok=false when dir is
// outside root.
func relTo(root, dir string) (string, bool) {
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false
	}
	return rel, true
}

// findModules walks root for go.mod, pruning the same directories a workspace
// snapshot prunes. Descending into node_modules to look for a go.mod is
// seconds of syscalls for an answer that is always no.
func findModules(ctx context.Context, root string) ([]string, error) {
	ex := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	var out []string
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
		// testdata and _-prefixed directories hold no module the toolchain
		// would ever build, and a go.mod under one is a fixture. `go list`
		// ignores them, so listing them here would produce a module whose
		// packages never appear.
		if d.IsDir() && rel != "." && ignoredDir(path.Base(rel)) {
			return fs.SkipDir
		}
		if !d.IsDir() && d.Name() == goMod {
			out = append(out, path.Dir(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func ignoredDir(base string) bool {
	return base == "testdata" || strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".")
}

// goList runs the listing for one module.
func goList(ctx context.Context, dir string) ([]*pkg, error) {
	// A cancelled context must not start a subprocess at all. exec.CommandContext
	// would notice eventually; noticing before the fork is what makes an
	// interrupted build stop promptly rather than after the last module.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := []string{
		"list", "-deps", "-test", "-e",
		// A field list rather than the whole record. -deps -test over a real
		// workspace is thousands of packages, and the omitted fields (every
		// source file name of every dependency) are most of the bytes.
		"-json=ImportPath,Dir,Standard,Imports,TestImports,XTestImports,Module,Error",
		"./...",
	}
	cmd := exec.CommandContext(ctx, "go", args...) // #nosec G204 -- fixed argv, no caller input
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("go list in %s: %w: %s", dir, err, strings.TrimSpace(stderr.String()))
	}

	dec := json.NewDecoder(&stdout)
	var out []*pkg
	var broken []string
	for dec.More() {
		var p pkg
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("go list in %s: reading its output: %w", dir, err)
		}
		if p.Error != nil {
			// -e keeps the listing going past a package that will not resolve,
			// which is what lets this report the problem in senro's own words
			// instead of a raw toolchain failure. It is still a failure: a
			// listing missing a package is a graph missing its dependents, and
			// a graph missing dependents is a build that skips them.
			broken = append(broken, fmt.Sprintf("%s: %s", p.ImportPath, p.Error.Err))
			continue
		}
		q := p
		out = append(out, &q)
	}
	if len(broken) > 0 {
		sort.Strings(broken)
		if len(broken) > 3 {
			broken = append(broken[:3], fmt.Sprintf("(and %d more)", len(broken)-3))
		}
		return nil, fmt.Errorf("go list in %s could not resolve %s; the unit graph would be "+
			"missing them and every unit that depends on them", dir, strings.Join(broken, "; "))
	}
	return out, nil
}
