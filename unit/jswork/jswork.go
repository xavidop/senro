// Package jswork discovers the packages of a JavaScript workspace, and knows
// which one depends on which.
//
// npm, pnpm, Yarn (classic and Berry) and Bun are four package managers with
// four lockfiles and one idea of what a workspace is, so this graph is one
// graph and not four:
//
//	verify.Expand("test", jswork.Packages()).
//		Affected(change.FromTrigger(ev)).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("npm", "test")).WorkDir(u.Dir)
//		})
//
// A unit is one workspace package: its ID and Dir are the package directory
// relative to the root, and its Name is the "name" from its package.json,
// which is what `pnpm --filter` and `npm -w` want.
//
// # It reads manifests, and runs nothing
//
// No package manager is needed and none is run, so this works with no node
// installed and nothing yet installed into the tree. The lockfiles are not
// read at all: four formats, and none says anything about the workspace
// graph the manifests do not already say.
//
// # Discovery is the only part that differs by manager
//
// Every directory matching the workspace's member patterns that holds a
// package.json is a unit. The patterns come from two places, unioned: the
// root package.json's "workspaces" (an ARRAY, or Yarn v1's OBJECT with
// "packages" in it), and pnpm-workspace.yaml's (or .yml's) "packages" list.
// Both are read because a pnpm repository's root package.json usually has
// NO "workspaces" field at all, so reading only package.json would find
// nothing there; the union also covers a repository mid-migration.
//
// A pattern beginning with "!" EXCLUDES, wherever it came from. An excluded
// package is owned by no unit, so a change to one runs everything rather
// than nothing.
//
// A tree with neither declaration is an error rather than an empty graph,
// and so is a declaration matching no package: an expansion that silently
// produced no steps is indistinguishable from one whose root was wrong. So
// is a pnpm-workspace.yaml whose member list this reader cannot read; see
// pnpmPackages.
//
// The dependency graph itself does not differ by manager: an edge is one
// package.json naming another package's "name" in one of the four
// dependency fields. See ReverseDeps, and Owns for the ownership rules.
//
// # The honest limit: this is the DECLARED graph
//
// Nothing here parses JavaScript, so a package that imports another WITHOUT
// declaring it has no edge, and a change to the imported package will not
// run it. That is the same hole every tool in this ecosystem has (turbo,
// nx, lerna and `pnpm --filter` all read the declared graph), and an
// undeclared import is already a bug pnpm's isolated node_modules fails on.
// If your repository hoists and relies on undeclared imports, run every
// unit or declare the dependency. TypeScript project references and
// tsconfig "paths" are not read either; a package wired up only that way
// needs the dependency in its package.json too.
package jswork

import (
	"context"
	"encoding/json"
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

// manifestName marks a package, at the root and at every member.
const manifestName = "package.json"

// pnpmWorkspaceFiles are the names pnpm accepts for its workspace file. Both,
// because a repository that spells it .yml is a repository whose graph would
// otherwise come out empty.
var pnpmWorkspaceFiles = []string{"pnpm-workspace.yaml", "pnpm-workspace.yml"}

// Graph is a jswork unit graph. Build one with Packages.
//
// One Graph memoizes one reading per root, so the three calls an affected set
// makes walk the tree once between them. See gowork's Graph for why the memo
// has no expiry.
type Graph struct {
	mu    sync.Mutex
	cache map[string]*listing
}

// Packages makes one unit per workspace package.
func Packages() *Graph { return &Graph{} }

// Graph answers every question an affected set needs. Asserted here so a
// signature that drifts is a compile error in this package rather than a
// plan-time error in somebody's monorepo.
var _ unit.Affector = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string { return "jswork packages" }

// Units reports every workspace package under root, sorted by ID.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return append([]Unit(nil), l.units...), nil
}

// Owns reports which package each of files belongs to, in three rules:
//
//  1. A file DIRECTLY in the workspace root belongs to EVERY package. That is
//     the root package.json, the lockfile, pnpm-workspace.yaml, the shared
//     tsconfig, the eslint config and the turbo.json, every one of which can
//     change what every package builds.
//  2. Otherwise the nearest package directory at or above the file owns it.
//  3. Otherwise NO package owns it, and unit.Affected runs everything. A
//     docs/ directory beside the packages is that case, and so is a package
//     the workspace patterns EXCLUDE: it is not a unit, so nothing owns its
//     files, and a change to it is a change nothing can be concluded about.
//
// Paths are slash-separated, relative to root, and never stat'ed, so a file a
// change deleted is answered for exactly like one it added.
func (g *Graph) Owns(ctx context.Context, root string, files []string) ([][]string, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return l.owner.OwnersOf(files), nil
}

// ReverseDeps reports the packages that DIRECTLY depend on each package,
// sorted.
//
// All four dependency fields are read: dependencies, devDependencies,
// peerDependencies and optionalDependencies. A dev dependency is an edge for
// the same reason a test-only import is one in Go, and a peer dependency is an
// edge because a package that has to be rebuilt when its peer changes is
// exactly what a peer dependency describes.
//
// A dependency resolves to a package two ways, and either draws the edge:
//
//   - its NAME matching another package's "name", whatever the version
//     range says: "workspace:*", "^1.2.3" and "*" all draw the edge.
//     Resolving ranges is the package manager's job, and discarding
//     unparsed ones would drop every edge in a pnpm or Yarn Berry
//     repository. A range that means "fetch the published one instead"
//     over-runs; the opposite mistake skips a build. "npm:@acme/core@^1"
//     resolves through to @acme/core. A package's DIRECTORY name is never
//     what an edge is matched on.
//   - a "file:", "link:" or "portal:" specifier's PATH, resolved against
//     the depending package's own directory.
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

type listing struct {
	units []Unit
	owner *unit.PathOwner
	rdeps map[string][]string
}

func (g *Graph) load(ctx context.Context, root string) (*listing, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("jswork: %w", err)
	}
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

// pkgJSON is the slice of a package.json this graph reads.
type pkgJSON struct {
	Name                 string            `json:"name"`
	Workspaces           json.RawMessage   `json:"workspaces"`
	Dependencies         map[string]string `json:"dependencies"`
	DevDependencies      map[string]string `json:"devDependencies"`
	PeerDependencies     map[string]string `json:"peerDependencies"`
	OptionalDependencies map[string]string `json:"optionalDependencies"`
}

func (p *pkgJSON) deps() []map[string]string {
	return []map[string]string{
		p.Dependencies, p.DevDependencies, p.PeerDependencies, p.OptionalDependencies,
	}
}

func (g *Graph) discover(ctx context.Context, root string) (*listing, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("jswork: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("jswork: %s: %s is not a directory", g.Describe(), root)
	}

	include, exclude, err := memberPatterns(root)
	if err != nil {
		return nil, fmt.Errorf("jswork: %s: %w", g.Describe(), err)
	}
	if len(include) == 0 {
		return nil, fmt.Errorf("jswork: %s: %s declares no workspaces and there is no %s, so "+
			"there is nothing to fan out over; add a \"workspaces\" array or use a different "+
			"unit graph", g.Describe(), manifestName, pnpmWorkspaceFiles[0])
	}

	dirs, err := memberDirs(ctx, root, include, exclude)
	if err != nil {
		return nil, fmt.Errorf("jswork: %s: %w", g.Describe(), err)
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("jswork: %s: the workspace patterns (%s) match no directory "+
			"holding a %s under %s", g.Describe(), strings.Join(include, ", "), manifestName, root)
	}

	units := make([]Unit, 0, len(dirs))
	manifests := make(map[string]*pkgJSON, len(dirs))
	for _, d := range dirs {
		m, err := readManifest(filepath.Join(root, filepath.FromSlash(d), manifestName))
		if err != nil {
			return nil, fmt.Errorf("jswork: %s: %s: %w", g.Describe(),
				path.Join(d, manifestName), err)
		}
		name := m.Name
		if name == "" {
			// A workspace package with no name cannot be depended on by name
			// and cannot be filtered by one either, but it is still a
			// directory that has to be built. Naming it after its directory
			// keeps a step for it rather than dropping the unit.
			name = d
		}
		units = append(units, Unit{ID: d, Name: name, Dir: d})
		manifests[d] = m
	}

	byName := make(map[string]string, len(units))
	for _, u := range units {
		if _, seen := byName[u.Name]; !seen {
			byName[u.Name] = u.ID
		}
	}
	byDir := make(map[string]bool, len(units))
	for _, u := range units {
		byDir[u.Dir] = true
	}

	rev := make(map[string]map[string]bool)
	for _, u := range units {
		for _, table := range manifests[u.Dir].deps() {
			for _, spec := range sortedPairs(table) {
				for _, to := range resolve(u.Dir, spec, byName, byDir) {
					if to == u.ID {
						continue
					}
					if rev[to] == nil {
						rev[to] = make(map[string]bool)
					}
					rev[to][u.ID] = true
				}
			}
		}
	}
	l := &listing{units: units, owner: unit.NewPathOwner(units),
		rdeps: make(map[string][]string, len(rev))}
	for to, set := range rev {
		l.rdeps[to] = unit.SortedKeys(set)
	}
	return l, nil
}

// pair is one dependency entry, read in sorted key order so nothing here
// depends on Go's map iteration.
type pair struct{ key, value string }

func sortedPairs(m map[string]string) []pair {
	out := make([]pair, 0, len(m))
	for k, v := range m {
		out = append(out, pair{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// resolve turns one dependency entry into the unit IDs it could mean.
func resolve(from string, p pair, byName map[string]string, byDir map[string]bool) []string {
	var out []string
	name := p.key
	if alias, ok := strings.CutPrefix(p.value, "npm:"); ok {
		if n := aliasName(alias); n != "" {
			name = n
		}
	}
	if id, ok := byName[name]; ok {
		out = append(out, id)
	}
	for _, proto := range []string{"file:", "link:", "portal:"} {
		rest, ok := strings.CutPrefix(p.value, proto)
		if !ok {
			continue
		}
		if d, ok := unit.CleanRel(path.Join(from, rest)); ok && byDir[d] {
			out = append(out, d)
		}
		break
	}
	return out
}

// aliasName is the package name inside an "npm:" specifier, whose version is
// separated by the LAST "@": a scoped name starts with one of its own.
func aliasName(s string) string {
	if s == "" {
		return ""
	}
	body := s
	if strings.HasPrefix(body, "@") {
		if i := strings.LastIndex(body[1:], "@"); i >= 0 {
			return body[:i+1]
		}
		return body
	}
	if i := strings.LastIndex(body, "@"); i > 0 {
		return body[:i]
	}
	return body
}

func readManifest(p string) (*pkgJSON, error) {
	body, err := os.ReadFile(p) // #nosec G304 -- a path derived from the root this graph was given
	if err != nil {
		return nil, err
	}
	var m pkgJSON
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// memberPatterns reads the workspace declaration, from both places it can
// live. Both, unioned, because a repository migrating between package
// managers has both for a while and a graph that read only one of them would
// come out missing half its packages.
func memberPatterns(root string) (include, exclude []string, err error) {
	add := func(pats []string) {
		for _, p := range pats {
			if p = strings.TrimSpace(p); p == "" {
				continue
			}
			if neg, ok := strings.CutPrefix(p, "!"); ok {
				exclude = append(exclude, path.Clean(neg))
				continue
			}
			include = append(include, path.Clean(p))
		}
	}

	rootManifest := filepath.Join(root, manifestName)
	if body, rerr := os.ReadFile(rootManifest); rerr == nil { // #nosec G304 -- the root's own manifest
		var m pkgJSON
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, nil, fmt.Errorf("%s: %w", manifestName, err)
		}
		pats, err := workspacesField(m.Workspaces)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", manifestName, err)
		}
		add(pats)
	} else if !os.IsNotExist(rerr) {
		return nil, nil, rerr
	}

	for _, name := range pnpmWorkspaceFiles {
		body, rerr := os.ReadFile(filepath.Join(root, name)) // #nosec G304 -- a fixed name under the root
		if rerr != nil {
			if os.IsNotExist(rerr) {
				continue
			}
			return nil, nil, rerr
		}
		pats, ok := pnpmPackages(string(body))
		if !ok {
			return nil, nil, fmt.Errorf("%s has no \"packages:\" list; this reader could not "+
				"find the members and will not guess at an empty workspace", name)
		}
		add(pats)
	}
	return include, exclude, nil
}

// workspacesField reads the two shapes "workspaces" comes in: npm's array,
// and Yarn v1's object with the array under "packages" beside its nohoist
// list.
func workspacesField(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr, nil
	}
	var obj struct {
		Packages []string `json:"packages"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("\"workspaces\" is neither an array nor an object with "+
			"\"packages\" in it: %w", err)
	}
	return obj.Packages, nil
}

// pnpmPackages reads the "packages:" list out of a pnpm-workspace.yaml.
//
// A narrow reader for one key of one file, NOT a YAML implementation:
// senro's root module takes no third-party dependency. Block sequences and
// flow sequences are read; comments and both quoting styles are honoured.
//
// Everything else (an anchor, an alias, a block scalar, a nested mapping, a
// plain scalar where a list belongs) reports ok=false, and the caller turns
// that into an error naming the file: a member list that silently comes out
// short is a package a change broke and the build skipped.
func pnpmPackages(body string) (pats []string, ok bool) {
	lines := strings.Split(body, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimRight(lines[i], "\r")
		if indentOf(line) != 0 {
			continue
		}
		rest, isKey := strings.CutPrefix(strings.TrimSpace(line), "packages:")
		if !isKey {
			continue
		}
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(rest, "[") {
			return flowSeq(rest)
		}
		if rest != "" && !strings.HasPrefix(rest, "#") {
			// "packages: something" is a scalar, an alias or a block scalar
			// where a list belongs. Not a list, so not read.
			return nil, false
		}
		for j := i + 1; j < len(lines); j++ {
			item := strings.TrimRight(lines[j], "\r")
			trimmed := strings.TrimSpace(item)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			if indentOf(item) == 0 {
				break // back at the top level: the list is over
			}
			entry, isItem := strings.CutPrefix(trimmed, "-")
			if !isItem {
				return nil, false // a nested mapping, not a sequence of patterns
			}
			p, ok := unquoteYAML(strings.TrimSpace(entry))
			if !ok {
				return nil, false
			}
			pats = append(pats, p)
		}
		return pats, true
	}
	return nil, false
}

func indentOf(line string) int {
	return len(line) - len(strings.TrimLeft(line, " \t"))
}

// flowSeq reads "[a, b, c]" on one line, which is the other way a short
// packages list gets written.
func flowSeq(s string) ([]string, bool) {
	s = strings.TrimPrefix(s, "[")
	i := strings.LastIndex(s, "]")
	if i < 0 {
		return nil, false // a flow sequence spread over lines, which this does not read
	}
	var out []string
	for _, part := range strings.Split(s[:i], ",") {
		p, ok := unquoteYAML(strings.TrimSpace(part))
		if !ok {
			return nil, false
		}
		if p != "" {
			out = append(out, p)
		}
	}
	return out, true
}

// yamlIndicators are the characters that start a YAML construct this reader
// does not read: an alias, an anchor, a tag other than the "!" negation pnpm
// members use, a block scalar, a nested collection, a complex key.
//
// Checked on the RAW text, before quoting is stripped, because a quoted
// string containing one of them is just a string.
const yamlIndicators = "*&|>{}[]?%@`"

// unquoteYAML strips one layer of quoting and a trailing comment, and reports
// whether what it was given is a plain string at all.
func unquoteYAML(s string) (string, bool) {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1], true
		}
	}
	if s != "" && strings.ContainsRune(yamlIndicators, rune(s[0])) {
		return "", false
	}
	if i := strings.Index(s, " #"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s), true
}

// memberDirs walks root for directories holding a package.json and keeps
// the ones the patterns select. A walk rather than a glob expansion because
// a pattern may hold "**", and pruned the way every walk in senro is:
// "packages/**" over an installed node_modules tree would otherwise turn a
// bad pattern into a plan too wide to build.
func memberDirs(ctx context.Context, root string, include, exclude []string) ([]string, error) {
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
		if !d.IsDir() {
			return nil
		}
		if rel != "." && ex.Match(rel, true) {
			return fs.SkipDir
		}
		if rel != "." && strings.HasPrefix(path.Base(rel), ".") {
			return fs.SkipDir
		}
		if rel == "." || !matches(include, exclude, rel) {
			return nil
		}
		if fi, statErr := os.Stat(filepath.Join(p, manifestName)); statErr == nil && !fi.IsDir() {
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func matches(include, exclude []string, rel string) bool {
	for _, p := range exclude {
		if workspace.MatchGlob(p, rel) {
			return false
		}
	}
	for _, p := range include {
		if workspace.MatchGlob(p, rel) {
			return true
		}
	}
	return false
}
