// Package maven discovers the modules of a Maven reactor, and knows which one
// depends on which.
//
// It implements the affected-set interface, so an expansion over it runs only
// the modules a change reaches:
//
//	verify.Expand("test", maven.Modules()).
//		Affected(change.FromTrigger(ev)).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("mvn", "-pl", u.Name, "test"))
//		})
//
// A unit is one reactor project: its ID and Dir are the project directory
// relative to the root, and its Name is "groupId:artifactId", which is what
// `mvn -pl` takes.
//
// # It reads poms, and runs nothing
//
// A pom.xml states its modules and dependencies as data, and this package
// reads them without running anything, so it works with no mvn installed
// and an empty ~/.m2. For a Gradle build, whose settings file is a program
// rather than data, use github.com/xavidop/senro/unit/gradle.
//
// # What is a unit
//
// The reactor: the root pom.xml, and the transitive closure of its <modules>,
// profiles included. Modules declared only inside a <profile> are units too,
// because this graph cannot know which profiles a build will activate and a
// module nothing ever builds is worse than a module built too often.
//
// A tree with no pom.xml at the root is an error rather than an empty graph.
// A repository holding several unrelated Maven projects with no aggregator
// above them is not a reactor and is not supported; point one expansion at
// each project root.
//
// A <module> that is not on disk is an error too. Maven itself fails on one,
// and a reactor read as smaller than it is is a build that skips what it did
// not see.
//
// # Where it deliberately runs too much, and one place it must not
//
// See Owns and ReverseDeps. Every ambiguity resolves towards running more
// modules, with one deliberate exception: a <dependencyManagement> entry is
// NOT a dependency unless it is a <scope>import</scope> BOM. A root pom
// manages versions for the whole reactor, and every module descends from
// the root, so treating managed versions as dependencies would make every
// change run the whole repository: an over-approximation that covers
// everything is the feature switched off while still looking on.
package maven

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/xavidop/senro/internal/unit"
)

// Unit is the value a template receives, re-exported so a pipeline never has
// to name an internal package.
type Unit = unit.Unit

// manifestName is the file that marks a Maven project.
const manifestName = "pom.xml"

// maxProjects bounds the reactor walk. A <modules> graph is a tree in every
// sane repository, and a cycle in it (a module that aggregates its own
// ancestor) would otherwise walk forever; the visited set already stops that,
// and this stops a pathological generated tree from exhausting memory first.
const maxProjects = 20000

// Graph is a maven unit graph. Build one with Modules.
//
// One Graph memoizes one reading per root, so the three calls an affected set
// makes read the poms once between them. See gowork's Graph for why the memo
// has no expiry.
type Graph struct {
	mu    sync.Mutex
	cache map[string]*listing
}

// Modules makes one unit per reactor project.
//
// Aggregators included. A pom-packaging project builds almost nothing, so its
// step usually does almost nothing, and leaving it out would lose the edge
// that makes a change to a parent pom run its children.
func Modules() *Graph { return &Graph{} }

// Graph answers every question an affected set needs. Asserted here so a
// signature that drifts is a compile error in this package rather than a
// plan-time error in somebody's monorepo.
var _ unit.Affector = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string { return "maven modules" }

// Units reports every reactor project under root, sorted by ID.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return append([]Unit(nil), l.units...), nil
}

// Owns reports which project each of files belongs to, in three rules:
//
//  1. A file DIRECTLY in the reactor root belongs to EVERY project: the root
//     pom.xml, a .mvn directory's neighbours, a Jenkinsfile, a shared
//     checkstyle config.
//  2. Otherwise the nearest project directory at or above the file owns it,
//     which puts src/main, src/test and a module's own pom.xml on that module.
//  3. Otherwise no project owns it and everything runs. In a reactor this is
//     rare, because the root project's directory is an ancestor of every path
//     under it, and everything descends from the root project anyway.
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

// ReverseDeps reports the projects that DIRECTLY depend on each project,
// sorted.
//
// Four sources of an edge, all of them things that make one project's build
// read another:
//
//   - <dependencies>, at every scope. A test-scoped dependency is an edge for
//     the same reason a test-only import is one in Go.
//   - <parent>, and <modules> from the aggregator's side. A change to a parent
//     pom changes the properties, the plugin configuration and the managed
//     versions every child builds with.
//   - <dependencyManagement> entries with <scope>import</scope>, which import
//     a BOM and are read at build time. Other dependencyManagement entries are
//     NOT edges; see the package doc for why that one exception matters more
//     than it looks.
//   - <build><plugins> and <pluginManagement>, so a repository that builds its
//     own Maven plugin rebuilds the modules that use it.
//
// Coordinates are interpolated against the properties of the project and its
// parents, plus the ${project.*} built-ins, which is what resolves the
// ${project.groupId} and ${project.version} a real multi-module pom is full
// of. A coordinate that STILL does not resolve is handled by running more:
//
//   - an unresolved groupId matches on artifactId alone,
//   - an unresolved artifactId, which no real pom writes, makes the project
//     depend on every project in the reactor, so any change runs it.
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
		return nil, fmt.Errorf("maven: %w", err)
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

// coord is a Maven coordinate as written, before interpolation.
type coord struct {
	GroupID    string `xml:"groupId"`
	ArtifactID string `xml:"artifactId"`
	Version    string `xml:"version"`
	Scope      string `xml:"scope"`
	Type       string `xml:"type"`
}

// props is a <properties> element, whose children are arbitrary names.
type props map[string]string

// UnmarshalXML reads every child element of <properties> as a name and a
// value. encoding/xml has no map support, so this is the one place this
// package hand-decodes.
func (p *props) UnmarshalXML(d *xml.Decoder, _ xml.StartElement) error {
	if *p == nil {
		*p = props{}
	}
	for {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			var v string
			if err := d.DecodeElement(&v, &t); err != nil {
				return err
			}
			(*p)[t.Name.Local] = strings.TrimSpace(v)
		case xml.EndElement:
			return nil
		}
	}
}

// section is the part of a pom that a <profile> can carry a second copy of.
type section struct {
	Modules              []string `xml:"modules>module"`
	Properties           props    `xml:"properties"`
	Dependencies         []coord  `xml:"dependencies>dependency"`
	DependencyManagement struct {
		Dependencies []coord `xml:"dependencies>dependency"`
	} `xml:"dependencyManagement"`
	Build struct {
		Plugins          []coord `xml:"plugins>plugin"`
		PluginManagement struct {
			Plugins []coord `xml:"plugins>plugin"`
		} `xml:"pluginManagement"`
	} `xml:"build"`
}

// pom is the slice of a pom.xml this graph reads.
//
// The struct tags name local elements only, which is what makes them match
// inside the http://maven.apache.org/POM/4.0.0 namespace every real pom
// declares.
type pom struct {
	section
	XMLName    xml.Name `xml:"project"`
	GroupID    string   `xml:"groupId"`
	ArtifactID string   `xml:"artifactId"`
	Version    string   `xml:"version"`
	Packaging  string   `xml:"packaging"`
	Parent     struct {
		GroupID      string `xml:"groupId"`
		ArtifactID   string `xml:"artifactId"`
		Version      string `xml:"version"`
		RelativePath string `xml:"relativePath"`
	} `xml:"parent"`
	Profiles []section `xml:"profiles>profile"`
}

// project is one parsed pom and where it sits.
type project struct {
	dir string
	pom *pom
	// group and version after inheritance from <parent>, which is where a
	// module that declares neither gets them.
	group   string
	version string
}

func (p *project) name() string { return p.group + ":" + p.pom.ArtifactID }

func (g *Graph) discover(ctx context.Context, root string) (*listing, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("maven: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("maven: %s: %s is not a directory", g.Describe(), root)
	}
	if _, err := os.Stat(filepath.Join(root, manifestName)); err != nil {
		return nil, fmt.Errorf("maven: %s: no %s at %s, so there is no reactor to read; point "+
			"the expansion at the directory holding the aggregator pom, or use a different unit "+
			"graph", g.Describe(), manifestName, root)
	}
	projects, err := reactor(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("maven: %s: %w", g.Describe(), err)
	}
	return build(projects), nil
}

// reactor reads the root pom and the transitive closure of its modules.
func reactor(ctx context.Context, root string) ([]*project, error) {
	seen := map[string]bool{}
	var out []*project
	// A queue rather than recursion, so a deep reactor cannot blow the stack
	// on somebody's generated monorepo.
	queue := []string{"."}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dir := queue[0]
		queue = queue[1:]
		if seen[dir] {
			continue
		}
		seen[dir] = true
		if len(out) >= maxProjects {
			return nil, fmt.Errorf("the reactor under %s has more than %d projects; that is a "+
				"generated <modules> tree rather than a repository, and reading it would not end",
				root, maxProjects)
		}
		p, err := readPom(filepath.Join(root, filepath.FromSlash(dir), manifestName))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path.Join(dir, manifestName), err)
		}
		out = append(out, &project{dir: dir, pom: p})

		// The main <modules> list is unconditional, and Maven fails on an
		// entry that is not there. A profile's is conditional, so a missing
		// one is a layout this build was never going to use.
		for _, m := range p.Modules {
			d, err := moduleDir(root, dir, m)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path.Join(dir, manifestName), err)
			}
			queue = append(queue, d)
		}
		for _, prof := range p.Profiles {
			for _, m := range prof.Modules {
				if d, err := moduleDir(root, dir, m); err == nil {
					queue = append(queue, d)
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dir < out[j].dir })
	return out, nil
}

// moduleDir resolves one <module> entry, which names either a directory or
// the pom file inside it.
func moduleDir(root, from, module string) (string, error) {
	rel, ok := unit.CleanRel(path.Join(from, module))
	if !ok {
		return "", fmt.Errorf("<module>%s</module> resolves outside the reactor root", module)
	}
	rel = strings.TrimSuffix(rel, "/"+manifestName)
	if path.Base(rel) == manifestName {
		rel = path.Dir(rel)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel), manifestName)); err != nil {
		return "", fmt.Errorf("<module>%s</module> has no %s: %w", module, manifestName, err)
	}
	return rel, nil
}

func readPom(p string) (*pom, error) {
	body, err := os.ReadFile(p) // #nosec G304 -- a path derived from the root this graph was given
	if err != nil {
		return nil, err
	}
	var out pom
	if err := xml.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.ArtifactID == "" {
		return nil, fmt.Errorf("has no <artifactId>, so it names no project")
	}
	return &out, nil
}

// build turns the reactor into the graph.
func build(projects []*project) *listing {
	byCoord := make(map[string]string, len(projects)) // "group:artifact" -> unit ID
	byArtifact := make(map[string][]string, len(projects))
	byDir := make(map[string]*project, len(projects))
	for _, p := range projects {
		byDir[p.dir] = p
	}
	// Inheritance first: a module that declares no groupId or version takes
	// its parent's, and the parent is usually the pom one directory up.
	for _, p := range projects {
		p.group, p.version = inherited(p, byDir)
	}
	for _, p := range projects {
		byCoord[p.group+":"+p.pom.ArtifactID] = p.dir
		byArtifact[p.pom.ArtifactID] = append(byArtifact[p.pom.ArtifactID], p.dir)
	}
	for k := range byArtifact {
		sort.Strings(byArtifact[k])
	}

	units := make([]Unit, 0, len(projects))
	for _, p := range projects {
		units = append(units, Unit{ID: p.dir, Name: p.name(), Dir: p.dir})
	}
	sort.Slice(units, func(i, j int) bool { return units[i].ID < units[j].ID })

	all := make([]string, 0, len(units))
	for _, u := range units {
		all = append(all, u.ID)
	}

	rev := make(map[string]map[string]bool)
	edge := func(to, from string) {
		if to == "" || to == from {
			return
		}
		if rev[to] == nil {
			rev[to] = make(map[string]bool)
		}
		rev[to][from] = true
	}
	for _, p := range projects {
		vars := properties(p, byDir)
		for _, c := range dependencyCoords(p) {
			for _, to := range match(c, vars, byCoord, byArtifact, all) {
				edge(to, p.dir)
			}
		}
		// <parent>: this project reads it, so it depends on it.
		if p.pom.Parent.ArtifactID != "" {
			pc := coord{GroupID: p.pom.Parent.GroupID, ArtifactID: p.pom.Parent.ArtifactID}
			for _, to := range match(pc, vars, byCoord, byArtifact, all) {
				edge(to, p.dir)
			}
		}
		// <modules>: the aggregator's pom is read when its module builds, so
		// the edge runs the modules when the aggregator changes. It is
		// usually the same edge <parent> already drew; it is here for the
		// reactor whose aggregator is not also the parent.
		for _, m := range moduleDirs(p) {
			if _, ok := byDir[m]; ok {
				edge(p.dir, m)
			}
		}
	}
	l := &listing{units: units, owner: unit.NewPathOwner(units),
		rdeps: make(map[string][]string, len(rev))}
	for to, set := range rev {
		l.rdeps[to] = unit.SortedKeys(set)
	}
	return l
}

// moduleDirs is every directory this project aggregates, profiles included.
func moduleDirs(p *project) []string {
	var out []string
	add := func(mods []string) {
		for _, m := range mods {
			rel, ok := unit.CleanRel(path.Join(p.dir, m))
			if !ok {
				continue
			}
			if path.Base(rel) == manifestName {
				rel = path.Dir(rel)
			}
			out = append(out, rel)
		}
	}
	add(p.pom.Modules)
	for _, prof := range p.pom.Profiles {
		add(prof.Modules)
	}
	return out
}

// dependencyCoords is every coordinate in this pom that makes its build read
// another project, profiles included.
func dependencyCoords(p *project) []coord {
	var out []coord
	add := func(s *section) {
		out = append(out, s.Dependencies...)
		for _, c := range s.DependencyManagement.Dependencies {
			// Only an imported BOM. See the package doc: treating the rest as
			// dependencies makes a root pom depend on the whole reactor.
			if strings.EqualFold(c.Scope, "import") {
				out = append(out, c)
			}
		}
		out = append(out, s.Build.Plugins...)
		out = append(out, s.Build.PluginManagement.Plugins...)
	}
	add(&p.pom.section)
	for i := range p.pom.Profiles {
		add(&p.pom.Profiles[i])
	}
	return out
}

// match resolves one coordinate to the reactor projects it could name.
func match(c coord, vars map[string]string, byCoord map[string]string,
	byArtifact map[string][]string, all []string,
) []string {
	artifact := interpolate(c.ArtifactID, vars)
	if unresolved(artifact) {
		// No real pom writes this, and there is nothing left to match on. The
		// safe answer is that this project could depend on anything, so any
		// change runs it.
		return all
	}
	group := interpolate(c.GroupID, vars)
	if unresolved(group) || group == "" {
		// The artifactId is the only evidence left. Matching on it alone can
		// draw an edge to a project that happens to share a name with an
		// external dependency, which over-runs; the other mistake skips a
		// build.
		return byArtifact[artifact]
	}
	if id, ok := byCoord[group+":"+artifact]; ok {
		return []string{id}
	}
	return nil
}

func unresolved(s string) bool { return strings.Contains(s, "${") }

// properties are the values ${...} resolves against: this project's own
// <properties>, its parents' (a child overrides), and the ${project.*}
// built-ins a multi-module pom writes its own coordinates with.
func properties(p *project, byDir map[string]*project) map[string]string {
	out := map[string]string{}
	// Ancestors first, so the nearest pom's value wins on a collision.
	chain := ancestry(p, byDir)
	for i := len(chain) - 1; i >= 0; i-- {
		for k, v := range chain[i].pom.Properties {
			out[k] = v
		}
		for _, prof := range chain[i].pom.Profiles {
			for k, v := range prof.Properties {
				// A profile's properties are conditional. Taking them anyway
				// resolves a coordinate that would otherwise be a wildcard,
				// and a wrong resolution can only draw an edge to a project
				// that exists, which over-runs at worst.
				if _, set := out[k]; !set {
					out[k] = v
				}
			}
		}
	}
	for k, v := range map[string]string{
		"groupId": p.group, "artifactId": p.pom.ArtifactID, "version": p.version,
		"project.groupId": p.group, "project.artifactId": p.pom.ArtifactID,
		"project.version": p.version,
		"pom.groupId":     p.group, "pom.artifactId": p.pom.ArtifactID, "pom.version": p.version,
		"project.parent.groupId": p.pom.Parent.GroupID,
		"project.parent.version": p.pom.Parent.Version,
	} {
		if v != "" {
			out[k] = v
		}
	}
	return out
}

// ancestry is this project and its <parent> chain, as far as the chain stays
// inside the reactor.
func ancestry(p *project, byDir map[string]*project) []*project {
	out := []*project{p}
	seen := map[string]bool{p.dir: true}
	for cur := p; ; {
		next := parentOf(cur, byDir)
		if next == nil || seen[next.dir] {
			return out
		}
		seen[next.dir] = true
		out = append(out, next)
		cur = next
	}
}

// parentOf finds a project's parent pom inside the reactor, by relativePath
// when it has one and by directory convention otherwise.
func parentOf(p *project, byDir map[string]*project) *project {
	if p.pom.Parent.ArtifactID == "" {
		return nil
	}
	if rp := strings.TrimSpace(p.pom.Parent.RelativePath); rp != "" {
		rel, ok := unit.CleanRel(path.Join(p.dir, rp))
		if ok {
			if path.Base(rel) == manifestName {
				rel = path.Dir(rel)
			}
			if q, ok := byDir[rel]; ok {
				return q
			}
		}
	}
	// Maven's default relativePath is "..".
	if p.dir != "." {
		if q, ok := byDir[path.Dir(p.dir)]; ok && q.pom.ArtifactID == p.pom.Parent.ArtifactID {
			return q
		}
	}
	for _, q := range byDir {
		if q.dir != p.dir && q.pom.ArtifactID == p.pom.Parent.ArtifactID {
			return q
		}
	}
	return nil
}

// inherited is a project's effective groupId and version, taken from its
// <parent> when it declares neither of its own.
func inherited(p *project, byDir map[string]*project) (group, version string) {
	group, version = p.pom.GroupID, p.pom.Version
	if group != "" && version != "" {
		return group, version
	}
	if group == "" && p.pom.Parent.GroupID != "" {
		group = p.pom.Parent.GroupID
	}
	if version == "" && p.pom.Parent.Version != "" {
		version = p.pom.Parent.Version
	}
	if group != "" && version != "" {
		return group, version
	}
	// A parent that declares its own coordinates only through ITS parent.
	if q := parentOf(p, byDir); q != nil {
		pg, pv := inherited(q, byDir)
		if group == "" {
			group = pg
		}
		if version == "" {
			version = pv
		}
	}
	return group, version
}

// interpolate expands ${...} against vars, repeatedly, because a property's
// value can name another property. It stops at a small depth rather than
// looping on a self-referential pom.
func interpolate(s string, vars map[string]string) string {
	for range 8 {
		if !strings.Contains(s, "${") {
			return strings.TrimSpace(s)
		}
		next := expandOnce(s, vars)
		if next == s {
			return strings.TrimSpace(s)
		}
		s = next
	}
	return strings.TrimSpace(s)
}

func expandOnce(s string, vars map[string]string) string {
	var b strings.Builder
	for {
		i := strings.Index(s, "${")
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		j := strings.Index(s[i:], "}")
		if j < 0 {
			b.WriteString(s)
			return b.String()
		}
		key := s[i+2 : i+j]
		v, ok := vars[key]
		b.WriteString(s[:i])
		if !ok {
			// Left as written, so unresolved() still sees it and the caller
			// over-approximates rather than matching a literal "${...}".
			b.WriteString(s[i : i+j+1])
		} else {
			b.WriteString(v)
		}
		s = s[i+j+1:]
	}
}
