// Package gradle discovers the projects of a Gradle build, and knows which one
// depends on which, when the build says so in a form that can be read.
//
//	verify.Expand("test", gradle.Projects()).
//		Affected(change.FromTrigger(ev)).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("./gradlew", u.Name+":test"))
//		})
//
// A unit is one Gradle project: its ID and Dir are the project directory
// relative to the root, and its Name is the project path, ":libs:core", which
// is what a gradle invocation takes. ID and Dir are the same string, and
// neither is the Name, because settings.gradle can put a project anywhere.
//
// # It reads the declarative subset, and refuses the rest
//
// settings.gradle(.kts) is a program rather than data, but the overwhelming
// majority of settings files are a list of literal includes. This package
// reads that declarative subset exactly and REFUSES on the rest, with an
// error naming the line it stopped at. The refusal is the design: reading
// only the includes that ARE literal would produce a project list shorter
// than the build's that looks exactly like a correct one, and a short list
// is an affected set that skips the project a change broke. Refusing is
// recoverable; a plausible wrong answer is not.
//
// From settings.gradle (or settings.gradle.kts when there is none, gradle's
// own precedence) it reads:
//
//   - `include ':a'`, `include ':a', ':b'` and `include(":a", ":b")`, in
//     either DSL. Every ANCESTOR of an included path is a project too:
//     `include ':libs:core'` is two projects, ':libs' and ':libs:core'.
//   - `project(':a:b').projectDir = file('elsewhere')`. A projectDir that is
//     not a literal is a refusal, except "$rootDir/x" and "$settingsDir/x",
//     which name the directory this graph was handed. An unset projectDir
//     comes from the ROOT directory and the whole project path, NOT from a
//     parent's reassigned directory (gradle 9.7.0 looks for :tools:codegen
//     in tools/codegen even when :tools moved).
//   - `includeBuild 'build-logic'`, recorded as a build this graph does not
//     read.
//   - `rootProject.name`, `enableFeaturePreview` and imports; the bodies of
//     pluginManagement, dependencyResolutionManagement, plugins, buildscript
//     and similar blocks are SKIPPED rather than read (an includeBuild is
//     still picked out of them).
//
// Everything else (an `if`, a loop, an `apply from:`, a variable, an
// interpolated include path, `includeFlat`) is a refusal.
//
// From each project's build.gradle(.kts) it reads `project(':lib')` and
// `project(path: ':lib')` anywhere in the script (evaluationDependsOn and
// cross-project task reads are couplings too), the `projects.lib` type-safe
// accessors mapped back through Gradle's own rendering, and `apply from:`
// scripts, read as part of the project that applied them.
//
// A settings file it cannot read fails Units and therefore everything. A
// computed project reference in buildSrc or an included build fails the
// affected set alone: Units still works, so a fan-out over every project
// still works, and only Affected refuses.
//
// # Ambiguity runs MORE, with two refusals
//
// Every ambiguity resolves towards running more projects: a needless run
// costs CI minutes, a skipped broken project reports a green build for a
// tree that does not build. Every project depends on its PARENT up to the
// root (subprojects{} configures children), so a change to the root build
// script or settings reruns everything. buildSrc and included builds are
// separate builds whose output is on every build script's classpath: a
// change under one affects EVERYTHING, and a project reference inside one
// makes every project depend on the referenced project (which can put a
// CYCLE in ReverseDeps; unit.Affected marks on push and terminates). A
// computed `project(...)`, or a `projects.x` resolving to no project, makes
// THAT project depend on every project.
//
// Two cases refuse the affected set instead, because their only available
// over-approximation is "every change runs the whole repository": a
// computed project reference in a separate build, and one in the ROOT
// project's build script. Both leave Units working.
//
// # It reads files, and runs nothing
//
// No gradle, no daemon, no JVM and no network. There is deliberately no
// execute-based mode: a second mode that is right where the first is wrong
// would just be the wrong one running by accident on somebody's CI.
//
// The honest limit, shared with unit/jswork: this is the DECLARED graph. A
// project that reads another's files without declaring a dependency gets no
// edge, and neither does one wired up by a plugin this reader cannot see.
// If your build works that way, fan out over these units and run every one.
package gradle

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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

// ErrNotDeclarative reports that part of a Gradle build is a program this
// package will not pretend to interpret. Deliberately an error rather than
// a smaller graph or a wider affected set: both are indistinguishable in a
// log from a correct answer, and a CI that cannot tell them apart will
// eventually trust a green build that skipped the project a change broke.
var ErrNotDeclarative = errors.New("this build cannot be read declaratively")

const (
	settingsGroovy = "settings.gradle"
	settingsKotlin = "settings.gradle.kts"
	buildGroovy    = "build.gradle"
	buildKotlin    = "build.gradle.kts"
)

// maxProjects bounds the project set, the way maven's maxProjects bounds a
// reactor: a settings file with more includes than this is generated, and a
// step per project would be a mistake whichever of them today's change touched.
const maxProjects = 20000

// maxSharedFiles bounds the walk of buildSrc and the included builds. A
// convention-plugin build is tens of files; something with more than this in it
// is a vendored dependency tree, and reading all of it would cost more than the
// affected set saves.
const maxSharedFiles = 5000

// maxApplied bounds how many `apply from:` scripts one project's chain may
// pull in, so a pair of scripts applying each other cannot loop.
const maxApplied = 64

// sharedPrune are directories a separate build's walk does not descend into.
var sharedPrune = map[string]bool{
	"build": true, ".gradle": true, ".git": true, "node_modules": true, ".idea": true,
}

// Graph is a gradle unit graph. Build one with Projects.
//
// One Graph memoizes one reading per root, so the three calls an affected set
// makes read the scripts once between them.
type Graph struct {
	mu    sync.Mutex
	cache map[string]*listing
}

// Projects makes one unit per Gradle project, container projects included.
//
// A container is a project Gradle created out of an include of something below
// it, ':libs' out of `include ':libs:core'`. It is a real project, `gradle -q
// projects` lists it, it is allowed a build script of its own, and leaving it
// out would lose both the files in its directory and the edge that makes a
// change to it rerun everything under it.
func Projects() *Graph { return &Graph{} }

// Graph answers every question an affected set needs. Asserted here so a
// signature that drifts is a compile error in this package rather than a
// plan-time error in somebody's monorepo.
var _ unit.Affector = (*Graph)(nil)

// Describe names this graph for a plan and for an error message.
func (g *Graph) Describe() string { return "gradle projects" }

// Units reports every project of the build under root, sorted by ID.
func (g *Graph) Units(ctx context.Context, root string) ([]Unit, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	return append([]Unit(nil), l.units...), nil
}

// Owns reports which project each of files belongs to.
//
// A file under buildSrc or an included build belongs to NO project, and
// unit.Affected turns that into a run of everything: those are separate
// builds whose output is on every build script's classpath. That rule comes
// first, ahead of nearest-project, because an included build can live
// inside a project's directory.
//
// Otherwise the shared rules: a file DIRECTLY in the root directory belongs
// to every project (settings.gradle, gradle.properties, the root build
// script and gradlew live exactly there); otherwise the nearest project
// directory at or above the file owns it.
//
// Paths are slash-separated, relative to root, and never stat'ed, so a file
// a change deleted is answered for exactly like one it added.
func (g *Graph) Owns(ctx context.Context, root string, files []string) ([][]string, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	out := make([][]string, len(files))
	for i, f := range files {
		rel, ok := unit.CleanRel(f)
		if ok && l.separateBuild(rel) {
			continue
		}
		out[i] = l.owner.Owners(f)
	}
	return out, nil
}

// ReverseDeps reports the projects that DIRECTLY depend on each project,
// sorted, or refuses in the two cases the package doc sets out. It refuses
// HERE rather than from Units so discovery keeps working: a fan-out that
// runs every project is still worth having, and only the narrowing is
// unavailable.
func (g *Graph) ReverseDeps(ctx context.Context, root string) (map[string][]string, error) {
	l, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	if l.edgeErr != nil {
		return nil, l.edgeErr
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
	// separate are the root-relative directories of builds this graph does not
	// read: buildSrc, and every includeBuild.
	separate []string
	rdeps    map[string][]string
	// edgeErr is why the edges could not be read, when they could not be.
	edgeErr error
}

func (l *listing) separateBuild(rel string) bool {
	for _, d := range l.separate {
		if rel == d || strings.HasPrefix(rel, d+"/") {
			return true
		}
	}
	return false
}

func (g *Graph) load(ctx context.Context, root string) (*listing, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("gradle: %w", err)
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

// project is one Gradle project and where it sits.
type project struct {
	path string // ":", ":libs", ":libs:core"
	dir  string // ".", "libs", "libs/core", slash-separated and root-relative
}

func (g *Graph) discover(ctx context.Context, root string) (*listing, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("gradle: %s: %w", g.Describe(), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("gradle: %s: %s is not a directory", g.Describe(), root)
	}
	name, src, err := settingsFile(root)
	if err != nil {
		return nil, fmt.Errorf("gradle: %s: %w", g.Describe(), err)
	}
	set, err := readSettings(name, src)
	if err != nil {
		return nil, fmt.Errorf("gradle: %s: %w", g.Describe(), err)
	}
	projects, err := layout(root, set)
	if err != nil {
		return nil, fmt.Errorf("gradle: %s: %w", g.Describe(), err)
	}
	return g.build(ctx, root, set, projects)
}

// settingsFile reads the settings script. settings.gradle wins over
// settings.gradle.kts when both are there, which is what gradle 9.7.0 does.
func settingsFile(root string) (string, string, error) {
	for _, name := range []string{settingsGroovy, settingsKotlin} {
		src, err := readFile(filepath.Join(root, name))
		if err == nil {
			return name, src, nil
		}
		if !os.IsNotExist(err) {
			return "", "", fmt.Errorf("%s: %w", name, err)
		}
	}
	return "", "", fmt.Errorf("no %s and no %s at %s, so there is no build to read; point the "+
		"expansion at the directory holding the settings file, or use a different unit graph",
		settingsGroovy, settingsKotlin, root)
}

// layout turns the includes into projects, with the directory each one is
// actually in, and refuses a layout Gradle itself would refuse.
func layout(root string, s *settings) ([]*project, error) {
	byPath := map[string]*project{":": {path: ":", dir: "."}}
	order := []string{":"}
	for _, inc := range s.includes {
		segs := splitPath(inc)
		if len(segs) == 0 {
			return nil, fmt.Errorf("%s: include %q names no project", s.file, inc)
		}
		for i := range segs {
			p := ":" + strings.Join(segs[:i+1], ":")
			if _, ok := byPath[p]; ok {
				continue
			}
			if len(byPath) >= maxProjects {
				return nil, fmt.Errorf("%s includes more than %d projects; that is a generated "+
					"settings file rather than a repository, and a step per project would be a "+
					"mistake whichever of them changed", s.file, maxProjects)
			}
			// A project's default directory comes from the ROOT and its whole
			// path, not from its parent's directory: gradle 9.7.0 looks for
			// :tools:codegen in tools/codegen even when :tools has been moved.
			byPath[p] = &project{path: p, dir: strings.Join(segs[:i+1], "/")}
			order = append(order, p)
		}
	}
	for p, dir := range s.dirs {
		q, ok := byPath[p]
		if !ok {
			return nil, fmt.Errorf("%s reassigns the projectDir of %s, which no include creates; "+
				"gradle fails on that too", s.file, p)
		}
		q.dir = dir
	}

	byDir := make(map[string]string, len(order))
	out := make([]*project, 0, len(order))
	for _, p := range order {
		q := byPath[p]
		if q.path != ":" {
			rel, ok := unit.CleanRel(q.dir)
			if !ok {
				return nil, fmt.Errorf("project %s sits at %q, outside the root; a unit senro "+
					"cannot name with a root-relative path is a unit no changed file could ever "+
					"be attributed to", q.path, q.dir)
			}
			q.dir = rel
		}
		fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(q.dir)))
		if err != nil || !fi.IsDir() {
			return nil, fmt.Errorf("project %s has no directory at %q. Gradle 9 refuses to "+
				"configure a project whose directory does not exist, and a step whose working "+
				"directory is not there could not run either", q.path, q.dir)
		}
		if other, dup := byDir[q.dir]; dup {
			return nil, fmt.Errorf("projects %s and %s are both at %q, so they would share one "+
				"unit id and one of them would silently disappear", other, q.path, q.dir)
		}
		byDir[q.dir] = q.path
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dir < out[j].dir })
	return out, nil
}

// build reads the build scripts and assembles the graph.
func (g *Graph) build(ctx context.Context, root string, s *settings,
	projects []*project,
) (*listing, error) {
	units := make([]Unit, 0, len(projects))
	dirOf := make(map[string]string, len(projects))
	byAccessor := make(map[string]string, len(projects))
	for _, p := range projects {
		units = append(units, Unit{ID: p.dir, Name: p.path, Dir: p.dir})
		dirOf[p.path] = p.dir
		if p.path != ":" {
			segs := splitPath(p.path)
			acc := make([]string, len(segs))
			for i, seg := range segs {
				acc[i] = accessorName(seg)
			}
			byAccessor[strings.Join(acc, ".")] = p.path
		}
	}
	l := &listing{units: units, owner: unit.NewPathOwner(units)}

	// buildSrc is implicit: it is a separate build whenever the directory is
	// there, with nothing in settings.gradle to say so.
	if fi, err := os.Stat(filepath.Join(root, "buildSrc")); err == nil && fi.IsDir() {
		l.separate = append(l.separate, "buildSrc")
	}
	for _, b := range s.builds {
		rel, ok := unit.CleanRel(b)
		if !ok {
			return nil, fmt.Errorf("gradle: %s: %s includes the build at %q, which is outside the "+
				"root: %w. Part of this build lives where senro cannot see it, its convention "+
				"plugins can give any project a dependency, and neither the files nor the edges "+
				"could be read. Point the expansion at a directory holding the whole composite, "+
				"or fan out over unit/glob without an affected set",
				g.Describe(), s.file, b, ErrNotDeclarative)
		}
		l.separate = append(l.separate, rel)
	}
	sort.Strings(l.separate)

	rev := make(map[string]map[string]bool, len(projects))
	edge := func(to, from string) {
		if to == "" || from == "" || to == from {
			return
		}
		if rev[to] == nil {
			rev[to] = make(map[string]bool)
		}
		rev[to][from] = true
	}

	// A project is configured by its parent, and every project by the root.
	for _, p := range projects {
		if p.path != ":" {
			edge(dirOf[parentPath(p.path)], p.dir)
		}
	}

	// Each project's own script, plus whatever it applies.
	for _, p := range projects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		paths, dynamic := scriptRefs(root, p.dir, byAccessor)
		for _, ref := range paths {
			if to, ok := dirOf[ref]; ok {
				edge(to, p.dir)
			} else {
				// The script names a project the settings file does not
				// create. Gradle fails on that, so this is a broken build; the
				// safe reading is that the edge could go anywhere.
				dynamic = true
			}
		}
		if !dynamic {
			continue
		}
		if p.path == ":" {
			l.edgeErr = fmt.Errorf("gradle: %s: the root build script computes the project a "+
				"dependency points at rather than naming one: %w. Every project already depends "+
				"on the root, so the only reading left is that every project depends on every "+
				"project, which is this feature switched off while still looking switched on. "+
				"Fan out over these units without Affected and run every one",
				g.Describe(), ErrNotDeclarative)
			break
		}
		for _, q := range projects {
			edge(q.dir, p.dir)
		}
	}

	// buildSrc and the included builds, whose convention plugins are applied
	// by projects that never mention them.
	if l.edgeErr == nil {
		shared, where, err := sharedRefs(ctx, root, l.separate, byAccessor)
		if err != nil {
			return nil, err
		}
		switch {
		case where != "":
			l.edgeErr = fmt.Errorf("gradle: %s: %s computes the project a dependency points at "+
				"rather than naming one: %w. A convention plugin is applied by projects whose "+
				"own build script does not mention it, so there is no one project to attribute "+
				"the edge to, and the only reading left is that every project depends on every "+
				"project, which is this feature switched off while still looking switched on. "+
				"Fan out over these units without Affected and run every one",
				g.Describe(), where, ErrNotDeclarative)
		default:
			for _, ref := range shared {
				to, ok := dirOf[ref]
				if !ok {
					continue // a project of the separate build's own graph, not of this one
				}
				for _, q := range projects {
					edge(to, q.dir)
				}
			}
		}
	}

	l.rdeps = make(map[string][]string, len(rev))
	for to, set := range rev {
		l.rdeps[to] = unit.SortedKeys(set)
	}
	return l, nil
}

// scriptRefs reads one project's build script and every script it applies by a
// path this reader can resolve, and reports the project paths it names and
// whether any reference was one it could not read.
func scriptRefs(root, dir string, byAccessor map[string]string) ([]string, bool) {
	first, ok := buildScript(root, dir)
	if !ok {
		return nil, false
	}
	type job struct{ file, base string }
	var paths []string
	dynamic := false
	seen := map[string]bool{first: true}
	queue := []job{{file: first, base: dir}}
	for len(queue) > 0 {
		j := queue[0]
		queue = queue[1:]
		src, err := readFile(filepath.Join(root, filepath.FromSlash(j.file)))
		if err != nil {
			// An applied script that is not there: the project's dependencies
			// are somewhere this reader cannot see.
			dynamic = true
			continue
		}
		r := scan(dropEOL(lex(src)), byAccessor)
		paths = append(paths, r.paths...)
		dynamic = dynamic || r.dynamic
		for _, a := range r.applies {
			base := j.base
			if a.fromRoot {
				base = "."
			}
			rel, ok := unit.CleanRel(path.Join(base, a.path))
			if !ok || len(seen) >= maxApplied {
				dynamic = true
				continue
			}
			if seen[rel] {
				continue
			}
			seen[rel] = true
			queue = append(queue, job{file: rel, base: path.Dir(rel)})
		}
	}
	return paths, dynamic
}

// buildScript is the root-relative path of a project's build script.
// build.gradle wins over build.gradle.kts when both are there, which is what
// gradle 9.7.0 does.
func buildScript(root, dir string) (string, bool) {
	for _, name := range []string{buildGroovy, buildKotlin} {
		rel := path.Join(dir, name)
		if fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err == nil &&
			!fi.IsDir() {
			return rel, true
		}
	}
	return "", false
}

// sharedExts are the files in a separate build that can hold a project
// reference: a build script, a precompiled script plugin, or the Kotlin or
// Groovy source of a plugin.
var sharedExts = []string{".gradle", ".kts", ".kt", ".groovy"}

// sharedRefs scans buildSrc and the included builds. It returns the project
// paths they name, and the first file that computed one instead of naming it.
func sharedRefs(ctx context.Context, root string, dirs []string,
	byAccessor map[string]string,
) ([]string, string, error) {
	var out []string
	seen := 0
	for _, d := range dirs {
		base := filepath.Join(root, filepath.FromSlash(d))
		if fi, err := os.Stat(base); err != nil || !fi.IsDir() {
			continue
		}
		var found string
		err := filepath.WalkDir(base, func(p string, e fs.DirEntry, err error) error {
			if err != nil {
				return nil // an unreadable corner of a build this graph only samples
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.IsDir() {
				if p != base && sharedPrune[e.Name()] {
					return fs.SkipDir
				}
				return nil
			}
			if seen >= maxSharedFiles {
				return fs.SkipAll
			}
			if !hasExt(e.Name(), sharedExts) {
				return nil
			}
			seen++
			src, err := readFile(p)
			if err != nil {
				return nil
			}
			r := scan(dropEOL(lex(src)), byAccessor)
			out = append(out, r.paths...)
			if r.dynamic && found == "" {
				rel, _ := filepath.Rel(root, p)
				found = filepath.ToSlash(rel)
			}
			return nil
		})
		if err != nil {
			return nil, "", err
		}
		if found != "" {
			return nil, found, nil
		}
	}
	sort.Strings(out)
	return out, "", nil
}

func hasExt(name string, exts []string) bool {
	for _, e := range exts {
		if strings.HasSuffix(name, e) {
			return true
		}
	}
	return false
}

func readFile(p string) (string, error) {
	b, err := os.ReadFile(p) // #nosec G304 -- a path derived from the root this graph was given
	if err != nil {
		return "", err
	}
	return string(b), nil
}
