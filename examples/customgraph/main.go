// Command customgraph fans a step out over a unit graph that senro does not
// ship, written entirely against senro's published API: nothing here imports
// internal/, nothing is registered, and the graph below is an ordinary type
// with the right methods. Copy it into your own repository and it works.
//
// The graph reads a components.json at the root of the tree:
//
//	{
//	  "components": [
//	    { "name": "shared",   "dir": "libs/shared" },
//	    { "name": "billing",  "dir": "services/billing",  "needs": ["shared"] },
//	    { "name": "checkout", "dir": "services/checkout", "needs": ["billing"] }
//	  ]
//	}
//
// A made-up format, which is the point: a repository whose layout only it
// knows about is exactly the case senro cannot ship a graph for.
//
// Run it from the repository root. With no --changed, every component runs:
//
//	go run ./examples/customgraph
//	# 3 steps: libs/shared, services/billing, services/checkout
//
// With one, only what the change reaches:
//
//	go run ./examples/customgraph --changed services/checkout/checkout.txt
//	# 1 step:  services/checkout. Nothing depends on it.
//
//	go run ./examples/customgraph --changed libs/shared/shared.txt
//	# 3 steps: everything. checkout does not depend on shared, but it depends
//	#          on billing, which does. That hop is the whole point.
//
//	go run ./examples/customgraph --changed Makefile
//	# 3 steps, for a different reason: the Makefile belongs to no component,
//	#          so nothing can be concluded about it and everything runs.
//
// See https://senro.dev/docs/unit-graphs/ for the walkthrough this file is
// the worked example of.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/exec"
)

// ComponentGraph is a unit graph over a components.json. It satisfies
// senro.UnitGraph (Units, Describe) and senro.UnitAffector (Owns,
// ReverseDeps); the assertions below only make a drifted signature a compile
// error here rather than a plan-time error in a pipeline.
type ComponentGraph struct{ file string }

// Components makes a graph that reads name from the root of whatever tree the
// expansion is built in.
func Components(name string) *ComponentGraph { return &ComponentGraph{file: name} }

var (
	_ senro.UnitGraph    = (*ComponentGraph)(nil)
	_ senro.UnitAffector = (*ComponentGraph)(nil)
)

// component is one entry of the file.
type component struct {
	Name  string   `json:"name"`
	Dir   string   `json:"dir"`
	Needs []string `json:"needs"`
}

// Describe names this graph in a plan and in an error message; the error a
// user sees when an expansion finds nothing is built out of it.
func (g *ComponentGraph) Describe() string { return "components in " + g.file }

// Units reports every component, IN A DETERMINISTIC ORDER. Sorted, because
// an expansion derives child step ids from this slice in order: an order
// that varies between builds varies the plan digest every cache entry hangs
// off. A graph that walked a large tree should also check the context as it
// goes; this one reads a single small file.
func (g *ComponentGraph) Units(ctx context.Context, root string) ([]senro.Unit, error) {
	comps, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	out := make([]senro.Unit, 0, len(comps))
	for _, c := range comps {
		out = append(out, senro.Unit{ID: c.Dir, Name: c.Name, Dir: c.Dir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Owns reports which component each changed file belongs to. The result is
// PARALLEL to files, and an EMPTY element means no unit owns it, which senro
// turns into a run of every unit; that is the answer whenever unsure. The
// rules: a file directly at the root belongs to every component; otherwise
// the nearest component directory at or above it owns it; otherwise nothing
// does and everything runs. Paths must NOT be stat'ed: a deleted file has to
// be answered for from the path alone, and its dependents most need
// rebuilding.
func (g *ComponentGraph) Owns(ctx context.Context, root string, files []string) ([][]string, error) {
	comps, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	all := make([]string, 0, len(comps))
	dirs := make([]string, 0, len(comps))
	for _, c := range comps {
		all = append(all, c.Dir)
		dirs = append(dirs, c.Dir)
	}
	sort.Strings(all)
	// Longest first, so a component nested inside another one wins.
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })

	out := make([][]string, len(files))
	for i, f := range files {
		rel, ok := cleanRel(f)
		if !ok {
			continue // rule 3: outside the tree, so nothing can be concluded
		}
		if path.Dir(rel) == "." {
			out[i] = all // rule 1
			continue
		}
		if d := nearest(dirs, path.Dir(rel)); d != "" {
			out[i] = []string{d} // rule 2
		}
	}
	return out, nil
}

// ReverseDeps reports who depends on whom, DIRECTLY: senro computes the
// transitive closure itself, and a pre-flattened graph would hide any bug in
// that closure behind its own. Values are sorted for the same reason Units
// is.
func (g *ComponentGraph) ReverseDeps(ctx context.Context, root string) (map[string][]string, error) {
	comps, err := g.load(ctx, root)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]string, len(comps))
	for _, c := range comps {
		byName[c.Name] = c.Dir
	}
	out := make(map[string][]string, len(comps))
	for _, c := range comps {
		for _, n := range c.Needs {
			to, ok := byName[n]
			if !ok || to == c.Dir {
				// A "needs" naming nothing is dropped rather than guessed
				// at; there is no step to run for it either way.
				continue
			}
			out[to] = append(out[to], c.Dir)
		}
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out, nil
}

// load reads and validates the file; every question this graph is asked goes
// through here. An unreadable or contradictory file is an ERROR, not an
// empty graph: an expansion that silently produced no steps looks exactly
// like a passing build.
func (g *ComponentGraph) load(ctx context.Context, root string) ([]component, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := os.ReadFile(filepath.Join(root, g.file)) // #nosec G304 -- the file the graph was built with
	if err != nil {
		return nil, fmt.Errorf("customgraph: %s: %w", g.Describe(), err)
	}
	var doc struct {
		Components []component `json:"components"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("customgraph: %s: %w", g.Describe(), err)
	}
	if len(doc.Components) == 0 {
		return nil, fmt.Errorf("customgraph: %s: declares no components", g.Describe())
	}
	seen := make(map[string]bool, len(doc.Components))
	for _, c := range doc.Components {
		switch {
		case c.Name == "" || c.Dir == "":
			return nil, fmt.Errorf("customgraph: %s: a component is missing its name or its dir",
				g.Describe())
		case strings.ContainsAny(c.Dir, "[]=,@"):
			// An expansion builds "test[unit=<id>]" out of these and the
			// grammar has no escape for those five characters.
			return nil, fmt.Errorf("customgraph: %s: component dir %q contains a character "+
				"that would corrupt its own step id", g.Describe(), c.Dir)
		case seen[c.Dir]:
			return nil, fmt.Errorf("customgraph: %s: two components live in %q", g.Describe(), c.Dir)
		}
		seen[c.Dir] = true
	}
	return doc.Components, nil
}

// cleanRel normalises a changed path and reports whether it is inside the
// tree at all. It never touches the filesystem; see Owns.
func cleanRel(p string) (string, bool) {
	p = filepath.ToSlash(strings.TrimSpace(p))
	if p == "" || strings.HasPrefix(p, "/") {
		return "", false
	}
	p = path.Clean(p)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", false
	}
	return p, true
}

// nearest is the longest entry of dirs that is rel or a parent of it. dirs
// must already be sorted longest first.
func nearest(dirs []string, rel string) string {
	for _, d := range dirs {
		if d == rel || strings.HasPrefix(rel, d+"/") {
			return d
		}
	}
	return ""
}

func main() { os.Exit(run()) }

func run() int {
	changed := flag.String("changed", "",
		"comma-separated paths this run changed; empty means everything changed")
	workspace := flag.String("workspace", "examples/customgraph/workspace",
		"the tree to fan out over")
	flag.Parse()

	// An expansion's root is the directory the pipeline is BUILT in, so this
	// program moves into the tree it fans out over; a real pipeline lives at
	// the top of its own repository and needs none of this.
	if err := os.Chdir(*workspace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := senro.Run(context.Background(), pipeline(changes(*changed))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// changes turns the flag into a senro.ChangeSource. An empty flag is
// change.Everything(), NOT change.Paths() with no arguments: "nothing
// changed" builds no steps, and a caller that does not KNOW what changed has
// to say "everything".
func changes(list string) senro.ChangeSource {
	if strings.TrimSpace(list) == "" {
		return change.Everything()
	}
	return change.Paths(strings.Split(list, ",")...)
}

func pipeline(src senro.ChangeSource) *senro.Pipeline {
	p := senro.New("customgraph")
	p.Workflow("verify").
		Expand("test", Components("components.json")).
		Affected(src).
		MaxParallel(4).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("sh", "-c",
				"echo 'test "+u.Name+"' # "+u.Dir))
		})
	return p
}
