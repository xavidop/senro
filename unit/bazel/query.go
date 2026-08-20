package bazel

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/xavidop/senro/internal/unit"
)

// QueryGraph is a Bazel unit graph that can also compute an affected set, by
// asking BAZEL for the dependency edges instead of reading them out of BUILD
// files.
//
// # Why this is a second graph and not a flag on the first
//
// Packages() deliberately refuses an affected set, and that refusal is right
// for the reason its doc gives: a BUILD file is a Starlark program, its edges
// routinely are not written in it, and a statically-parsed approximation is a
// missing edge, which is a package left out, which is a green build for a
// tree that does not build.
//
// bazel query has no such problem. It answers the question exactly, because
// it is bazel. What it costs is everything Packages() was protecting:
//
//   - It needs bazel on the machine, and it is a BUILD, not a lookup: a JVM
//     starts, every BUILD file is evaluated, and under bzlmod repository
//     rules RUN during module resolution, which is arbitrary code executing
//     while senro is still planning.
//   - Its answer depends on the machine: the bazel version, the module
//     lockfile, the configuration.
//
// Those are real, so this graph is a SEPARATE, explicitly chosen thing rather
// than a mode Packages() slips into. Choosing it is choosing to run bazel
// during planning.
//
// # It never skips
//
// The objection that kept query out of Packages() was that a graph which
// "skips cleanly when bazel is absent" computes different sets on CI and on a
// laptop with nothing saying which happened. So this one does not skip: bazel
// missing, bazel failing, or output it cannot parse is an ERROR, and the
// expansion fails. A graph that guessed would be the failure mode this whole
// package exists to avoid.
//
// # What it asks, and what it does with the answer
//
// One query for the whole workspace, not one per package:
//
//	bazel query --output=xml 'kind(rule, //...)'
//
// which prints every rule in this workspace and the labels it takes as
// inputs. Each edge is mapped from TARGET to PACKAGE, because a unit here is
// a package, and edges to other repositories (labels starting with "@") are
// dropped: they are not units of this workspace and cannot be affected by a
// change inside it.
//
// Units come from the same tree walk Packages() uses, not from the query. A
// package is a directory holding a BUILD file, which is a fact about the tree
// and needs no toolchain; only the EDGES need bazel.
type QueryGraph struct {
	pkgs Graph

	mu    sync.Mutex
	cache map[string]map[string][]string

	// run is the query, swappable for tests. A test that shelled out to a
	// real bazel would be testing bazel, would need one installed, and could
	// not run in this repository's CI.
	run func(ctx context.Context, root string) ([]byte, error)
}

// Query makes a Bazel unit graph that computes affected sets by running
// `bazel query`. See QueryGraph for what that costs and why it is a separate
// graph from Packages.
func Query() *QueryGraph {
	return &QueryGraph{run: runBazelQuery}
}

var (
	_ unit.Graph    = (*QueryGraph)(nil)
	_ unit.Affector = (*QueryGraph)(nil)
)

// Describe names this graph for a plan and for an error message. Distinct
// from Packages()'s, so a plan digest and an error both say which of the two
// was used.
func (q *QueryGraph) Describe() string { return "bazel query" }

// Units reports every Bazel package under root, exactly as Packages does.
func (q *QueryGraph) Units(ctx context.Context, root string) ([]Unit, error) {
	return q.pkgs.Units(ctx, root)
}

// Owns reports which package each changed file belongs to: the nearest
// ancestor directory that is a package, which is Bazel's own rule for which
// package a file is in.
//
// Pure path arithmetic over the discovered packages, with no bazel involved,
// and never a stat: a DELETED file is the change whose dependents most need
// rebuilding, and it must be answered for like any other.
//
// A file under no package at all gets an empty entry, which unit.Affected
// reads as "this could have affected anything".
func (q *QueryGraph) Owns(ctx context.Context, root string, files []string) ([][]string, error) {
	us, err := q.Units(ctx, root)
	if err != nil {
		return nil, err
	}
	dirs := make([]string, 0, len(us))
	for _, u := range us {
		dirs = append(dirs, u.ID)
	}
	// Longest first, so "libs/core" wins over "libs" for libs/core/x.go and a
	// nested package is never attributed to its parent.
	dirs = unit.LongestFirst(dirs)

	out := make([][]string, len(files))
	for i, f := range files {
		rel, ok := unit.CleanRel(path.Clean(f))
		if !ok {
			continue
		}
		if owner := unit.Nearest(dirs, rel); owner != "" {
			out[i] = []string{owner}
		}
	}
	return out, nil
}

// ReverseDeps reports each package's direct dependents, from one bazel query
// over the whole workspace.
func (q *QueryGraph) ReverseDeps(ctx context.Context, root string) (map[string][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if rd, ok := q.cache[root]; ok {
		return rd, nil
	}

	body, err := q.run(ctx, root)
	if err != nil {
		return nil, fmt.Errorf(
			"bazel: %s: running bazel query in %s: %w; this graph computes its affected set FROM "+
				"bazel, so it fails rather than guessing at one. Use bazel.Packages() to fan out "+
				"over every package instead",
			q.Describe(), root, err)
	}
	rd, err := reverseDepsFromXML(body)
	if err != nil {
		return nil, fmt.Errorf("bazel: %s: %w", q.Describe(), err)
	}
	if q.cache == nil {
		q.cache = make(map[string]map[string][]string, 1)
	}
	q.cache[root] = rd
	return rd, nil
}

// queryResult is the shape of `bazel query --output=xml`: rules, and the
// labels each takes as an input. Only the two attributes this needs are
// declared; encoding/xml ignores the rest.
type queryResult struct {
	Rules []struct {
		Name   string `xml:"name,attr"`
		Inputs []struct {
			Name string `xml:"name,attr"`
		} `xml:"rule-input"`
	} `xml:"rule"`
}

// reverseDepsFromXML turns target edges into package edges, reversed.
//
// A rule's input is a target this rule depends on, so the input's package is
// a dependency of the rule's package, and this map is keyed the other way: it
// answers "who breaks when this changes".
func reverseDepsFromXML(body []byte) (map[string][]string, error) {
	var q queryResult
	if err := xml.Unmarshal(stripXMLDecl(body), &q); err != nil {
		return nil, fmt.Errorf("parsing bazel query --output=xml: %w", err)
	}
	seen := make(map[string]map[string]bool)
	for _, r := range q.Rules {
		to, ok := packageOf(r.Name)
		if !ok {
			continue
		}
		for _, in := range r.Inputs {
			from, ok := packageOf(in.Name)
			if !ok || from == to {
				// A rule's inputs include its own sources; a package
				// depending on itself is not an edge.
				continue
			}
			if seen[from] == nil {
				seen[from] = make(map[string]bool)
			}
			seen[from][to] = true
		}
	}
	out := make(map[string][]string, len(seen))
	for from, tos := range seen {
		ids := make([]string, 0, len(tos))
		for to := range tos {
			ids = append(ids, to)
		}
		// Sorted: ReverseDeps must be deterministic, and a map's order is
		// not. See unit.Affector.
		sort.Strings(ids)
		out[from] = ids
	}
	return out, nil
}

// packageOf is the package id for a Bazel label: "//apps/web:web" is
// "apps/web", "//:root" is ".".
//
// Labels naming another repository ("@rules_go//go:def") report false: they
// are not units of this workspace, so nothing here can be affected by them
// and nothing here can affect them.
func packageOf(label string) (string, bool) {
	if !strings.HasPrefix(label, "//") {
		return "", false
	}
	rest := strings.TrimPrefix(label, "//")
	if i := strings.IndexByte(rest, ':'); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" {
		return ".", true
	}
	return rest, true
}

// runBazelQuery is the real query.
//
// --noshow_progress and --ui_event_filters keep bazel's own chatter off
// stdout, which is being parsed. Errors carry stderr, because "bazel query
// failed" without bazel's own message is not something a person can act on.
func runBazelQuery(ctx context.Context, root string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "bazel",
		"query", "--output=xml", "--noshow_progress",
		"--ui_event_filters=,+error", "kind(rule, //...)")
	cmd.Dir = root
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}
		return nil, err
	}
	return out, nil
}

// stripXMLDecl removes a leading <?xml ...?> declaration.
//
// bazel emits `<?xml version="1.1" ...?>`, and Go's encoding/xml refuses any
// version but 1.0 outright. The declaration carries nothing this parser
// needs: the document body is the rules and their inputs, and bazel's output
// is UTF-8 either way, which is the encoding Go assumes with no declaration.
//
// Dropping it rather than rewriting the version, so nothing here pretends to
// have validated a version it did not.
func stripXMLDecl(body []byte) []byte {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if !bytes.HasPrefix(trimmed, []byte("<?xml")) {
		return body
	}
	end := bytes.Index(trimmed, []byte("?>"))
	if end < 0 {
		return body
	}
	return trimmed[end+2:]
}
