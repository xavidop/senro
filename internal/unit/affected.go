package unit

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// Affector is a Graph that can also answer the two questions an affected
// set is computed from: which unit a changed file belongs to, and which
// units break when a unit changes.
//
// A SEPARATE interface, not two more methods on Graph: Graph is published
// (senro.UnitGraph aliases it), and growing a published interface breaks
// every outside implementation. An optional interface lets a graph opt in
// when it has a basis for an answer (gowork does; glob answers through
// ErrNoAffectedSet). The cost is the standard optional-interface trade:
// Affected asserts at plan time rather than compile time, and the error
// names the graph.
type Affector interface {
	Graph

	// Owns reports which units each of files belongs to. PARALLEL to files;
	// an empty element means no unit owns it, which Affected reads as "this
	// could have affected anything". One file can belong to many units (a
	// go.mod belongs to every package of its module). Files are
	// slash-separated, relative to root, treated as PATHS and never
	// stat'ed, so a deleted file (the change whose dependents most need
	// rebuilding) is answered for like any other.
	Owns(ctx context.Context, root string, files []string) ([][]string, error)

	// ReverseDeps reports each unit's DIRECT dependents, keyed and valued by
	// Unit.ID; absent and empty mean the same. Direct only: the transitive
	// closure is Affected's, so a graph implements the cheap half and cannot
	// get the expensive half subtly wrong. Values must be deterministic.
	ReverseDeps(ctx context.Context, root string) (map[string][]string, error)
}

// ErrNoAffectedSet reports that a graph has no basis for answering which
// units a change affects. An error, not an empty set or a silent "run
// everything": a guess that looks like a computed answer is how a monorepo
// CI skips the unit a change actually broke.
var ErrNoAffectedSet = errors.New("cannot compute an affected set")

// Result is what an affected-set computation concluded.
type Result struct {
	// Units are the units that must run, in graph order, so child ids and
	// the plan's digest do not depend on how this computed them.
	Units []Unit
	// All reports that every unit is affected, whether reached by the change
	// or over-approximated. See Why.
	All bool
	// Total is how many units the graph found in all: what a width guard
	// like MaxNodes checks against, since a 40k-unit graph is a mistake
	// whatever today's pull request touched.
	Total int
	// Why is one line naming the reason this set is what it is.
	Why string
}

// Affected reports which of g's units a change to files must run: a unit
// runs if the change touched a file it owns, or if it depends, at any
// depth, on a unit that did.
//
// Where unsure it over-approximates: running an extra unit costs CI
// minutes, skipping a broken one reports a green build for a tree that does
// not build. So a file no unit owns affects everything; a file attributed
// to a unit the graph did not report (the graph disagreeing with itself)
// affects everything. An empty file list affects nothing: the caller saying
// the change is genuinely empty. A caller that does not KNOW what changed
// must not call this; senro's change sources answer "everything" for that
// case instead.
func Affected(ctx context.Context, g Graph, root string, files []string) (Result, error) {
	// Checked here, not left to the graph: the first step shells out to a
	// toolchain over a whole workspace, and the answer a cancelled build
	// gets must not depend on which Graph it was handed.
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	a, ok := g.(Affector)
	if !ok {
		return Result{}, fmt.Errorf("unit: %s %w: it discovers units but knows nothing about "+
			"which unit depends on which", g.Describe(), ErrNoAffectedSet)
	}
	units, err := g.Units(ctx, root)
	if err != nil {
		return Result{}, err
	}
	res := Result{Total: len(units)}
	if len(files) == 0 {
		res.Why = "nothing changed"
		return res, nil
	}

	byID := make(map[string]bool, len(units))
	for _, u := range units {
		byID[u.ID] = true
	}

	owners, err := a.Owns(ctx, root, files)
	if err != nil {
		return Result{}, err
	}
	if len(owners) != len(files) {
		return Result{}, fmt.Errorf("unit: %s: Owns was asked about %d files and answered for %d; "+
			"the answer has to be parallel to the question", g.Describe(), len(files), len(owners))
	}

	seed := make(map[string]bool)
	for i, ids := range owners {
		if len(ids) == 0 {
			return everything(units, fmt.Sprintf("%q belongs to no unit, so it could have "+
				"changed what any of them builds", files[i])), nil
		}
		for _, id := range ids {
			if !byID[id] {
				return everything(units, fmt.Sprintf("%q was attributed to unit %q, which %s "+
					"did not report", files[i], id, g.Describe())), nil
			}
			seed[id] = true
		}
	}

	rdeps, err := a.ReverseDeps(ctx, root)
	if err != nil {
		return Result{}, err
	}

	// Breadth-first over dependents, marking on push, so a cycle (which this
	// interface cannot forbid) terminates instead of hanging a build.
	hit := make(map[string]bool, len(seed))
	queue := make([]string, 0, len(seed))
	for _, u := range units { // units order, not map order: determinism, again
		if seed[u.ID] {
			hit[u.ID] = true
			queue = append(queue, u.ID)
		}
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, dep := range rdeps[id] {
			// A dependent that is not a unit is a graph bug; dropping it is
			// safe, since Units reported no step to run for it either way.
			if !byID[dep] || hit[dep] {
				continue
			}
			hit[dep] = true
			queue = append(queue, dep)
		}
	}

	res.Units = make([]Unit, 0, len(hit))
	for _, u := range units {
		if hit[u.ID] {
			res.Units = append(res.Units, u)
		}
	}
	res.All = len(res.Units) == len(units)
	res.Why = fmt.Sprintf("%d of %d units, from %d changed %s",
		len(res.Units), len(units), len(files), plural(len(files), "file", "files"))
	return res, nil
}

// everything is the over-approximation, in one place so no caller can
// forget to set All.
func everything(units []Unit, why string) Result {
	return Result{Units: units, Total: len(units), All: true, Why: why + "; every unit runs"}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// SortedKeys is the deterministic key order a ReverseDeps implementation
// needs for its values, here rather than re-written in every graph.
func SortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
