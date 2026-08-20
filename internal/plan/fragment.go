package plan

import (
	"encoding/json"
	"fmt"
)

// FragmentVersion is the wire version of a plan fragment. It is checked on
// every parse rather than assumed: a fragment is written by tools this
// repository does not ship, in languages it does not choose, and a fragment
// from a future senro must be refused by name instead of silently losing
// whatever field this build does not know about.
const FragmentVersion = 1

// Fragment is a piece of graph a generator produced: the nodes to splice
// into the run that is already executing, and the boundary its dependents
// wait on.
//
// Node ids here are RELATIVE. The engine prefixes each with the generator's
// own id at splice time, so a fragment does not have to know where in the
// graph it will land, and the ids it produces are hierarchical and stable
// (design §2.8: deploy/discover-clusters/apply-cm4-jpmc).
//
// This is the public schema. "Write a plan fragment to this path" is a
// contract any language can honour, and it is the same shape a Go generator
// serializes to, so both forms take one validation path and one CAS blob.
type Fragment struct {
	Version int    `json:"version"`
	Nodes   []Node `json:"nodes"`
	// Boundary names the fragment nodes the generator's existing dependents
	// must wait on. Empty is legal and means those dependents wait only on
	// the generator itself.
	Boundary []string `json:"boundary,omitempty"`
}

// ParseFragment decodes a fragment and checks its version.
//
// It deliberately does NOT check the fragment against a graph: that needs
// the generator's id and the run's existing nodes, and belongs at splice
// time where an all-or-nothing refusal can be reported against the step that
// produced it.
func ParseFragment(b []byte) (*Fragment, error) {
	var f Fragment
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("plan: fragment is not valid JSON: %w", err)
	}
	if f.Version != FragmentVersion {
		return nil, fmt.Errorf(
			"plan: fragment version %d, want %d; this senro cannot read a fragment written for another",
			f.Version, FragmentVersion)
	}
	return &f, nil
}

// SpliceFragment checks f against the graph it is about to join and returns
// the nodes to add, fully prefixed and resolved, together with the prefixed
// boundary.
//
// All-or-nothing, and that is a correctness requirement rather than a
// nicety: a partially spliced fragment leaves the graph in a state no re-run
// can reproduce (design §2.8), so every check runs against a fully resolved
// candidate set before the caller is given a single node to append.
//
// existing is the run's live node set by id. Only membership is read, which
// is all that "does this collide" and "is this graph still a DAG" need.
func SpliceFragment(f *Fragment, generatorID string, existing map[string]*Node) ([]Node, []string, error) {
	return prefixFragment(f, generatorID, existing, true)
}

// SubgraphPlan turns a fragment into a plan a NESTED run executes, for
// senro.RunSubgraph.
//
// The same checks and the same prefixing a splice uses, with one difference:
// a rootless node is NOT anchored to the parent step. The parent is not in
// this plan (it is the step that is running the plan), so an edge to it would
// dangle where a splice's edge resolves.
func SubgraphPlan(f *Fragment, parentID string) (*Plan, error) {
	nodes, _, err := prefixFragment(f, parentID, nil, false)
	if err != nil {
		return nil, err
	}
	return &Plan{Version: 1, Nodes: nodes}, nil
}

// prefixFragment validates f and rewrites it under prefixID.
//
// anchor says whether a node with no in-fragment needs gains a need on
// prefixID itself: true when splicing into a running graph that contains it,
// false for a nested plan that does not.
func prefixFragment(f *Fragment, generatorID string, existing map[string]*Node, anchor bool) ([]Node, []string, error) {
	if f == nil {
		return nil, nil, fmt.Errorf("plan: generator %q produced no fragment", generatorID)
	}
	prefix := func(id string) string { return generatorID + "/" + id }

	// Pass 1: every node on its own, and its id, before any edge is read.
	// The relative id is checked for emptiness HERE rather than left to
	// nodeShape, because "" prefixes to "generator/" which is not empty and
	// would sail through.
	local := make(map[string]bool, len(f.Nodes))
	for i := range f.Nodes {
		if f.Nodes[i].ID == "" {
			return nil, nil, fmt.Errorf("plan: generator %q produced a node with an empty id", generatorID)
		}
		if local[f.Nodes[i].ID] {
			return nil, nil, fmt.Errorf(
				"plan: generator %q produced the id %q twice; a fragment names each of its nodes once",
				generatorID, f.Nodes[i].ID)
		}
		local[f.Nodes[i].ID] = true
	}

	// Pass 2: edges, resolved against the fragment alone. A need naming
	// anything else is refused: it is the only way a fragment could reach
	// into the graph it is joining, and additive-only is what keeps every
	// recorded cache key and every attached RunState valid across a splice.
	for i := range f.Nodes {
		for _, need := range f.Nodes[i].Needs {
			if !local[need] {
				return nil, nil, fmt.Errorf(
					"plan: generator %q: node %q needs %q, which is not in the fragment; "+
						"a fragment may only depend on its own nodes",
					generatorID, f.Nodes[i].ID, need)
			}
		}
	}
	for _, b := range f.Boundary {
		if !local[b] {
			return nil, nil, fmt.Errorf(
				"plan: generator %q declares %q as its boundary, but the fragment has no such node",
				generatorID, b)
		}
	}
	if err := fragmentAcyclic(f, generatorID); err != nil {
		return nil, nil, err
	}

	// Pass 3: the prefixed form, and only now against the running graph.
	nodes := make([]Node, 0, len(f.Nodes))
	for i := range f.Nodes {
		n := f.Nodes[i]
		n.ID = prefix(n.ID)
		if _, clash := existing[n.ID]; clash {
			return nil, nil, fmt.Errorf(
				"plan: generator %q produced node %q, which the run already has",
				generatorID, n.ID)
		}
		needs := make([]string, 0, len(n.Needs))
		for _, need := range n.Needs {
			needs = append(needs, prefix(need))
		}
		// A node needing nothing inside the fragment is anchored to the
		// generator. Splice timing already orders it, but dependentsClosure
		// walks the GRAPH, so without this edge a rerun_from on the generator
		// would not reach what it generated.
		if len(needs) == 0 && anchor {
			needs = []string{generatorID}
		}
		n.Needs = needs
		if err := nodeShape(n); err != nil {
			return nil, nil, fmt.Errorf("plan: generator %q: %w", generatorID, err)
		}
		nodes = append(nodes, n)
	}

	boundary := make([]string, 0, len(f.Boundary))
	for _, b := range f.Boundary {
		boundary = append(boundary, prefix(b))
	}
	return nodes, boundary, nil
}

// fragmentAcyclic reports a cycle among the fragment's own nodes.
//
// Checked explicitly even though refusing outside needs already stops a
// fragment closing a loop through the existing graph: the cost is one
// traversal, and the alternative is the scheduler meeting the cycle as its
// "dependency cycle or dangling need" abort, which kills the run with a
// message about the graph rather than about the generator that produced it.
func fragmentAcyclic(f *Fragment, generatorID string) error {
	const (
		unvisited = 0
		onStack   = 1
		done      = 2
	)
	byID := make(map[string]*Node, len(f.Nodes))
	for i := range f.Nodes {
		byID[f.Nodes[i].ID] = &f.Nodes[i]
	}
	state := make(map[string]int, len(f.Nodes))
	var walk func(id string) error
	walk = func(id string) error {
		switch state[id] {
		case onStack:
			return fmt.Errorf(
				"plan: generator %q produced a fragment with a dependency cycle through %q",
				generatorID, id)
		case done:
			return nil
		}
		state[id] = onStack
		for _, need := range byID[id].Needs {
			if err := walk(need); err != nil {
				return err
			}
		}
		state[id] = done
		return nil
	}
	for i := range f.Nodes {
		if err := walk(f.Nodes[i].ID); err != nil {
			return err
		}
	}
	return nil
}
