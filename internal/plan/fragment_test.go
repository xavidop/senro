package plan_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

// generator is the one existing node a fragment is spliced under, which is
// all SpliceFragment needs of the running graph beyond the ids already in it.
func generatorGraph(ids ...string) map[string]*plan.Node {
	g := make(map[string]*plan.Node, len(ids))
	for _, id := range ids {
		g[id] = &plan.Node{ID: id, Kind: "exec", Cmd: []string{"true"}}
	}
	return g
}

func fragment(nodes []plan.Node, boundary ...string) *plan.Fragment {
	return &plan.Fragment{Version: plan.FragmentVersion, Nodes: nodes, Boundary: boundary}
}

// A fragment names its nodes relatively, so the same generator body can be
// written once and land wherever the generator sits. The splice is what turns
// those into the run's own hierarchical ids, and it has to rewrite the
// fragment's internal edges to match or every one of them dangles.
func TestSpliceFragmentPrefixesIdsAndRewritesInternalNeeds(t *testing.T) {
	f := fragment([]plan.Node{
		{ID: "preflight", Kind: "exec", Cmd: []string{"true"}},
		{ID: "apply", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"preflight"}},
	}, "apply")

	nodes, boundary, err := plan.SpliceFragment(f, "discover", generatorGraph("discover"))
	if err != nil {
		t.Fatalf("SpliceFragment: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(nodes))
	}
	if nodes[0].ID != "discover/preflight" || nodes[1].ID != "discover/apply" {
		t.Errorf("ids = %q, %q; want them prefixed with the generator", nodes[0].ID, nodes[1].ID)
	}
	if len(nodes[1].Needs) != 1 || nodes[1].Needs[0] != "discover/preflight" {
		t.Errorf("Needs = %v, want [discover/preflight]: an internal edge must be rewritten too", nodes[1].Needs)
	}
	if len(boundary) != 1 || boundary[0] != "discover/apply" {
		t.Errorf("boundary = %v, want [discover/apply]", boundary)
	}
}

// A fragment node that needs nothing inside the fragment is given the
// generator itself. Splice timing alone would order it correctly, but the
// GRAPH would not say so, and dependentsClosure walks the graph rather than
// the timing: without this edge a rerun_from on the generator would not reach
// the nodes it generated.
func TestSpliceFragmentGivesARootlessNodeTheGeneratorAsItsNeed(t *testing.T) {
	f := fragment([]plan.Node{
		{ID: "preflight", Kind: "exec", Cmd: []string{"true"}},
		{ID: "apply", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"preflight"}},
	}, "apply")

	nodes, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover"))
	if err != nil {
		t.Fatalf("SpliceFragment: %v", err)
	}
	if len(nodes[0].Needs) != 1 || nodes[0].Needs[0] != "discover" {
		t.Errorf("rootless node Needs = %v, want [discover]", nodes[0].Needs)
	}
	if len(nodes[1].Needs) != 1 {
		t.Errorf("a node with its own in-fragment need must not also gain the generator: Needs = %v", nodes[1].Needs)
	}
}

// An id that already exists is refused. Splicing it would give the run two
// nodes answering to one name, and every cache entry, log file and event
// keyed by that id would describe whichever won.
func TestSpliceFragmentRefusesAnIdThatCollidesWithTheRunningGraph(t *testing.T) {
	f := fragment([]plan.Node{{ID: "apply", Kind: "exec", Cmd: []string{"true"}}})

	_, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover", "discover/apply"))
	if err == nil {
		t.Fatal("SpliceFragment must refuse an id already in the graph")
	}
	if !strings.Contains(err.Error(), "discover/apply") {
		t.Errorf("error %q must name the colliding id", err)
	}
}

// A need pointing outside the fragment is refused: it is the one way a
// fragment could reach into the graph it is being added to, and the
// additive-only rule (design §2.8) is what keeps every recorded cache key and
// every attached client's RunState valid across a splice.
func TestSpliceFragmentRefusesANeedOutsideTheFragment(t *testing.T) {
	f := fragment([]plan.Node{
		{ID: "apply", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"some-other-step"}},
	})

	_, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover", "some-other-step"))
	if err == nil {
		t.Fatal("SpliceFragment must refuse a need naming a node outside the fragment")
	}
	if !strings.Contains(err.Error(), "some-other-step") {
		t.Errorf("error %q must name the offending need", err)
	}
}

// A boundary naming something the fragment does not contain is a typo that
// would otherwise leave the generator's dependents waiting on a node that
// never arrives.
func TestSpliceFragmentRefusesABoundaryItDoesNotContain(t *testing.T) {
	f := fragment([]plan.Node{{ID: "apply", Kind: "exec", Cmd: []string{"true"}}}, "verify")

	_, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover"))
	if err == nil {
		t.Fatal("SpliceFragment must refuse a boundary naming a node the fragment does not contain")
	}
	if !strings.Contains(err.Error(), "verify") {
		t.Errorf("error %q must name the missing boundary node", err)
	}
}

// Two nodes with one id inside a single fragment, caught before either is
// added: the run must never see a half-applied fragment (design §2.8).
func TestSpliceFragmentRefusesDuplicateIdsWithinTheFragment(t *testing.T) {
	f := fragment([]plan.Node{
		{ID: "apply", Kind: "exec", Cmd: []string{"true"}},
		{ID: "apply", Kind: "exec", Cmd: []string{"false"}},
	})

	_, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover"))
	if err == nil {
		t.Fatal("SpliceFragment must refuse a fragment carrying one id twice")
	}
}

// A cycle inside the fragment, which the scheduler would otherwise meet as
// its "dependency cycle or dangling need" abort: the run would die on a graph
// it could not explain, a long way from the generator that produced it.
func TestSpliceFragmentRefusesACycleInsideTheFragment(t *testing.T) {
	f := fragment([]plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"b"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a"}},
	})

	_, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover"))
	if err == nil {
		t.Fatal("SpliceFragment must refuse a fragment whose nodes form a cycle")
	}
}

// An empty fragment is legal and means "nothing to do here". Its dependents
// run immediately rather than being skipped, so it must not be an error.
func TestSpliceFragmentAcceptsAnEmptyFragment(t *testing.T) {
	nodes, boundary, err := plan.SpliceFragment(fragment(nil), "discover", generatorGraph("discover"))
	if err != nil {
		t.Fatalf("an empty fragment is legal: %v", err)
	}
	if len(nodes) != 0 || len(boundary) != 0 {
		t.Errorf("nodes = %v, boundary = %v; want both empty", nodes, boundary)
	}
}

// A malformed node is refused with the same vocabulary an authored one gets:
// a generated step is still a step, and "kind exec with no command" should not
// read differently because a generator produced it.
func TestSpliceFragmentRefusesAMalformedNode(t *testing.T) {
	f := fragment([]plan.Node{{ID: "apply", Kind: "exec"}})

	_, _, err := plan.SpliceFragment(f, "discover", generatorGraph("discover"))
	if err == nil {
		t.Fatal("SpliceFragment must refuse a node that would not pass Validate")
	}
}
