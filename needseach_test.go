package senro_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/unit/glob"
)

// mkdirs makes every directory under root, which is how these tests give
// glob.Dirs a unit set to discover.
func mkdirs(t *testing.T, root string, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// sortedNeedsOf is one node's dependency set, sorted, for comparison against a
// literal. Sorted because a dependency set is unordered (plan.Digest sorts it
// for exactly that reason), so an assertion that pinned the order would fail
// on a change that means nothing.
func sortedNeedsOf(t *testing.T, pl *senro.Plan, id string) []string {
	t.Helper()
	n, ok := pl.Node(id)
	if !ok {
		t.Fatalf("no node %q; nodes are %v", id, nodeIDs(pl))
	}
	out := append([]string(nil), n.Needs...)
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestNeedsEachWiresOneEdgePerUnit is the whole point of NeedsEach: the child
// for a unit waits on the OTHER expansion's child for the same unit, and on
// nothing else. The all-of barrier would have given each of these two
// dependencies rather than one.
func TestNeedsEachWiresOneEdgePerUnit(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web", "apps/api")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Expand("test", glob.Dirs("apps/*")).
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, unit := range []string{"apps/api", "apps/web"} {
		got := sortedNeedsOf(t, pl, "test[unit="+unit+"]")
		want := []string{"build[unit=" + unit + "]"}
		if !sameStrings(got, want) {
			t.Errorf("test[unit=%s] needs %v, want %v", unit, got, want)
		}
	}
	// The upstream children wait on nothing: NeedsEach adds edges in one
	// direction only.
	for _, unit := range []string{"apps/api", "apps/web"} {
		if got := sortedNeedsOf(t, pl, "build[unit="+unit+"]"); len(got) != 0 {
			t.Errorf("build[unit=%s] needs %v, want nothing", unit, got)
		}
	}
}

// TestNeedsEachDownstreamUnitWithNoCounterpartWaitsForTheWholeExpansion is the
// mismatch case in the direction that can go wrong. "docs" has a test and no
// build, so there is no per-unit edge to draw. Dropping the edge would let
// test[unit=docs] run before anything the build expansion produces exists,
// and dropping the STEP would silently stop testing docs. Neither: it falls
// back to the barrier NeedsEach replaced, which is the most conservative
// ordering that is still expressible.
func TestNeedsEachDownstreamUnitWithNoCounterpartWaitsForTheWholeExpansion(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web", "apps/api", "docs/site")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	// One unit set is apps/*, the other is apps/* plus docs.
	w.Expand("test", glob.Dirs("*/*")).
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// The paired ones stay paired.
	for _, unit := range []string{"apps/api", "apps/web"} {
		got := sortedNeedsOf(t, pl, "test[unit="+unit+"]")
		want := []string{"build[unit=" + unit + "]"}
		if !sameStrings(got, want) {
			t.Errorf("test[unit=%s] needs %v, want %v", unit, got, want)
		}
	}
	// The unpaired one waits for every child of the expansion it names.
	got := sortedNeedsOf(t, pl, "test[unit=docs/site]")
	want := []string{"build[unit=apps/api]", "build[unit=apps/web]"}
	if !sameStrings(got, want) {
		t.Errorf("the unpaired child needs %v, want the whole upstream expansion %v", got, want)
	}
}

// TestNeedsEachUpstreamUnitWithNoCounterpartIsNotAnError is the mismatch in
// the other direction: a unit that builds and has nothing to test. Nothing is
// under-connected by that, so it is not a refusal; the upstream child simply
// has no per-unit dependent.
func TestNeedsEachUpstreamUnitWithNoCounterpartIsNotAnError(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web", "apps/api", "apps/vendored")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	// A narrower graph downstream: two of the three units.
	w.Expand("test", glob.Dirs("apps/a*")).
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v; an upstream unit with no downstream counterpart is ordinary", err)
	}
	got := sortedNeedsOf(t, pl, "test[unit=apps/api]")
	if want := []string{"build[unit=apps/api]"}; !sameStrings(got, want) {
		t.Errorf("test[unit=apps/api] needs %v, want %v", got, want)
	}
	// Nothing gained a dependency on the unpaired upstream child, which is
	// what would happen if the mismatch had been handled by falling back to
	// the barrier for everybody.
	for _, n := range pl.Nodes {
		for _, need := range n.Needs {
			if need == "build[unit=apps/vendored]" {
				t.Errorf("node %q waits on the unpaired upstream child", n.ID)
			}
		}
	}
}

// TestNeedsEachWithDisjointUnitSetsIsTheAllOfBarrier pins the degenerate case
// of the fallback: when NO unit pairs up, every child falls back, and the
// result is exactly the barrier a pipeline gets today. NeedsEach can only
// order more, never less.
func TestNeedsEachWithDisjointUnitSetsIsTheAllOfBarrier(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "libs/one", "libs/two", "apps/web", "apps/api")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("libs/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Expand("test", glob.Dirs("apps/*")).
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := []string{"build[unit=libs/one]", "build[unit=libs/two]"}
	for _, unit := range []string{"apps/api", "apps/web"} {
		got := sortedNeedsOf(t, pl, "test[unit="+unit+"]")
		if !sameStrings(got, want) {
			t.Errorf("test[unit=%s] needs %v, want the whole upstream expansion %v", unit, got, want)
		}
	}
}

// TestNeedsEachOnAnEmptyExpansionAddsNoEdge covers the fallback's own edge
// case: an expansion that matched nothing has no children to wait for, and
// falling back to "all of them" is falling back to none. That is the same
// thing an empty group already means, and it must not be a build failure.
func TestNeedsEachOnAnEmptyExpansionAddsNoEdge(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("nothing-here/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Expand("test", glob.Dirs("apps/*")).
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := sortedNeedsOf(t, pl, "test[unit=apps/web]"); len(got) != 0 {
		t.Errorf("test[unit=apps/web] needs %v, and the expansion it names has no children", got)
	}
}

// TestNeedsEachIsAddedToNeedsRatherThanReplacingIt checks the two compose: a
// per-unit edge and a whole-expansion-wide upstream step are different
// dependencies and a child gets both.
func TestNeedsEachIsAddedToNeedsRatherThanReplacingIt(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Step("install", exec.Command("echo", "install"))
	w.Expand("build", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Expand("test", glob.Dirs("apps/*")).
		Needs("install").
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := sortedNeedsOf(t, pl, "test[unit=apps/web]")
	want := []string{"build[unit=apps/web]", "install"}
	if !sameStrings(got, want) {
		t.Errorf("test[unit=apps/web] needs %v, want %v", got, want)
	}
}

// TestNeedsEachRefusesAnUnknownExpansion: NeedsEach names EXPANSIONS and
// Needs names STEPS, the two read identically at a call site, and a step id
// passed here is the mistake worth catching by name at build time. A silent
// no-op would be an expansion that waits for nothing.
func TestNeedsEachRefusesAnUnknownExpansion(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Step("install", exec.Command("echo", "install"))
	w.Expand("test", glob.Dirs("apps/*")).
		NeedsEach("install").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a NeedsEach naming a step rather than an expansion")
	}
	for _, want := range []string{"install", "test", "NeedsEach", "Needs"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q: %v", want, err)
		}
	}
}

// TestNeedsEachRefusesItself: an expansion whose children waited on their own
// siblings is a cycle at best, and each child waiting on itself at worst. The
// generic cycle check would report it in terms of step ids nobody wrote.
func TestNeedsEachRefusesItself(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		NeedsEach("test").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted an expansion that needs each of itself")
	}
	if !strings.Contains(err.Error(), "test") {
		t.Errorf("the error does not name the expansion: %v", err)
	}
}

// TestNeedsEachEdgesAreDeterministic: the edges NeedsEach adds are part of
// the plan and therefore part of its digest, so they must be built the same
// way every time. The mismatch fallback is the risk here: it expands to a set
// of ids, and a set built by ranging over a map would digest differently
// between two builds of one pipeline.
func TestNeedsEachEdgesAreDeterministic(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web", "apps/api", "apps/admin", "docs/site")
	t.Chdir(root)

	build := func() *senro.Plan {
		p := senro.New("mono")
		w := p.Workflow("verify")
		w.Expand("build", glob.Dirs("apps/*")).
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("echo", "build", u.Base()))
			})
		w.Expand("test", glob.Dirs("*/*")).
			NeedsEach("build").
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("echo", "test", u.Base()))
			})
		pl, err := p.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return pl
	}
	first, second := build().Digest(), build().Digest()
	if first != second {
		t.Fatal("two builds of one NeedsEach pipeline produced two digests")
	}
}

// TestNeedsEachPipelines is the property the feature exists for, proven from
// api.Event.Seq rather than a stopwatch: the fast unit's downstream step
// STARTS before the slow unit's upstream step FINISHES, impossible under the
// all-of barrier. It asserts the ordering BOTH ways, because a build with no
// edges at all would interleave too: each unit's test must also start after
// its OWN build finished.
func TestNeedsEachPipelines(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/fast", "apps/slow")
	t.Chdir(root)

	// The slow unit's build blocks until the fast unit's test creates its
	// marker: a synchronisation the ledger then describes, where a fixed
	// sleep would be a race. The wait is BOUNDED: under the all-of barrier
	// the marker never arrives, and an unbounded wait would turn a
	// regression into a hung test.
	gate := filepath.Join(t.TempDir(), "fast-test-started")
	const waitForGate = "n=0; while [ ! -f %s ]; do n=$((n+1)); " +
		"if [ $n -gt 600 ]; then echo 'the fast unit test never started' >&2; exit 9; fi; " +
		"sleep 0.05; done"

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			if u.Base() == "slow" {
				return senro.NewStep(exec.Command("sh", "-c", fmt.Sprintf(waitForGate, gate)))
			}
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Expand("test", glob.Dirs("apps/*")).
		MaxParallel(4).
		NeedsEach("build").
		Template(func(u senro.Unit) *senro.StepBuilder {
			if u.Base() == "fast" {
				return senro.NewStep(exec.Command("sh", "-c", "touch "+gate))
			}
			return senro.NewStep(exec.Command("echo", "test", u.Base()))
		})

	runDir := t.TempDir()
	if err := senro.Run(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID("needseach-1"), senro.WithCacheDir(t.TempDir()),
	); err != nil {
		t.Fatalf("Run: %v; build[unit=apps/slow] gives up when the fast unit's test has not "+
			"started within thirty seconds, which is what a fan-out that stalled at the barrier "+
			"looks like", err)
	}
	events := readLedgerAt(t, runDir)
	startedFastTest, ok := seqOf(events, api.StepStarted, "test[unit=apps/fast]")
	if !ok {
		t.Fatal("the fast unit's test never started")
	}
	finishedSlowBuild, ok := seqOf(events, api.StepFinished, "build[unit=apps/slow]")
	if !ok {
		t.Fatal("the slow unit's build never finished")
	}
	if startedFastTest > finishedSlowBuild {
		t.Errorf("test[unit=apps/fast] started at seq %d, after build[unit=apps/slow] finished at "+
			"seq %d: the fan-out stalled at the barrier instead of pipelining",
			startedFastTest, finishedSlowBuild)
	}
	// And the edges really are there: each unit's test ran after its own
	// build. Without this half, a run with no dependency edges whatsoever
	// would satisfy the assertion above.
	for _, unit := range []string{"apps/fast", "apps/slow"} {
		startedTest, ok := seqOf(events, api.StepStarted, "test[unit="+unit+"]")
		if !ok {
			t.Fatalf("test[unit=%s] never started", unit)
		}
		finishedBuild, ok := seqOf(events, api.StepFinished, "build[unit="+unit+"]")
		if !ok {
			t.Fatalf("build[unit=%s] never finished", unit)
		}
		if startedTest < finishedBuild {
			t.Errorf("test[unit=%s] started at seq %d, before build[unit=%s] finished at seq %d: "+
				"the per-unit edge is missing", unit, startedTest, unit, finishedBuild)
		}
	}
}

// seqOf is the ledger sequence of the first event of a type for one step.
func seqOf(events []api.Event, ty api.Type, step string) (uint64, bool) {
	for _, e := range events {
		if e.Type == ty && e.Step == step {
			return e.Seq, true
		}
	}
	return 0, false
}

// TestAFanOutWithoutNeedsEachIsUnchanged: NeedsEach is an addition. A
// pipeline that never calls it must build the plan it always built, digest
// included.
func TestAFanOutWithoutNeedsEachIsUnchanged(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web", "apps/api")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Needs("install").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Step("install", exec.Command("echo", "install"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// The digest of this plan is pinned so that adding NeedsEach to
	// ExpandBuilder, or a new field to any of the types it touches, cannot
	// silently move the identity of every fan-out that never uses it.
	const want = "sha256:6b09e75ef5b4e0eaa5e84d4ee31e378d254dc9d679d3737ef2a3f3ffb33cae57"
	if got := pl.Digest(); got != want {
		t.Errorf("a fan-out that never calls NeedsEach digests %s, want %s", got, want)
	}
}
