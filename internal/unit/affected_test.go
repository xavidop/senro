package unit_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
)

// fake is a hand-built Affector with no filesystem or toolchain behind it:
// every question this file asks is about unit.Affected itself.
type fake struct {
	units []unit.Unit
	// owns maps a changed path to its owners; no entry means owned by
	// nothing.
	owns map[string][]string
	// rdeps maps a unit ID to its DIRECT dependents.
	rdeps map[string][]string

	ownsErr  error
	rdepsErr error
	unitsErr error
}

func (f *fake) Describe() string { return "fake" }

func (f *fake) Units(context.Context, string) ([]unit.Unit, error) {
	return f.units, f.unitsErr
}

func (f *fake) Owns(_ context.Context, _ string, files []string) ([][]string, error) {
	if f.ownsErr != nil {
		return nil, f.ownsErr
	}
	out := make([][]string, len(files))
	for i, p := range files {
		out[i] = f.owns[p]
	}
	return out, nil
}

func (f *fake) ReverseDeps(context.Context, string) (map[string][]string, error) {
	return f.rdeps, f.rdepsErr
}

// chain is A <- B <- C: B imports A, C imports B, and C does not import A.
// D is off on its own and must never be dragged in.
func chain() *fake {
	return &fake{
		units: []unit.Unit{
			{ID: "a", Name: "a", Dir: "a"},
			{ID: "b", Name: "b", Dir: "b"},
			{ID: "c", Name: "c", Dir: "c"},
			{ID: "d", Name: "d", Dir: "d"},
		},
		owns: map[string][]string{
			"a/a.go": {"a"},
			"b/b.go": {"b"},
			"c/c.go": {"c"},
			"d/d.go": {"d"},
		},
		rdeps: map[string][]string{
			"a": {"b"},
			"b": {"c"},
		},
	}
}

func affectedIDs(t *testing.T, g unit.Graph, files []string) []string {
	t.Helper()
	res, err := unit.Affected(context.Background(), g, "/root", files)
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	out := make([]string, 0, len(res.Units))
	for _, u := range res.Units {
		out = append(out, u.ID)
	}
	return out
}

func eq(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestAffectedFollowsDependentsTransitively: a closure that stopped after
// its first hop would still pass "A changes, B runs". C is two hops from
// the change and must still run.
func TestAffectedFollowsDependentsTransitively(t *testing.T) {
	got := affectedIDs(t, chain(), []string{"a/a.go"})
	if want := []string{"a", "b", "c"}; !eq(got, want) {
		t.Fatalf("Affected(a/a.go) = %v, want %v (C imports B imports A, so all three)", got, want)
	}
}

// TestAffectedDoesNotWalkDependenciesTheOtherWay stops "transitive" from
// meaning "everything connected": a change to C breaks nothing, since
// nobody imports it.
func TestAffectedDoesNotWalkDependenciesTheOtherWay(t *testing.T) {
	got := affectedIDs(t, chain(), []string{"c/c.go"})
	if want := []string{"c"}; !eq(got, want) {
		t.Fatalf("Affected(c/c.go) = %v, want %v; C's own dependencies are not affected by C changing", got, want)
	}
	got = affectedIDs(t, chain(), []string{"b/b.go"})
	if want := []string{"b", "c"}; !eq(got, want) {
		t.Fatalf("Affected(b/b.go) = %v, want %v", got, want)
	}
}

// TestAffectedKeepsTheGraphsOwnOrder pins that the result is a filter of
// Units, not a set rebuilt from map iteration: child ids come from this
// order, and a nondeterministic one moves the plan digest between builds.
func TestAffectedKeepsTheGraphsOwnOrder(t *testing.T) {
	f := chain()
	f.units = []unit.Unit{
		{ID: "c", Dir: "c"}, {ID: "b", Dir: "b"}, {ID: "a", Dir: "a"}, {ID: "d", Dir: "d"},
	}
	got := affectedIDs(t, f, []string{"a/a.go"})
	if want := []string{"c", "b", "a"}; !eq(got, want) {
		t.Fatalf("Affected = %v, want the graph's own order %v", got, want)
	}
}

// TestAffectedTreatsAFileNoUnitOwnsAsAffectingEverything is the deliberate
// over-approximation: a Makefile or CI workflow lives in no unit and can
// change what every unit builds.
func TestAffectedTreatsAFileNoUnitOwnsAsAffectingEverything(t *testing.T) {
	res, err := unit.Affected(context.Background(), chain(), "/root", []string{"c/c.go", "Makefile"})
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	if !res.All {
		t.Errorf("All = false, want true: a file no unit owns has to affect everything")
	}
	if len(res.Units) != 4 {
		t.Fatalf("Units = %v, want all four", res.Units)
	}
	if !strings.Contains(res.Why, "Makefile") {
		t.Errorf("Why = %q, want it to name the file that forced a full run", res.Why)
	}
}

// TestAffectedOfNoFilesIsNoUnits: nothing changed must not read as
// everything changed. Nil and empty mean the same here; "I do not know" is
// expressed by not calling this at all.
func TestAffectedOfNoFilesIsNoUnits(t *testing.T) {
	for _, files := range [][]string{nil, {}} {
		res, err := unit.Affected(context.Background(), chain(), "/root", files)
		if err != nil {
			t.Fatalf("Affected(%v): %v", files, err)
		}
		if len(res.Units) != 0 || res.All {
			t.Errorf("Affected(%v) = %v (all=%v), want nothing", files, res.Units, res.All)
		}
		if res.Total != 4 {
			t.Errorf("Total = %d, want 4", res.Total)
		}
	}
}

// TestAffectedRefusesAGraphThatCannotComputeOne is the honesty requirement:
// glob knows which directories exist and nothing about imports, so the
// answer must be an error, not a silent "here is everything".
func TestAffectedRefusesAGraphThatCannotComputeOne(t *testing.T) {
	_, err := unit.Affected(context.Background(), onlyUnits{}, "/root", []string{"a/a.go"})
	if err == nil {
		t.Fatal("Affected on a graph with no dependency information returned no error")
	}
	if !errors.Is(err, unit.ErrNoAffectedSet) {
		t.Errorf("error %v is not ErrNoAffectedSet", err)
	}
	if !strings.Contains(err.Error(), "only units") {
		t.Errorf("error %q does not name the graph that could not answer", err)
	}
}

type onlyUnits struct{}

func (onlyUnits) Describe() string { return "only units" }
func (onlyUnits) Units(context.Context, string) ([]unit.Unit, error) {
	return []unit.Unit{{ID: "a", Dir: "a"}}, nil
}

// TestAffectedTerminatesOnACycle: ReverseDeps is an interface anyone can
// implement, and a closure revisiting a node would hang the build.
func TestAffectedTerminatesOnACycle(t *testing.T) {
	f := chain()
	f.rdeps = map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"a"}}
	got := affectedIDs(t, f, []string{"a/a.go"})
	if want := []string{"a", "b", "c"}; !eq(got, want) {
		t.Fatalf("Affected = %v, want %v", got, want)
	}
}

// TestAffectedIgnoresADependentThatIsNotAUnit: ReverseDeps naming an ID no
// unit has is a bug in a graph, and it must not produce a phantom unit with
// no directory that an expansion would then build a step for.
func TestAffectedIgnoresADependentThatIsNotAUnit(t *testing.T) {
	f := chain()
	f.rdeps = map[string][]string{"a": {"b", "ghost"}}
	got := affectedIDs(t, f, []string{"a/a.go"})
	if want := []string{"a", "b"}; !eq(got, want) {
		t.Fatalf("Affected = %v, want %v", got, want)
	}
}

// TestAffectedReportsAnOwnerThatIsNotAUnit as everything: a graph
// disagreeing with itself, and guessing which half is right is the guess
// that skips a broken unit.
func TestAffectedReportsAnOwnerThatIsNotAUnit(t *testing.T) {
	f := chain()
	f.owns = map[string][]string{"a/a.go": {"nowhere"}}
	res, err := unit.Affected(context.Background(), f, "/root", []string{"a/a.go"})
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	if !res.All || len(res.Units) != 4 {
		t.Fatalf("Affected = %v (all=%v), want everything", res.Units, res.All)
	}
}

// TestAffectedSurfacesEveryUnderlyingError: none of the three calls may be
// swallowed into a partial answer, because a partial answer here is a skipped
// unit.
func TestAffectedSurfacesEveryUnderlyingError(t *testing.T) {
	boom := errors.New("boom")
	for name, mutate := range map[string]func(*fake){
		"Units":       func(f *fake) { f.unitsErr = boom },
		"Owns":        func(f *fake) { f.ownsErr = boom },
		"ReverseDeps": func(f *fake) { f.rdepsErr = boom },
	} {
		t.Run(name, func(t *testing.T) {
			f := chain()
			mutate(f)
			if _, err := unit.Affected(context.Background(), f, "/root", []string{"a/a.go"}); !errors.Is(err, boom) {
				t.Fatalf("error = %v, want it to carry the %s failure", err, name)
			}
		})
	}
}

// TestAffectedRefusesAnOwnsThatDoesNotAnswerEveryFile: the contract is a
// result parallel to files, and a graph returning a shorter slice would
// otherwise silently drop the changed files past its end.
func TestAffectedRefusesAnOwnsThatDoesNotAnswerEveryFile(t *testing.T) {
	f := chain()
	short := shortOwns{f}
	_, err := unit.Affected(context.Background(), short, "/root", []string{"a/a.go", "b/b.go"})
	if err == nil {
		t.Fatal("Affected accepted an Owns that answered for fewer files than it was given")
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error %q does not say how many files were asked about", err)
	}
}

type shortOwns struct{ *fake }

func (shortOwns) Owns(context.Context, string, []string) ([][]string, error) {
	return [][]string{{"a"}}, nil
}

// TestAffectedCancels: Units is the call that shells out to the toolchain and
// a cancelled context has to come back as an error, not as a smaller unit set.
func TestAffectedCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := unit.Affected(ctx, chain(), "/root", []string{"a/a.go"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
