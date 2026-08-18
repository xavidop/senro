package main

import (
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/change"
)

// This file is the proof, not a demonstration. Everything below runs against
// the graph defined in main.go, which is an ordinary type in an ordinary
// package outside senro's own, and it flows through the published
// Expand(...).Affected(...) path end to end.

func nodeIDs(pl *senro.Plan) []string {
	out := make([]string, 0, len(pl.Nodes))
	for _, n := range pl.Nodes {
		out = append(out, n.ID)
	}
	return out
}

func same(got, want []string) bool {
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

// planFor builds the example's own pipeline against the example's own
// workspace, which is what makes this an end-to-end test of the public API
// and not a unit test of the graph's methods.
func planFor(t *testing.T, src senro.ChangeSource) *senro.Plan {
	t.Helper()
	t.Chdir("workspace")
	pl, err := pipeline(src).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return pl
}

// TestAThirdPartyGraphNeedsNothingInternal: if any file here imported a path
// under senro's internal/, a reader copying this example into their own
// repository would find it does not compile. Parsing this package's own
// source keeps the claim true as the example is edited.
func TestAThirdPartyGraphNeedsNothingInternal(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}
		for _, imp := range f.Imports {
			checked++
			p := strings.Trim(imp.Path.Value, `"`)
			if p == "github.com/xavidop/senro/internal" || strings.Contains(p, "/internal/") {
				t.Errorf("%s imports %q; a graph outside the senro module cannot, so this "+
					"example would not compile in the repository a reader copies it into",
					e.Name(), p)
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no imports at all; this test would pass vacuously")
	}
}

// TestTheGraphSatisfiesThePublishedInterfaces at run time as well as compile
// time, so a failure names which interface is missing.
func TestTheGraphSatisfiesThePublishedInterfaces(t *testing.T) {
	var g senro.UnitGraph = Components("components.json")
	if _, ok := g.(senro.UnitAffector); !ok {
		t.Fatal("the graph is a UnitGraph but not a UnitAffector, so Affected would be refused")
	}
	if g.Describe() == "" {
		t.Error("Describe returned nothing; it is what an error message names the graph by")
	}
}

// TestAffectedIsTransitiveOverThreeUnits is the end-to-end case: a change to
// shared, which checkout has never heard of, still builds checkout, because
// checkout needs billing and billing needs shared.
func TestAffectedIsTransitiveOverThreeUnits(t *testing.T) {
	pl := planFor(t, change.Paths("libs/shared/shared.txt"))
	want := []string{
		"test[unit=libs/shared]",
		"test[unit=services/billing]",
		"test[unit=services/checkout]",
	}
	if got := nodeIDs(pl); !same(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
}

// TestAffectedNarrowsToOneUnit. Nothing depends on checkout, so a change to
// it builds it alone, and the other two are not in the plan AT ALL: they are
// not steps that get skipped.
func TestAffectedNarrowsToOneUnit(t *testing.T) {
	pl := planFor(t, change.Paths("services/checkout/checkout.txt"))
	if got := nodeIDs(pl); !same(got, []string{"test[unit=services/checkout]"}) {
		t.Fatalf("nodes = %v, want checkout alone", got)
	}
}

// TestAFileNoComponentOwnsRunsEverything. The Makefile at the root belongs to
// no component, so the graph's Owns answers with an empty entry and senro
// reads that as "this could have changed anything".
func TestAFileNoComponentOwnsRunsEverything(t *testing.T) {
	pl := planFor(t, change.Paths("Makefile"))
	want := []string{
		"test[unit=libs/shared]",
		"test[unit=services/billing]",
		"test[unit=services/checkout]",
	}
	if got := nodeIDs(pl); !same(got, want) {
		t.Fatalf("nodes = %v, want every unit", got)
	}
}

// TestEverythingChangedBuildsEveryUnit, which is the path a run takes when
// the change source reports All and the graph is never asked to narrow at
// all.
func TestEverythingChangedBuildsEveryUnit(t *testing.T) {
	pl := planFor(t, change.Everything())
	if got := nodeIDs(pl); len(got) != 3 {
		t.Fatalf("nodes = %v, want all three", got)
	}
}

// TestNothingChangedBuildsNothing: an empty path list is "the change is
// genuinely empty", which is a different answer from "everything", and the
// example's own flag handling turns an absent flag into the second.
func TestNothingChangedBuildsNothing(t *testing.T) {
	pl := planFor(t, change.Paths())
	if got := nodeIDs(pl); len(got) != 0 {
		t.Fatalf("nodes = %v, want none", got)
	}
}

// TestABadComponentsFileFailsTheBuild rather than producing an empty fan-out.
// An expansion that silently produced no steps looks exactly like a passing
// build.
func TestABadComponentsFileFailsTheBuild(t *testing.T) {
	t.Chdir(t.TempDir())
	p := senro.New("customgraph")
	p.Workflow("verify").
		Expand("test", Components("components.json")).
		Template(func(senro.Unit) *senro.StepBuilder { return senro.NewStep(nil) })
	if _, err := p.Build(); err == nil {
		t.Fatal("Build over a missing components.json returned no error")
	}
}
