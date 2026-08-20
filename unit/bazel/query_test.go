package bazel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/bazel"
)

// queryXML is what `bazel query --output=xml 'kind(rule, //...)'` prints for
// the fixture workspace: each rule, and the labels it takes as inputs.
//
// A canned string rather than a real bazel invocation, because the point of
// the test is the mapping from target edges to PACKAGE edges. Whether bazel
// prints this shape is bazel's contract, checked once by reading its docs,
// not something a unit test can discover.
const queryXML = `<?xml version="1.1" encoding="UTF-8" standalone="no"?>
<query version="2">
  <rule class="go_library" name="//libs/core:core">
    <rule-input name="//libs/core:core.go"/>
  </rule>
  <rule class="go_library" name="//libs/store:store">
    <rule-input name="//libs/core:core"/>
  </rule>
  <rule class="go_binary" name="//apps/web:web">
    <rule-input name="//libs/store:store"/>
    <rule-input name="@rules_go//go:def"/>
  </rule>
</query>`

// The whole point: a change under libs/core must reach the packages that
// transitively depend on it, and nothing else.
func TestQueryComputesAnAffectedSetFromPackageEdges(t *testing.T) {
	g := bazel.Query()
	bazel.SetQueryRunnerForTest(g, func(context.Context, string) ([]byte, error) {
		return []byte(queryXML), nil
	})

	got, err := unit.Affected(context.Background(), g, workspace, []string{"libs/core/core.go"})
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	want := map[string]bool{"libs/core": true, "libs/store": true, "apps/web": true}
	if got.All {
		t.Fatalf("affected reported ALL units; want just the three connected to libs/core (%s)", got.Why)
	}
	for _, u := range got.Units {
		if !want[u.ID] {
			t.Errorf("affected includes %q, which nothing connects to libs/core", u.ID)
		}
		delete(want, u.ID)
	}
	for id := range want {
		t.Errorf("affected is missing %q", id)
	}
}

// A change no package owns could have affected anything, so the graph must
// not answer with a narrow set. This is the failure that matters: a package
// left out of an affected set is a green build for a tree that does not
// build.
//
// The file here is OUTSIDE the workspace root, which is the realistic way to
// reach this: senro's change sources report paths relative to the repository,
// and a Bazel root is often a subdirectory of one. Inside a workspace whose
// root is itself a package, Bazel's own rule means every file has an owner.
func TestQueryTreatsAnUnownedChangeAsAffectingEverything(t *testing.T) {
	g := bazel.Query()
	bazel.SetQueryRunnerForTest(g, func(context.Context, string) ([]byte, error) {
		return []byte(queryXML), nil
	})

	got, err := unit.Affected(context.Background(), g, workspace, []string{"../outside/x.go"})
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	if !got.All {
		t.Errorf("a change no package owns must affect every unit, not a computed subset (%s)", got.Why)
	}
}

// A file inside a package that is not the root belongs to that package, and a
// file the root package owns belongs to the root: Bazel attributes a file to
// its NEAREST enclosing package, and so must this.
func TestQueryAttributesAFileToItsNearestPackage(t *testing.T) {
	g := bazel.Query()
	owners, err := g.Owns(context.Background(), workspace,
		[]string{"libs/core/core.go", "docs/readme.md"})
	if err != nil {
		t.Fatalf("Owns: %v", err)
	}
	if len(owners[0]) != 1 || owners[0][0] != "libs/core" {
		t.Errorf("owners of libs/core/core.go = %v, want [libs/core]", owners[0])
	}
	// docs holds no BUILD file, so the nearest package above it is the root.
	if len(owners[1]) != 1 || owners[1][0] != "." {
		t.Errorf("owners of docs/readme.md = %v, want [.]: the nearest package is the root", owners[1])
	}
}

// bazel missing is a hard error, never a silent "run everything" and never a
// skip. A graph that quietly answered differently on CI and on a laptop is
// the machine-dependence that kept this out of the tree-walking graph.
func TestQueryFailsLoudlyWhenBazelCannotRun(t *testing.T) {
	g := bazel.Query()
	bazel.SetQueryRunnerForTest(g, func(context.Context, string) ([]byte, error) {
		return nil, errors.New("exec: \"bazel\": executable file not found in $PATH")
	})

	_, err := unit.Affected(context.Background(), g, workspace, []string{"libs/core/core.go"})
	if err == nil {
		t.Fatal("a graph that cannot reach bazel must fail, not guess")
	}
}

// The tree-walking graph still refuses, and must keep refusing: this adds a
// second graph, it does not weaken the first.
func TestPackagesStillRefusesAnAffectedSet(t *testing.T) {
	_, err := unit.Affected(context.Background(), bazel.Packages(), workspace, []string{"libs/core/core.go"})
	if !errors.Is(err, unit.ErrNoAffectedSet) {
		t.Errorf("err = %v, want ErrNoAffectedSet", err)
	}
}
