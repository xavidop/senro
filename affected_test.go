package senro_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/trigger"
	"github.com/xavidop/senro/unit/glob"
	"github.com/xavidop/senro/unit/gowork"
)

// monorepo is the same shape unit/gowork's own tests use, at the level a
// pipeline sees it: liba <- libb <- appc, plus a package only appc's TEST
// imports.
func monorepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"go.work":  "go 1.24\n\nuse (\n\t./liba\n\t./libb\n\t./appc\n)\n",
		"Makefile": "all:\n\techo hi\n",

		"liba/go.mod": "module example.com/liba\n\ngo 1.24\n",
		"liba/a.go":   "package liba\n\nfunc A() string { return \"a\" }\n",

		"libb/go.mod":   "module example.com/libb\n\ngo 1.24\n\nrequire example.com/liba v0.0.0\n",
		"libb/b.go":     "package libb\n\nimport \"example.com/liba\"\n\nfunc B() string { return liba.A() }\n",
		"libb/sub/s.go": "package sub\n\nfunc S() string { return \"s\" }\n",

		"appc/go.mod": "module example.com/appc\n\ngo 1.24\n\nrequire example.com/libb v0.0.0\n",
		"appc/main.go": "package main\n\nimport \"example.com/libb\"\n\n" +
			"func main() { println(libb.B()) }\n",
		"appc/main_test.go": "package main\n\nimport (\n\t\"testing\"\n\n\t\"example.com/libb/sub\"\n)\n\n" +
			"func TestX(t *testing.T) { _ = sub.S() }\n",
	}
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func affectedPlan(t *testing.T, src senro.ChangeSource) *senro.Plan {
	t.Helper()
	p := senro.New("mono")
	p.Workflow("verify").
		Expand("test", gowork.Modules()).
		Affected(src).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
		})
	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return pl
}

// TestAffectedExpansionMaterialisesOnlyTheAffectedUnits is the feature, end
// to end through the public API: a change to liba builds liba, libb and appc,
// and a change to appc builds appc alone.
func TestAffectedExpansionMaterialisesOnlyTheAffectedUnits(t *testing.T) {
	t.Chdir(monorepo(t))

	pl := affectedPlan(t, change.Paths("liba/a.go"))
	want := []string{"test[unit=appc]", "test[unit=liba]", "test[unit=libb]"}
	if got := nodeIDs(pl); !sameIDs(got, want) {
		t.Fatalf("nodes = %v, want %v (appc imports libb imports liba)", got, want)
	}

	pl = affectedPlan(t, change.Paths("appc/main.go"))
	if got := nodeIDs(pl); !sameIDs(got, []string{"test[unit=appc]"}) {
		t.Fatalf("nodes = %v, want appc alone: nothing imports it", got)
	}
}

// TestAffectedExpansionSeesATestOnlyImport at the pipeline level. Nothing but
// appc's _test.go mentions libb/sub, and a graph that read only a package's
// ordinary Imports would leave appc out of this plan.
func TestAffectedExpansionSeesATestOnlyImport(t *testing.T) {
	t.Chdir(monorepo(t))
	pl := affectedPlan(t, change.Paths("libb/sub/s.go"))
	if got := nodeIDs(pl); !sameIDs(got, []string{"test[unit=appc]", "test[unit=libb]"}) {
		t.Fatalf("nodes = %v, want appc and libb", got)
	}
}

// TestAffectedExpansionRunsEverythingForAFileNoUnitOwns: a Makefile above
// every module can change what all of them build.
func TestAffectedExpansionRunsEverythingForAFileNoUnitOwns(t *testing.T) {
	t.Chdir(monorepo(t))
	pl := affectedPlan(t, change.Paths("Makefile"))
	want := []string{"test[unit=appc]", "test[unit=liba]", "test[unit=libb]"}
	if got := nodeIDs(pl); !sameIDs(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
}

// TestAffectedExpansionWithNothingChangedMaterialisesNoChildren. An empty
// affected set is an ordinary empty expansion, which the plan already knows
// how to hold: the group is declared and has no members.
func TestAffectedExpansionWithNothingChangedMaterialisesNoChildren(t *testing.T) {
	t.Chdir(monorepo(t))
	pl := affectedPlan(t, change.Paths())
	if got := nodeIDs(pl); len(got) != 0 {
		t.Fatalf("nodes = %v, want none", got)
	}
	if _, ok := pl.Group("test"); !ok {
		t.Error("the expansion's group is missing, so nothing could report it as skipped")
	}
}

// TestAffectedRefusesAGlobGraph is the honesty requirement, at the level a
// pipeline author meets it. glob has no idea which directory imports which,
// and an expansion that silently covered every unit would be
// indistinguishable from one that computed a real answer.
func TestAffectedRefusesAGlobGraph(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	p.Workflow("verify").
		Expand("lint", glob.Dirs("apps/*")).
		Affected(change.Paths("apps/web/index.ts")).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted an affected expansion over a glob graph")
	}
	for _, want := range []string{"glob dirs apps/*", "cannot compute an affected set", "gowork"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestAffectedOverAGlobGraphIsFineWhenTheModeIsAll: a push to the default
// branch covers everything by definition, and saying so must not require a
// graph that can narrow. This is what lets one pipeline declare Affected once
// and still build on the days, and the graphs, where narrowing is impossible.
func TestAffectedOverAGlobGraphIsFineWhenTheModeIsAll(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	p.Workflow("verify").
		Expand("lint", glob.Dirs("apps/*")).
		Affected(change.FromTrigger(&trigger.Event{
			Kind: trigger.Push, Branch: "main", DefaultBranch: "main",
		})).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := nodeIDs(pl); !sameIDs(got, []string{"lint[unit=apps/api]", "lint[unit=apps/web]"}) {
		t.Fatalf("nodes = %v, want both", got)
	}
}

// TestMaxNodesIsCheckedAgainstTheWholeGraph, not against the narrowed set: a
// graph too wide to fan out over is a mistake worth refusing whether or not
// today's change happened to touch two of its units.
func TestMaxNodesIsCheckedAgainstTheWholeGraph(t *testing.T) {
	t.Chdir(monorepo(t))
	p := senro.New("mono")
	p.Workflow("verify").
		Expand("test", gowork.Modules()).
		Affected(change.Paths("appc/main.go")).
		MaxNodes(2).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	_, err := p.Build()
	if err == nil {
		t.Fatal("MaxNodes(2) accepted a three-unit graph because the change only reached one")
	}
	if !strings.Contains(err.Error(), "found 3 units") {
		t.Errorf("error %q does not report the size of the whole graph", err)
	}
}

// TestAffectedTwiceIsRefused, like two Templates: the second would silently
// win, and which change source a build used is not something to guess at.
func TestAffectedTwiceIsRefused(t *testing.T) {
	t.Chdir(t.TempDir())
	p := senro.New("mono")
	p.Workflow("verify").
		Expand("test", gowork.Modules()).
		Affected(change.Everything()).
		Affected(change.Paths("a.go")).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	if _, err := p.Build(); err == nil || !strings.Contains(err.Error(), "two change sources") {
		t.Fatalf("Build = %v, want a refusal", err)
	}
}

func TestAffectedWithANilSourceIsRefused(t *testing.T) {
	t.Chdir(t.TempDir())
	p := senro.New("mono")
	p.Workflow("verify").
		Expand("test", gowork.Modules()).
		Affected(nil).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	if _, err := p.Build(); err == nil || !strings.Contains(err.Error(), "nil change source") {
		t.Fatalf("Build = %v, want a refusal naming change.Everything", err)
	}
}

// TestAnExpansionWithNoAffectedIsUnchanged is the compatibility check: every
// pipeline written before this existed has to build the plan it always did.
func TestAnExpansionWithNoAffectedIsUnchanged(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	build := func() *senro.Plan {
		p := senro.New("mono")
		p.Workflow("verify").
			Expand("lint", glob.Dirs("apps/*")).
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("lint", u.Dir))
			})
		pl, err := p.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return pl
	}
	first, second := build(), build()
	if got := nodeIDs(first); !sameIDs(got, []string{"lint[unit=apps/api]", "lint[unit=apps/web]"}) {
		t.Fatalf("nodes = %v", got)
	}
	if first.Digest() != second.Digest() {
		t.Errorf("digest moved between two builds: %s vs %s", first.Digest(), second.Digest())
	}
}

// TestAChangeSourceFailureFailsTheBuild rather than quietly becoming an empty
// or a full set. A build that cannot tell what changed has to say so.
func TestAChangeSourceFailureFailsTheBuild(t *testing.T) {
	t.Chdir(monorepo(t))
	p := senro.New("mono")
	p.Workflow("verify").
		Expand("test", gowork.Modules()).
		Affected(change.FromTrigger(&trigger.Event{
			Kind: trigger.PullRequest, Branch: "main", DefaultBranch: "main",
			Base: trigger.Base{From: "aaaa", To: "bbbb"},
		})).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	_, err := p.Build()
	if err == nil {
		t.Fatal("a change source that could not answer produced a plan anyway")
	}
	if !strings.Contains(err.Error(), "expansion \"test\"") {
		t.Errorf("error %q does not name the expansion", err)
	}
}

// TestAffectedRunErrorIsNotARunError: an expansion that cannot be resolved is
// a Build failure, and Run returns it unwrapped, with no run directory left
// behind, exactly as every other Build failure does.
func TestAffectedIsABuildErrorNotARunError(t *testing.T) {
	t.Chdir(t.TempDir())
	p := senro.New("mono")
	p.Workflow("verify").
		Expand("lint", glob.Dirs("apps/*")).
		Affected(change.Paths("x.go")).
		Template(func(senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("true"))
		})
	err := senro.Run(t.Context(), p)
	var re *senro.RunError
	if err == nil || errors.As(err, &re) {
		t.Fatalf("Run = %v, want a plain build error", err)
	}
	if entries, rderr := os.ReadDir("runs"); rderr == nil && len(entries) > 0 {
		t.Errorf("a run directory was created for a pipeline that never built: %v", entries)
	}
}

func sameIDs(got, want []string) bool {
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
