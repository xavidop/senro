package senro

// White-box tests of applyTestSelection: it is unexported, and run_test.go
// is public-API only by rule.

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

func selPlan(tests *plan.TestsSpec) *plan.Plan {
	return &plan.Plan{Nodes: []plan.Node{{
		ID: "t", Kind: "exec", Cmd: []string{"go", "test", "-json", "./..."}, Tests: tests,
	}}}
}

func TestWithTestSelectionAppendsTheSpecsSelect(t *testing.T) {
	p := selPlan(&plan.TestsSpec{Format: "gotest", Select: []string{"-run", "^({tests})$"}})
	got, err := applyTestSelection(p, map[string][]string{"t": {"TestA", "TestB"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"go", "test", "-json", "./...", "-run", "^(TestA|TestB)$"}
	if !reflect.DeepEqual(got.Nodes[0].Cmd, want) {
		t.Errorf("Cmd = %v, want %v", got.Nodes[0].Cmd, want)
	}
	if len(p.Nodes[0].Cmd) != 4 {
		t.Errorf("the caller's plan was mutated: %v", p.Nodes[0].Cmd)
	}
}

func TestWithTestSelectionRefusesAStepWithoutASpec(t *testing.T) {
	_, err := applyTestSelection(selPlan(nil), map[string][]string{"t": {"A"}}, nil)
	if err == nil || !strings.Contains(err.Error(), `step "t" declares no Tests`) {
		t.Errorf("err = %v", err)
	}
}

func TestWithTestSelectionRefusesAStepWithoutASelect(t *testing.T) {
	_, err := applyTestSelection(selPlan(&plan.TestsSpec{Format: "tap"}), map[string][]string{"t": {"A"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "Selecting") {
		t.Errorf("err = %v", err)
	}
}

// A selection on a step WithOnlySteps leaves out would be skipped with it,
// and the rerun would report green having run none of the named tests.
func TestWithTestSelectionRefusesAStepOutsideWithOnlySteps(t *testing.T) {
	p := selPlan(&plan.TestsSpec{Format: "gotest", Select: []string{"-run", "^({tests})$"}})
	p.Nodes = append(p.Nodes, plan.Node{ID: "other", Kind: "exec", Cmd: []string{"true"}})
	_, err := applyTestSelection(p, map[string][]string{"t": {"TestA"}}, map[string]bool{"other": true})
	if err == nil || !strings.Contains(err.Error(), `step "t", which WithOnlySteps leaves out`) {
		t.Errorf("err = %v", err)
	}
	if _, err := applyTestSelection(p, map[string][]string{"t": {"TestA"}}, map[string]bool{"t": true}); err != nil {
		t.Errorf("a step inside WithOnlySteps was refused: %v", err)
	}
}

func TestWithTestSelectionRefusesAnUnknownStep(t *testing.T) {
	_, err := applyTestSelection(selPlan(nil), map[string][]string{"nope": {"A"}}, nil)
	if err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Errorf("err = %v", err)
	}
}

// go test splits -run on "/" outside parentheses, so a subtest inside the
// anchored group would match nothing; it collapses to its parent, once.
// Metacharacters are quoted so a name selects itself and nothing else.
func TestWithTestSelectionShapesGoTestNames(t *testing.T) {
	p := selPlan(&plan.TestsSpec{Format: "gotest", Select: []string{"-run", "^({tests})$"}})
	got, err := applyTestSelection(p, map[string][]string{
		"t": {"TestP/sub (x)", "TestP", "TestP/other", "TestA+B"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Nodes[0].Cmd; c[len(c)-1] != `^(TestP|TestA\+B)$` {
		t.Errorf("Cmd = %v", c)
	}
}

// Outside go test senro does not know the selector's syntax, so names are
// substituted verbatim and quoting is the Select argv's business.
func TestWithTestSelectionLeavesOtherFormatsVerbatim(t *testing.T) {
	p := selPlan(&plan.TestsSpec{Format: "tap", Select: []string{"--only", "{tests}"}})
	got, err := applyTestSelection(p, map[string][]string{"t": {"a/b", "c+d"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Nodes[0].Cmd; c[len(c)-1] != "a/b|c+d" {
		t.Errorf("Cmd = %v", c)
	}
}

func TestWithTestSelectionUsesTheSpecsJoin(t *testing.T) {
	p := selPlan(&plan.TestsSpec{Format: "junit", Path: "r.xml", Select: []string{"-k", "{tests}"}, Join: " or "})
	got, err := applyTestSelection(p, map[string][]string{"t": {"a", "b"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Nodes[0].Cmd; c[len(c)-1] != "a or b" {
		t.Errorf("Cmd = %v", c)
	}
}

func TestServiceClosureIncludesTransitiveNeeds(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "base", Kind: "exec"},
		{ID: "db", Kind: "exec", Needs: []string{"base"}, Service: &plan.ServiceSpec{}},
		{ID: "api", Kind: "exec", Needs: []string{"db"}, Service: &plan.ServiceSpec{}},
		{ID: "test", Kind: "exec", Needs: []string{"api"}},
		{ID: "lint", Kind: "exec"},
	}}
	got, err := serviceClosure(p)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"api", "base", "db"}; !reflect.DeepEqual(got, want) {
		t.Errorf("closure = %v, want %v", got, want)
	}
}

func TestServiceClosureRefusesAPipelineWithoutServices(t *testing.T) {
	_, err := serviceClosure(selPlan(nil))
	if err == nil || err.Error() != "senro: this pipeline declares no services" {
		t.Errorf("err = %v", err)
	}
}
