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
	got, err := applyTestSelection(p, map[string][]string{"t": {"TestA", "TestB"}})
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
	_, err := applyTestSelection(selPlan(nil), map[string][]string{"t": {"A"}})
	if err == nil || !strings.Contains(err.Error(), `step "t" declares no Tests`) {
		t.Errorf("err = %v", err)
	}
}

func TestWithTestSelectionRefusesAStepWithoutASelect(t *testing.T) {
	_, err := applyTestSelection(selPlan(&plan.TestsSpec{Format: "tap"}), map[string][]string{"t": {"A"}})
	if err == nil || !strings.Contains(err.Error(), "Selecting") {
		t.Errorf("err = %v", err)
	}
}

func TestWithTestSelectionRefusesAnUnknownStep(t *testing.T) {
	_, err := applyTestSelection(selPlan(nil), map[string][]string{"nope": {"A"}})
	if err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Errorf("err = %v", err)
	}
}

func TestWithTestSelectionUsesTheSpecsJoin(t *testing.T) {
	p := selPlan(&plan.TestsSpec{Format: "junit", Path: "r.xml", Select: []string{"-k", "{tests}"}, Join: " or "})
	got, err := applyTestSelection(p, map[string][]string{"t": {"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Nodes[0].Cmd; c[len(c)-1] != "a or b" {
		t.Errorf("Cmd = %v", c)
	}
}
