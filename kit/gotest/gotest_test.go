package gotest_test

import (
	"reflect"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/gotest"
)

func node(t *testing.T, sb *senro.StepBuilder) *plan.Node {
	t.Helper()
	p := senro.New("p")
	p.Workflow("w").Add("test", sb)
	pl, err := p.Build()
	if err != nil {
		t.Fatal(err)
	}
	n, ok := pl.Node("test")
	if !ok {
		t.Fatal("no node")
	}
	return n
}

func TestStepRunsGoTestJSONWithReporting(t *testing.T) {
	n := node(t, gotest.Step())
	wantCmd := []string{"go", "test", "-json", "./..."}
	if !reflect.DeepEqual(n.Cmd, wantCmd) || n.WorkDir != "" {
		t.Fatalf("node = %+v", n)
	}
	if n.Tests == nil || n.Tests.Format != "gotest" || !reflect.DeepEqual(n.Tests.Select, []string{"-run", "^({tests})$"}) {
		t.Fatalf("tests = %+v", n.Tests)
	}
}

func TestOptionsOrderTheArgv(t *testing.T) {
	n := node(t, gotest.Step(gotest.Race(), gotest.Tags("integration", "e2e"), gotest.Args("-count=1"), gotest.Packages("./cmd/...", "./pkg/..."), gotest.Dir("svc/api")))
	want := []string{"go", "test", "-json", "-race", "-tags", "integration,e2e", "-count=1", "./cmd/...", "./pkg/..."}
	if !reflect.DeepEqual(n.Cmd, want) || n.WorkDir != "svc/api" {
		t.Fatalf("node = %+v", n)
	}
}

func TestTemplateUsesTheUnitDir(t *testing.T) {
	tmpl := gotest.Template(gotest.Dir("ignored"))
	a := tmpl(senro.Unit{ID: "svc/api", Dir: "svc/api"})
	b := tmpl(senro.Unit{ID: "svc/api", Dir: "svc/api"})
	if a == b {
		t.Fatal("Template must return a fresh builder per call")
	}
	if n := node(t, a); n.WorkDir != "svc/api" {
		t.Fatalf("WorkDir = %q", n.WorkDir)
	}
	if n := node(t, tmpl(senro.Unit{ID: "root", Dir: "."})); n.WorkDir != "" {
		t.Fatalf("root WorkDir = %q", n.WorkDir)
	}
}

func TestStepWithDotDirLeavesWorkDirEmpty(t *testing.T) {
	if n := node(t, gotest.Step(gotest.Dir("."))); n.WorkDir != "" {
		t.Fatalf("WorkDir = %q, want empty", n.WorkDir)
	}
}

func TestTemplateBuildersDoNotShareSlices(t *testing.T) {
	tmpl := gotest.Template(gotest.Args("-count=1"), gotest.Packages("./a"))
	a := node(t, tmpl(senro.Unit{ID: "x", Dir: "x"}))
	b := node(t, tmpl(senro.Unit{ID: "x", Dir: "x"}))
	for i := range a.Cmd {
		a.Cmd[i] = "mutated"
	}
	want := []string{"go", "test", "-json", "-count=1", "./a"}
	if !reflect.DeepEqual(b.Cmd, want) {
		t.Fatalf("second builder's cmd = %v, want %v", b.Cmd, want)
	}
}

func TestDetectFindsEveryModuleOutsideSkippedDirs(t *testing.T) {
	got, err := gotest.Detect("testdata/tree")
	if err != nil {
		t.Fatal(err)
	}
	want := []kit.Match{
		{Kit: "gotest", Dir: ".", Reason: "go.mod"},
		{Kit: "gotest", Dir: "svc/api", Reason: "go.mod"},
		{Kit: "gotest", Dir: "tools", Reason: "go.mod"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}
