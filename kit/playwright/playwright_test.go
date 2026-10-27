package playwright_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/playwright"
)

func node(t *testing.T, sb *senro.StepBuilder) *plan.Node {
	t.Helper()
	p := senro.New("p")
	p.Workflow("w").Add("e2e", sb)
	pl, err := p.Build()
	if err != nil {
		t.Fatal(err)
	}
	n, ok := pl.Node("e2e")
	if !ok {
		t.Fatal("no node")
	}
	return n
}

func TestStepRunsPlaywrightWithJUnit(t *testing.T) {
	n := node(t, playwright.Step())
	want := []string{"npx", "playwright", "test", "--reporter=list,junit"}
	if !reflect.DeepEqual(n.Cmd, want) {
		t.Fatalf("cmd = %v", n.Cmd)
	}
	if !slices.Contains(n.Env, "PLAYWRIGHT_JUNIT_OUTPUT_NAME=.senro/playwright.xml") {
		t.Fatalf("env = %v", n.Env)
	}
	tr := n.Tests
	if tr == nil || tr.Format != "junit" || tr.Path != ".senro/playwright.xml" ||
		!reflect.DeepEqual(tr.Select, []string{"-g", "{tests}"}) || tr.Join != "|" {
		t.Fatalf("tests = %+v", tr)
	}
	y := node(t, playwright.Step(playwright.Dir("testdata/yarn"), playwright.Args("--workers=1")))
	if strings.Join(y.Cmd, " ") != "yarn playwright test --reporter=list,junit --workers=1" || y.WorkDir != "testdata/yarn" {
		t.Fatalf("yarn node = %+v", y)
	}
}

func TestTemplateIsFreshPerCall(t *testing.T) {
	tpl := playwright.Template()
	a := tpl(senro.Unit{ID: "a", Dir: "testdata/yarn"})
	b := tpl(senro.Unit{ID: "b", Dir: "testdata/yarn"})
	if a == b {
		t.Fatal("template returned the same builder twice")
	}
	n := node(t, a)
	if n.Cmd[0] != "yarn" || n.WorkDir != "testdata/yarn" {
		t.Fatalf("node = %+v", n)
	}
}

func TestDetect(t *testing.T) {
	cases := map[string]string{
		"ts":   "playwright.config.ts",
		"js":   "playwright.config.js",
		"none": "",
	}
	for dir, reason := range cases {
		got, err := playwright.Detect("testdata/" + dir)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		var want []kit.Match
		if reason != "" {
			want = []kit.Match{{Kit: "playwright", Dir: ".", Reason: reason}}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %+v want %+v", dir, got, want)
		}
	}
}
