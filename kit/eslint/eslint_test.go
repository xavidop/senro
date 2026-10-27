package eslint_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/eslint"
)

func node(t *testing.T, sb *senro.StepBuilder) *plan.Node {
	t.Helper()
	p := senro.New("p")
	p.Workflow("w").Add("lint", sb)
	pl, err := p.Build()
	if err != nil {
		t.Fatal(err)
	}
	n, ok := pl.Node("lint")
	if !ok {
		t.Fatal("no node")
	}
	return n
}

func TestStepRunsESLintWithJUnit(t *testing.T) {
	n := node(t, eslint.Step())
	want := []string{"npx", "eslint", ".", "--format", "junit", "--output-file", ".senro/eslint.xml"}
	if !reflect.DeepEqual(n.Cmd, want) {
		t.Fatalf("cmd = %v", n.Cmd)
	}
	tr := n.Tests
	if tr == nil || tr.Format != "junit" || tr.Path != ".senro/eslint.xml" || tr.Select != nil {
		t.Fatalf("tests = %+v", tr)
	}
	p := node(t, eslint.Step(eslint.Dir("testdata/pnpm")))
	if strings.Join(p.Cmd[:3], " ") != "pnpm exec eslint" || p.WorkDir != "testdata/pnpm" {
		t.Fatalf("pnpm node = %+v", p)
	}
	o := node(t, eslint.Step(eslint.Paths("src", "test"), eslint.Args("--max-warnings=0")))
	want = []string{"npx", "eslint", "src", "test", "--format", "junit", "--output-file", ".senro/eslint.xml", "--max-warnings=0"}
	if !reflect.DeepEqual(o.Cmd, want) {
		t.Fatalf("cmd = %v", o.Cmd)
	}
}

func TestPathsWithNoArgumentsFallsBackToDot(t *testing.T) {
	n := node(t, eslint.Step(eslint.Paths()))
	if n.Cmd[1] != "eslint" || n.Cmd[2] != "." {
		t.Fatalf("cmd = %v", n.Cmd)
	}
}

func TestStepWithDotDirLeavesWorkDirEmpty(t *testing.T) {
	if n := node(t, eslint.Step(eslint.Dir("."))); n.WorkDir != "" {
		t.Fatalf("WorkDir = %q, want empty", n.WorkDir)
	}
}

func TestTemplateIsFreshPerCall(t *testing.T) {
	tpl := eslint.Template(eslint.Args("--quiet"))
	a := tpl(senro.Unit{ID: "a", Dir: "testdata/pnpm"})
	b := tpl(senro.Unit{ID: "b", Dir: "testdata/pnpm"})
	if a == b {
		t.Fatal("template returned the same builder twice")
	}
	n := node(t, a)
	if strings.Join(n.Cmd[:3], " ") != "pnpm exec eslint" || n.WorkDir != "testdata/pnpm" {
		t.Fatalf("node = %+v", n)
	}
}

func TestDetect(t *testing.T) {
	cases := map[string]string{
		"flat":   "eslint.config.mjs",
		"legacy": ".eslintrc.json",
		"dep":    "package.json devDependencies",
		"none":   "",
		"nopkg":  "",
	}
	for dir, reason := range cases {
		got, err := eslint.Detect("testdata/" + dir)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		var want []kit.Match
		if reason != "" {
			want = []kit.Match{{Kit: "eslint", Dir: ".", Reason: reason}}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %+v want %+v", dir, got, want)
		}
	}
}

func TestDetectReportsABrokenPackageJSON(t *testing.T) {
	_, err := eslint.Detect("testdata/broken")
	if err == nil || !strings.Contains(err.Error(), "broken/package.json") {
		t.Fatalf("err = %v", err)
	}
}
