package pytest_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/pytest"
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

func TestStepRunsPytestWithJUnit(t *testing.T) {
	n := node(t, pytest.Step())
	want := []string{"python", "-m", "pytest", "--junitxml=.senro/pytest.xml"}
	if !reflect.DeepEqual(n.Cmd, want) {
		t.Fatalf("cmd = %v", n.Cmd)
	}
	tr := n.Tests
	if tr == nil || tr.Format != "junit" || tr.Path != ".senro/pytest.xml" ||
		!reflect.DeepEqual(tr.Select, []string{"-k", "{tests}"}) || tr.Join != " or " {
		t.Fatalf("tests = %+v", tr)
	}
}

func TestRunnerFollowsTheLockfile(t *testing.T) {
	n := node(t, pytest.Step(pytest.Dir("testdata/uv")))
	if len(n.Cmd) < 3 || strings.Join(n.Cmd[:3], " ") != "uv run pytest" || n.WorkDir != "testdata/uv" {
		t.Fatalf("node = %+v", n)
	}
	tn := node(t, pytest.Template()(senro.Unit{ID: "u", Dir: "testdata/uv"}))
	if strings.Join(tn.Cmd[:3], " ") != "uv run pytest" {
		t.Fatalf("template cmd = %v", tn.Cmd)
	}
	o := node(t, pytest.Step(pytest.Dir("testdata/uv"), pytest.Runner("pipenv", "run")))
	if strings.Join(o.Cmd[:3], " ") != "pipenv run pytest" {
		t.Fatalf("override cmd = %v", o.Cmd)
	}
}

func TestPathsAndArgs(t *testing.T) {
	n := node(t, pytest.Step(pytest.Paths("tests/unit"), pytest.Args("-x")))
	want := []string{"python", "-m", "pytest", "--junitxml=.senro/pytest.xml", "-x", "tests/unit"}
	if !reflect.DeepEqual(n.Cmd, want) {
		t.Fatalf("cmd = %v", n.Cmd)
	}
}

func TestDetectRecognisesEveryConfigShape(t *testing.T) {
	cases := map[string]string{
		"ini_options": "pyproject.toml [tool.pytest.ini_options]",
		"deps":        "pyproject.toml [project] dependencies",
		"optional":    "pyproject.toml [project.optional-dependencies]",
		"poetry":      "pyproject.toml [tool.poetry]",
		"pytestini":   "pytest.ini",
		"tox":         "tox.ini [pytest]",
		"setupcfg":    "setup.cfg [tool:pytest]",
		"uv":          "pyproject.toml [tool.pytest.ini_options]",
		"none":        "",
	}
	for dir, reason := range cases {
		got, err := pytest.Detect("testdata/" + dir)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		var want []kit.Match
		if reason != "" {
			want = []kit.Match{{Kit: "pytest", Dir: ".", Reason: reason}}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %+v want %+v", dir, got, want)
		}
	}
}

func TestDetectFindsANestedProject(t *testing.T) {
	got, err := pytest.Detect("testdata/nested")
	if err != nil {
		t.Fatal(err)
	}
	want := []kit.Match{{Kit: "pytest", Dir: "sub", Reason: "pyproject.toml [tool.pytest.ini_options]"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestDetectFindsPytestInTheMainPoetryDependencies(t *testing.T) {
	got, err := pytest.Detect("testdata/poetrymain")
	if err != nil {
		t.Fatal(err)
	}
	want := []kit.Match{{Kit: "pytest", Dir: ".", Reason: "pyproject.toml [tool.poetry]"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestDetectToleratesWhitespaceAroundTheRequirementName(t *testing.T) {
	for _, req := range []string{" pytest", "pytest\t>=7", "\tpytest ", "PyTest>=7"} {
		dir := t.TempDir()
		body := "[project]\nname = \"x\"\ndependencies = [\"" + req + "\"]\n"
		if err := os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := pytest.Detect(dir)
		if err != nil {
			t.Fatalf("%q: %v", req, err)
		}
		if len(got) != 1 || got[0].Reason != "pyproject.toml [project] dependencies" {
			t.Errorf("%q: got %+v", req, got)
		}
	}
}

func TestStepWithDotDirLeavesWorkDirEmpty(t *testing.T) {
	if n := node(t, pytest.Step(pytest.Dir("."))); n.WorkDir != "" {
		t.Fatalf("WorkDir = %q, want empty", n.WorkDir)
	}
}

func TestDetectReportsABrokenPyproject(t *testing.T) {
	_, err := pytest.Detect("testdata/broken")
	if err == nil || !strings.Contains(err.Error(), "broken/pyproject.toml") {
		t.Fatalf("err = %v", err)
	}
}
