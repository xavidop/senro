// Package pytest is a preset step for pytest with per-test reporting wired in.
//
// It runs
//
//	<runner...> pytest --junitxml=.senro/pytest.xml <Args...> <Paths...>
//
// and hands the JUnit report to testreport.JUnitXML, so a failing run names
// the tests that failed and a retry can rerun only those with -k.
//
// One step in a workflow:
//
//	w.Add("test", pytest.Step(pytest.Args("-x")))
//
// One step per Python project, fanned out over a graph:
//
//	verify.Expand("test", pyproject.Packages()).
//		Template(pytest.Template())
//
// # Runner
//
// The runner is the command prefix that puts pytest inside the project's
// environment: `uv run` beside a uv.lock, `poetry run` beside a poetry.lock,
// `python -m` otherwise. Step(Dir(d)) and Template resolve it with
// py.Detect relative to the process working directory at build time (a
// pipeline is built in its repository root, which is where `senro run` runs
// it). Runner overrides detection.
//
// # Where it applies
//
// Detect walks a repository and reports every directory that configures
// pytest, skipping vendored, generated and hidden directories. A directory
// matches on the first of these it finds: a [tool.pytest.ini_options] table,
// pytest among [project] dependencies, pytest among
// [project.optional-dependencies], pytest among the [tool.poetry]
// dependencies, a pytest.ini, a [pytest] section in tox.ini, or a
// [tool:pytest] section in setup.cfg. That is what `senro init` uses to
// decide this kit fits.
package pytest

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/toml"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/internal/py"
	"github.com/xavidop/senro/testreport"
)

// Name is the kit's name, as it appears in a kit.Match.
const Name = "pytest"

// reportFile is where the JUnit report is written.
var reportFile = kit.ReportPath(Name)

type config struct {
	dir         string
	args, paths []string
	runner      []string
}

// Option adjusts the step Step and Template build.
type Option func(*config)

// Dir runs the step from d, relative to the repository root. Template ignores
// it and uses the unit's directory.
func Dir(d string) Option { return func(c *config) { c.dir = d } }

// Args adds extra arguments to pytest, after --junitxml and before the paths.
func Args(a ...string) Option {
	return func(c *config) { c.args = append(c.args, a...) }
}

// Paths sets the files or directories pytest collects. The default is none,
// so pytest uses its own configuration.
func Paths(p ...string) Option {
	return func(c *config) { c.paths = append(c.paths, p...) }
}

// Runner sets the command prefix that precedes pytest, for example
// Runner("pipenv", "run"), instead of detecting it from a lockfile.
func Runner(argv ...string) Option {
	return func(c *config) { c.runner = append([]string(nil), argv...) }
}

func newConfig(opts []Option) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}

func build(c config) *senro.StepBuilder {
	runner := c.runner
	if len(runner) == 0 {
		d := c.dir
		if d == "" {
			d = "."
		}
		runner = py.Detect(d, ".").Argv
	}
	argv := append([]string(nil), runner...)
	argv = append(argv, "pytest", "--junitxml="+reportFile)
	argv = append(argv, c.args...)
	argv = append(argv, c.paths...)
	sb := senro.NewStep(exec.Command(argv...)).
		Tests(testreport.JUnitXML(reportFile).Selecting("-k", testreport.Placeholder).JoinedBy(" or "))
	if c.dir != "" {
		sb = sb.WorkDir(c.dir)
	}
	return sb
}

// Step returns a fresh builder for the pytest step. Adopt it with
// WorkflowBuilder.Add.
func Step(opts ...Option) *senro.StepBuilder {
	return build(newConfig(opts))
}

// Template returns a function for Expand ... Template that builds one pytest
// step per unit, run from the unit's directory. The Dir option is ignored; a
// unit at "" or "." runs from the repository root. Every call returns a fresh
// builder.
func Template(opts ...Option) func(senro.Unit) *senro.StepBuilder {
	return func(u senro.Unit) *senro.StepBuilder {
		c := newConfig(opts)
		c.dir = ""
		if u.Dir != "" && u.Dir != "." {
			c.dir = u.Dir
		}
		return build(c)
	}
}

// Detect reports every directory under root that configures pytest, in
// directory order, one match per directory. A pyproject.toml that does not
// parse is an error naming the file.
func Detect(root string) ([]kit.Match, error) {
	var ms []kit.Match
	err := kit.Walk(root, func(dir, rel string) error {
		reason, err := detectDir(dir)
		if err != nil {
			return err
		}
		if reason != "" {
			ms = append(ms, kit.Match{Kit: Name, Dir: rel, Reason: reason})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	kit.SortMatches(ms)
	return ms, nil
}

// detectDir returns the first reason dir configures pytest, or "".
func detectDir(dir string) (string, error) {
	if reason, err := fromPyproject(dir); err != nil || reason != "" {
		return reason, err
	}
	if fileExists(filepath.Join(dir, "pytest.ini")) {
		return "pytest.ini", nil
	}
	for _, f := range []struct{ file, section string }{
		{"tox.ini", "[pytest]"},
		{"setup.cfg", "[tool:pytest]"},
	} {
		ok, err := hasSection(filepath.Join(dir, f.file), f.section)
		if err != nil {
			return "", err
		}
		if ok {
			return f.file + " " + f.section, nil
		}
	}
	return "", nil
}

func fromPyproject(dir string) (string, error) {
	path := filepath.Join(dir, "pyproject.toml")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("pytest: read %s: %w", path, err)
	}
	t, err := toml.Parse(data)
	if err != nil {
		return "", fmt.Errorf("pytest: parse %s: %w", path, err)
	}
	if t.Sub("tool", "pytest", "ini_options") != nil {
		return "pyproject.toml [tool.pytest.ini_options]", nil
	}
	if anyPytest(t.StrList("project", "dependencies")) {
		return "pyproject.toml [project] dependencies", nil
	}
	opt := t.Sub("project", "optional-dependencies")
	for _, k := range opt.Keys() {
		if anyPytest(opt.StrList(k)) {
			return "pyproject.toml [project.optional-dependencies]", nil
		}
	}
	if poetryHasPytest(t) {
		return "pyproject.toml [tool.poetry]", nil
	}
	return "", nil
}

func poetryHasPytest(t toml.Table) bool {
	if anyPytest(t.Sub("tool", "poetry", "dependencies").Keys()) {
		return true
	}
	groups := t.Sub("tool", "poetry", "group")
	for _, g := range groups.Keys() {
		if anyPytest(groups.Sub(g, "dependencies").Keys()) {
			return true
		}
	}
	return false
}

func anyPytest(deps []string) bool {
	for _, d := range deps {
		if i := strings.IndexAny(d, " [<>=!~;"); i >= 0 {
			d = d[:i]
		}
		if strings.EqualFold(d, "pytest") {
			return true
		}
	}
	return false
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// hasSection reports whether the INI-style file at path has a line that is
// exactly the section header. A missing file has no section.
func hasSection(path, header string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("pytest: read %s: %w", path, err)
	}
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == header {
			return true, nil
		}
	}
	return false, sc.Err()
}
