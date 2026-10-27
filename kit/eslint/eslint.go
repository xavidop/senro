// Package eslint is a preset step for ESLint with per-test reporting wired in.
//
// It runs
//
//	<runner...> eslint <Paths... | .> --format junit --output-file .senro/eslint.xml <Args...>
//
// and hands the JUnit report to testreport.JUnitXML, so a failing run names
// the files and rules that failed. ESLint reports rule hits per file, so
// there is nothing to select on a retry and the step reruns whole.
//
// One step in a workflow:
//
//	w.Add("lint", eslint.Step(eslint.Args("--max-warnings=0")))
//
// One step per JavaScript package, fanned out over a graph:
//
//	verify.Expand("lint", npm.Packages()).
//		Template(eslint.Template())
//
// # Runner
//
// The runner is the command prefix that runs the project's own ESLint: `pnpm
// exec` beside a pnpm-lock.yaml, `yarn` beside a yarn.lock, `bunx` beside a
// bun lockfile, `npx` otherwise. Step(Dir(d)) and Template resolve it with
// pm.Detect relative to the process working directory at build time (a
// pipeline is built in its repository root, which is where `senro run` runs
// it).
//
// # Where it applies
//
// Detect walks a repository and reports every directory that has a
// package.json and either an ESLint config file or eslint among the
// package's dependencies, skipping vendored, generated and hidden
// directories. That is what `senro init` uses to decide this kit fits.
package eslint

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/internal/pm"
	"github.com/xavidop/senro/testreport"
)

// Name is the kit's name, as it appears in a kit.Match.
const Name = "eslint"

// reportFile is where the JUnit report is written.
var reportFile = kit.ReportPath(Name)

// configFiles are the ESLint config files Detect looks for, in order.
var configFiles = []string{
	"eslint.config.js", "eslint.config.mjs", "eslint.config.cjs",
	"eslint.config.ts", "eslint.config.mts", "eslint.config.cts",
	".eslintrc", ".eslintrc.js", ".eslintrc.cjs",
	".eslintrc.json", ".eslintrc.yaml", ".eslintrc.yml",
}

type config struct {
	dir         string
	args, paths []string
}

// Option adjusts the step Step and Template build.
type Option func(*config)

// Dir runs the step from d, relative to the repository root. Template ignores
// it and uses the unit's directory.
func Dir(d string) Option { return func(c *config) { c.dir = d } }

// Args adds extra arguments to ESLint, after --output-file.
func Args(a ...string) Option {
	return func(c *config) { c.args = append(c.args, a...) }
}

// Paths sets the files or directories ESLint lints. The default is ".".
func Paths(p ...string) Option {
	return func(c *config) { c.paths = append(c.paths, p...) }
}

func newConfig(opts []Option) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}

func build(c config) *senro.StepBuilder {
	dir := kit.UnitDir(c.dir)
	argv := append([]string(nil), pm.Detect(cmp.Or(dir, "."), ".").Exec...)
	argv = append(argv, "eslint")
	if len(c.paths) == 0 {
		argv = append(argv, ".")
	} else {
		argv = append(argv, c.paths...)
	}
	argv = append(argv, "--format", "junit", "--output-file", reportFile)
	argv = append(argv, c.args...)
	sb := senro.NewStep(exec.Command(argv...)).Tests(testreport.JUnitXML(reportFile))
	if dir != "" {
		sb = sb.WorkDir(dir)
	}
	return sb
}

// Step returns a fresh builder for the ESLint step. Adopt it with
// WorkflowBuilder.Add.
func Step(opts ...Option) *senro.StepBuilder {
	return build(newConfig(opts))
}

// Template returns a function for Expand ... Template that builds one ESLint
// step per unit, run from the unit's directory. The Dir option is ignored; a
// unit at "" or "." runs from the repository root. Every call returns a fresh
// builder.
func Template(opts ...Option) func(senro.Unit) *senro.StepBuilder {
	return func(u senro.Unit) *senro.StepBuilder {
		c := newConfig(opts)
		c.dir = kit.UnitDir(u.Dir)
		return build(c)
	}
}

// Detect reports every directory under root that has a package.json and
// configures ESLint, in directory order, one match per directory. A
// package.json that does not parse is an error naming the file.
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

// detectDir returns the first reason dir configures ESLint, or "".
func detectDir(dir string) (string, error) {
	path := filepath.Join(dir, "package.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("eslint: read %s: %w", path, err)
	}
	var pkg struct{ Dependencies, DevDependencies map[string]string }
	if err := json.Unmarshal(data, &pkg); err != nil {
		return "", fmt.Errorf("eslint: parse %s: %w", path, err)
	}
	if f, ok := kit.FirstFile(dir, configFiles...); ok {
		return f, nil
	}
	if _, ok := pkg.DevDependencies["eslint"]; ok {
		return "package.json devDependencies", nil
	}
	if _, ok := pkg.Dependencies["eslint"]; ok {
		return "package.json dependencies", nil
	}
	return "", nil
}
