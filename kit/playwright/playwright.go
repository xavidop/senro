// Package playwright is a preset step for Playwright Test with per-test
// reporting wired in.
//
// It runs
//
//	<runner...> playwright test --reporter=list,junit <Args...>
//
// with PLAYWRIGHT_JUNIT_OUTPUT_NAME=.senro/playwright.xml in its environment,
// and hands the JUnit report to testreport.JUnitXML, so a failing run names
// the tests that failed.
//
// A retry reruns the whole step, not only the failed tests: Playwright's
// JUnit reporter names a test by its title path joined with " › ", while
// --grep matches the title path joined with spaces, so a name from the
// report cannot be handed back to -g as it is.
//
// One step in a workflow:
//
//	w.Add("e2e", playwright.Step(playwright.Args("--workers=2")))
//
// One step per JavaScript package, fanned out over a graph:
//
//	verify.Expand("e2e", npm.Packages()).
//		Template(playwright.Template())
//
// # Runner
//
// The runner is the command prefix that runs the project's own Playwright:
// `pnpm exec` beside a pnpm-lock.yaml, `yarn` beside a yarn.lock, `bunx`
// beside a bun lockfile, `npx` otherwise. Step(Dir(d)) and Template resolve it
// with pm.Detect relative to the process working directory at build time (a
// pipeline is built in its repository root, which is where `senro run` runs
// it).
//
// # Where it applies
//
// Detect walks a repository and reports every directory that holds a
// playwright.config file, skipping vendored, generated and hidden
// directories. That is what `senro init` uses to decide this kit fits.
package playwright

import (
	"cmp"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/internal/pm"
	"github.com/xavidop/senro/testreport"
)

// Name is the kit's name, as it appears in a kit.Match.
const Name = "playwright"

// reportFile is where the JUnit report is written.
var reportFile = kit.ReportPath(Name)

// configFiles are the Playwright config files Detect looks for, in order.
var configFiles = []string{
	"playwright.config.ts", "playwright.config.js", "playwright.config.mjs",
	"playwright.config.cjs", "playwright.config.mts", "playwright.config.cts",
}

type config struct {
	dir  string
	args []string
}

// Option adjusts the step Step and Template build.
type Option func(*config)

// Dir runs the step from d, relative to the repository root. Template ignores
// it and uses the unit's directory.
func Dir(d string) Option { return func(c *config) { c.dir = d } }

// Args adds extra arguments to playwright test, after --reporter.
func Args(a ...string) Option {
	return func(c *config) { c.args = append(c.args, a...) }
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
	argv = append(argv, "playwright", "test", "--reporter=list,junit")
	argv = append(argv, c.args...)
	sb := senro.NewStep(exec.Command(argv...)).
		Env("PLAYWRIGHT_JUNIT_OUTPUT_NAME", reportFile).
		Tests(testreport.JUnitXML(reportFile))
	if dir != "" {
		sb = sb.WorkDir(dir)
	}
	return sb
}

// Step returns a fresh builder for the Playwright step. Adopt it with
// WorkflowBuilder.Add.
func Step(opts ...Option) *senro.StepBuilder {
	return build(newConfig(opts))
}

// Template returns a function for Expand ... Template that builds one
// Playwright step per unit, run from the unit's directory. The Dir option is
// ignored; a unit at "" or "." runs from the repository root. Every call
// returns a fresh builder.
func Template(opts ...Option) func(senro.Unit) *senro.StepBuilder {
	return func(u senro.Unit) *senro.StepBuilder {
		c := newConfig(opts)
		c.dir = kit.UnitDir(u.Dir)
		return build(c)
	}
}

// Detect reports every directory under root that holds a playwright.config
// file, in directory order, one match per directory.
func Detect(root string) ([]kit.Match, error) {
	var ms []kit.Match
	err := kit.Walk(root, func(dir, rel string) error {
		if f, ok := kit.FirstFile(dir, configFiles...); ok {
			ms = append(ms, kit.Match{Kit: Name, Dir: rel, Reason: f})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	kit.SortMatches(ms)
	return ms, nil
}
