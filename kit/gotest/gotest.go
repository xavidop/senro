// Package gotest is a preset step for `go test` with per-test reporting wired
// in.
//
// It runs
//
//	go test -json [-race] [-tags a,b] <Args...> <Packages...>
//
// and hands the JSON stream to testreport.GoTestJSON, so a failing run names
// the tests that failed and a retry can rerun only those. Packages default to
// ./... when none are given.
//
// One step in a workflow:
//
//	w.Add("test", gotest.Step(gotest.Race()))
//
// One step per Go module, fanned out over a graph:
//
//	verify.Expand("test", gowork.Modules()).
//		Affected(change.FromTrigger(ev)).
//		Template(gotest.Template(gotest.Race()))
//
// # Where it applies
//
// Detect walks a repository and reports every directory holding a go.mod,
// skipping vendored, generated and hidden directories. That is what
// `senro init` uses to decide this kit fits.
package gotest

import (
	"strings"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/testreport"
)

// Name is the kit's name, as it appears in a kit.Match.
const Name = "gotest"

type config struct {
	dir                  string
	args, packages, tags []string
	race                 bool
}

// Option adjusts the step Step and Template build.
type Option func(*config)

// Dir runs the step from d, relative to the repository root. Template ignores
// it and uses the unit's directory.
func Dir(d string) Option { return func(c *config) { c.dir = d } }

// Args adds extra arguments to `go test`, after -race and -tags and before the
// packages.
func Args(a ...string) Option {
	return func(c *config) { c.args = append(c.args, a...) }
}

// Packages sets the packages to test. The default is ./...
func Packages(p ...string) Option {
	return func(c *config) { c.packages = append(c.packages, p...) }
}

// Race adds -race.
func Race() Option { return func(c *config) { c.race = true } }

// Tags adds -tags with the given build tags, comma-joined.
func Tags(t ...string) Option {
	return func(c *config) { c.tags = append(c.tags, t...) }
}

func newConfig(opts []Option) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}

func build(c config) *senro.StepBuilder {
	argv := []string{"go", "test", "-json"}
	if c.race {
		argv = append(argv, "-race")
	}
	if len(c.tags) > 0 {
		argv = append(argv, "-tags", strings.Join(c.tags, ","))
	}
	argv = append(argv, c.args...)
	if len(c.packages) == 0 {
		argv = append(argv, "./...")
	} else {
		argv = append(argv, c.packages...)
	}
	sb := senro.NewStep(exec.Command(argv...)).Tests(testreport.GoTestJSON())
	if dir := kit.UnitDir(c.dir); dir != "" {
		sb = sb.WorkDir(dir)
	}
	return sb
}

// Step returns a fresh builder for the go test step. Adopt it with
// WorkflowBuilder.Add.
func Step(opts ...Option) *senro.StepBuilder {
	return build(newConfig(opts))
}

// Template returns a function for Expand ... Template that builds one go test
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

// Detect reports every directory under root that holds a go.mod, in
// directory order. Skipped directories are not entered.
func Detect(root string) ([]kit.Match, error) {
	var ms []kit.Match
	err := kit.Walk(root, func(dir, rel string) error {
		if f, ok := kit.FirstFile(dir, "go.mod"); ok {
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
