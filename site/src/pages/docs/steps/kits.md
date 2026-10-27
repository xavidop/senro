---
layout: ../../../layouts/DocsLayout.astro
title: Kits
---

# Kits

Most pipelines start with the same four steps: run the Go tests, run pytest, lint with ESLint, run
the Playwright suite. Each one needs the same care to be useful in senro: the right command line,
a report format senro can read, and a way to rerun only the tests that failed. A **kit** is that
care, packaged. It is a preset step for one tool, with [per-test results](/docs/steps/tests/) already
wired in, and it knows how to find the places in a repository where the tool applies.

```go
import (
	"github.com/xavidop/senro/kit/gotest"
	"github.com/xavidop/senro/kit/playwright"
)

verify := p.Workflow("verify")
verify.Add("test", gotest.Step(gotest.Race()))
verify.Add("e2e", playwright.Step(playwright.Dir("e2e")))
```

A kit builds an ordinary `*senro.StepBuilder`. Everything on [Steps](/docs/steps/) still applies:
chain `Needs`, `Env`, `Timeout`, `Retry` or `When` onto it as on any other step. If you would rather
not write this file by hand, [`senro init`](/docs/cli/init/) detects which kits fit a repository and
writes it.

## Two forms: `Step` and `Template`

Every kit has the same surface.

| Call | What it builds |
|---|---|
| `kit.Step(opts...)` | One step, run from the directory given by the `Dir` option (the pipeline's working directory by default) |
| `kit.Template(opts...)` | A function for `Expand(...).Template(...)`: one step per [unit](/docs/monorepo/), run from the unit's directory |
| `kit.Detect(root)` | The directories under `root` where the kit applies, as `[]kit.Match` |

`Step` goes into a workflow with `Add`. `(*senro.WorkflowBuilder).Add(id, sb)` adopts a builder made
elsewhere under `id` and returns it, so the rest of the chain works as it does after `Step`:

```go
verify.Add("test", gotest.Step()).Needs("lint").Timeout(10 * time.Minute)
```

`Add` refuses, when the pipeline is built and naming the id: a builder that was already added (here
or to another workflow), a handler builder, and a nil builder. The id rules are those of `Step`.

`Template` is for a repository with many modules or packages. It returns a fresh builder on every
call, as `Expand` requires, and it ignores the `Dir` option because the unit decides where the step
runs:

```go
import (
	"github.com/xavidop/senro/kit/gotest"
	"github.com/xavidop/senro/unit/gowork"
)

verify.Expand("test", gowork.Modules()).
	Template(gotest.Template(gotest.Race()))
```

See [Fan-out](/docs/monorepo/fan-out/) for what `Expand` does with the template.

## Options every kit takes

| Option | What it sets |
|---|---|
| `Dir(d)` | The step's working directory, relative to the repository root. Empty means the pipeline's directory. `Template` ignores it |
| `Args(a...)` | Extra arguments for the tool, placed as each section below says |

Options apply in order, and the list options (`Args`, `Packages`, `Tags`, `Paths`) append.

## The report is written for you

The three kits whose tools write a JUnit file put it under `.senro/` in the step's working
directory (`.senro/pytest.xml`, `.senro/eslint.xml`, `.senro/playwright.xml`). Every kit skips
`.senro` when it walks a repository, so a report is never mistaken for source. Add `.senro/` to your
`.gitignore`.

## Kits are not cached

Neither form calls `Pure()`. Whether a test run may be skipped when nothing changed is your call,
because only you know whether the tests touch a network, a clock or a database. To opt in, add it
yourself and declare the inputs:

```go
verify.Expand("test", gowork.Modules()).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return gotest.Template()(u).Pure().Inputs(u.Sources()...)
	})
```

See [Caching a step](/docs/data/caching/) for what a `Pure()` step promises.

## gotest

Runs `go test -json`, then `-race` if asked, then `-tags a,b` if asked, then your `Args`, then the
packages (`./...` unless you set some):

```
go test -json [-race] [-tags a,b] [Args...] <Packages... | ./...>
```

The JSON stream is read with `testreport.GoTestJSON()`, so a failing run names the tests that
failed and `senro rerun --failed-tests` reruns only those.

| Option | What it does |
|---|---|
| `Packages(p...)` | The packages to test. Default `./...` |
| `Race()` | Adds `-race` |
| `Tags(t...)` | Adds `-tags` with the tags comma-joined |
| `Args(a...)` | Extra `go test` arguments, after `-race` and `-tags` and before the packages |

**Where it applies:** every directory holding a `go.mod`. For a repository with several, use
`Template` over [`gowork.Modules()`](/docs/monorepo/).

## pytest

```
<runner...> pytest --junitxml=.senro/pytest.xml [Args...] [Paths...]
```

The report is read with `testreport.JUnitXML(".senro/pytest.xml")` and a rerun selects tests with
`-k "a or b"`: `Selecting("-k", "{tests}").JoinedBy(" or ")`.

`<runner>` is the prefix that puts pytest inside the project's environment, found from the nearest
lockfile at or above the step's directory:

| Lockfile | Runner |
|---|---|
| `uv.lock` | `uv run` |
| `poetry.lock` | `poetry run` |
| none | `python -m` |

The runner is resolved when the pipeline is built, relative to the directory the pipeline runs in
(where `senro run` was started). `Runner("pipenv", "run")` overrides the detection.

| Option | What it does |
|---|---|
| `Paths(p...)` | Files or directories pytest collects. Default none, so pytest uses its own configuration |
| `Runner(argv...)` | The command prefix, instead of the detected one |
| `Args(a...)` | Extra pytest arguments, after `--junitxml` and before the paths |

**Where it applies:** a directory matches on the first of these it finds: a
`[tool.pytest.ini_options]` table in `pyproject.toml`; `pytest` among `[project]` dependencies,
among any `[project.optional-dependencies]` group, or among the `[tool.poetry]` dependencies or
dependency groups; a `pytest.ini`; a `[pytest]` section in `tox.ini`; a `[tool:pytest]` section in
`setup.cfg`. A `pyproject.toml` that does not parse is an error naming the file. For several
projects, use `Template` over [`pyproject.Packages()`](/docs/monorepo/).

## eslint

```
<runner...> eslint <Paths... | .> --format junit --output-file .senro/eslint.xml [Args...]
```

The report is read with `testreport.JUnitXML(".senro/eslint.xml")`. ESLint's JUnit cases are rule
hits per file, not test names a rerun could select, so this kit declares no selection and a rerun
of the step runs it whole.

`<runner>` comes from the nearest JavaScript lockfile at or above the step's directory:

| Lockfile | Runner |
|---|---|
| `pnpm-lock.yaml` | `pnpm exec` |
| `yarn.lock` | `yarn` |
| `bun.lock` or `bun.lockb` | `bunx` |
| `package-lock.json`, or none | `npx` |

| Option | What it does |
|---|---|
| `Paths(p...)` | Files or directories to lint. Default `.` |
| `Args(a...)` | Extra ESLint arguments, after `--output-file` |

**Where it applies:** a directory with a `package.json` and either an ESLint config file
(`eslint.config.js`, `.mjs`, `.cjs`, `.ts`, `.mts` or `.cts`; `.eslintrc`; `.eslintrc.js`, `.cjs`,
`.json`, `.yaml` or `.yml`) or `eslint` in `dependencies` or `devDependencies`. A `package.json`
that does not parse is an error naming the file. There is no graph to fan out over here: one step
per match, each with its `Dir`.

## playwright

```
<runner...> playwright test --reporter=list,junit [Args...]
```

with `PLAYWRIGHT_JUNIT_OUTPUT_NAME=.senro/playwright.xml` set in the step's environment. The report
is read with `testreport.JUnitXML(".senro/playwright.xml")` and a rerun selects tests with
`-g "a|b"`: `Selecting("-g", "{tests}").JoinedBy("|")`. The runner is found the same way as for
[eslint](#eslint).

| Option | What it does |
|---|---|
| `Args(a...)` | Extra `playwright test` arguments, after `--reporter` |

**Where it applies:** a directory holding a `playwright.config.ts`, `.js`, `.mjs`, `.cjs`, `.mts`
or `.cts`. One step per match, each with its `Dir`.

## Finding where a kit applies

`Detect(root)` returns a `[]kit.Match` for one kit; `kit/all.Detect(ctx, root)` runs all four and
returns the union sorted by kit and then directory. A `kit.Match` has the kit's name, the
directory relative to `root` with forward slashes (`.` for the root), and a `Reason`: the file that
decided it, such as `go.mod` or `pyproject.toml [tool.pytest.ini_options]`.

Detection walks the tree in sorted order and skips every directory whose name starts with `.` or
`_` (`.git` included), plus `node_modules`, `vendor`, `testdata`, `venv`, `env`, `build`, `dist`,
`target` and `site-packages`. A directory that cannot be read is skipped rather than stopping the
walk. `kit/all` is the one package that imports all four kits, so a pipeline that only wants
`gotest` does not link the others.

## What kits do not do

- They do not install anything. The tool must already be on the `PATH` of wherever the step runs.
- They do not call `Pure()`; see above.
- They do not choose a step id or an order. You pick the id in `Add`, and nothing here adds
  `Needs`: unit tests and a linter are independent checks.
