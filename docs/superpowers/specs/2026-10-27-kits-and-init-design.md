# Kits and `senro init`: design

Phase 3 of the Dagger 1.0 gap plan. Dagger ships ready-made modules (Go, pytest, ESLint,
Playwright) that find a project's toolchains and read their native configuration, so a new user
gets a working check without writing one. Senro has eight unit graphs that find the units of a
repository and, since Phase 1, per-test reporting, but a new user still writes `ci/main.go` by
hand and has to know which `go test` flags and which `Tests` declaration go together. This phase
adds preset steps that carry that knowledge, and a command that writes the first pipeline from
what it finds in the repository.

## Intent

Someone with a Go, Python, or JavaScript repository runs `senro init`, gets a `ci/main.go` that
tests and lints what is there with per-test results already wired in, and runs it with
`senro run ./ci`. Someone writing a pipeline by hand reaches for `gotest.Step()` instead of
remembering `-json` and `testreport.GoTestJSON()` together.

## Goals

- A `kit/` tree of preset steps, one Go package per tool: `kit/gotest`, `kit/pytest`,
  `kit/eslint`, `kit/playwright`. Each is plain Go: a function returning a `*senro.StepBuilder`
  with the command, the report declaration (`Tests`) and the re-run selection the tool supports,
  and a template form for fan-out over a unit graph. Each reads the project's native
  configuration where that changes the command: the lockfile that picks the package manager, the
  lockfile that picks the Python runner.
- Each kit detects itself: `Detect(root)` reports where in a tree the kit applies and why.
  `kit/all.Detect` runs every kit.
- `senro init [DIR]` detects the kits, writes `ci/main.go` from them (an `Expand` over the matching
  unit graph when a kit matches several directories), makes sure a Go module can build it, and
  says what to run next.
- A step built outside a workflow (a kit preset, or `senro.NewStep`) can be adopted by a workflow
  under an id: `WorkflowBuilder.Add(id, step)`.
- Docs for the kits and the command; the skill learns both; the quickstart offers `senro init`
  as the shortcut.

## Non-goals (this phase)

- Kits for Cargo, Maven, Gradle, Bazel, jest, vitest or mocha. The four kits are the ones Dagger
  ships. Everything else is a follow-up with the same shape.
- Reading a tool's configuration to change what the tool itself does (pytest's `testpaths`,
  ESLint's `ignores`). The tool reads its own config; the kit only has to invoke it correctly.
- A kit choosing an executor or an image. Kits return local steps; the author adds `On(...)`.
- `senro init` editing an existing `ci/main.go`, or a GitHub workflow file. It writes a new
  pipeline or refuses.
- Watching or re-running `senro init` as the repository changes.

## Anti-goals

- A kit must not hide the command it runs. The step's argv in `plan.json` is the whole truth, and
  the kit doc shows it.
- `senro init` must not overwrite a file without `--force`, and must not touch a `go.mod` it did
  not create except through `go get` of senro itself, which it reports.
- A pipeline without kits sees no change: `senro.go`'s plan lowering, `plan.Node` and `RunState`
  JSON are byte-identical for existing pipelines.

## Decisions

### Public API: adopting a step

```go
// package senro
// Add adopts a step built elsewhere (a kit preset, or NewStep) as a step of w with the given id,
// and returns it so Needs, Env, When and the rest chain as on Step.
func (w *WorkflowBuilder) Add(id string, sb *StepBuilder) *StepBuilder
```

Build refuses, naming the id: a builder that already has an id (added twice, or to two
workflows); a handler builder (`senro.Handler`); a nil builder. The id rules are those of `Step`.

### The `kit` package

```go
// package kit (stdlib only)
// Match is one place a kit applies.
type Match struct {
    Kit    string // "gotest", "pytest", "eslint", "playwright"
    Dir    string // root-relative, forward slashes, "." for the root
    Reason string // the file that decided it: "go.mod", "pyproject.toml [tool.pytest.ini_options]", "eslint.config.js"
}
// ReportDir is where kits write report files, relative to the step's working directory.
const ReportDir = ".senro"
```

`kit.Walk(root, fn)` is the shared tree walk every detector uses: it visits directories in sorted
order and skips `.git`, every other dot- and underscore-prefixed directory, `node_modules`,
`vendor`, `testdata`, `venv`, `env`, `build`, `dist`, `target`, `site-packages` (the union of the
unit graphs' skip lists). Matches are sorted by `Dir`.

`kit/internal/pm` detects a JavaScript package manager from the nearest lockfile at or above a
directory, stopping at root: `pnpm-lock.yaml` → pnpm, `yarn.lock` → yarn, `bun.lock`/`bun.lockb` →
bun, `package-lock.json` → npm, none → npm. It returns the argv prefix that runs a project binary:
`pnpm exec`, `yarn`, `bunx`, `npx`.

`kit/internal/py` detects the Python runner the same way: `uv.lock` → `uv run`, `poetry.lock` →
`poetry run`, none → `python -m`.

`kit/all.Detect(ctx, root) ([]kit.Match, error)` calls every kit's `Detect` and returns the
union sorted by `Kit` then `Dir`. It is the only package that imports all four kits.

### The four kits

Every kit has the same surface:

```go
type Option func(*config)
func Step(opts ...Option) *senro.StepBuilder                      // one step, WorkDir from Dir
func Template(opts ...Option) func(senro.Unit) *senro.StepBuilder  // one step per unit, WorkDir u.Dir (Dir ignored)
func Detect(root string) ([]kit.Match, error)
func Dir(d string) Option      // working directory, root-relative; "" means the step's default
func Args(a ...string) Option  // appended to the tool's argv, after the kit's own flags
```

`Template` returns a fresh builder per call, as `Expand.Template` requires. Neither form calls
`Pure()`: whether a test run is cacheable is the author's call, and the doc says how to add it
(`.Pure().Inputs(u.Sources()...)`).

**gotest**

- Command: `go test -json [-race] [-tags T] [Args...] <Packages...>`; `Packages` defaults to
  `./...`. Options: `Packages(...)`, `Race()`, `Tags(...)`.
- Tests: `testreport.GoTestJSON()`.
- Detect: every directory holding a `go.mod`. Reason `go.mod`.
- Fan-out graph for `senro init`: `gowork.Modules()`.

**pytest**

- Command: `<runner> pytest --junitxml=.senro/pytest.xml [Args...]`, where `<runner>` is the
  detected Python runner (`python -m`, `uv run`, `poetry run`). Option `Runner(argv ...string)`
  overrides it.
- Tests: `testreport.JUnitXML(".senro/pytest.xml").Selecting("-k", "{tests}").JoinedBy(" or ")`.
- Detect: a directory with `pyproject.toml` containing a `[tool.pytest.ini_options]` table or
  `pytest` in `[project] dependencies` or any `[project.optional-dependencies]` group or
  `[tool.poetry.dependencies]`/`[tool.poetry.group.*.dependencies]`; or `pytest.ini`; or
  `tox.ini`/`setup.cfg` with a `[pytest]`/`[tool:pytest]` section. Reason names the file and the
  table. Uses `internal/toml` for pyproject.
- Fan-out graph: `pyproject.Packages()`.

**eslint**

- Command: `<pm> eslint . --format junit --output-file .senro/eslint.xml [Args...]`. `Paths(...)`
  replaces `.`.
- Tests: `testreport.JUnitXML(".senro/eslint.xml")` with no selection: ESLint's JUnit test cases are
  rule hits per file, not re-runnable names.
- Detect: a directory with a `package.json` and either an ESLint config file
  (`eslint.config.{js,mjs,cjs,ts,mts,cts}`, `.eslintrc`, `.eslintrc.{js,cjs,json,yaml,yml}`) or
  `eslint` in `dependencies`/`devDependencies`. Reason names the config file, or
  `package.json devDependencies`.
- Fan-out: none; one step per match with `Dir`.

**playwright**

- Command: `<pm> playwright test --reporter=list,junit [Args...]` with
  `Env("PLAYWRIGHT_JUNIT_OUTPUT_NAME", ".senro/playwright.xml")`.
- Tests: `testreport.JUnitXML(".senro/playwright.xml")` with no selection. (Amended in the final
  review: Playwright's JUnit reporter names a case by its title path joined with ` › `, while
  `--grep` matches the path joined by spaces, so a recorded name can never select its test. The
  docs say per-test re-run is not available for Playwright.)
- Detect: a directory with `playwright.config.{js,mjs,cjs,ts,mts,cts}`. Reason names the file.
- Fan-out: none; one step per match with `Dir`.

Step ids are chosen by the caller (`Add`) or the expansion. `senro init` uses the kit name for a
single match (`gotest`, `pytest`, `eslint`, `playwright`) and the expansion id for a fan-out.

### `senro init`

```
senro init [DIR] [--out ci] [--force] [--dry-run] [--no-tidy]
```

`DIR` defaults to `.`; it must be a directory. The command:

1. Runs `kit/all.Detect`. With no match it prints what it looked for and exits 2.
2. Groups matches by kit. For `gotest` and `pytest` with more than one match it emits an `Expand`
   over the kit's graph with the kit's `Template()`; otherwise one `Add` per match, with `Dir(...)`
   when the match is not the root. A root `gotest` match whose repository also has nested modules
   is still one `Expand` over `gowork.Modules()` (the root module is one of its units).
3. Renders `<out>/main.go` from a Go template, formats it with `go/format`, and refuses to write
   over an existing file unless `--force`. `--dry-run` prints the file to stdout and writes nothing
   (and skips step 4).
4. Makes the package buildable: if no `go.mod` exists at `DIR` or `<out>`, writes `<out>/go.mod`
   with `module <basename(DIR)>/ci` and the Go version of the running toolchain, then runs
   `go get github.com/xavidop/senro@<version>` and `go mod tidy` in `<out>`; if `DIR/go.mod`
   exists, runs the same two commands in `DIR`. `<version>` is the CLI's own module version from
   `runtime/debug.ReadBuildInfo`, or `latest` for a development build. `--no-tidy` skips both
   commands and prints them instead. A failing command is a warning, not a failure: the file is
   written, and the warning says the module needs it before `senro run`.
5. Prints, to stderr, each match on one line (`gotest    .          go.mod`), the files written,
   and `Next: senro run ./ci`. Exit 0.

The generated file is the quickstart's shape: a doc comment naming the detected tools, package
`main`, `senro.New("<basename(DIR)>")`, one workflow `verify`, `attach.Listen` with
`attach.AutoUnixSocket`, `senro.Run` with `WithAttach`, `os.Exit(1)` on error. Steps have no
`Needs`: the kits are independent checks. Imports are exactly those used.

`SENRO_INIT_REPLACE=<path>` (undocumented, read only by `senro init`) adds
`replace github.com/xavidop/senro => <path>` to a `go.mod` it writes. It exists so this
repository's tests can build a generated pipeline against the working tree.

### Docs

`site/src/pages/docs/steps/kits.md` (new, sidebar after "Services"): one section per kit with the
exact argv, the report wired in, the options, and the `Add`/`Template` forms. `cli/init.md` (new,
sidebar under CLI): flags, what it detects, what it writes, exit codes. `quickstart.md` gains a
line before step 1: `senro init` writes this file for you. `skills/senro/SKILL.md` gains a "Kits"
section and `references/cli.md` gains `senro init`. README feature bullet. `steps/index.md`
mentions `Add`.

## Left to the builder

Exact refusal wording (same voice as the existing validators); the Go template's comment text;
whether detectors cap directory depth beyond the skip list; the pyproject parsing helpers' shape
given `internal/toml`'s API.
