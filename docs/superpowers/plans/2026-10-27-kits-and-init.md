# Kits and `senro init` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preset steps for `go test`, pytest, ESLint and Playwright with per-test reporting wired in, and a `senro init` command that writes a first pipeline from what it detects.

**Architecture:** A stdlib `kit` package holds the shared `Match`, `ReportDir` and tree walk; four leaf packages `kit/{gotest,pytest,eslint,playwright}` each expose `Step`, `Template`, `Detect` and options and import the public `senro`, `exec` and `testreport` packages; `kit/all` aggregates detection; `cmd/senro` gains `init`, which renders a Go template and prepares a module. `WorkflowBuilder.Add` lets a workflow adopt a preset builder.

**Tech Stack:** Go 1.26, stdlib only (`text/template`, `go/format`, `runtime/debug`, `internal/toml` for pyproject).

**Spec:** `docs/superpowers/specs/2026-10-27-kits-and-init-design.md`

## Global Constraints

- stdlib only in `kit/...`; `kit` itself imports nothing from this module.
- No em dashes anywhere (code, comments, docs, commit messages).
- Existing pipelines' `plan.json` and `RunState` JSON stay byte-identical: no new `plan.Node` fields.
- Never use `cm4` or a real customer name in examples or fixtures.
- Report paths are `kit.ReportDir + "/<kit>.xml"` with `ReportDir = ".senro"`.
- Detection skip list (exact): `node_modules`, `vendor`, `testdata`, `venv`, `env`, `build`, `dist`, `target`, `site-packages`, and any directory whose base starts with `.` or `_`.
- Every `Template` returns a fresh `*senro.StepBuilder` per call.
- Commit on the current branch only; never push; trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- `make all` (lint, vet, tests, site link check) must stay green.

## Review Focus

1. A kit `Detect` on a root containing a `node_modules` tree with its own `go.mod`/`package.json`: must not match inside it (Task 2 walk test, Task 3/5 detect tests).
2. `Add` of a builder that was already added to another workflow: Build refuses with both ids named, never silently creates two nodes (Task 1).
3. `senro init` on a repository whose root has a `go.mod` and a nested module: one `Expand` over `gowork.Modules()`, not an `Add` plus an `Expand` (Task 6).
4. `senro init` in a directory with an existing `ci/main.go` and no `--force`: exit 2, nothing written, including no `go.mod` (Task 6).
5. pytest detection on a `pyproject.toml` that is not valid TOML: a clear error naming the file, not a panic and not a silent non-match (Task 4).

---

### Task 1: `WorkflowBuilder.Add`

**Files:**
- Modify: `senro.go` (next to `Step`, line ~210; lowering near line 1361 where `sb.errs` is checked)
- Test: `senro_test.go`

**Interfaces:**
- Produces: `func (w *WorkflowBuilder) Add(id string, sb *StepBuilder) *StepBuilder`.

- [ ] **Step 1: Write the failing tests** in `senro_test.go`:

```go
func TestAddAdoptsABuilderUnderAnID(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("w")
	sb := senro.NewStep(exec.Command("go", "test", "-json", "./...")).Tests(testreport.GoTestJSON())
	got := w.Add("test", sb)
	if got != sb { t.Fatal("Add must return the builder it was given") }
	pl, err := p.Build()
	if err != nil { t.Fatal(err) }
	n, ok := pl.Node("test")
	if !ok || n.Tests == nil || n.Tests.Format != "gotest" { t.Fatalf("node = %+v", n) }
}

func TestAddRefusesABuilderAddedTwice(t *testing.T) {
	p := senro.New("p")
	w1, w2 := p.Workflow("a"), p.Workflow("b")
	sb := senro.NewStep(exec.Command("true"))
	w1.Add("one", sb)
	w2.Add("two", sb)
	_, err := p.Build()
	if err == nil || !strings.Contains(err.Error(), `"one"`) || !strings.Contains(err.Error(), `"two"`) {
		t.Fatalf("err = %v, want both ids named", err)
	}
}

func TestAddRefusesAHandlerAndNil(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("w")
	w.Add("h", senro.Handler("x", exec.Command("true")))
	if _, err := p.Build(); err == nil || !strings.Contains(err.Error(), "handler") { t.Fatalf("handler: %v", err) }
	p2 := senro.New("p")
	p2.Workflow("w").Add("n", nil)
	if _, err := p2.Build(); err == nil || !strings.Contains(err.Error(), `"n"`) { t.Fatalf("nil: %v", err) }
}
```

- [ ] **Step 2: Run** `go test ./ -run 'TestAdd' -count=1`; expected: compile failure, `Add` undefined.

- [ ] **Step 3: Implement `Add`.** A nil `sb` becomes a fresh builder carrying an error (`senro: Add %q: nil step`). A builder with a non-empty `id` records `senro: step %q was already added as %q` into `sb.errs` (first id first, so both are named) and is NOT appended again. A handler records `senro: Add %q: a handler (senro.Handler) cannot be a workflow step`. Otherwise set `sb.id = id` and append to `w.steps`. Build surfaces `sb.errs[0]` through the existing check at line ~1361; make sure a builder appended once but erroring twice still yields the error.

- [ ] **Step 4: Run** `go test ./ -run 'TestAdd|TestTestsLowers' -count=1`; expected PASS. Run `go vet ./` and `golangci-lint run ./` (as `make lint` does).

- [ ] **Step 5: Commit** `feat(senro): WorkflowBuilder.Add adopts a preset step`.

---

### Task 2: `kit` package and shared detectors

**Files:**
- Create: `kit/kit.go`, `kit/kit_test.go`, `kit/internal/pm/pm.go`, `kit/internal/pm/pm_test.go`, `kit/internal/py/py.go`, `kit/internal/py/py_test.go`

**Interfaces:**
- Produces:
  - `type Match struct { Kit, Dir, Reason string }`
  - `const ReportDir = ".senro"`
  - `func ReportPath(kitName string) string` → `".senro/<kitName>.xml"` (slash-joined, used by every kit)
  - `func Walk(root string, fn func(dir, rel string) error) error`: calls `fn` for `root` (rel `"."`) and every non-skipped directory below, in lexical order, with `rel` using forward slashes; `fn` returning `fs.SkipDir` skips that subtree.
  - `func SortMatches(ms []Match)`: by `Kit`, then `Dir`.
  - `pm.Detect(dir, root string) pm.Manager` with `Manager{Name string; Exec []string}`: `Name` one of `pnpm|yarn|bun|npm`, `Exec` one of `{"pnpm","exec"}`, `{"yarn"}`, `{"bunx"}`, `{"npx"}`. Walks from `dir` up to `root` inclusive, first lockfile wins; default npm.
  - `py.Detect(dir, root string) py.Runner` with `Runner{Name string; Argv []string}`: `uv` → `{"uv","run"}`, `poetry` → `{"poetry","run"}`, default `python` → `{"python","-m"}`.

- [ ] **Step 1: Write failing tests.** `kit_test.go`: `TestWalkSkipsIgnoredDirectories` builds a temp tree `a/`, `a/node_modules/x/`, `.git/`, `_gen/`, `b/vendor/`, `b/testdata/`, `b/ok/` and asserts the visited rels are exactly `[".", "a", "b", "b/ok"]`; `TestWalkHonoursSkipDir` returns `fs.SkipDir` from `a` and asserts `a/ok` was not visited; `TestReportPath` asserts `ReportPath("pytest") == ".senro/pytest.xml"`; `TestSortMatches` sorts `[{pytest,"."},{gotest,"svc"},{gotest,"."}]` to `gotest .`, `gotest svc`, `pytest .`. `pm_test.go`: table of lockfile → `Name`/`Exec`, plus a case where the lockfile is in the parent (`dir = "apps/web"`, lockfile at root) and a case with none → npm. `py_test.go`: same shape for `uv.lock`, `poetry.lock`, none.

- [ ] **Step 2: Run** `go test ./kit/... -count=1`; expected: compile failures.

- [ ] **Step 3: Implement** the three packages. `Walk` uses `filepath.WalkDir`, sorts entries lexically (WalkDir already does), skips per the Global Constraints list, converts rel with `filepath.ToSlash`. Document each exported symbol in the repository's voice.

- [ ] **Step 4: Run** `go test ./kit/... -count=1` → PASS; `golangci-lint run ./kit/...` → clean.

- [ ] **Step 5: Commit** `feat(kit): shared match, report path, tree walk and tool detection`.

---

### Task 3: `kit/gotest`

**Files:**
- Create: `kit/gotest/gotest.go`, `kit/gotest/gotest_test.go`, `kit/gotest/testdata/tree/` (a root `go.mod`, `svc/api/go.mod`, `node_modules/dep/go.mod`, `tools/go.mod`)

**Interfaces:**
- Consumes: Task 1 `Add`; Task 2 `kit.Match`, `kit.Walk`, `kit.SortMatches`.
- Produces:
  - `type Option func(*config)`
  - `func Step(opts ...Option) *senro.StepBuilder`
  - `func Template(opts ...Option) func(senro.Unit) *senro.StepBuilder`
  - `func Detect(root string) ([]kit.Match, error)`
  - `func Dir(d string) Option`, `func Args(a ...string) Option`, `func Packages(p ...string) Option`, `func Race() Option`, `func Tags(t ...string) Option`
  - `const Name = "gotest"`; a `Match.Kit` is `Name`.

- [ ] **Step 1: Write failing tests.**

```go
func TestStepRunsGoTestJSONWithReporting(t *testing.T) {
	p := senro.New("p")
	p.Workflow("w").Add("test", gotest.Step())
	pl, err := p.Build(); if err != nil { t.Fatal(err) }
	n, _ := pl.Node("test")
	wantCmd := []string{"go", "test", "-json", "./..."}
	if !reflect.DeepEqual(n.Cmd, wantCmd) || n.WorkDir != "" { t.Fatalf("node = %+v", n) }
	if n.Tests == nil || n.Tests.Format != "gotest" || !reflect.DeepEqual(n.Tests.Select, []string{"-run", "^({tests})$"}) { t.Fatalf("tests = %+v", n.Tests) }
}

func TestOptionsOrderTheArgv(t *testing.T) {
	// go test -json -race -tags integration,e2e -count=1 ./cmd/... ./pkg/...
	sb := gotest.Step(gotest.Race(), gotest.Tags("integration", "e2e"), gotest.Args("-count=1"), gotest.Packages("./cmd/...", "./pkg/..."), gotest.Dir("svc/api"))
	... Build via Add, assert Cmd and WorkDir == "svc/api"
}

func TestTemplateUsesTheUnitDir(t *testing.T) {
	tmpl := gotest.Template(gotest.Dir("ignored"))
	a := tmpl(senro.Unit{ID: "svc/api", Dir: "svc/api"})
	b := tmpl(senro.Unit{ID: "svc/api", Dir: "svc/api"})
	if a == b { t.Fatal("Template must return a fresh builder per call") }
	... Add a under "x", Build, assert WorkDir == "svc/api"
}

func TestDetectFindsEveryModuleOutsideSkippedDirs(t *testing.T) {
	got, err := gotest.Detect("testdata/tree")
	want := []kit.Match{{Kit: "gotest", Dir: ".", Reason: "go.mod"}, {Kit: "gotest", Dir: "svc/api", Reason: "go.mod"}, {Kit: "gotest", Dir: "tools", Reason: "go.mod"}}
	...
}
```

Check the exact field name for the node's argv in `internal/plan/plan.go` (`Cmd` or similar) before writing; use whatever `plan.Node` calls it.

- [ ] **Step 2: Run** `go test ./kit/gotest/ -count=1`; expected: compile failures.

- [ ] **Step 3: Implement.** `config{dir string; args, packages, tags []string; race bool}`; `build(cfg)` makes the argv `go test -json [-race] [-tags a,b] args... packages...` (packages default `["./..."]`), `senro.NewStep(exec.Command(argv...)).Tests(testreport.GoTestJSON())`, `WorkDir(cfg.dir)` when non-empty. `Template` ignores `Dir` and sets `WorkDir(u.Dir)` unless `u.Dir` is `""` or `"."`. `Detect` walks with `kit.Walk`, matching a `go.mod` file in the directory. Package doc shows the argv and the `Add`/`Template` forms.

- [ ] **Step 4: Run** `go test ./kit/... -count=1` → PASS; lint clean.

- [ ] **Step 5: Commit** `feat(kit/gotest): preset go test step with per-test reporting`.

---

### Task 4: `kit/pytest`

**Files:**
- Create: `kit/pytest/pytest.go`, `kit/pytest/pytest_test.go`, `kit/pytest/testdata/` fixtures: `ini_options/pyproject.toml` (has `[tool.pytest.ini_options]`), `deps/pyproject.toml` (`pytest` in `[project] dependencies`), `optional/pyproject.toml` (`pytest` in `[project.optional-dependencies] dev`), `poetry/pyproject.toml` (`[tool.poetry.group.dev.dependencies] pytest`), `pytestini/pytest.ini`, `tox/tox.ini` (`[pytest]`), `setupcfg/setup.cfg` (`[tool:pytest]`), `none/pyproject.toml` (no pytest), `broken/pyproject.toml` (invalid TOML), `uv/pyproject.toml` + `uv/uv.lock` (ini_options).

**Interfaces:**
- Consumes: Task 2 `kit.*`, `py.Detect`, `internal/toml.Parse`.
- Produces: same surface as Task 3 plus `func Runner(argv ...string) Option`, `func Paths(p ...string) Option`; `const Name = "pytest"`.

- [ ] **Step 1: Write failing tests.**
  - `TestStepRunsPytestWithJUnit`: default `Step()` with no lockfile → `Cmd == ["python","-m","pytest","--junitxml=.senro/pytest.xml"]`, `Tests == {Format:"junit", Path:".senro/pytest.xml", Select:["-k","{tests}"], Join:" or "}`.
  - `TestRunnerFollowsTheLockfile`: `Step(Dir("testdata/uv"))` → argv starts `uv run pytest`. (`Step` resolves the runner from `Dir` relative to the process cwd at build time; `Template` resolves from `u.Dir`.) `Runner("pipenv","run")` overrides.
  - `TestPathsAndArgs`: `Paths("tests/unit")`, `Args("-x")` → `... pytest --junitxml=.senro/pytest.xml -x tests/unit`.
  - `TestDetectRecognisesEveryConfigShape`: table over the fixture dirs; `Reason` values exactly: `pyproject.toml [tool.pytest.ini_options]`, `pyproject.toml [project] dependencies`, `pyproject.toml [project.optional-dependencies]`, `pyproject.toml [tool.poetry]`, `pytest.ini`, `tox.ini [pytest]`, `setup.cfg [tool:pytest]`; `none` → no match.
  - `TestDetectReportsABrokenPyproject`: `Detect("testdata/broken")` returns an error containing `broken/pyproject.toml`.

- [ ] **Step 2: Run** → compile failures.

- [ ] **Step 3: Implement.** INI-style files: a line-scanner that finds a `[pytest]` or `[tool:pytest]` section header (trimmed, exact). pyproject: `toml.Parse`, then `Sub("tool","pytest","ini_options") != nil`, then dependency lists via `StrList("project","dependencies")` and each group of `Sub("project","optional-dependencies")`, then `Sub("tool","poetry","dependencies")` keys and `Sub("tool","poetry","group")` groups' `dependencies` keys; a dependency string matches when its requirement name (text before the first of ` [<>=!~;`) equals `pytest` case-insensitively. Report order: first reason found in the order listed above.

- [ ] **Step 4: Run** `go test ./kit/... -count=1` → PASS; lint clean.

- [ ] **Step 5: Commit** `feat(kit/pytest): preset pytest step with JUnit reporting and -k re-runs`.

---

### Task 5: `kit/eslint` and `kit/playwright`

**Files:**
- Create: `kit/eslint/eslint.go`, `kit/eslint/eslint_test.go`, `kit/eslint/testdata/` (`flat/package.json`+`eslint.config.mjs`, `legacy/package.json`+`.eslintrc.json`, `dep/package.json` with `"devDependencies":{"eslint":"^9"}`, `none/package.json`, `pnpm/package.json`+`eslint.config.js`+`pnpm-lock.yaml`), `kit/playwright/playwright.go`, `kit/playwright/playwright_test.go`, `kit/playwright/testdata/` (`ts/playwright.config.ts`, `js/playwright.config.js`, `none/package.json`, `yarn/playwright.config.ts`+`yarn.lock`).

**Interfaces:**
- Consumes: Task 2 `kit.*`, `pm.Detect`.
- Produces: eslint: Task 3 surface plus `func Paths(p ...string) Option`; `const Name = "eslint"`. playwright: Task 3 surface; `const Name = "playwright"`.

- [ ] **Step 1: Write failing tests.**
  - eslint `TestStepRunsESLintWithJUnit`: default → `Cmd == ["npx","eslint",".","--format","junit","--output-file",".senro/eslint.xml"]`, `Tests == {Format:"junit", Path:".senro/eslint.xml"}` with nil `Select`. `Step(Dir("testdata/pnpm"))` → starts `pnpm exec eslint`. `Paths("src","test")` replaces `.`; `Args("--max-warnings=0")` appended last.
  - eslint `TestDetect`: `flat` → Reason `eslint.config.mjs`; `legacy` → `.eslintrc.json`; `dep` → `package.json devDependencies`; `none` → no match; a dir without `package.json` but with `eslint.config.js` → no match (the kit needs a package to run in).
  - playwright `TestStepRunsPlaywrightWithJUnit`: default → `Cmd == ["npx","playwright","test","--reporter=list,junit"]`, `Env` contains `PLAYWRIGHT_JUNIT_OUTPUT_NAME=.senro/playwright.xml`, `Tests == {Format:"junit", Path:".senro/playwright.xml", Select:["-g","{tests}"], Join:"|"}`. `Dir("testdata/yarn")` → `yarn playwright test ...`.
  - playwright `TestDetect`: `ts` → `playwright.config.ts`; `js` → `playwright.config.js`; `none` → no match.
  - Both: `TestTemplateIsFreshPerCall` as in Task 3.

- [ ] **Step 2: Run** → compile failures.

- [ ] **Step 3: Implement** both packages in the Task 3 shape. ESLint config file candidates, in order: `eslint.config.js`, `.mjs`, `.cjs`, `.ts`, `.mts`, `.cts`, `.eslintrc`, `.eslintrc.js`, `.cjs`, `.json`, `.yaml`, `.yml`. Playwright candidates: `playwright.config.ts`, `.js`, `.mjs`, `.cjs`, `.mts`, `.cts`. `package.json` dependency check uses `encoding/json` into `struct{ Dependencies, DevDependencies map[string]string }`; an unparseable `package.json` is an error naming the file.

- [ ] **Step 4: Run** `go test ./kit/... -count=1` → PASS; lint clean.

- [ ] **Step 5: Commit** `feat(kit): eslint and playwright presets`.

---

### Task 6: `kit/all` and `senro init`

**Files:**
- Create: `kit/all/all.go`, `kit/all/all_test.go`, `cmd/senro/cmd_init.go`, `cmd/senro/init_render.go`, `cmd/senro/cmd_init_test.go`, `cmd/senro/testdata/init/` fixtures: `gomono/` (root `go.mod` + `svc/go.mod`), `single/` (root `go.mod` only), `web/` (`package.json` + `eslint.config.js` + `e2e/playwright.config.ts` + `pnpm-lock.yaml`), `py/` (`pyproject.toml` with ini_options, no go.mod), `empty/` (a README only), `existing/` (root `go.mod` + `ci/main.go` containing `package main`).
- Modify: `cmd/senro/main.go` (dispatch `case "init"` and the `usage` text).

**Interfaces:**
- Consumes: Tasks 2 to 5.
- Produces:
  - `all.Detect(ctx context.Context, root string) ([]kit.Match, error)`.
  - `cmdInit(args []string, stdout, stderr io.Writer) int`.
  - `type initOptions struct{ dir, out string; force, dryRun, noTidy bool }`, `parseInitOptions(args []string) (initOptions, error)`.
  - `renderInit(name string, matches []kit.Match) ([]byte, error)`: the formatted `main.go`.
  - `initModule(ctx, dir, out, version string, noTidy bool, stderr io.Writer) error`: step 4 of the spec.

- [ ] **Step 1: Write failing tests.**
  - `all_test.go`: a temp tree with a `go.mod` and a `playwright.config.ts` → two matches sorted `gotest` before `playwright`.
  - `TestInitRendersOneExpandForAGoMonorepo`: `renderInit("gomono", matches from gomono)` contains `verify.Expand("gotest", gowork.Modules()).Template(gotest.Template())` and does NOT contain `verify.Add(`; imports include `github.com/xavidop/senro/unit/gowork` and `github.com/xavidop/senro/kit/gotest`, not `kit/pytest`.
  - `TestInitRendersAddForASingleMatch`: `single` → `verify.Add("gotest", gotest.Step())`, no `unit/gowork` import.
  - `TestInitRendersDirForNestedMatches`: `web` → `verify.Add("eslint", eslint.Step())` and `verify.Add("playwright", playwright.Step(playwright.Dir("e2e")))`.
  - `TestInitRenderedFileIsGofmtClean`: `format.Source(out)` equals `out` for every fixture.
  - `TestInitRefusesAnExistingPipeline`: copy `existing` to a temp dir, `cmdInit([]string{dir})` returns 2, stderr names `ci/main.go` and `--force`, the file is unchanged and no `ci/go.mod` exists.
  - `TestInitExitsTwoWhenNothingIsDetected`: `empty` → 2, stderr lists the four kits.
  - `TestInitDryRunWritesNothing`: `single` copy, `--dry-run` → 0, stdout holds `package main`, `ci/` does not exist.
  - `TestInitGeneratedPipelineBuilds` (skips without `go` on PATH; `t.Parallel` off): copy `py` to a temp dir, set `SENRO_INIT_REPLACE` to this repository's root (`filepath.Abs("../..")`), run `cmdInit([]string{dir})`, expect 0, then `go build ./...` in `<dir>/ci` succeeds. The written `ci/go.mod` must contain the `replace` line and `require github.com/xavidop/senro`.

- [ ] **Step 2: Run** `go test ./cmd/senro/ -run 'TestInit' -count=1` and `go test ./kit/all/` → compile failures.

- [ ] **Step 3: Implement.**
  - `all.Detect`: call the four `Detect`s, append, `kit.SortMatches`.
  - `parseInitOptions`: positional `DIR` (default `.`), flags `--out` (default `ci`), `--force`, `--dry-run`, `--no-tidy`; unknown flag → usage error.
  - `renderInit`: group matches by kit; `gotest` and `pytest` with more than one match render `Expand` over `gowork.Modules()` / `pyproject.Packages()`; everything else renders `Add` with `Dir(...)` when `Dir != "."`; ids are the kit name, and when the same kit renders several `Add`s the id is `<kit>-<last path segment of Dir>` with the root one keeping the bare name. Template text follows the quickstart (`attach.Listen`, `senro.WithAttach`, `os.Exit(1)`); the doc comment lists the matches. Run through `go/format`.
  - `initModule`: version from `debug.ReadBuildInfo()` main module (`(devel)` or empty → `latest`); write `go.mod` with `module <base>/ci`, `go <major.minor>` from `runtime.Version()`, and the `replace` line when `SENRO_INIT_REPLACE` is set; run `go get github.com/xavidop/senro@<version>` then `go mod tidy` with `exec.CommandContext`, stderr forwarded; failures are warnings prefixed `senro init: warning:`.
  - `cmdInit`: order per the spec; stderr summary lines use `%-10s %-10s %s`.
  - `main.go`: `case "init"` and a usage line `senro init [DIR] [--out ci] [--force] [--dry-run] [--no-tidy]`.

- [ ] **Step 4: Run** `go test ./cmd/senro/ -run 'TestInit' -count=1 && go test ./kit/... -count=1` → PASS; `golangci-lint run ./cmd/senro/... ./kit/...` clean.

- [ ] **Step 5: Commit** `feat(cli): senro init writes a pipeline from the detected kits`.

---

### Task 7: Docs, skill, README

**Files:**
- Create: `site/src/pages/docs/steps/kits.md`, `site/src/pages/docs/cli/init.md`
- Modify: `site/src/layouts/DocsLayout.astro` (sidebar: `steps/kits` "Kits" after `steps/services`; `cli/init` "senro init" in the CLI group), `site/src/pages/docs/quickstart.md`, `site/src/pages/docs/steps/index.md` (`Add`), `skills/senro/SKILL.md` (a "Kits" section after "Services"), `skills/senro/references/cli.md` (`senro init`), `README.md` (feature bullet and quickstart mention).

- [ ] **Step 1: Write `steps/kits.md`:** what a kit is, the `Add` and `Template` forms, one section per kit with the exact argv, the report wired in, the re-run selection (or why none), options, detection rule, and how to make it `Pure()`.
- [ ] **Step 2: Write `cli/init.md`:** synopsis, detection table (kit, what it looks for), what gets written, module handling, exit codes 0/2, `--dry-run`, `--force`, `--no-tidy`.
- [ ] **Step 3: Update** quickstart (one line before step 1: `senro init` writes this file; link), `steps/index.md`, sidebar, SKILL.md, `references/cli.md`, README.
- [ ] **Step 4: Verify.** `source ~/.nvm/nvm.sh && nvm use 22` then the site link check that `make all` runs (see the Makefile) passes; no em dashes: `grep -rn "—" site/src/pages/docs/steps/kits.md site/src/pages/docs/cli/init.md skills/ README.md` prints nothing.
- [ ] **Step 5: Commit** `docs: kits and senro init`.
