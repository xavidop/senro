---
layout: ../../../layouts/DocsLayout.astro
title: "CLI: init"
---

# CLI: init

`senro init` writes a first pipeline for you. It looks at a repository, finds the test and lint
tools it already uses, and writes a `ci/main.go` with one [kit](/docs/steps/kits/) step per tool,
in the same shape as the [quickstart](/docs/quickstart/). For the full command table and exit codes,
see [CLI](/docs/cli/).

```bash
senro init [DIR] [--out ci] [--force] [--dry-run] [--no-tidy]

senro init                  # the current directory, writes ./ci/main.go
senro init --dry-run        # print the file, write nothing
senro init ../service       # another repository
senro init --out pipeline   # write ./pipeline/main.go instead
```

`DIR` defaults to `.` and must be a directory.

## What it detects

| Kit | It looks for |
|---|---|
| `gotest` | A `go.mod` |
| `pytest` | pytest configured in `pyproject.toml` (a `[tool.pytest.ini_options]` table, or `pytest` as a dependency), a `pytest.ini`, a `[pytest]` section in `tox.ini`, or a `[tool:pytest]` section in `setup.cfg` |
| `eslint` | A `package.json` with an ESLint config file, or `eslint` as a dependency |
| `playwright` | A `playwright.config` file |

The exact rules are on [Kits](/docs/steps/kits/). Vendored, generated and hidden directories
(`node_modules`, `vendor`, `testdata`, `build`, anything starting with `.` or `_`, and the rest of
the list there) are not searched.

If nothing matches, `senro init` says what it looked for and exits `2` without writing anything.

## What it writes

The file is the quickstart's shape: a doc comment listing what was detected, `package main`,
`senro.New` named after the directory, one workflow called `verify`, `attach.Listen`, and
`senro.Run` with `WithAttach`. There are no `Needs` between the steps, because the kits are
independent checks. Imports are exactly the ones used. Here is the dry run for a repository with an
ESLint config at the root and a Playwright config in `e2e`:

```
$ senro init --dry-run web
// senro init wrote this file from the checks it found below; edit it freely.
//
//	eslint     .          eslint.config.js
//	playwright e2e        playwright.config.ts
package main

import (
	"context"
	"log"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/kit/eslint"
	"github.com/xavidop/senro/kit/playwright"
)

func main() {
	ctx := context.Background()

	p := senro.New("web")

	verify := p.Workflow("verify")
	verify.Add("eslint", eslint.Step())
	verify.Add("playwright", playwright.Step(playwright.Dir("e2e")))

	// Opens a unix socket a second terminal can attach to while this runs.
	att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket})
	if err != nil {
		log.Fatal(err)
	}
	defer att.Close()

	// Run builds p first, so a duplicate id or a bad option surfaces here,
	// before anything executes.
	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
```

If the run fails, the generated `main` prints the error from `senro.Run` before it exits `1`.

How matches become steps:

- **One match** for a kit becomes one `verify.Add("<kit>", <kit>.Step(...))`, with
  `Dir("<dir>")` when the match is not the repository root. The step id is the kit's name.
- **Several `gotest` matches** become one `verify.Expand` over `gowork.Modules()` with
  `gotest.Template()`. A root module that also has nested modules is still one `Expand`; the root
  is one of its units.
- **Several `pytest` matches** become one `verify.Expand` over `pyproject.Packages()` with
  `pytest.Template()`, but only when every match is a Python distribution (a `pyproject.toml` that
  declares a name, a `setup.py` or a `setup.cfg`), since those are the graph's units. If any match
  is not, a `pytest.ini` or `tox.ini` on its own say, each match becomes its own `Add` with its
  `Dir`, named like the `eslint` steps below.
- **Several `eslint` or `playwright` matches** become one `Add` each, each with its `Dir`. The id
  is the kit's name and the last segment of the directory: `eslint-web` for `apps/web`. The root
  match keeps the bare kit name, and a segment two directories share gets `-2`, `-3` and so on
  (`eslint-web-2`).

Matches inside `--out` itself are left out, so running `senro init --force` again does not pick up
the `go.mod` it wrote in `ci/` as a Go module to test.

## The Go module

The file is a Go package, so it needs a module. `senro init` handles three cases:

- If `DIR/go.mod` exists (a Go repository) and `--out` is inside `DIR`, the pipeline joins that
  module.
- Otherwise the pipeline gets a module of its own at `<out>/go.mod`. That covers a repository
  with no `go.mod` and an `--out` somewhere else (an absolute path, say, or `../ci`). An existing
  `<out>/go.mod` is used as it is; a missing one is written with the module path
  `<DIR's name>/<out's name>` (so `myrepo/ci` by default) and the Go version of the toolchain
  running `senro`.

Then, in the module's directory, it runs `go get github.com/xavidop/senro@<version>` and
`go mod tidy`. The version is the one `senro` itself was built from, or `latest` for a development
build. A command that fails, or a missing Go toolchain, is a **warning**, not a failure: the file
is already written, and the warning says the module needs those commands before `senro run`.
`--no-tidy` skips both and prints them instead. With no `go` on the `PATH` it does the same: it
prints a warning and the two commands, and still exits `0`.

## Flags

| Flag | What it does |
|---|---|
| `--out DIR` | Where to write `main.go`: relative to `DIR`, or an absolute path (default `ci`) |
| `--force` | Replace an existing `<out>/main.go`. Without it, `senro init` refuses |
| `--dry-run` | Print the file to stdout and write nothing, not even a `go.mod` |
| `--no-tidy` | Do not run `go get` and `go mod tidy`; print them |

## Output

On success, `senro init` prints to stderr what it found, the files it wrote, and what to do next.
Here it is with `--no-tidy` in a Go repository with a root module and a nested one in `svc`:

```
$ senro init --no-tidy
senro init: detected
  gotest     .          go.mod
  gotest     svc        go.mod
Wrote ci/main.go
Run these in /work/gomono before senro run:
  go get github.com/xavidop/senro@latest
  go mod tidy
Next: senro run ./ci
```

- `Run these in` names the module's directory as an absolute path (`/work/gomono` here), and
  `@latest` is what a development build of `senro` asks for; a release names its own version.
- The pipeline joined the root `go.mod`, so no module file was written. In a repository without
  one, a `Wrote ci/go.mod` line follows `Wrote ci/main.go`, and the commands run in `ci`.
- Without `--no-tidy`, the two commands run instead of being printed, with their own output.
  When `senro init` used an existing module (`DIR`'s, or one already at `<out>`), a line
  `Updated go.mod: go get github.com/xavidop/senro@<version>` appears as soon as `go get`
  succeeds. `go mod tidy` runs after it, so a failing tidy shows up as a warning below that line
  rather than hiding that `go.mod` was edited.
- `Next:` follows `--out`: `senro run ./<out>`, or the bare path for an absolute `--out`. The path is
  shell-quoted when it needs it. When the process is not running in `DIR`, it is prefixed with
  `cd <DIR> &&`.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | The file was written, or printed with `--dry-run` |
| `1` | Detection failed (a `package.json` or `pyproject.toml` that does not parse, say), or a file could not be written |
| `2` | A usage error: an unknown flag, `DIR` is not a directory, nothing was detected, `--out` exists and is not a directory, or `<out>/main.go` exists and `--force` was not given |

A failing `go get` or `go mod tidy` does not change the exit code.

## After it

`init` writes a starting point, not a finished pipeline. Open `ci/main.go`, then:

```bash
senro run ./ci
```

Add `Needs`, `Retry` or `Timeout` to the steps as you would to any other, and pass options such as
`gotest.Race()`; the [kits page](/docs/steps/kits/) lists them.
