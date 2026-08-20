---
layout: ../../../../layouts/DocsLayout.astro
title: gowork
---

# `gowork`: Go

One unit per Go module, or per Go package. **Computes an affected set**, so a change to one package
runs only the packages that import it, transitively.

```go
import (
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/unit/gowork"
)

verify.Expand("test", gowork.Modules()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
	})
```

## Modules or packages

| | A unit is | `Name` is |
|---|---|---|
| `gowork.Modules()` | One per `go.mod` | The module path, `github.com/acme/api` |
| `gowork.Packages()` | One per Go package, finer grained | The import path, `github.com/acme/api/internal/store` |

`Modules()` is usually right for separately released services. `Packages()` pays off in a large
single module, where a change to one package should not retest the other four hundred.

Both come out of **one** listing, so they can never disagree: `Modules` is `Packages` collapsed
onto module directories, edges included.

```go
// One step per module: go test ./... inside each.
verify.Expand("test", gowork.Modules()).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
	})

// One step per package: go test on the package by import path.
verify.Expand("test", gowork.Packages()).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("go", "test", u.Name))
	})
```

## It runs `go list`

This is the one shipped graph that uses the ecosystem's own tool, so it needs a `go` binary on
`PATH`. In exchange, its answer is the toolchain's answer rather than a manifest parse.

Under the hood: `go list -deps -test -e -json`, once per module found under the root, in that
module's own directory.

- **`-deps`** reaches a package in another workspace module, so cross-module edges are real edges.
- **`-test`** because a test-only import is still an import. A graph missing them would disconnect
  packages that only a test connects. `TestImports` and `XTestImports` are both read.

A large listing takes seconds and is exactly the call somebody presses Ctrl-C on, so the graph
honours its context.

## Worth knowing

**`go.work` is not required.** Module discovery is a walk for `go.mod` files, so a repository with
several modules and no workspace file works fine.

**A tree with no `go.mod` anywhere is an error**, not an empty graph. An expansion that silently
produced no steps looks exactly like one that passed.

**A `go.mod` change affects every package in its module.** A dependency bump changes what all of
them compile against, so `Owns` attributes the file to all of them.

**A file no unit owns runs everything.** A `Makefile` above every module genuinely can change what
all of them build, so senro over-approximates rather than guessing.

## Where to go next

- **[Affected units](/docs/monorepo/affected/)**: what `Affected` narrows, and where `change`
  comes from.
- **[Per-unit edges](/docs/monorepo/needs-each/)**: `test[unit=api]` waiting on `build[unit=api]`.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
