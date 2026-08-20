---
layout: ../../../../layouts/DocsLayout.astro
title: Unit graphs
---

# Unit graphs

A unit graph is the second argument to [`Expand`](/docs/monorepo/fan-out/). It tells senro what to
fan out over: the list of apps, modules, crates or packages your repository already has.

```go
import "github.com/xavidop/senro/unit/gowork"

verify.Expand("test", gowork.Modules()).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
	})
```

Eight ship under `github.com/xavidop/senro/unit`. Pick the one that matches your repository.

## Choosing one

| Graph | A unit is | Can narrow to affected units? |
|---|---|---|
| [`glob`](/docs/monorepo/unit-graphs/glob/) | Any directory matching a pattern | No |
| [`gowork`](/docs/monorepo/unit-graphs/gowork/) | A Go module, or a Go package | **Yes** |
| [`cargo`](/docs/monorepo/unit-graphs/cargo/) | A Rust crate | **Yes** |
| [`jswork`](/docs/monorepo/unit-graphs/jswork/) | An npm, pnpm, Yarn or Bun workspace package | **Yes** |
| [`maven`](/docs/monorepo/unit-graphs/maven/) | A Maven reactor project | **Yes** |
| [`gradle`](/docs/monorepo/unit-graphs/gradle/) | A Gradle project | **Yes** |
| [`pyproject`](/docs/monorepo/unit-graphs/pyproject/) | A Python distribution | No |
| [`bazel`](/docs/monorepo/unit-graphs/bazel/) | A Bazel package | Only with `bazel.Query()` |

None of these fits? [Write your own](/docs/monorepo/unit-graphs/custom/): two methods, and
`Expand` cannot tell the difference.

## What "can narrow" means

Every graph can list units. Only some can also answer **"which units does this change reach?"**,
which is what [`Affected`](/docs/monorepo/affected/) needs:

```go
verify.Expand("test", gowork.Modules()).
	Affected(change.FromTrigger(ev)).     // only gowork, cargo, jswork, maven, gradle, bazel.Query
	Template(...)
```

Adding `.Affected(...)` over a graph that cannot answer is **refused at build time**, with a
message naming the graphs that can:

```
senro: expansion "lint": unit: glob dirs apps/* cannot compute an affected set: it discovers
units but knows nothing about which unit depends on which. Fan out over a graph that knows which
unit depends on which (gowork, cargo, jswork, maven or gradle, under
github.com/xavidop/senro/unit), or drop Affected and run every unit
```

Quietly running every unit would look friendlier and be wrong: an expansion that covered
everything looks exactly like one that computed a real answer, and a CI that cannot tell them
apart will eventually trust a green build that skipped the unit a change broke.

The question every graph is answering when it declines is the same one: **can a unit in this
ecosystem depend on another without saying so anywhere the graph can read?** For a glob pattern
and for Python, the answer is yes.

## No toolchain required

Every graph except `gowork` and `bazel.Query()` reads manifests or walks the tree, without running
the ecosystem's own tool. They work on a machine with no cargo, node, mvn, gradle, JDK or bazel
installed, which is usually the machine planning the run.

## What a unit is

Whatever the graph, a `senro.Unit` is three fields:

| Field | What it is |
|---|---|
| `ID` | The unit's stable identity, and what lands in the child step's id: `test[unit=apps/web]`. |
| `Name` | What a tool calls the unit: a module path, a crate name, a `groupId:artifactId`. What a template passes to `--filter`, `-p` or `-pl`. |
| `Dir` | The unit's directory, relative to the root. `u.Sources()` builds a `Pure()` step's inputs from it. |

## Where to go next

- **[Fan-out](/docs/monorepo/fan-out/)**: `Expand`, `Template`, `MaxParallel` and `MaxNodes`.
- **[Affected units](/docs/monorepo/affected/)**: narrowing a run to what a change reaches.
- **[Write your own](/docs/monorepo/unit-graphs/custom/)**: a layout no shipped graph reads.
