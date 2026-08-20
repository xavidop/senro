---
layout: ../../../../layouts/DocsLayout.astro
title: bazel
---

# `bazel`

Two graphs, and the choice between them is whether you are willing to run bazel while senro is
still planning.

| | Discovery | Affected set |
|---|---|---|
| `bazel.Packages()` | A tree walk. No bazel needed. | No |
| `bazel.Query()` | The same tree walk. | **Yes**, by running `bazel query` |

```go
import "github.com/xavidop/senro/unit/bazel"

verify.Expand("test", bazel.Packages()).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("bazel", "test", u.Name+"/..."))
	})
```

## What counts as a unit

One **Bazel package**: a directory holding a `BUILD` or `BUILD.bazel` file.

| Field | For `apps/web` |
|---|---|
| `Name` | `//apps/web`, the package label (`//` for the root) |
| `ID`, `Dir` | `apps/web` |

Discovery is a pruned walk plus one small file read. **No bazel is needed and none is run**; a
test empties `PATH` and checks exactly that.

Left out, because Bazel itself would refuse to build them from this root: directories in
`.bazelignore`, directories inside a nested repository (their own `MODULE.bazel`, `REPO.bazel` or
`WORKSPACE`), and directories with no `BUILD` file. A root that is not a workspace root, or a
workspace with no `BUILD` file anywhere, is an error rather than an empty graph.

**Not one unit per target.** A macro computes its targets' names, so enumerating them means
evaluating Starlark or running bazel. `bazel test //apps/web/...` covers the package anyway.

## Why `bazel.Packages()` refuses `Affected`

The only way to answer without running bazel is to parse `BUILD` files, and that cannot be done
correctly. A `BUILD` file is a Starlark program: macros compute their own deps, `glob()`,
`select()` and comprehensions build them dynamically, a dep can be a variable or an alias, and
edges also come from toolchains, implicit rule dependencies and the `.bzl` files themselves.

Each is a missing edge, and a missing edge is a green build for a tree that does not build.

## `bazel.Query()`

Answers by **running bazel**, which has no such problem:

```go
verify.Expand("test", bazel.Query()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("bazel", "test", u.Name+"/..."))
	})
```

One query for the whole workspace, `bazel query --output=xml 'kind(rule, //...)'`, whose target
edges are mapped to package edges. Units still come from the tree walk, because a package is a
directory holding a `BUILD` file and that needs no toolchain; only the edges need bazel. Labels
into other repositories (`@rules_go//...`) are dropped: they are not units of this workspace.

Choosing it is choosing to run bazel while senro is still planning, and that is the whole cost:

- It is a **build, not a lookup**. A JVM starts, every `BUILD` file is evaluated, and under bzlmod
  repository rules run, which is arbitrary code executing during planning.
- The answer **depends on the machine**: bazel version, `.bazelrc`, `--config`, the module
  lockfile.

**It never skips.** bazel missing, bazel failing, or output it cannot parse is an error and the
expansion fails. A graph that skipped cleanly when bazel was absent would compute one set on CI
and a different one on a laptop, with nothing saying which happened.

## Which to use

**Prefer `bazel.Packages()` and fan out over everything.** It is close to what a Bazel repository
wants anyway, since bazel does its own incrementality per invocation: running `bazel test` on a
package bazel already has cached costs almost nothing.

Reach for `bazel.Query()` when the packages themselves are expensive to even start (a container
build per package, a deploy per package) and the planning-time cost is worth paying.

## Where to go next

- **[Affected units](/docs/monorepo/affected/)**: what `Affected` narrows.
- **[Fan-out](/docs/monorepo/fan-out/)**: `MaxParallel` and `MaxNodes` for a large workspace.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
