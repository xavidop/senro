---
layout: ../../../../layouts/DocsLayout.astro
title: glob
---

# `glob`

One unit per directory matching a pattern. The simplest graph there is, and the one to reach for
when your repository has a convention but no manifest describing it.

```go
import "github.com/xavidop/senro/unit/glob"

verify.Expand("lint", glob.Dirs("apps/*")).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("make", "lint")).WorkDir(u.Dir)
	})
```

## The two constructors

| | |
|---|---|
| `glob.Dirs(pattern)` | One unit per matching **directory**. |
| `glob.Files(pattern)` | One unit per directory that **contains** a matching file. |

```go
glob.Dirs("apps/*")                  // apps/web, apps/api, apps/admin
glob.Files("services/*/go.mod")      // one unit per service that has a go.mod
glob.Files("**/Dockerfile")          // one unit per directory holding a Dockerfile
```

Two matching files in one directory still produce **one** unit.

Patterns use senro's standard [path pattern syntax](/docs/data/workspaces/#pattern-syntax): `*`
and `?` match within a path segment, `**` spans segments.

## What a unit looks like

`ID`, `Name` and `Dir` are all the slash-separated path relative to the root:

```go
senro.Unit{ID: "apps/web", Name: "apps/web", Dir: "apps/web"}
```

So `u.Base()` is `"web"`, which is usually what a deployment step wants to name.

## It cannot narrow to affected units

A path does not say what it imports, so `glob` knows nothing about which unit depends on which.
`Affected` over it is **refused at build time**:

```
senro: expansion "lint": unit: glob dirs apps/* cannot compute an affected set: it discovers
units but knows nothing about which unit depends on which.
```

That is the honest answer, not a limitation being worked around. If you want a narrowed run, fan
out over a graph that reads your ecosystem's manifests: [`gowork`](/docs/monorepo/unit-graphs/gowork/),
[`cargo`](/docs/monorepo/unit-graphs/cargo/), [`jswork`](/docs/monorepo/unit-graphs/jswork/),
[`maven`](/docs/monorepo/unit-graphs/maven/) or [`gradle`](/docs/monorepo/unit-graphs/gradle/).

Fanning out over everything is still worth having: twenty apps linting in parallel with a
`MaxParallel` cap beats one `for` loop in a shell script.

## When it is the right choice

- Your repository's layout is a convention (`apps/*`, `services/*`) with no manifest naming the
  members.
- Every unit is genuinely independent, so there is nothing to narrow anyway.
- You are mixing ecosystems and just want "one step per directory that has a `Makefile`".

## Where to go next

- **[Fan-out](/docs/monorepo/fan-out/)**: `Template`, `MaxParallel`, `MaxNodes` and per-unit edges.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
- **[Write your own](/docs/monorepo/unit-graphs/custom/)**: when your layout is described by a file
  only you have.
