---
layout: ../../../../layouts/DocsLayout.astro
title: jswork
---

# `jswork`: JavaScript

One unit per workspace package, across npm, pnpm, Yarn (classic and Berry) and Bun. **Computes an
affected set.**

```go
import (
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/unit/jswork"
)

verify.Expand("test", jswork.Packages()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("pnpm", "--filter", u.Name, "test"))
	})
```

`Name` is the package's `"name"` from its `package.json`, which is what `--filter` takes.

## One graph, four package managers

Only **discovery** differs by manager, and senro reads both places at once:

- the root `package.json`'s `"workspaces"` (an array, or Yarn v1's object with `"packages"`);
- `pnpm-workspace.yaml`'s `packages` list.

The two are unioned, because a pnpm root usually has no `"workspaces"` key at all. A `!` pattern
excludes, from either.

**The dependency graph does not differ by manager.** An edge is one `package.json` naming another
package's `"name"` in `dependencies`, `devDependencies`, `peerDependencies` or
`optionalDependencies`. Any version range draws the edge: `workspace:*`, `^1.2.3` and `*` alike.

> The edge is on the package **name**, not the directory name. A package at `libs/core` named
> `@acme/core` is depended on as `@acme/core`.

**Lockfiles are not read.** Four formats, one binary, and none of them says anything about the
workspace graph the manifests do not.

## The honest limit

This is the **declared** graph. Nothing parses JavaScript, so an undeclared import gets no edge.

That is the same hole turbo, nx, lerna and `pnpm --filter` have, so it is not a senro-specific
risk. But it is a real one: if your repository hoists with npm or Yarn and relies on undeclared
imports, the affected set will be wrong, and you should fan out over everything instead.

TypeScript project references are a second route this graph does not read.

## Worth knowing

**The root manifest and lockfile affect everything.**

**A change to an excluded package affects everything**, because senro cannot tell what depends on
something it does not track.

**Deleted files are answered from their path alone**, never by stat'ing disk. A deletion is
exactly the change whose dependents most need rebuilding, and the file is already gone by plan
time.

## Where to go next

- **[Affected units](/docs/monorepo/affected/)**: what `Affected` narrows.
- **[Fan-out](/docs/monorepo/fan-out/)**: `MaxParallel` for a workspace with a hundred packages.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
