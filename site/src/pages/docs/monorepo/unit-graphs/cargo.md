---
layout: ../../../../layouts/DocsLayout.astro
title: cargo
---

# `cargo`: Rust

One unit per crate in a Cargo workspace. **Computes an affected set.**

```go
import (
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/unit/cargo"
)

verify.Expand("test", cargo.Crates()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("cargo", "test", "-p", u.Name))
	})
```

`Name` is the `[package]` name, which is exactly what `cargo test -p <name>` takes.

## What counts as a crate

Every `Cargo.toml` with a `[package]` name, minus what a `[workspace]` table `exclude`s.

**Not the `members` list**: a path dependency inside the workspace becomes a member whether or not
`members` names it, so senro finds it either way.

## Where the edges come from

`[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`, and each of those under a
`[target.<cfg>]` table. The cfg expression is not evaluated, because the graph cannot know the
target you will build for, so a target-specific dependency always draws its edge.

A dependency resolves either way round:

- by its `path` (and for `dep.workspace = true`, the path in the root's
  `[workspace.dependencies]`);
- by its **crate name** matching a crate in the tree, which covers `[patch]` and `[replace]`
  redirecting a registry dependency at a local crate.

## No cargo needed

The graph reads the manifests. `cargo metadata --no-deps` would be Cargo's own answer, but it
needs cargo installed on whatever machine is planning the run; reading the manifests is the trade
this graph makes deliberately.

## Worth knowing

**The workspace manifest and lockfile affect everything.** A change to the root `Cargo.toml` or
`Cargo.lock` runs every crate, because it can change what all of them compile against.

**A file no crate owns runs everything**, for the same reason.

## Where to go next

- **[Affected units](/docs/monorepo/affected/)**: what `Affected` narrows.
- **[Sharding](/docs/monorepo/partition/)**: splitting a large crate list across runners.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
