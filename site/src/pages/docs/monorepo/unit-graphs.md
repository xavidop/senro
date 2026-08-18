---
layout: ../../../layouts/DocsLayout.astro
title: The shipped unit graphs
---

# The shipped unit graphs

A unit graph is the second argument to [`Expand`](/docs/monorepo/fan-out/): it discovers units and,
optionally, answers who depends on whom, which is what [`Affected`](/docs/monorepo/affected/) needs.
Eight ship under `github.com/xavidop/senro/unit`.

| Graph | Units | Affected set? |
|---|---|---|
| [`unit/glob`](#glob) | Directories matching a pattern | No, and says so |
| [`unit/gowork`](#gowork-go) | Go modules or packages | Yes |
| [`unit/cargo`](#cargo-rust) | Rust crates | Yes |
| [`unit/jswork`](#jswork-javascript) | npm, pnpm, Yarn and Bun workspace packages | Yes |
| [`unit/maven`](#maven-java) | Maven reactor projects | Yes |
| [`unit/gradle`](#gradle-java) | Gradle projects | Yes |
| [`unit/pyproject`](#pyproject-python) | Python distributions | No, deliberately |
| [`unit/bazel`](#bazel) | Bazel packages | No, deliberately |

Whether a graph computes an affected set comes down to one question: **can a unit depend on another
without saying so anywhere the graph can read?** If yes, the graph declines, and `Affected` over it
is refused at build time rather than quietly running everything.

Every graph except `gowork` reads manifests or the tree without running the ecosystem's own tool,
so each works on a machine with no cargo, node, mvn, gradle, JDK or bazel installed.

## `glob`

[`glob.Dirs` and `glob.Files`](/docs/monorepo/fan-out/#discovering-units-with-glob) match paths, and
a path does not say what it imports. `Affected` over a glob graph is **refused at build time**:

```
senro: expansion "lint": unit: glob dirs apps/* cannot compute an affected set: it discovers
units but knows nothing about which unit depends on which. Fan out over a graph that knows which
unit depends on which (gowork, cargo, jswork, maven or gradle, under
github.com/xavidop/senro/unit), or drop Affected and run every unit
```

> Quietly running every unit would look friendlier and be wrong: an expansion that covered
> everything looks exactly like one that computed a real answer, and a CI that cannot tell them
> apart will eventually trust a green build that skipped the unit a change broke.

## `gowork`: Go

Asks the Go toolchain. **Computes an affected set.**

- **`gowork.Modules()`**: one unit per `go.mod`. `Name` is the module path, `Dir` and `ID` the
  directory relative to the root. Usually right for separately released services.
- **`gowork.Packages()`**: one unit per Go package, finer grained. `Name` is the import path.
- Both come out of **one** listing, so they can never disagree: `Modules` is `Packages` collapsed
  onto module directories, edges included.
- It needs a `go` binary on `PATH`, and it honours its context: a large listing takes seconds and
  is exactly the call somebody cancels.
- A tree with no `go.mod` anywhere is an **error**, not an empty graph. An expansion that silently
  produced no steps looks exactly like one whose pattern was wrong.
- `go.work` is not required; module discovery is a walk for `go.mod` files.

Under the hood it runs `go list -deps -test -e -json` once per module found under the root, in that
module's own directory. `-deps` reaches a package in another workspace module; `-test` because a
test-only import is still an import, and a graph missing it would disconnect the packages only a
test connects. `TestImports` and `XTestImports` are read too.

## `cargo`: Rust

**Computes an affected set.** `cargo.Crates()`. `Name` is the `[package]` name, which is what
`cargo test -p <name>` takes.

- A crate is every `Cargo.toml` with a `[package]` name, minus what a `[workspace]` table
  `exclude`s. Not the `members` list: a path dependency inside the workspace becomes a member
  whether or not `members` names it.
- Edges come from `[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`, and each under a
  `[target.<cfg>]` table. The cfg expression is not evaluated; the graph cannot know the target.
- A dependency resolves by its `path` (for `dep.workspace = true`, the path in the root's
  `[workspace.dependencies]`) or by its crate name matching a crate in the tree. The second covers
  `[patch]` and `[replace]` redirecting a registry dependency at a local crate.

> `cargo metadata --no-deps` would be Cargo's own answer, but it needs cargo installed. Reading the
> manifests is the trade this graph makes deliberately.

## `jswork`: JavaScript

**Computes an affected set.** `jswork.Packages()`. One graph for npm, pnpm, Yarn (classic and
Berry) and Bun.

- Only **discovery** differs by manager. Members come from the root `package.json`'s `"workspaces"`
  (an array, or Yarn v1's object with `"packages"`) **and** from `pnpm-workspace.yaml`'s `packages`
  list, unioned: a pnpm root usually has no `"workspaces"`. A `!` pattern excludes, from either.
- The **dependency graph does not differ by manager**: an edge is one `package.json` naming another
  package's `"name"` in `dependencies`, `devDependencies`, `peerDependencies` or
  `optionalDependencies`, at any version range (`workspace:*`, `^1.2.3` and `*` all draw the edge).
  The edge is on the package **name**, not the directory name.
- The lockfiles are not read: four formats, one binary, and none says anything about the workspace
  graph the manifests do not.

> **The honest limit**: this is the *declared* graph. Nothing parses JavaScript, so an undeclared
> import gets no edge; that is the same hole turbo, nx, lerna and `pnpm --filter` have. If your
> repository hoists with npm or Yarn and relies on undeclared imports, run every unit instead.
> TypeScript project references are a second route this graph does not read.

## `maven`: Java

**Computes an affected set.** `maven.Modules()`. `Name` is `groupId:artifactId`, which is what
`mvn -pl <name>` takes.

- A unit is one reactor project: the root `pom.xml` plus the transitive closure of `<modules>`,
  profiles included, since the graph cannot know which profiles a build activates.
- Aggregators are units too. Leaving them out would lose the edge that makes a change to a parent
  pom run its children.
- Edges come from `<dependencies>` at every scope, from `<parent>` and `<modules>`, from a
  `<scope>import</scope>` BOM, and from plugins built inside the reactor. Coordinates are
  interpolated against the pom's properties, its parents' and the `${project.*}` built-ins.
- A `<dependencyManagement>` entry is deliberately **not** a dependency unless it is an imported
  BOM. Treating managed versions as dependencies would make every change run everything, the
  feature switched off while looking switched on.

## `gradle`: Java

**Computes an affected set.** `gradle.Projects()`. `Name` is the project path, `:libs:core`, which
is what `./gradlew :libs:core:test` takes; `ID` and `Dir` are the project directory, which a
`settings.gradle` can put anywhere. Nothing is run: no Gradle, no daemon, no JDK needed.

`settings.gradle(.kts)` is a program, not data, but most settings files are a list of literal
`include` calls. The graph reads that declarative subset exactly and **refuses the rest**, with an
error naming the line it stopped at.

> The refusal is the design. Reading only the literal includes of a generated settings file would
> produce a plausible short project list, and a short list is an affected set that skips the
> project a change broke. Refusing is recoverable; a plausible wrong answer is not.

## `pyproject`: Python

**Does not compute an affected set, deliberately.** `pyproject.Packages()`.

A unit is one distribution: a directory whose `pyproject.toml` names one (PEP 621's `[project]` or
Poetry's `[tool.poetry]`), or which holds a `setup.py` or `setup.cfg`. A nameless `pyproject.toml`
(a virtual uv root, a ruff-only config) is not a unit. `[tool.uv.workspace] exclude` is honoured.

`Affected` is **refused**, like over `glob`. The manifests do carry dependency declarations, but in
Python a declaration is not what makes an import work:

- `uv sync` and `pip install -e` make every workspace member importable, so a package can
  `import acme_core` with nothing in its own `pyproject.toml`, and it works for years undetected.
- A src layout on `PYTHONPATH`, a `conftest.py`, an entry-point pytest plugin and an import inside
  a function are four more real dependencies with nothing static to read.
- `dynamic = ["dependencies"]` says outright the dependencies are not in the manifest.
- uv, Poetry, Hatch and setuptools disagree about where an intra-repo dependency is even written.

Any of those is a missing edge, and a missing edge is a green build for a broken tree. Fan out and
run every unit, still worth having on twenty services. If your repository genuinely declares and
enforces every dependency, [write a graph that knows it](/docs/extend/unit-graph/).

## `bazel`

**Does not compute an affected set, deliberately.** `bazel.Packages()`.

- A unit is one **Bazel package**: a directory holding a `BUILD` or `BUILD.bazel` file. `Name` is
  the package label (`//apps/web`, `//` for the root); `ID` and `Dir` are the directory.
- Discovery is a pruned walk plus one small file read. **No bazel is needed and none is run**; a
  test empties `PATH` and checks exactly that.
- Left out, because Bazel itself would refuse to build them from this root: directories in
  `.bazelignore`, directories inside a nested repository (their own `MODULE.bazel`, `REPO.bazel` or
  `WORKSPACE`), and directories with no `BUILD` file. A root that is not a workspace root, or a
  workspace with no `BUILD` file anywhere, is an error rather than an empty graph.
- Not one unit per **target**: a macro computes its targets' names, so enumerating them means
  evaluating Starlark or running bazel. `bazel test //apps/web/...` covers the package anyway.

`Affected` is refused for two compounding reasons:

- **`bazel query` was rejected.** It answers exactly, but it is effectively a build: a JVM server,
  every `BUILD` file evaluated, and under bzlmod archives fetched and repository rules run during
  planning. The answer would also depend on the machine (`.bazelrc`, `--config`, bazel present or
  not), and could not be tested against a checked-in fixture, senro's CI included.
- **Parsing `BUILD` files was rejected.** A `BUILD` file is a Starlark program: macros compute their
  own deps, `glob()`, `select()` and comprehensions build them dynamically, a dep can be a variable
  or an alias, and edges also come from toolchains, implicit rule dependencies and the `.bzl` files
  themselves. Each is a missing edge, and a missing edge is a green build for a tree that does not
  build.

So the fan-out runs every unit, close to what a Bazel repository wants anyway, since bazel does its
own incrementality per invocation. If you want bazel's own query to drive the fan-out, that is a
graph of about forty lines in your own repository: [write it](/docs/extend/unit-graph/).

## Why declining is cheap

`senro.UnitGraph` is published, and growing it breaks every outside implementation. So the two
affected-set questions, "which unit owns this file" and "who depends on this unit", live on a second
optional interface, `senro.UnitAffector`, which a hand-rolled graph need not implement to compile.

`Expand` asks for the capability and reports its absence as the build error quoted under
[`glob`](#glob). The cost is that a slightly wrong method signature is caught at plan time rather
than compile time, the same trade every optional interface in the standard library makes.

## Where to go next

- **[Implement a unit graph](/docs/extend/unit-graph/)**: both interfaces, with a worked example.
- **[Running only what changed](/docs/monorepo/affected/)**: what the affected set is computed from.
