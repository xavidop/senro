---
layout: ../../../../layouts/DocsLayout.astro
title: gradle
---

# `gradle`: Java

One unit per Gradle project. **Computes an affected set.** Nothing is run: no Gradle, no daemon,
no JDK.

```go
import (
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/unit/gradle"
)

verify.Expand("test", gradle.Projects()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("./gradlew", u.Name+":test"))
	})
```

| Field | For `:libs:core` |
|---|---|
| `Name` | `:libs:core`, the project path, which is what `./gradlew :libs:core:test` takes |
| `ID`, `Dir` | The project directory, which a `settings.gradle` can put anywhere |

## It reads the declarative subset of your build

`settings.gradle(.kts)` and `build.gradle(.kts)` are programs, not data. Most of them are a list
of literal `include` calls and literal `project(":libs:core")` dependencies, and the graph reads
exactly that, in both Groovy and the Kotlin DSL.

**When a build computes something instead of naming it, the graph refuses rather than guesses.**
The error wraps `gradle.ErrNotDeclarative` and says which file it stopped at, so `errors.Is` can
tell that case apart from a broken build:

- A build script that **computes the project a dependency points at** makes the affected set
  refuse. There is no one project to attribute the edge to, and the only remaining reading is
  "every project depends on every project", which is this feature switched off while still looking
  switched on.
- A `settings.gradle` with more includes than the bound is treated as **generated**, and refused
  for the same reason.
- An `include` that names no project, a `projectDir` reassignment for a project no `include`
  creates, a project outside the root, or two projects sharing one directory are each an error
  naming what it found.

Reading only the literal parts of a generated build would produce a plausible **short** project
list, and a short list is an affected set that skips the project a change broke. Refusing is
recoverable; a plausible wrong answer is not.

The message tells you the way out: **fan out over these units without `Affected` and run every
one.** Discovery still works; only the narrowing is refused.

## Worth knowing

**Type-safe project accessors are resolved**, so `implementation(projects.libs.core)` draws the
same edge `project(":libs:core")` does.

**A project dir override moves the unit.** `project(':app').projectDir = file('src/app')` puts the
unit at `src/app`, and the graph follows it.

**A container project runs the projects under it.** A change to `:libs` runs `:libs:core` and its
siblings.

**A root file affects everything**: `build.gradle`, `gradle.properties`, the version catalog.

## Where to go next

- **[Affected units](/docs/monorepo/affected/)**: what `Affected` narrows.
- **[Sharding](/docs/monorepo/partition/)**: splitting a large project list across runners.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
