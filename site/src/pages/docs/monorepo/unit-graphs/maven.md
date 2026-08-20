---
layout: ../../../../layouts/DocsLayout.astro
title: maven
---

# `maven`: Java

One unit per Maven reactor project. **Computes an affected set.**

```go
import (
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/unit/maven"
)

verify.Expand("test", maven.Modules()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("mvn", "-pl", u.Name, "test"))
	})
```

`Name` is `groupId:artifactId`, which is what `mvn -pl <name>` takes.

## What counts as a unit

The root `pom.xml` plus the transitive closure of its `<modules>`, **profiles included**, since
the graph cannot know which profiles a build will activate.

**Aggregators are units too.** Leaving them out would lose the edge that makes a change to a
parent pom run its children.

## Where the edges come from

- `<dependencies>` at every scope, `test` included.
- `<parent>` and `<modules>`.
- A `<scope>import</scope>` BOM.
- Plugins built inside the reactor.

Coordinates are interpolated against the pom's own properties, its parents', and the
`${project.*}` built-ins, so a version held in a property still resolves.

**A `<dependencyManagement>` entry is deliberately not a dependency** unless it is an imported
BOM. Treating managed versions as dependencies would make every change run everything: the feature
switched off while looking switched on.

## No Maven needed

The graph reads the poms. No `mvn`, no JDK, no repository download on the machine planning the run.

## Worth knowing

**The root pom affects everything**, which is usually what you want: it holds the versions
everything else inherits.

**A file under no module belongs to the root project**, so a change to a top-level script or
config runs the reactor.

## Where to go next

- **[Affected units](/docs/monorepo/affected/)**: what `Affected` narrows.
- **[Sharding](/docs/monorepo/partition/)**: splitting a large reactor across runners.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
