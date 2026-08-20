---
layout: ../../../layouts/DocsLayout.astro
title: Monorepos
---

# Monorepos

One repository, many units: packages, modules, crates, services. senro has four tools for that
shape, and each one solves a different problem. This page says which one you want.

```go
import (
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/unit/gowork"
)

verify := p.Workflow("verify")
verify.Expand("test", gowork.Modules()).
	Affected(change.FromTrigger(ev)).
	MaxParallel(4).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
	})
```

One step per Go module, narrowed to the modules this run's change actually reaches, four at a time.

## The four tools

| Tool | The problem it solves |
|---|---|
| [`Expand`](/docs/monorepo/fan-out/) | Writing one `Step` per package stopped scaling. One step per unit, discovered from the tree. |
| [`Affected`](/docs/monorepo/affected/) | Every change runs every unit. Run only the units a change reaches, plus everything depending on them. |
| [`NeedsEach`](/docs/monorepo/needs-each/) | One slow module holds up every other module's tests. One edge per unit instead of a barrier. |
| [`Partition`](/docs/monorepo/partition/) | Fifty units and eight machines. Fewer steps than units, balanced by how long each one took last time. |

They compose. A single expansion can be affected-narrowed, ordered per unit against another
expansion, and partitioned into shards.

## Which one is your problem?

- **"I keep adding the same step for each new package."** Start with
  [`Expand`](/docs/monorepo/fan-out/) and stop there. It is useful on its own.
- **"CI takes twenty minutes to tell me a docs typo is fine."**
  [`Affected`](/docs/monorepo/affected/), over a graph that reads your ecosystem's manifests.
- **"The fan-out is fast but the stage after it waits for the slowest child."**
  [`NeedsEach`](/docs/monorepo/needs-each/).
- **"Fifty tiny steps, each paying a container pull."**
  [`Partition`](/docs/monorepo/partition/).

## What a unit is

A **unit graph** discovers units and, optionally, answers who depends on whom. Eight ship with
senro; five of them can compute an affected set, and the other three refuse rather than guess. The
table, and the trap for each ecosystem, are in
[The shipped unit graphs](/docs/monorepo/unit-graphs/).

If no shipped graph fits your layout, a graph is a type in your own module with the right methods:
see [Implement a unit graph](/docs/monorepo/unit-graphs/custom/).

## Where to go next

- **[Fan out with `Expand`](/docs/monorepo/fan-out/)**: `Template`, child ids, `MaxParallel`,
  `MaxNodes`.
- **[Per-unit edges](/docs/monorepo/needs-each/)**: `NeedsEach` and how it differs from a barrier.
- **[Partition](/docs/monorepo/partition/)**: shards and the duration history.
- **[Running only what changed](/docs/monorepo/affected/)**: `Affected` and the `change` package.
- **[Triggers](/docs/triggers/)**: where the change a run consumes comes from.
