---
layout: ../../../layouts/DocsLayout.astro
title: Fan out with Expand
---

# Fan out with `Expand`

`(*WorkflowBuilder).Expand(id, graph)` adds one step per unit a graph discovers. Adding a new app
under `apps/` becomes a new step nobody had to write.

```go
import (
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/unit/glob"
)

verify := p.Workflow("verify")
verify.Expand("lint", glob.Dirs("apps/*")).
	MaxParallel(4).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("pnpm", "--filter", u.Name, "lint")).
			Pure().Inputs(u.Sources()...)
	})
```

`Expand` walks the repository once and builds the same steps that one `verify.Step(...)` call per
directory would have.

## Discovering units with `glob`

The graph decides what a unit is. `github.com/xavidop/senro/unit/glob` is the simplest one: it
matches paths.

- **`glob.Dirs(pattern)`**: one unit per matching **directory**.
- **`glob.Files(pattern)`**: one unit per directory that **contains** a matching file, so
  `glob.Files("services/*/go.mod")` is one unit per service with a `go.mod`. Two matches in one
  directory still produce one unit.
- Patterns use senro's standard [path pattern syntax](/docs/data/workspaces/#pattern-syntax).
- `ID` and `Name` are both the slash-separated path relative to the root.

A path does not say what it imports, so a glob expansion always covers every unit. To run only what
a change affects, fan out over a graph that reads the ecosystem's manifests instead. All eight
shipped graphs are listed in [The shipped unit graphs](/docs/monorepo/unit-graphs/).

## What a `Template` receives

`Template` is called once per unit, in deterministic sorted order. It must return a **fresh**
`*senro.StepBuilder` each call, built with `senro.NewStep`:

- Not `Workflow.Step`, which would attach the step to the workflow a second time.
- Not `senro.Handler`, which marks it as a failure or always handler.

| On `Unit` | What it is |
|---|---|
| `u.ID`, `u.Name` | The unit's stable identity. For `glob` both are the slash-separated path relative to the root. |
| `u.Dir` | The unit's directory, relative to the root. What a template passes to `WorkDir`. |
| `u.Base()` | The last path segment: `"web"` for `"apps/web"`. Usually what a deployment step names. |
| `u.Sources()` | Every file under the unit's directory, as `[]artifact.Selector`, ready for `.Inputs(...)` on a [`Pure()`](/docs/data/caching/) template. Declare narrower `Inputs` yourself if you need them. |

## Child ids are deterministic

Each child's id is built from the expansion id and the unit, `lint[unit=apps/web]`, never a name
the template chooses. A chosen name could not be guaranteed unique across units. Because ids come
from the sorted unit set alone, the same repository always builds the same children, and a re-run
reconstitutes exactly the same step graph.

## `MaxParallel` and `MaxNodes`

- **`.MaxParallel(n)`** bounds how many of *this expansion's* children run at once, on top of the
  run's overall limit. That overall limit is the number of CPUs on the coordinator machine, and
  there is no option to change it for the whole run.
- **`.MaxNodes(n)`** refuses an expansion wider than `n` (default 500) when the pipeline is
  **built**, naming the pattern and the count. A scheduler should not discover it has forty
  thousand sandboxes to hold open. `MaxNodes` is checked against the **whole** graph: neither
  [`Affected`](/docs/monorepo/affected/) nor [`Partition`](/docs/monorepo/partition/) is a way
  around it.

## `Needs`: order the whole expansion

`.Needs(ids ...string)` on an `ExpandBuilder` declares upstream steps that **every child** waits
for, the same dependency [`(*StepBuilder).Needs`](/docs/steps/ordering/) declares for one step.
`Expand("lint", ...).Needs("install")` orders the whole fan-out after the step producing what its
children read.

This is a **barrier**. When the downstream work is itself per unit, you want
[`NeedsEach`](/docs/monorepo/needs-each/) instead.

## Expansion happens once, at build time

`Expand` resolves when `Build()` runs, not mid-run: every unit is discovered and written into the
plan before the first step starts.

- A repository with three matching directories and one with four are two **different** pipelines,
  with two plan digests.
- Nothing a step produces can add a node to a graph already executing.

## What `Expand` doesn't do (yet)

- A step's own output producing new nodes mid-run, and `RunSubgraph`, are designed but **not
  built**.
- There is deliberately no `FailFast` for an expansion. senro reports every failing sibling
  individually rather than cancelling the rest of a fan-out when one child fails.

## Where to go next

- **[The shipped unit graphs](/docs/monorepo/unit-graphs/)**: what each graph calls a unit.
- **[Per-unit edges](/docs/monorepo/needs-each/)**: `NeedsEach`, so fast tests don't wait behind
  slow builds.
- **[Partition](/docs/monorepo/partition/)**: fewer steps than units, balanced by duration.
- **[Running only what changed](/docs/monorepo/affected/)**: `Affected`.
- **[Conditions](/docs/steps/conditions/)**: `(*ExpandBuilder).When`, which prunes children the
  plan still contains.
