---
layout: ../../../layouts/DocsLayout.astro
title: Fan out with Expand
---

# Fan out with `Expand`

`(*WorkflowBuilder).Expand(id, graph)` adds one step per unit that a graph discovers. Add a new
app under `apps/`, and you get a new step for it automatically. Nobody has to write it.

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

`Expand` walks the repository once. It builds the same steps you'd get from calling
`verify.Step(...)` once per directory.

## Discovering units with `glob`

The graph decides what counts as a unit. `github.com/xavidop/senro/unit/glob` is the simplest
graph: it just matches paths.

- **`glob.Dirs(pattern)`**: one unit per matching **directory**.
- **`glob.Files(pattern)`**: one unit per directory that **contains** a matching file, so
  `glob.Files("services/*/go.mod")` is one unit per service with a `go.mod`. Two matches in one
  directory still produce one unit.
- Patterns use senro's standard [path pattern syntax](/docs/data/workspaces/#pattern-syntax).
- `ID` and `Name` are both the slash-separated path relative to the root.

A path alone doesn't say what it imports, so a glob expansion always covers every unit. To run
only what a change affects, fan out over a graph that reads the ecosystem's manifests instead. All
eight shipped graphs are listed in [The shipped unit graphs](/docs/monorepo/unit-graphs/).

## What a `Template` receives

`Template` is called once per unit, always in the same sorted order. Each call must return a
**fresh** `*senro.StepBuilder`, built with `senro.NewStep`:

- Not `Workflow.Step`: that would attach the step to the workflow a second time.
- Not `senro.Handler`: that marks it as a failure or always handler.

| On `Unit` | What it is |
|---|---|
| `u.ID`, `u.Name` | The unit's stable identity. For `glob` both are the slash-separated path relative to the root. |
| `u.Dir` | The unit's directory, relative to the root. What a template passes to `WorkDir`. |
| `u.Base()` | The last path segment: `"web"` for `"apps/web"`. Usually what a deployment step names. |
| `u.Sources()` | Every file under the unit's directory, as `[]artifact.Selector`, ready for `.Inputs(...)` on a [`Pure()`](/docs/data/caching/) template. Declare narrower `Inputs` yourself if you need them. |

## Child ids are deterministic

Each child's id comes from the expansion id and the unit, like `lint[unit=apps/web]`. It's never
a name the template picks, because a picked name couldn't be guaranteed unique across units. Since ids
come only from the sorted unit set, the same repository always builds the same children. A re-run
produces exactly the same step graph.

## `MaxParallel` and `MaxNodes`

- **`.MaxParallel(n)`** limits how many children of *this expansion* run at once, within the run's
  overall limit. That overall limit is the number of CPUs on the coordinator machine, and there's
  no option to change it for the whole run.
- **`.MaxNodes(n)`** rejects an expansion wider than `n` (default 500) at **build** time, and names
  the pattern and count that caused it. This stops a scheduler from discovering, mid-run, that it
  has forty thousand sandboxes to hold open. `MaxNodes` checks the **whole** graph, so neither
  [`Affected`](/docs/monorepo/affected/) nor [`Partition`](/docs/monorepo/partition/) can get
  around it.

## `Needs`: order the whole expansion

`.Needs(ids ...string)` on an `ExpandBuilder` declares upstream steps that **every child** waits
for. It's the same kind of dependency that [`(*StepBuilder).Needs`](/docs/steps/ordering/)
declares for a single step. `Expand("lint", ...).Needs("install")` makes the whole fan-out wait
for the step that produces what its children read.

This is a **barrier**. When the downstream work is itself per unit, you want
[`NeedsEach`](/docs/monorepo/needs-each/) instead.

## Expansion happens once, at build time

`Expand` resolves when `Build()` runs, not while the pipeline is running. Every unit is discovered
and written into the plan before the first step starts.

- A repository with three matching directories and one with four produce two **different**
  pipelines, with two different plan digests.
- Nothing a step produces can add a node to a graph that's already running.

## What `Expand` doesn't do (yet)

- If a step's own output needs to add new nodes mid-run, that's a **generator**, not an expansion:
  see [Generated subgraphs](/docs/monorepo/generators/). For control flow that isn't a graph at
  all, a function can run one directly with `senro.RunSubgraph`.
- There's no `FailFast` option for an expansion, by design. If one child fails, senro reports it
  and keeps running the rest of the fan-out instead of cancelling everything.

## Where to go next

- **[The shipped unit graphs](/docs/monorepo/unit-graphs/)**: what each graph calls a unit.
- **[Per-unit edges](/docs/monorepo/needs-each/)**: `NeedsEach`, so fast tests don't wait behind
  slow builds.
- **[Partition](/docs/monorepo/partition/)**: fewer steps than units, balanced by duration.
- **[Running only what changed](/docs/monorepo/affected/)**: `Affected`.
- **[Conditions](/docs/steps/conditions/)**: `(*ExpandBuilder).When`, which prunes children the
  plan still contains.
