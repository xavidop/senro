---
layout: ../../../layouts/DocsLayout.astro
title: Generate a subgraph
---

# Generate a subgraph

`Expand` fans out over a list senro can discover **before the run starts**, by globbing the tree.
A generator fans out over a list that only exists **after something has run**: the resources
`terraform plan` says changed, the clusters an API reports right now, the shards a timing tool
just computed.

Without one, that work goes in a loop inside a single step. In this example:

```sh
for c in $(list-clusters); do ./preflight "$c" && ./apply "$c"; done
```

senro can't see inside that loop. It's one step, so it gets one cache entry, one log, one state,
and one retry. All forty clusters deploy under one name. One failure ends the whole loop, and
retrying means retrying every cluster.

A generator turns the same work into real steps:

```go
l.Step("discover", exec.Command("./bin/list-clusters")).
    Mount(ws.At("/src", senro.RO)).
    Generates(senro.Generate(func(ctx senro.GenCtx) (*senro.Fragment, error) {
        var cs []Cluster
        if err := ctx.OutputJSON("clusters.json", &cs); err != nil {
            return nil, err
        }
        f := senro.NewFragment()
        for _, c := range cs {
            pre := f.Step("preflight-"+c.Name, exec.Command("./preflight", c.Name))
            f.Boundary(f.Step("apply-"+c.Name, exec.Command("./apply", c.Name)).Needs(pre.ID()))
        }
        return f, nil
    }))
```

Each generated step is an ordinary step. It gets its own state, log, cache entry, and retry,
scheduled under the run's `MaxParallel`. Retrying one cluster retries just that cluster.

### What's actually in a `Fragment`

A `Fragment` is nothing more than a list of steps to add, plus an optional boundary. It never talks
to the engine, the closure just builds one and hands it back:

- `senro.NewFragment()` starts empty.
- `f.Step(id, action)` adds one step to the fragment and returns a `*StepBuilder`, the same builder
  `l.Step` gives you, so `.Needs(...)`, `.Mount(...)`, and the rest work exactly the way they do on
  an ordinary step.
- `f.Boundary(...)` names which of the fragment's own steps count as "the generator is done" (see
  below).

senro takes it from there: it serializes the fragment, checks it, and splices its steps into the
run under the generator's id.

## Ids are hierarchical

A fragment names its steps **relatively**, and senro prefixes each one with the generator's id.
The fragment above produces `discover/preflight-west` and `discover/apply-west`.

That's what lets a fragment be written once, without knowing where it will sit in the graph. It's
also why two different generators can both produce an `apply` step without colliding: the prefix
makes the full id unique even when the relative name repeats.

```mermaid
flowchart TD
    subgraph discover["generator: discover"]
        direction TB
        a1["discover/preflight-west"]
        a2["discover/apply-west"]
    end
    subgraph rollout["generator: rollout"]
        direction TB
        b1["rollout/apply-west"]
    end
```

## The boundary is what dependents wait for

Say a `publish` step should run only after every cluster is deployed:

```go
discover := l.Step("discover", exec.Command("./bin/list-clusters")).
    Mount(ws.At("/src", senro.RO)).
    Generates(senro.Generate(func(ctx senro.GenCtx) (*senro.Fragment, error) {
        // same generator as before: reads clusters.json, builds preflight/apply steps
        ...
    }))

l.Step("publish", exec.Command("./notify-slack")).Needs(discover.ID())
```

Here's the trap: `discover` is the step that *runs the generator function*, and generating a
fragment is fast, it's just building a Go value. `discover` finishes the instant it hands that
fragment back to senro, which is the moment `apply-west` and the other generated steps **start**,
not the moment they're done. Left alone, `publish` (which only `Needs(discover.ID())`) fires while
clusters are still mid-deploy.

`Boundary` fixes this by redirecting `discover`'s existing dependents onto the fragment's real
finish line instead of onto `discover` itself. Inside the generator function, pass it the
`*StepBuilder` that `f.Step(...)` returned for each "actually done" step, `apply-west` in this
case:

```go
apply := f.Step("apply-"+c.Name, exec.Command("./apply", c.Name)).Needs(pre.ID())
f.Boundary(apply)
```

Now `publish` effectively waits on `discover/apply-west` (and every other step named in the
boundary), not on `discover`. Pass `Boundary` several steps if dependents should wait for more
than one to finish, which is exactly what the first example on this page does in one line:
`f.Boundary(f.Step("apply-"+c.Name, ...).Needs(pre.ID()))`.

```mermaid
flowchart LR
    subgraph noBoundary["no Boundary: publish fires too early"]
        direction TB
        d1["discover"] --> dep1["publish"]
    end
    subgraph withBoundary["Boundary(applyStep): publish fires on time"]
        direction TB
        d2["discover"] --> pre["preflight-west"] --> ap["apply-west"]
        ap --> dep2["publish"]
    end
```

Declaring no boundary is legal, and correct when nothing downstream consumes what the generator
produced, e.g. a generator that fans out cleanup work nobody waits on. In that case dependents wait
only on `discover` itself.

An empty fragment (`return senro.NewFragment(), nil`) is legal too. It means "nothing to do here,"
and the generator's dependents run immediately rather than being skipped.

## Any language can write one

The Go closure is one way to write a generator. The other doesn't involve Go at all: the step's
command writes a JSON file, and `GenerateFromJSON` just points senro at it.

**`fragment.json` is that file**, and nothing more exotic: the same `nodes` + `boundary` shape a
`Fragment` builds in Go, written to disk by whatever tool the step ran. That's what makes it work
from a shell script, a Python tool, or a wrapper around Terraform, none of which can return a Go
value:

```go
l.Step("plan-infra", exec.Command("./bin/plan-infra")).
    Mount(ws.At("/src", senro.RW)).
    Generates(senro.GenerateFromJSON("fragment.json"))
```

Three things have to line up for this to work:

1. `./bin/plan-infra` runs `terraform plan`, looks at what changed, and **writes `fragment.json`**
   into the step's own output directory as a side effect, the same directory `Mount` gave it.
2. The step succeeds (exit code 0). A failed step produces no fragment, Go or JSON, senro doesn't
   go looking.
3. senro reads `fragment.json` back, validates it, and splices it into the graph, exactly like it
   would a Go closure's return value.

A minimal `fragment.json` your script could write:

```json
{
  "version": 1,
  "nodes": [
    {"id": "apply-vpc", "kind": "exec", "cmd": ["terraform", "apply", "-target=vpc"]},
    {"id": "apply-db",  "kind": "exec", "cmd": ["terraform", "apply", "-target=db"],
     "needs": ["apply-vpc"]}
  ],
  "boundary": ["apply-db"]
}
```

Same rules as the Go form: `id`s are relative (senro prefixes them with `plan-infra/`), `needs`
can only name other nodes in this same file, and `boundary` lists which of those nodes
`plan-infra`'s dependents should actually wait for.

Both forms go through exactly the same validation, because a Go fragment is serialized to this
exact schema before the engine reads it. Nothing in senro's core reads Go value data directly,
it always reads this JSON shape, so writing it by hand is a first-class option, not a fallback.

The file path resolves against the step's own output root, the same one `Outputs` reads from.
That's why the step needs `Mount`: with no workspace mounted, there's no directory for senro to
look in for `fragment.json` once the step exits.

## Your generator doesn't have to be deterministic

When a generator produces a fragment, senro **records it** in the content store (the same
content-addressed store that holds cached step outputs) and saves its digest in the generator's
cache entry. A cached generator doesn't run again at all. Its subgraph
is restored from that recording.

So a generator can call an API, read a clock, or iterate a map. It doesn't need to be
reproducible, because a re-run doesn't depend on it producing the same answer twice. It depends
on the recording.

Many workflow engines forbid this, and require deterministic code so replay works. senro takes a
different approach: it records the answer instead of recomputing it.

Two consequences follow from that:

- If a cache entry's recorded fragment has been garbage collected, that cache hit is no longer
  usable. senro re-runs the generator instead of serving a run with the work missing.
- `run.rerun_from` on a generator replays what it produced. The generated nodes are already in the
  graph, so re-running the generator re-runs them instead of creating a second copy.

## Fragments only add

A fragment can add nodes, add edges between its own nodes, and attach its boundary to the
generator's existing dependents. It cannot modify, remove, or re-parent anything already in the
run, and its steps can only depend on its own steps.

That rule keeps the splice safe: every cache key already recorded, and every attached client's
view of the run, stays valid across it. If a fragment breaks any of this, senro refuses it
**entirely**, and the generator step fails. Nothing is ever half-applied, because a half-applied
fragment would be a graph no re-run could reproduce.

## Generators are bounded

A generated step can itself be a generator. Two limits stop that from turning into a fork bomb
holding your deploy credentials:

| Limit | Default | What it bounds |
| --- | --- | --- |
| `MaxDepth` | 3 | How deep generators may nest |
| `MaxNodes` | 5000 | Nodes in the whole run, the plan's own included |

Exceeding either limit fails the run, and names the whole generator **chain** responsible, not
just the last step in it.

## When to reach for what

1. **`When`** if the work is already in the graph and you need to skip it.
2. **`Expand`** if the list can be discovered from the tree before the run.
3. **A generator** if the list only exists once a step has run.

Generators are the most powerful option, and the hardest to reason about. Since the node count
isn't known in advance, senro shows counts of the nodes it knows about rather than a completion
percentage it would otherwise have to revise downward.

## When a loop is not a graph

Some control flow can't be drawn as a DAG at all: "roll out one cluster at a time until quorum,
then stop" depends on what earlier steps did. For that, a registered function can run a graph
itself:

```go
senro.RegisterFunc("deploy/rolling", func(ctx senro.Ctx, p RollParams) error {
    for _, c := range p.Clusters {
        f := senro.NewFragment()
        f.Step("apply-"+c.Name, exec.Command("./apply", c.Name))
        if err := senro.RunSubgraph(ctx, f); err != nil {
            return err
        }
        if quorumReached(ctx) {
            return nil
        }
    }
    return errNoQuorum
})
```

`RunSubgraph` blocks until the nested graph finishes, and returns its failure. So a rollout that
couldn't complete fails the step that was running it.

This is deliberately weaker than a generator, and it's worth understanding the trade-off. A
generator describes work and hands it to the scheduler, which is why its nodes are ordinary steps
you can cache and retry individually. A subgraph is work the function itself is doing: it belongs
to that one step, so the cache and re-run granularity is the whole subgraph, and re-running means
re-running the step. You can't use `senro rerun --step` to jump into the middle of one.

Use a generator whenever the work is a graph. Use this only when it genuinely isn't.

A subgraph runs on the coordinator. A func step running on a remote host can't start one, because
it's a staged binary on the far side of a transport, and the engine lives back on the coordinator.
If you call `RunSubgraph` from a remote step, it tells you so by name.

## Where to go next

- **[Fan out with `Expand`](/docs/monorepo/fan-out/)**: the plan-time fan-out to prefer when it fits.
- **[Caching a step](/docs/data/caching/)**: what makes a generator cacheable, and what a recorded fragment costs.
- **[`senro rerun`](/docs/cli/rerun/)**: replaying a recorded subgraph, and `--regenerate`.
