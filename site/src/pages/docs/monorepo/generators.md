---
layout: ../../../layouts/DocsLayout.astro
title: Generate a subgraph
---

# Generate a subgraph

`Expand` fans out over a list senro can discover **before the run starts**, by globbing the tree. A
generator fans out over a list that only exists **once something has run**: the resources
`terraform plan` says actually changed, the clusters an API reports right now, the shards a timing
tool just computed.

Without one, that work goes in a loop inside a single step:

```sh
for c in $(list-clusters); do ./preflight "$c" && ./apply "$c"; done
```

senro cannot see into that loop. There is one step, so there is one cache entry, one log, one state
and one retry. Forty clusters deploy under one name, one failure ends the loop, and retrying means
retrying all of them.

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

Each generated step is an ordinary step: its own state, its own log, its own cache entry, its own
retry, scheduled under the run's `MaxParallel`. Retrying one cluster retries one cluster.

## Ids are hierarchical

A fragment names its steps **relatively**, and senro prefixes each with the generator's id. The
fragment above produces `discover/preflight-cm4` and `discover/apply-cm4`.

That is what lets a fragment be written once without knowing where it sits, and it is why two
generators can both produce an `apply` without colliding.

## The boundary is what dependents wait for

A step that `Needs` the generator waits for the generator to **finish**, which is the moment the
generated work *starts*. `Boundary` says what "done" means:

```go
f.Boundary(applyStep)
```

Every existing dependent of the generator gains the boundary steps as dependencies. Declare no
boundary and those dependents wait only on the generator itself, which is the right answer when
nothing downstream consumes what it produced.

An empty fragment is legal. It means "nothing to do here", and the generator's dependents run
rather than being skipped.

## Any language can write one

The Go closure is one form. The other is a file, so a shell script, a Python tool or a Terraform
wrapper can produce a subgraph:

```go
l.Step("plan-infra", exec.Command("terraform", "plan", "-json", "-out=tf.plan")).
    Mount(ws.At("/src", senro.RW)).
    Generates(senro.GenerateFromJSON("fragment.json"))
```

The file is the public schema: plan nodes plus a boundary list.

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

Both forms take exactly the same validation, and a Go fragment is serialised to this schema before
the engine reads it, so the two cannot drift into accepting different things.

The path resolves against the same root a step's `Outputs` do, so a generator step has to mount a
workspace: a step with no workspace has nowhere senro can read what it produced.

## Your generator does not have to be deterministic

This is the part worth reading twice.

senro **records the fragment** when it is produced, into the content store, and puts its digest in
the generator's cache entry. A cached generator does not run at all: its subgraph is restored from
that recording.

So a generator may call an API, read a clock, or iterate a map. It is not reproducible, and it does
not have to be, because the run does not depend on it producing the same answer twice. It depends
on the record.

Workflow engines that build the graph from user code usually forbid exactly this, and make you
write deterministic code to get replay. senro records the answer instead.

Two consequences follow:

- A cache entry whose recorded fragment has been garbage collected is not a usable hit. senro
  re-runs the generator rather than serving a run with the work missing.
- `run.rerun_from` on a generator replays what it produced. The generated nodes are already in the
  graph, so re-running the generator re-runs them rather than creating a second set.

## Fragments only add

A fragment may add nodes, add edges among its own nodes, and attach its boundary to the
generator's existing dependents. It may not modify, remove or re-parent anything already in the
run, and its steps may only depend on its own steps.

That rule is what keeps a splice safe: every cache key already recorded and every attached client's
view of the run stay valid across it. A fragment that breaks any of it is refused **whole**, and
the generator step fails. Nothing is ever half-applied, because a half-applied fragment is a graph
no re-run could reproduce.

## Generators are bounded

A generated step can itself be a generator. Two limits keep that from being a fork bomb holding
your deploy credentials:

| Limit | Default | What it bounds |
| --- | --- | --- |
| `MaxDepth` | 3 | How deep generators may nest |
| `MaxNodes` | 5000 | Nodes in the whole run, the plan's own included |

Exceeding either fails the run and names the generator **chain** that did it, not just the last
step in it.

## When to reach for what

1. **`When`** if the work is already in the graph and you need to skip it.
2. **`Expand`** if the list can be discovered from the tree before the run.
3. **A generator** if the list only exists once a step has run.

Generators are the most powerful and the most expensive to reason about: the node count is not
known in advance, so senro shows counts of the nodes it knows rather than a completion percentage
it would have to revise downward.

## When a loop is not a graph

Some control flow genuinely cannot be drawn as a DAG: "roll out one cluster at a time until quorum,
then stop" depends on what the previous nodes did. For that, a registered function runs a graph
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

`RunSubgraph` blocks until the nested graph finishes and returns its failure, so a rollout that
could not be done fails the step that was doing it.

It is deliberately weaker than a generator, and the trade is worth saying out loud. A generator
DESCRIBES work and hands it to the scheduler, which is why its nodes are ordinary steps you can
cache and retry individually. A subgraph is work the FUNCTION is doing: it belongs to that step,
so the cache and re-run granularity is the whole subgraph and re-running means re-running the
step. You cannot `senro rerun --step` into the middle of one.

Reach for a generator whenever the work is a graph. Reach for this only when it genuinely is not.

A subgraph runs on the coordinator, so a func step running on a remote host cannot start one: it
is a staged binary on the far side of a transport, and the engine is back here. Called from there,
`RunSubgraph` says so by name.

## Where to go next

- **[Fan out with `Expand`](/docs/monorepo/fan-out/)**: the plan-time fan-out to prefer when it fits.
- **[Caching a step](/docs/data/caching/)**: what makes a generator cacheable, and what a recorded fragment costs.
- **[`senro rerun`](/docs/cli/rerun/)**: replaying a recorded subgraph, and `--regenerate`.
