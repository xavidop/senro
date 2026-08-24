---
layout: ../../../layouts/DocsLayout.astro
title: Conditions
---

# Conditions

`When` prunes part of the graph at run start, so a pull-request run and a main-branch run can share
one pipeline.

```go
deploy := p.Workflow("deploy",
	senro.Needs("build"),
	senro.When(senro.Branch("main")))
deploy.Step("apply", exec.Command("sh", "-c", "make deploy"))

senro.Run(ctx, p, senro.WithParams(senro.Params{"branch": currentBranch}))
```

`When` exists at three levels, all taking the same `Condition`:

| Call | Gates |
|---|---|
| `senro.When(cond)`, passed to `Workflow` | Every step of the workflow |
| `(*StepBuilder).When(cond)` | That one step |
| `(*ExpandBuilder).When(cond)` | Every child of an expansion |

**A condition is evaluated once, at the start of the run**, against facts the run already has, never
against anything a step produces.

## The three conditions

- **`senro.Branch(name)`**: true when the run's `"branch"` parameter equals `name`.
- **`senro.ParamIs(name, value)`**: true when the named run parameter equals `value`. `Branch` is
  this with the parameter fixed to `"branch"`.
- **`senro.EnvIs(name, value)`**: true when an environment variable equals `value`, read from **the
  coordinator's own process**, not the step's. Conditions run before any sandbox exists.

Parameters come from `senro.WithParams`; see [Run options and outcomes](/docs/run/options/),
or [Triggers](/docs/triggers/) for where `branch` comes from when an event started the run.

**There is deliberately no `And`, `Or` or `Not`.** Calling `When` more than once, at any level or
mix of levels, already means AND.

## senro does not read `git` for you

`currentBranch` above is whatever your pipeline binary decided, read from `git` or from CI's
environment. senro deliberately does not shell out to `git` itself: a plan depending on ambient
repository state would behave differently in a container or a detached checkout.

## A gated step is skipped, not failed

A step whose conditions are not all true settles as `skipped_condition`, and its dependents settle
the same way, cascading transitively.

The difference from a failure is the run's result:

- A run made entirely of `skipped_condition` steps still reports `succeeded`, which leaves a pull
  request's run green when its `Branch("main")`-gated deploy does not fire.
- Dependents of an actually failed step get `skipped_upstream_failed` instead, which makes the run
  `partial` or `failed`.
- **`ContinueOnError` does not rescue a `skipped_condition` dependent.** It promises a dependent
  survives a failure, not that it runs against output that was never produced.

See [Step states](/docs/steps/states/) for the whole taxonomy.

## Where to go next

- **[Step states](/docs/steps/states/)**: `skipped_condition` and how it propagates.
- **[Fan-out](/docs/monorepo/fan-out/)**: `When` on a whole expansion.
- **[Run options and outcomes](/docs/run/options/)**: `senro.WithParams`, which `Branch`
  and `ParamIs` read.
- **[Triggers](/docs/triggers/)**: run parameters an event supplies.
