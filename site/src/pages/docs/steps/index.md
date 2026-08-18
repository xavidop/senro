---
layout: ../../../layouts/DocsLayout.astro
title: Steps
---

# Steps

A step is one action with an id: a command, or a registered Go function. Steps are what a pipeline
is made of, and every page in this section configures one.

```go
p := senro.New("ci")

verify := p.Workflow("verify")
verify.Step("test", exec.Command("go", "test", "./...")).
	Timeout(5 * time.Minute)

plan, err := p.Build()
```

`Workflow(name, opts...)` adds a named group and returns a `*senro.WorkflowBuilder`.
`Step(id, action)` adds a step to it and returns a `*senro.StepBuilder`. Every builder method
returns the same builder, so calls chain. `Build()` resolves the whole pipeline into a validated,
immutable `Plan`; see [Concepts](/docs/concepts/) for why that boundary exists.

**Step ids are unique across the whole pipeline**, not per workflow, because a plan is flat.
`Build()` refuses a duplicate, and refuses a step with no action.

## The two step kinds

| Kind | What it runs |
|---|---|
| `exec.Command(name, args...)` | A command, exactly as given |
| `senro.Func(name, params)` | A Go function registered under `name`. See [Func steps](/docs/steps/functions/) |

Both kinds are built, scheduled, retried, cached and handled by exactly the same code, so reach for
`senro.Func` whenever the work is "call this Go function", not "shell out to a program".

### `exec.Command` interprets no shell

**Nothing is shell-interpreted.** No pipes, no globs, no variable expansion. Ask for a shell
explicitly when you want one:

```go
verify.Step("test", exec.Command("sh", "-c", "go test ./... | tee test.log"))
```

## What you can configure

| On `*senro.StepBuilder` | Page |
|---|---|
| `Needs` | [Ordering](/docs/steps/ordering/) |
| `Env`, `WorkDir`, `Timeout`, `ContinueOnError` | [Step settings](/docs/steps/settings/) |
| `Retry`, `RetryPolicy` | [Retries](/docs/steps/retries/) |
| `OnFailure`, `Always` | [Handlers](/docs/steps/handlers/) |
| `When` | [Conditions](/docs/steps/conditions/) |
| `Mount`, `NoSnapshot` | [Workspaces](/docs/data/workspaces/) |
| `Pure`, `Inputs`, `Outputs`, `CacheEnv` | [Caching a step](/docs/data/caching/) |
| `SecretEnv` | [Secrets](/docs/secrets/) |

**Where a step runs is a property of its workflow, not of the step**: `senro.On(...)` on the
`Workflow` call picks the executor for every step in it. See [Executors](/docs/executors/).

A workflow can also generate one step per unit your repository already has, a directory per app or
a file per service, with `(*WorkflowBuilder).Expand`. See [Fan-out](/docs/monorepo/fan-out/).

## Where to go next

- **[Ordering](/docs/steps/ordering/)**: the two `Needs`, and the graph they build.
- **[Step states](/docs/steps/states/)**: the ten states a step can end in.
- **[Executors](/docs/executors/)**: the four places a step can run.
- **[Reading a failed run](/docs/reference/debugging/)**: where the evidence is.
