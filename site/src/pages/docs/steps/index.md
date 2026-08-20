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

`exec.Command("go", "test", "./...")` runs the program `go` with two arguments, exactly as
written. There is no shell in between, so **none of the things a shell does happen**:

| You write | What a shell would do | What senro does |
|---|---|---|
| `exec.Command("ls", "*.go")` | Expand `*.go` to your files | Passes the literal `*.go` to `ls` |
| `exec.Command("echo", "$HOME")` | Substitute your home directory | Passes the literal `$HOME` |
| `exec.Command("go build && ls")` | Run two programs in sequence | Looks for one program whose whole name is `go build && ls` |
| `exec.Command("cd", "web")` | Change directory | Runs `/bin/cd`, which changes nothing |

Each has a direct replacement:

- **Globs, pipes, redirection, `&&`**: ask for a shell in so many words.

  ```go
  verify.Step("test", exec.Command("sh", "-c", "go test ./... | tee test.log"))
  ```

- **Environment variables**: declare them with `Env`, which the step sees for real.

  ```go
  verify.Step("test", exec.Command("go", "test", "./...")).Env("CGO_ENABLED", "0")
  ```

- **Changing directory**: use `WorkDir`.

  ```go
  verify.Step("build", exec.Command("pnpm", "build")).WorkDir("./web")
  ```

Both are on [Env, dir & timeout](/docs/steps/settings/).

> This is a feature, not a restriction to work around. An argument that is never re-parsed cannot
> be split on a space you did not expect, and a filename with a space in it is just a filename.

## What you can configure

Everything below is a method on the `*senro.StepBuilder` that `Step(...)` returned.

| I want to... | Call | Page |
|---|---|---|
| Run this after another step | `Needs` | [Ordering](/docs/steps/ordering/) |
| Set env vars, a directory, a time limit | `Env`, `WorkDir`, `Timeout` | [Env, dir & timeout](/docs/steps/settings/) |
| Let dependents run even if this fails | `ContinueOnError` | [Env, dir & timeout](/docs/steps/settings/) |
| Try again when it breaks | `Retry`, `RetryPolicy` | [Retries](/docs/steps/retries/) |
| Clean up or collect logs afterwards | `OnFailure`, `Always` | [Failure handlers](/docs/steps/handlers/) |
| Skip it unless something is true | `When` | [Conditions](/docs/steps/conditions/) |
| Give it files, and keep what it wrote | `Mount`, `NoSnapshot` | [Workspaces](/docs/data/workspaces/) |
| Skip it when nothing changed | `Pure`, `Inputs`, `Outputs`, `CacheEnv` | [Caching a step](/docs/data/caching/) |
| Give it a credential | `SecretEnv` | [Secrets](/docs/secrets/) |

## Two things that are not step settings

**Where a step runs belongs to its workflow.** `senro.On(...)` on the `Workflow` call picks the
executor for every step in it, so a step never carries one of its own:

```go
remote := p.Workflow("remote", senro.On(ssh.Host("build@ci-1")))
remote.Step("build", exec.Command("make", "build"))   // runs on ci-1
```

See [Executors](/docs/executors/).

**Generating steps instead of writing them** is a workflow-level call too. If your repository has
many apps, modules or packages and you want one step each, that is
[Monorepos](/docs/monorepo/), not a step setting.

## Where to go next

- **[Ordering](/docs/steps/ordering/)**: the two `Needs`, and the graph they build.
- **[Step states](/docs/steps/states/)**: the ten states a step can end in.
- **[Executors](/docs/executors/)**: the four places a step can run.
- **[Reading a failed run](/docs/reference/debugging/)**: where the evidence is.
