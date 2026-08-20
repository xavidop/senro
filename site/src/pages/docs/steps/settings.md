---
layout: ../../../layouts/DocsLayout.astro
title: Env, dir & timeout
---

# Env, dir & timeout

Four settings every step has, whatever it runs and wherever it runs. Every method returns the same
builder, so calls chain.

```go
verify.Step("deploy", exec.Command("./deploy.sh", "prod")).
	Env("DEPLOY_ENV", "prod").
	WorkDir("./infra").
	Timeout(5 * time.Minute).
	ContinueOnError()
```

| Method | What it does |
|---|---|
| `Env(key, value)` | One environment variable. Call it again for more. |
| `WorkDir(dir)` | The directory the command runs in. |
| `Timeout(d)` | Bounds **one attempt**, not the whole retry sequence. |
| `ContinueOnError()` | Dependents run even if this step fails. |

## `Env`

One pair per call:

```go
verify.Step("test", exec.Command("go", "test", "./...")).
	Env("CGO_ENABLED", "0").
	Env("GOFLAGS", "-count=1")
```

It is not variadic, because a variadic run of pairs has an arity the compiler cannot check:
`Env("A", "1", "B")` would compile and mean nothing.

### Your step gets exactly what you declared

**`Build()` adds nothing to a step's environment, not even a `PATH`.** Two developers on the same
commit get the same plan, because nothing about their shells leaks into it. That matters most on
the exact field a [cache key](/docs/data/cache-keys/) is computed from.

Search-path defaults are the executor's job instead:

- **The local executor supplies the coordinator's own `PATH`** to a step that declares none, and
  nothing else from the parent environment. That is why `exec.Command("go", "test", "./...")`
  finds `go` with no `Env` call at all.
- **Declaring a `PATH` yourself replaces that fallback**, it does not add to it. If you set
  `PATH=/opt/toolchain/bin`, `go` is no longer on it.

### Never put a credential in `Env`

Use [`SecretEnv`](/docs/secrets/) instead, which delivers a **file path** rather than a value.
senro refuses a plan that would route a resolved secret through an environment value, so this is
not a style preference.

## `WorkDir`

```go
verify.Step("build", exec.Command("pnpm", "build")).WorkDir("./web")
```

There is no `cd` in a step, because [nothing is shell-interpreted](/docs/steps/#execcommand-interprets-no-shell).
`WorkDir` is how you change directory.

When a step mounts a [workspace](/docs/data/workspaces/), `WorkDir` is usually the mount path:

```go
deploy.Step("build", exec.Command("./build.sh")).
	Mount(src.At("/repo", senro.RW)).
	WorkDir("/repo")
```

## `Timeout` bounds one attempt

A step with `Retry(3, ...)` and `Timeout(5*time.Minute)` can take **fifteen minutes** across three
attempts. The bound is per attempt, not per step.

An attempt that outlives the bound settles the step as
[`timed_out`](/docs/steps/states/).

> **One exception, on the coordinator.** Nothing can force a Go function to return, so a
> [`senro.Func`](/docs/steps/functions/) step running locally that ignores its context keeps
> running past the deadline and is merely *filed* as `timed_out` when it eventually finishes.
>
> Off the coordinator the function has a process of its own, which ends itself at the deadline.
> Declare a `Timeout` on every remote func step.

## `ContinueOnError` is for advisory steps

`ContinueOnError()` says: if this step **fails**, its dependents should run anyway, against
whatever it did produce. Linting is the usual case.

```go
verify.Step("lint", exec.Command("golangci-lint", "run")).ContinueOnError()
verify.Step("report", exec.Command("./collect-report.sh")).Needs("lint")   // runs either way
```

It is not a general rescue, and two limits catch people out:

- **It does not apply when the upstream never ran.** A dependent of a `skipped_condition` or
  `skipped_manual` step is skipped the same way, because nothing failed and nothing is being
  excused. `ContinueOnError` promises a dependent survives a *failure*, not that it runs against
  output that was never produced.
- **It still changes the run's rollup.** Without it, the failure makes the run `partial` or
  `failed`.

Both are on [Step states](/docs/steps/states/).

## The rest of the builder

| I want to... | Call | Page |
|---|---|---|
| Run this after another step | `Needs` | [Ordering](/docs/steps/ordering/) |
| Try again when it breaks | `Retry`, `RetryPolicy` | [Retries](/docs/steps/retries/) |
| Clean up or collect logs afterwards | `OnFailure`, `Always` | [Failure handlers](/docs/steps/handlers/) |
| Skip it unless something is true | `When` | [Conditions](/docs/steps/conditions/) |
| Give it files, and keep what it wrote | `Mount`, `NoSnapshot` | [Workspaces](/docs/data/workspaces/) |
| Skip it when nothing changed | `Pure`, `Inputs`, `Outputs`, `CacheEnv` | [Caching a step](/docs/data/caching/) |
| Give it a credential | `SecretEnv` | [Secrets](/docs/secrets/) |

A [handler](/docs/steps/handlers/) is a `*senro.StepBuilder` too, and `Env`, `SecretEnv`,
`WorkDir` and `Timeout` are exactly the four settings that work on one.
