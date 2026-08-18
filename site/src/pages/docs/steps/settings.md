---
layout: ../../../layouts/DocsLayout.astro
title: Step settings
---

# Step settings

`Env`, `WorkDir`, `Timeout` and `ContinueOnError` are the four settings every step has, whatever
kind it is and wherever it runs. Every method returns the same builder, so calls chain.

```go
verify.Step("deploy", exec.Command("./deploy.sh", "prod")).
	Env("DEPLOY_ENV", "prod").
	WorkDir("./infra").
	Timeout(5 * time.Minute).
	ContinueOnError()
```

| Method | What it does |
|---|---|
| `Env(key, value string)` | One environment variable per call, chained for more |
| `WorkDir(dir string)` | The working directory the command runs in |
| `Timeout(d time.Duration)` | Bounds **a single attempt**, not the whole retry sequence |
| `ContinueOnError()` | Dependents run even if this step fails |

`Env` takes one pair per call rather than a variadic run of them, because a variadic run of pairs
has an arity the compiler cannot check.

## `Env` is exactly what you declared

**`Build()` adds nothing to a step's environment, not even a `PATH`.** A `PATH` inherited from
whichever machine ran `Build()` would give two developers on the same commit two different plan
identities, on the exact field a cache key is computed from.

Search-path defaults belong to the executor instead:

- **The local executor supplies the coordinator's own `PATH`** to a step that declares none, and
  nothing else from the parent environment. That is why `exec.Command("go", "test", "./...")` finds
  `go` with no `Env` call.
- **Declaring a `PATH` yourself replaces that fallback**, it does not add to it.

A credential never belongs in `Env`. Use `SecretEnv`, which delivers a file path rather than a
value; see [Secrets](/docs/secrets/).

## `Timeout` bounds one attempt

A step with `Retry(3, ...)` and `Timeout(5*time.Minute)` can take fifteen minutes over three
attempts. An attempt that outlives the bound settles the step as `timed_out`; see
[Step states](/docs/steps/states/).

> **One exception.** On the coordinator, a `Timeout` on a `senro.Func` step bounds only how the
> outcome is reported: nothing can force a Go function to return, so one that ignores its context
> keeps running and is filed as `timed_out` when it finishes.
>
> Off the coordinator the function has a process of its own, which ends itself at the deadline. See
> [Func steps](/docs/steps/functions/).

## `ContinueOnError` is for advisory steps

`ContinueOnError()` says a dependent survives this step's **failure**, running against whatever
outputs the step produced. Use it for advisory work like linting.

It is not a general rescue:

- It does **not** apply when the upstream never ran. A dependent of a `skipped_condition` or
  `skipped_manual` step is skipped the same way, because nothing failed and nothing is blamed.
- It changes the run's rollup: without it, a failure makes the run `partial` or `failed`.

See [Step states](/docs/steps/states/) for both rules in full.

## The rest of the builder

| Method | Page |
|---|---|
| `Needs` | [Ordering](/docs/steps/ordering/) |
| `Retry`, `RetryPolicy` | [Retries](/docs/steps/retries/) |
| `OnFailure`, `Always` | [Handlers](/docs/steps/handlers/) |
| `When` | [Conditions](/docs/steps/conditions/) |
| `Mount`, `NoSnapshot` | [Workspaces](/docs/data/workspaces/) |
| `Pure`, `Inputs`, `Outputs`, `CacheEnv` | [Caching a step](/docs/data/caching/) |
| `SecretEnv` | [Secrets](/docs/secrets/) |

A handler is a `*senro.StepBuilder` too, and `Env`, `SecretEnv`, `WorkDir` and `Timeout` are exactly
the four of these that work on one. See [Handlers](/docs/steps/handlers/).
