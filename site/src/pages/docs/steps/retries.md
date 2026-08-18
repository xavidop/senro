---
layout: ../../../layouts/DocsLayout.astro
title: Retries
---

# Retries

A command exiting non-zero is not the same as the infrastructure that ran it breaking. senro makes
you say which one you want to retry.

```go
verify.Step("test", exec.Command("go", "test", "./...")).
	Retry(3, retry.OnInfra()) // only a dropped connection retries; a failing test never does
```

`Retry(maxAttempts int, p retry.Predicate)` retries while `p` matches.

> Retrying a failing `go test` until it happens to pass deletes the information the test gave you.
> Retrying an SSH connection that reset mid-copy does not, because the command never got to report a
> verdict.

## `maxAttempts` is total attempts

**`Retry(1, ...)` is refused at `Build()`**, because one attempt is no retry at all:

```
plan: step "flaky" retry policy allows 1 attempt(s), want at least 2
```

"Retry once on failure" is `Retry(2, ...)`. This is the refusal that surprises people most.

## The predicates

```go
retry.OnInfra()                            // the step's process failed to start or run at all
retry.OnExitCode(75)                       // specific exit codes only
retry.OnLogMatch(`connection refused`)     // last resort, matching on log text
retry.Any(retry.OnInfra(), retry.OnExitCode(75))
```

- **`retry.OnInfra()`** matches infrastructure failures only: the step's process failing to start or
  run at all, as opposed to running and exiting non-zero.
- **`retry.OnExitCode(codes ...int)`** filters `0` out of its list rather than refusing it. Exit `0`
  is success, so there is nothing to retry: `OnExitCode(0)` builds cleanly and never matches, and
  `OnExitCode(0, 75)` behaves exactly like `OnExitCode(75)`.
- **`retry.OnLogMatch(pattern)`** returns `(Predicate, error)`; the pattern compiles at
  construction, so a broken regexp fails when the pipeline is built, not on a failing host. Treat it
  as a last resort: the retry silently stops firing the day someone rewords the log message.
- **`retry.Any(preds ...Predicate)`** matches if any of its predicates does.
- **`retry.Func(f func(retry.Attempt) bool)` cannot be built into a plan.** A predicate carries a
  serialized form so a `Plan` (which is JSON) can record it and the engine can reconstruct it across
  a process boundary. A closure has none, so `Build()` refuses it explicitly rather than silently
  retrying on every failure.

## Backoff with `RetryPolicy`

`RetryPolicy(policy retry.Policy)` is the same thing with explicit backoff:

```go
verify.Step("publish", exec.Command("./publish.sh")).
	RetryPolicy(retry.Policy{
		MaxAttempts: 4,
		On:          retry.OnInfra(),
		Backoff:     retry.Backoff{Base: time.Second, Max: 30 * time.Second, Factor: 2},
	})
```

`retry.Backoff{Base, Max, Factor}` is exponential with jitter. **Jitter is what keeps every retrying
step from hammering the same failure point at the same moment.**

## What a retry does to the rest of the step

- **A step that failed and then passed settles as `recovered`, not `succeeded`.** Collapsing the two
  is how flaky infrastructure stays invisible for months. See [Step states](/docs/steps/states/).
- **`Timeout` bounds one attempt**, so a step with three attempts can take three timeouts' worth of
  wall clock. See [Step settings](/docs/steps/settings/).
- **`OnFailure` handlers run once retries are exhausted**, not per attempt. See
  [Handlers](/docs/steps/handlers/).
- **A panicking `senro.Func` step is not retried.** It settles as `panicked`.
- **A handler cannot declare a `Retry` of its own**; `Build()` refuses it.

## Knowing which attempt you are on

- A `senro.Func` step reads `ctx.Attempt()`, which is `1` on the first try. That is what an
  idempotency key needs to know before retrying against a remote API. See
  [Func steps](/docs/steps/functions/).
- A handler reads `SENRO_FAILURE_ATTEMPT`, the attempt the step actually reached, so it can find
  that attempt's log.

## Retrying a live run

`Retry` here is a build-time policy. An operator can also retry a step in a run that is already
going, with the `step.retry` control operation; the two are separate mechanisms. See
[Control operations](/docs/attach/control-ops/).

## Where to go next

- **[Handlers](/docs/steps/handlers/)**: what runs after the last attempt fails.
- **[Step states](/docs/steps/states/)**: `recovered`, `failed`, `timed_out`, `panicked`.
- **[Reading a failed run](/docs/reference/debugging/)**: finding a given attempt's logs.
