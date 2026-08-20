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

- **`retry.OnInfra()`** matches infrastructure failures only: an SSH reset, an image that will not
  pull, a pod evicted out from under the step. The substrate failed the step, not the other way
  round. A non-zero exit is never matched here.
- **`retry.OnExitCode(codes ...int)`** filters `0` out of its list rather than refusing it. Exit
  `0` is success, so there is nothing to retry: `OnExitCode(0)` builds cleanly and never matches,
  and `OnExitCode(0, 75)` behaves exactly like `OnExitCode(75)`.
- **`retry.OnLogMatch(pattern)`** returns `(Predicate, error)`; the pattern compiles at
  construction, so a broken regexp fails when the pipeline is built, not on a failing host. Treat
  it as a last resort: the retry silently stops firing the day somebody rewords the log message.
- **`retry.Any(preds ...Predicate)`** matches if any of its predicates does.

### `retry.Func`: a predicate senro cannot record

`retry.Func(f func(retry.Attempt) bool)` wraps a plain function, so you can decide from anything
on the attempt:

```go
type Attempt struct {
	Number   int    // 1 on the first try
	ExitCode int
	Err      error
	LogTail  string
}

p := retry.Func(func(a retry.Attempt) bool {
	// Give the first two attempts the benefit of the doubt on a 503,
	// then stop: something is genuinely down.
	return a.Number <= 2 && strings.Contains(a.LogTail, "503 Service Unavailable")
})
```

**A step using it cannot be built into a plan.** `Build()` refuses it:

```
senro: step "publish" retry predicate has no serialized form and cannot be built into a plan,
which is what retry.Func produces; use retry.OnInfra, retry.OnExitCode, retry.OnLogMatch or
retry.Any instead
```

A `Plan` is JSON. Every other predicate carries a written-down form (`"infra"`,
`"exit_code:75,111"`) that survives being saved to disk and reconstructed by the engine in another
process. A closure has none, so senro would have to either drop it silently and retry on every
failure, or refuse. It refuses.

So `retry.Func` is for code that holds a `retry.Policy` **directly** and never builds it into a
plan: your own retry loop, a test, a tool built on the `retry` package. Inside a pipeline, compose
the recordable predicates instead:

```go
// Instead of retry.Func matching on a 503 in the log:
lm, err := retry.OnLogMatch(`503 Service Unavailable`)
if err != nil {
	return err
}
verify.Step("publish", exec.Command("./publish.sh")).
	Retry(3, retry.Any(retry.OnInfra(), lm))
```

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

`retry.Backoff{Base, Max, Factor}` is exponential with jitter: 1s, 2s, 4s, 8s, capped at 30s, each
wait nudged by a random amount. **Jitter is what keeps every retrying step from hammering the same
failure point at the same moment.**

## What a retry does to the rest of the step

- **A step that failed and then passed settles as `recovered`, not `succeeded`.** Collapsing the
  two is how flaky infrastructure stays invisible for months. See
  [Step states](/docs/steps/states/).
- **`Timeout` bounds one attempt**, so a step with three attempts can take three timeouts' worth
  of wall clock. See [Env, dir & timeout](/docs/steps/settings/).
- **`OnFailure` handlers run once retries are exhausted**, not per attempt. See
  [Failure handlers](/docs/steps/handlers/).
- **A panicking `senro.Func` step is not retried.** It settles as `panicked`.
- **A handler cannot declare a `Retry` of its own**; `Build()` refuses it.

## Knowing which attempt you are on

- A `senro.Func` step reads `ctx.Attempt()`, which is `1` on the first try. That is what an
  idempotency key needs to know before retrying against a remote API. See
  [Func steps](/docs/steps/functions/).
- A handler reads `SENRO_FAILURE_ATTEMPT`, the attempt the step actually reached, so it can find
  that attempt's log.

## Retrying by hand, while the run is going

`Retry` is a **policy written into the plan**: senro decides on its own, using the predicate, with
nobody watching.

Separately, if you are [attached](/docs/attach/) to a run, you can retry a step **yourself**:
focus it in the TUI and press `r`. That works on any settled step, whether or not it declared a
`Retry`, and it is how you rerun something after fixing the machine under it.

The two never interact. A step with `Retry(3, retry.OnInfra())` that failed a test exhausted
nothing (the predicate never matched), and pressing `r` still runs it again. Refusals are
per-mechanism too: `r` is refused while a step is still running, or if it was never reached.

See [Control operations](/docs/attach/control-ops/) for `step.retry` on the wire.

## Where to go next

- **[Failure handlers](/docs/steps/handlers/)**: what runs after the last attempt fails.
- **[Step states](/docs/steps/states/)**: `recovered`, `failed`, `timed_out`, `panicked`.
- **[Reading a failed run](/docs/reference/debugging/)**: finding a given attempt's logs.
