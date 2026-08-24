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
retry.Named("http-status", HTTPStatus{...}) // your own Go function; see below
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
- **`retry.Named(name, params)`** is a Go function of your own, registered under a name so a plan
  can carry it. See [Deciding in Go](#deciding-in-go-retryregisterpredicate).

## Deciding in Go: `retry.RegisterPredicate`

When the four predicates above cannot express the rule, write the decision as a Go function and
give it a name. Registering it is what lets a plan carry it: what gets recorded is the **name and
the arguments**, not the closure.

It is the same shape [`senro.RegisterFunc`](/docs/steps/functions/) has. Register once, from an
`init`; name it at the call site.

```go
type HTTPStatus struct {
	Codes []int `json:"codes"`
}

func init() {
	retry.RegisterPredicate("http-status", func(p HTTPStatus, a retry.Attempt) bool {
		for _, c := range p.Codes {
			if strings.Contains(a.LogTail, strconv.Itoa(c)) {
				return true
			}
		}
		return false
	})
}

// in the pipeline
verify.Step("publish", exec.Command("./publish.sh")).
	Retry(3, retry.Named("http-status", HTTPStatus{Codes: []int{502, 503}}))
```

The plan records `func:http-status:{"codes":[502,503]}`, and the engine looks the name up to
reconstruct the predicate before the first attempt.

### What your function is handed

```go
type Attempt struct {
	Number   int    // 1 on the first try
	ExitCode int    // the workload's verdict
	Err      error  // set when the attempt failed to run at all
	LogTail  string // the tail of this attempt's output
}
```

So a rule that needs the attempt number, which none of the built-in predicates can express:

```go
func init() {
	retry.RegisterPredicate("infra-then-give-up", func(p struct{}, a retry.Attempt) bool {
		// Two goes at a broken substrate, then stop: past that it is not
		// a hiccup, and a third attempt only delays the report.
		return a.Number <= 2 && errors.Is(a.Err, executor.ErrInfra)
	})
}

verify.Step("publish", exec.Command("./publish.sh")).
	Retry(5, retry.Named("infra-then-give-up", nil))
```

Pass `nil` for a predicate that takes no parameters. It composes with everything else, because it
is recordable like everything else:

```go
retry.Any(retry.OnInfra(), retry.Named("http-status", HTTPStatus{Codes: []int{503}}))
```

### The rules

| | |
|---|---|
| **The name is API** | It is what `plan.json` records and what the engine looks up. Renaming it breaks any recorded plan that still names it, exactly as renaming a command would. |
| **Register from `init`** | Registering the same name twice panics, as does an empty name or one containing `:`. |
| **Parameters must be JSON-serializable** | And are decoded strictly. A recorded field your struct does not have means **no match**, never a blind retry. |
| **A name nothing registered is a build error** | `Build()` names it, and lists what this binary does have. A typo never becomes a policy that silently never fires. |
| **Do not block, do not have side effects** | It is asked once per failed attempt, inside the retry loop. |

### `retry.Func`, and why it cannot be built

`retry.Func(f func(retry.Attempt) bool)` wraps a bare function with no name attached. A step using
one is refused at `Build()`:

```
senro: step "publish" retry predicate has no serialized form and cannot be built into a plan,
which is what retry.Func produces; use retry.OnInfra, retry.OnExitCode, retry.OnLogMatch or
retry.Any instead
```

A `Plan` is JSON: every other predicate carries a written-down form (`"infra"`,
`"exit_code:75,111"`, `"func:http-status:{...}"`) that survives being saved to disk and
reconstructed by the engine in another process. A bare closure has none, so senro would have to
either drop it silently and retry on every failure, or refuse.

**`retry.RegisterPredicate` is the answer to that**: the same Go code, with a name a plan can
record. `retry.Func` remains for a `retry.Policy` you hold and apply directly, in code that never
builds a plan.

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
- **[Reading a failed run](/docs/run/debugging/)**: finding a given attempt's logs.
