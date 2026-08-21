---
layout: ../../../layouts/DocsLayout.astro
title: Handlers
---

# Handlers

A handler is what runs after a step, to clean up or to collect evidence. `OnFailure` runs its
handlers once retries are exhausted and the step still failed; `Always` runs its handlers whatever
the outcome.

```go
cleanup := senro.Handler("release-lock", exec.Command("./scripts/unlock.sh"))
diagnostics := senro.Handler("dump-log", exec.Command("cat", "test-results.log"))

deploy.Step("deploy", exec.Command("./deploy.sh")).
	Retry(2, retry.OnInfra()).
	OnFailure(diagnostics).
	Always(cleanup)
```

- Handlers run **in order**, as listed.
- **A handler failing never masks the step's own failure.** The run's recorded cause stays the
  original step, and the handler's failure is reported alongside it.

## Handlers are not steps

`senro.Handler(id, action)` returns a `*senro.StepBuilder` deliberately **not attached to any
workflow**. Passing a `Workflow.Step(...)` return value to `OnFailure` or `Always` is rejected at
`Build()`, because that step would then run twice: once on its own, once as the handler.

The error names exactly this and suggests `senro.Handler`, so the mistake is caught at build time
rather than as a double-executed cleanup in production.

## What a handler may declare

A handler is a `*StepBuilder`, so every step method is within reach. The ones with no meaning for a
handler are refused by `Build()` rather than quietly dropped.

| On a handler | Status |
|---|---|
| `Env`, `SecretEnv`, `WorkDir`, `Timeout` | Work as on any step |
| `Needs` | Refused: `plan: handler "notify" of step "s" must not declare Needs` |
| `Mount` | Refused: `plan: handler "collect" of step "s" declares its own mounts; a handler already has its parent's workspaces, mounted read-only at the same paths` |
| `When`, `Retry`, an executor of its own, cache settings, handlers of its own | Refused |

## A handler can be a Go function

A handler's action is a [step action](/docs/steps/#the-two-step-kinds), so it can be a
`senro.Func` as easily as a command:

```go
type CollectParams struct {
	Bucket string `json:"bucket"`
}

func init() { senro.RegisterFunc("ci/collect", Collect) }

func Collect(ctx senro.Ctx, p CollectParams) error {
	f, ok := ctx.Failure()
	if !ok {
		return errors.New("ci/collect only makes sense as a handler")
	}

	fmt.Fprintf(ctx.Stdout(), "%s ended %s (exit %d) on attempt %d\n",
		f.Step, f.State, f.ExitCode, f.Attempt)

	// Classify without opening the log file: the tail came with the failure.
	if strings.Contains(f.LogTail, "no space left on device") {
		return upload(ctx, p.Bucket, f.Step, f.LogTail)
	}
	return nil
}

// in the pipeline:
deploy.Step("apply", exec.Command("./deploy.sh")).
	OnFailure(senro.Handler("collect", senro.Func("ci/collect", CollectParams{Bucket: "ci-evidence"})))
```

### `ctx.Failure()`

This is the func equivalent of the `SENRO_FAILURE_*` variables an `exec` handler reads. It returns
a `senro.StepFailure` and an `ok` that is **false for an ordinary step**, so a function used both
ways can tell which it is.

| Field | What it holds |
|---|---|
| `Run` | The run's id |
| `Step` | The id of the step this handler belongs to. The **parent's**, not the handler's |
| `State` | Its terminal state, one of [the ten](/docs/steps/states/) |
| `ExitCode` | The exit code it ended on |
| `Attempt` | The attempt the step actually reached. `0` for a node that never ran one |
| `Error` | The substrate's own message when the attempt failed to run at all, and empty when the step ran and returned a verdict |
| `LogTail` | The tail of the failed attempt's combined output |

`Error` and `LogTail` are the two an environment has no room for, which is why an `exec` handler
has to go and open the log file and a function does not.

> An **`Always` handler** gets a `Failure` too, describing whatever happened, so `State` is how it
> tells a passing step from a failing one. `ok` says "I am a handler", never "something broke".

### It runs on the parent's executor

A `senro.Func` handler runs wherever its parent ran: the coordinator, an ssh host, a container, a
pod. senro stages the binary on the parent's target and re-enters it there, exactly as it does for
a func step, **reusing the copy the parent step already staged**. `ctx.Workspace(...)` and
`ctx.Secret(...)` report paths on that machine.

The one refusal left is a func handler that declares a **delegated** secret, for the same reason a
func step cannot: delegation hands the pod a source URI for the step's own *command* to resolve,
and a function reads `ctx.Secret(name)`, which is a file senro wrote. See
[Func steps off the coordinator](/docs/executors/func-remote/).

## What a handler inherits

A handler runs on the **same executor** as its step, with the **same workspaces**, mounted read-only
at the same paths. That is what makes a diagnostic handler worth attaching: it collects evidence
from the environment that actually broke.

```go
src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

deploy.Step("build", exec.Command("./build.sh")).
	Mount(src.At("/repo", senro.RW)).
	WorkDir("/repo").
	OnFailure(senro.Handler("dump-log", exec.Command("cat", "build.log")))
```

`build.sh` writes `build.log` into `/repo`; the handler reads it back from the same `/repo`, in a
sandbox of its own on the same executor. For a container step that is a fresh container from the
parent's image with the same bind mount.

**A handler with no `WorkDir` starts in the directory the step started in**, which is what lets the
unqualified `cat build.log` find the file.

- **The view is read-only.** The step's `ws.snapshot` digest was recorded while its sandbox was
  still open, and nothing snapshots again afterwards, so a handler write would move bytes the ledger
  already describes. The container executor enforces this as a read-only bind mount; the local
  executor cannot, exactly as for a step's own `senro.RO` mount. Write to the handler's own working
  directory.
- **Only declared workspaces are inherited.** A file the step wrote outside them went into the
  executor's private sandbox, which for a container no longer exists when the handler starts.
  Evidence a handler must read belongs in a workspace. Scratch caches are not inherited: they are
  caches, not evidence.
- **A handler cannot declare mounts of its own.** It already has its parent's, so `Build()` refuses
  a declaration that could only restate or contradict them.

## The failure arrives in the environment

An `exec` handler reads what broke out of four variables:

| Variable | What it holds |
|---|---|
| `SENRO_FAILURE_STEP` | The id of the step that failed |
| `SENRO_FAILURE_STATE` | Its terminal state, from [the ten](/docs/steps/states/) |
| `SENRO_FAILURE_EXIT_CODE` | The exit code it ended on |
| `SENRO_FAILURE_ATTEMPT` | The attempt the step actually reached, so a handler can find that attempt's log. `0` for a node that never ran an attempt, skipped or cancelled before it started |

```sh
#!/bin/sh
echo "$SENRO_FAILURE_STEP ended $SENRO_FAILURE_STATE" \
     "(exit $SENRO_FAILURE_EXIT_CODE) on attempt $SENRO_FAILURE_ATTEMPT"
```

A [func handler](#a-handler-can-be-a-go-function) reads the same evidence through
`ctx.Failure()`, plus the error text and the log tail. These four variables are unchanged either
way: a func handler does not take them away from anything.

## Where to go next

- **[Retries](/docs/steps/retries/)**: what has to be exhausted before `OnFailure` fires.
- **[Step states](/docs/steps/states/)**: what `SENRO_FAILURE_STATE` can say.
- **[Go functions as steps](/docs/steps/functions/)**: `senro.Func`, `senro.Ctx` and
  `RegisterFunc`.
- **[The event stream](/docs/reference/event-stream/)**: the `handler.started`, `handler.succeeded`
  and `handler.failed` events a handler run emits.
- **[Workspaces](/docs/data/workspaces/)**: what a handler gets to read back.
