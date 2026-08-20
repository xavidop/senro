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

A **`senro.Func` handler on a non-local step** is not supported either. A handler inherits its
parent's executor and declares none of its own, so there is nothing to key binary staging to; use an
`exec` handler for cleanup on the target. See [Func steps off the coordinator](/docs/executors/func-remote/).

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

| Variable | What it holds |
|---|---|
| `SENRO_FAILURE_STEP` | The id of the step that failed |
| `SENRO_FAILURE_STATE` | Its terminal state, from [the ten](/docs/steps/states/) |
| `SENRO_FAILURE_EXIT_CODE` | The exit code it ended on |
| `SENRO_FAILURE_ATTEMPT` | The attempt the step actually reached, so a handler can find that attempt's log. `0` for a node that never ran an attempt, skipped or cancelled before it started |

## Where to go next

- **[Retries](/docs/steps/retries/)**: what has to be exhausted before `OnFailure` fires.
- **[Step states](/docs/steps/states/)**: what `SENRO_FAILURE_STATE` can say.
- **[The event stream](/docs/reference/event-stream/)**: the `handler.started`, `handler.succeeded`
  and `handler.failed` events a handler run emits.
- **[Workspaces](/docs/data/workspaces/)**: what a handler gets to read back.
