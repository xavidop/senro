---
layout: ../../../layouts/DocsLayout.astro
title: Control operations
---

# Control operations

Attach is not read-only: a connected client can ask the engine to *do* something. This is the
reference for the wire shape, the eleven operations this build implements, and the refusal codes.

## Frame shape

A control request and its response are one JSON `api.Frame` each, exchanged over a single
endpoint, `POST /api/control`, correlated by `id`:

```json
{"v":1,"kind":"req","id":"c7","type":"step.retry","payload":{"step":"build"}}
{"v":1,"kind":"res","id":"c7","ok":true}
```

- A response carries no payload, ever. It says whether the operation was accepted, and on refusal
  says why in `error`. What it went on to do shows up in the event stream, the only record of it.
- An argument is exactly one key and nothing else, and a request carrying any other key is
  rejected outright, before it can reach the permanent ledger. `Frame` is the control channel's
  own shape and nothing else's, plain JSON deliberately, debuggable with `curl` alone.

`POST /api/control` is one of six routes:

```
GET  /api/state           a bare RunState
GET  /api/plan            the resolved plan, the same JSON as the run directory's plan.json
GET  /api/logs/{step}     raw log bytes for one step's one stream
GET  /api/stream?from=N   bare Event values as newline-delimited JSON, resuming at seq N
POST /api/control         the Frame request/response above
POST /api/shell           an interactive session on a live step, on a hijacked connection
```

Subscribing and reading logs are deliberately not control operations, and
[a session](/docs/attach/shell/) is `POST /api/shell`, which hijacks the connection and could not
be a frame. The [stream's](/docs/attach/#snapshot-then-subscribe) resume parameter is `from`, not
`from_seq`.

## The eleven operations

**These are the only eleven this build has**, and the only eleven `type` values
`POST /api/control` accepts; anything else is refused with `unknown_op`. Each has a declared constant in
[`api`](/docs/reference/api/), which declares only ops the engine actually serves.

| Operation | Argument | What it does |
|---|---|---|
| `run.cancel` | none | Cancels the run. The TUI's `c`/`Ctrl-C` and the CLI's own signal handling (`SIGINT`/`SIGTERM`) issue this, best-effort, with a short timeout |
| `step.retry` | `step` | Retries the named step in place, in the same run, incrementing its attempt count |
| `step.skip` | `step` | Takes a step out of the run. It settles as `skipped_manual` without being dispatched, and so does every step that needs it. See below |
| `breakpoint.set` | `step` | Stops the run before a step. See [Breakpoints](#breakpoints) |
| `breakpoint.clear` | `step` | Releases a held step |
| `run.rerun_from` | `step` | Re-runs a step and everything downstream of it, in a run that is still live. See below |
| `run.pause` | none | Stops the run dispatching anything new. See below |
| `run.resume` | none | Lets it dispatch again |
| `analysis.accept` | `id` | Accepts a [failure analyzer](/docs/analyzers/custom/)'s proposal and performs its remedy |
| `analysis.reject` | `id` | Rejects it; nothing is performed |
| `ws.snapshot` | `step` | Captures that step's workspaces now, for inspection. See [Forcing a snapshot](#forcing-a-snapshot) |

- `step.retry` is a *live* operation, distinct from the [`Retry` policy](/docs/steps/retries/)
  declared at build time: something a human or script asks for after a step has exhausted, or
  never had, an automatic policy. It dispatches one bare attempt directly, so it does not re-run
  the step's `OnFailure`/`Always` handlers; the ledger says so with a `handler.superseded` event.
- An analysis `id` is the one an `analysis.proposed` event carried, `<step>@<attempt>`. It names a
  proposal rather than a step on purpose, so a client cannot approve a proposal about one step
  into a retry of another: the step retried is the one the *engine's* record says it was about.
- Accepting emits `analysis.applied`, rejecting emits `analysis.rejected`. The proposal is then
  settled and a second decision refused, so two operators pressing `a` cannot retry a step twice.
- Accepting grants an analyzer no power a client did not already have. The only remedy this build
  can apply is a retry, served by exactly the code `step.retry` is served by, refusals included.
- Every accepted operation is also emitted as a `control.applied` lifecycle event carrying the
  originating client's identity, so the event stream is an audit trail nothing can bypass.

## Refusals are answers, not errors

A refused operation comes back as `ok:false` with a short, machine-readable reason: a protocol
code, not prose, so a client can branch on it and its wording does not change between releases.
An operation either applies completely or is refused, with no half-applied state.

| Reason | Meaning |
|---|---|
| `unknown_op` | This build doesn't implement that operation |
| `run_finished` | Nothing is left to act on the request; the run is over |
| `already_cancelled` | The run is already cancelling |
| `run_not_active` | The run is being torn down, so no new work may start |
| `missing_step` | The operation needs a `step` argument and got none |
| `unknown_step` | No such step in this run's plan |
| `step_running` | That step (or, for `run.rerun_from`, something in its closure) is mid-attempt |
| `step_not_failed` | `step.retry` only applies to a step that failed |
| `step_settled` | That step already ran: `step.skip` can't un-run it, and `ws.snapshot` would be a second, later answer to a question its own snapshot already answered |
| `step_not_settled` | `run.rerun_from` has nothing to re-run: that step hasn't run yet |
| `breakpoint_exists` | A breakpoint is already armed on that step |
| `no_breakpoint` | There's no breakpoint on that step to clear |
| `already_paused` | The run is already paused |
| `not_paused` | The run isn't paused, so there's nothing to resume |
| `missing_proposal` | An analysis operation needs an `id` argument and got none |
| `unknown_proposal` | No proposal in this run carries that id |
| `proposal_settled` | Somebody has already accepted or rejected that proposal |
| `no_remedy` | That proposal asked for nothing this build can perform, so there is nothing to apply |
| `no_workspace` | `ws.snapshot` has nothing to capture: that step mounts no workspace, or only cluster-backed ones |
| `snapshot_failed` | The capture itself failed. Not a refusal: nothing was wrong with the request |

## Forcing a snapshot

`ws.snapshot{step}` captures every workspace the named step mounts, right now, and emits one
`ws.snapshot` event per workspace so you can `senro ws pull` the digest and look at the files.

It is answerable for a step that has **not run yet**, and the case it exists for is a step held at
a breakpoint: the run has stopped there, so nothing is writing, and what you get is exactly what
that step is about to be given.

- A step **mid-attempt** is refused with `step_running`. It is writing the very directories the
  capture would read, and a torn tarball digested as if it were a state is worse than no answer.
- A step that has **settled** is refused with `step_settled`. Its own snapshot at settle time
  already records what it produced, failures included, and that is the digest `senro ws` reports.
- A step mounting **no workspace**, or only [claim-backed](/docs/executors/kubernetes/) ones whose
  content lives in the cluster, is refused with `no_workspace` rather than accepted as a no-op.

**A forced capture is never evidence.** It enters no cache key, replaces no workspace's recorded
state, and changes nothing about the plan or about what the step's own snapshot will say. The event
carries `"forced": true`, and `senro ws ls`, `ws pull` and `ws diff` skip it for that reason: they
report what the run produced. The digests are still pinned for the life of the run, so the snapshot
is there when you pull it.

The operation name is the same string as the event it causes, exactly as `breakpoint.set` causes
`breakpoint.hit`. Operations and event types never share a channel, so the reuse is unambiguous.

The capture does not run on the scheduler's own loop: a workspace can be gigabytes, and control
requests are served one at a time. The response arrives when the capture is finished, so `ok:true`
means the snapshot is in the ledger, not merely that the request was accepted. While it runs, the
step is treated as busy, so a second `ws.snapshot`, a `step.retry` and a `step.skip` on it are all
answered `step_running`.

## What happens below a skipped step

`step.skip` settles the named step as `skipped_manual`, and every step that needs it, directly or
transitively, settles as `skipped_manual` too. Not `skipped_upstream_failed`, and
`ContinueOnError` does not rescue them.

That is the rule the engine applies to a step skipped by a `When` condition too. senro
distinguishes two ways a step can stop its dependents:

- **The upstream failed.** Dependents are `skipped_upstream_failed`, the run rolls up as
  `partial`, and `ContinueOnError` is the author's explicit "run anyway" escape hatch.
- **The upstream never ran, and nothing broke.** Dependents inherit the same skip state, the run
  rolls up clean, and `ContinueOnError` does not apply: it promises a dependent survives a
  *failure*, not that it runs against output that was never produced.

A manual skip is unambiguously the second, and it does not poison the graph: only transitive
dependents are affected, unrelated branches run to completion, and the run finishes `succeeded`.

## Breakpoints

`breakpoint.set{step}` stops the run *before* a step. `breakpoint.clear{step}` is the release, and
`run.cancel` is the only other way out: a run held at a breakpoint waits indefinitely.

Nothing inside the engine blocks while it waits. `Sink.Emit` must never block, so the scheduler
declines to *dispatch* that step and returns to its ordinary idle wait, where it reads control
requests. No goroutine is parked, no parallelism slot is taken, and the run progresses elsewhere.

- The instant the scheduler first withholds the step, it emits `breakpoint.hit` once, carrying the
  client that armed it. That is the only thing distinguishing a held step from one still waiting
  on its dependencies, since a held step has no `step.started`, no `step.finished` and no state.
  Clients fold it into `StepState.Paused`, and both shipped renderers show it.
- Because a breakpoint gates *scheduling*, it does not intercept a `step.retry`, which dispatches
  an attempt directly. It does compose with `run.rerun_from`: arm it first, then rerun.
- A held step is the one state [`ws.snapshot`](#forcing-a-snapshot) is for: nothing is writing the
  step's workspaces, so a capture taken then is exactly what the step is about to be given.

## Pausing the whole run

`run.pause` stops the run dispatching anything new; `run.resume` is the release. Neither takes an
argument, and `run.cancel` is the only other way out: a paused run waits indefinitely.

**A pause is not a breakpoint.** A breakpoint withholds one nominated step; a pause withholds the
whole plan. The mechanism is the same one: the scheduler computes what it would dispatch and
declines, so a paused run answers control requests as fast as a busy one.

A step already mid-flight is **not** touched: it runs to completion and settles normally, and its
`step.finished` lands in the stream while the run is paused. senro cannot suspend a running
command (no checkpoint, a live sandbox, open log files, a process behind a daemon on containers).

The only thing "pause the running step" could mean is *kill it*, and a pause that killed work
would be a cancel that lied about being reversible. A step's own automatic retry policy keeps
running for the same reason: that is the step's execution continuing, not new work starting.

So the promise is the narrow one, **no new work is dispatched**:

- Settling isn't dispatching, and isn't suppressed. If a step fails while the run is paused, its
  dependents still settle as `skipped_upstream_failed` right then. If you paused in order to retry
  that step, `run.rerun_from` is what puts the dependents back.
- `step.retry` is not vetoed by a pause, exactly as it isn't by a breakpoint: it dispatches an
  attempt directly. A pause makes room for `step.retry`, `step.skip` and `run.rerun_from`.
- `run.rerun_from` composes the other way round, because it hands its nodes back to the
  *scheduler*: ask for a rerun while paused and it is queued, starting when you resume.
- A paused run is told from a hung one by the `control.applied` event recording the accepted
  `run.pause`. No second event announces that the scheduler acted, unlike `breakpoint.hit`, which
  exists because arming a breakpoint and withholding a step are separated in time and in identity.
  A pause has neither separation, taking effect the instant it is accepted. Clients fold it to
  `RunInfo.Paused`, and the TUI's footer reads `run: paused`.

## Rerunning part of a live run

`run.rerun_from{step}` puts the named step and its transitive dependents back to pending and hands
them to the scheduler, which runs them exactly as the first time: same parallelism limits, retry
policy, timeouts, cache lookup and handlers. Nothing outside that set is touched.

- Each re-run step announces itself with `step.retried` under a *new* attempt number. Attempt
  numbers never restart: a step's events and its log files are both filed under one
  (`runs/<id>/logs/<step>/<attempt>/{stdout,stderr}`), so the previous execution's record stays.
- The cache isn't bypassed, and doesn't need to be. Only a `Pure` step consults the action cache,
  and its output is by definition a function of its declared inputs, so serving the cached result
  *is* re-running it. A step with side effects you want repeated is not `Pure`.
- Handlers do run again, because a rerun is a genuine second execution of the step. The previous
  pass isn't rewritten; `handler.superseded` marks it as no longer describing the step's outcome.

## Version negotiation

On first contact (`GET /api/state`, before ever subscribing), a client compares its protocol
version against the engine's. Equal major and minor is silent. Equal major with a different minor
warns once and proceeds. A different major is an error naming which side is behind, replacing what
would otherwise be a confusing JSON decode failure:

```
api: engine speaks protocol v2.0, this client speaks v1.0: upgrade your CLI
```

## Where to go next

- **[Security](/docs/attach/security/)**: who is allowed to issue a control operation at all.
- **[The TUI](/docs/attach/tui/)**: the keys that map to these operations.
- **[The browser UI](/docs/attach/browser/)**: which operations `senro ui` offers, and when.
- **[The shell](/docs/attach/shell/)**: `POST /api/shell`, the sixth route.
