---
layout: ../../../layouts/DocsLayout.astro
title: Step states
---

# Step states

A step ends in exactly one of ten states, never a boolean, because "did it pass" hides information a
build system should surface. This is the full set the wire protocol declares, and all ten happen
today.

| State | How a step ends there | What its dependents get |
|---|---|---|
| `succeeded` | Passed without ever failing | They run |
| `recovered` | Failed at least once, then passed on retry | They run |
| `cached` | A `Pure()` step hit the local action cache: skipped entirely, recorded outputs restored | They run |
| `failed` | Ran, failed, and exhausted any retries | `skipped_upstream_failed`, unless the step declared `ContinueOnError` |
| `timed_out` | An attempt outlived the step's `Timeout` | `skipped_upstream_failed`, unless the step declared `ContinueOnError` |
| `cancelled` | The run was cancelled before the step could finish | Nothing further is dispatched; the run ends `cancelled` |
| `panicked` | A `senro.Func` step's registered function panicked; the panic is caught and reported rather than crashing the run | `skipped_upstream_failed`, unless the step declared `ContinueOnError` |
| `skipped_upstream_failed` | A step it depends on failed | `skipped_upstream_failed`, transitively |
| `skipped_condition` | Its `When` condition was not met at run start | `skipped_condition`, transitively. `ContinueOnError` does not rescue them |
| `skipped_manual` | An operator took it out of a live run with `step.skip` | `skipped_manual`, transitively. `ContinueOnError` does not rescue them |

`cached` is a real hit, not a placeholder: the step is not run and its recorded outputs are restored
from the action cache. See [Caching a step](/docs/data/caching/).

## `recovered` is not `succeeded`

A step that failed an attempt and then passed settles as `recovered`. The two are deliberately kept
apart: collapsing them is how flaky infrastructure stays invisible for months, with a build that
needed three attempts looking identical to one that needed one.

A run full of `recovered` steps is still a passing run. It is a passing run that is telling you
something. See [Retries](/docs/steps/retries/).

## `skipped_condition` is not `skipped_upstream_failed`

senro distinguishes two ways a step can stop its dependents, and the difference is whether anything
broke.

| | Something failed | Nothing failed |
|---|---|---|
| **Cause** | An upstream step `failed`, `timed_out` or `panicked` | An upstream step was gated off by `When`, or skipped by an operator |
| **Dependents settle as** | `skipped_upstream_failed` | `skipped_condition` or `skipped_manual`, the same state as the cause |
| **Run rolls up as** | `partial`, or `failed` | Clean: `succeeded` |
| **`ContinueOnError`** | Applies: the author's explicit "run anyway" | Does not apply |

`ContinueOnError` promises a dependent survives a *failure*, not that it runs against output that
was never produced. That is why it has nothing to say about the right-hand column.

The practical consequence: a pull request's run stays green when its `Branch("main")`-gated deploy
does not fire. See [Conditions](/docs/steps/conditions/) and
[Control operations](/docs/attach/control-ops/).

## How far a failure travels

- **Only downstream.** A failing step settles its direct dependents, and theirs in turn. Unrelated
  branches are not cancelled: they run to completion, so a failure produces one clear report instead
  of a half-explored graph.
- **A skip does not poison the graph either.** Only the transitive dependents of a
  `skipped_condition` or `skipped_manual` step are affected.

## The run's own rollup

`run.finished` carries a status and a per-state count of the steps:

```json
{"type":"run.finished","payload":{"status":"failed",
  "steps":{"failed":1,"skipped_upstream_failed":1,"succeeded":1}}}
```

Five statuses exist, in this precedence, strongest first:

| Run status | When |
|---|---|
| `cancelled` | Any step is `cancelled`. It outranks failure, since a step that failed while the run was being torn down says nothing useful about the workload |
| `failed` | Any step is `failed`, `timed_out` or `panicked` |
| `partial` | Nothing failed, but some step is `skipped_upstream_failed` |
| `succeeded_with_recovery` | Nothing above, and some step is `recovered` |
| `succeeded` | Everything else. `cached`, `skipped_condition` and `skipped_manual` all roll up clean |

## Where to go next

- **[Retries](/docs/steps/retries/)**: what produces `recovered` instead of `failed`.
- **[Handlers](/docs/steps/handlers/)**: `SENRO_FAILURE_STATE` carries one of these values.
- **[Conditions](/docs/steps/conditions/)**: what produces `skipped_condition`.
- **[Control operations](/docs/attach/control-ops/)**: `step.skip` and `skipped_manual`.
- **[The event stream](/docs/reference/event-stream/)**: where a state is recorded.
- **[Reading a failed run](/docs/reference/debugging/)**: reading these states out of a run.
