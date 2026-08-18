---
layout: ../../../layouts/DocsLayout.astro
title: Extending senro
---

# Extending senro

Every seam in senro is a small Go interface you implement in your own module. There is nothing to
register, no plugin loader, and nothing under `internal/` to import.

## The five extension points

| You want to | Implement | Page |
| --- | --- | --- |
| Fan out over a layout no shipped graph reads | `senro.UnitGraph`, `senro.UnitAffector` | [Unit graph](/docs/extend/unit-graph/) |
| Trigger on an event source senro does not parse | `trigger.Provider`, `trigger.Matcher` | [Trigger source](/docs/extend/trigger-source/) |
| Send a run's result somewhere senro has no destination for | `notify.Renderer`, `notify.Requester` | [Notifier](/docs/extend/notifier/) |
| Turn the event stream into traces, metrics or anything else | `senro.Sink` | [Trace exporter](/docs/extend/exporter/) |
| Have a program explain a failed step | `senro.Analyzer` | [Failure analyzer](/docs/extend/analyzer/) |

Each page ends with a worked example. The analyzer has two: a provider-free one you can run with no
key, and [`contrib/genkitanalyzer`](/docs/extend/analyzer-genkit/), a shipping package backed by a
real model that you install rather than copy.

## What they have in common

- **Structural satisfaction.** A type with the right methods is the interface. No registry, no
  build tag, no `init`.
- **A narrow import.** Each one needs `github.com/xavidop/senro` or one of its public
  subpackages, and nothing else of senro's. The test suite checks that mechanically for the
  worked examples rather than taking it on trust.
- **Your errors are senro's errors.** Every seam has exactly one way to say "I could not answer",
  and senro turns it into a message naming your implementation. A panic becomes the same error;
  none of them can end a run.
- **The built-ins take the same path.** `trigger.GitHub()` is a `Provider`, `notify.Webhook` is a
  `To(url, EventJSON(), ...)`, `notify.GitHubChecks` is a `Requester`. There is no private
  shortcut, so the public path is the tested one.
- **A worked example that compiles.** Each page links one under
  [`examples/`](https://github.com/xavidop/senro/tree/main/examples), driven end to end by senro's
  own tests.

## The smaller seams

These are extension points too, but they are small enough to be documented where they are used:

| Seam | What it is | Where |
| --- | --- | --- |
| `senro.RegisterFunc` | A Go function as a step kind, instead of a command | [Function steps](/docs/steps/functions/) |
| `change.Source` | Where "what changed" comes from, when it is not a trigger | [Affected sets](/docs/monorepo/affected/) |
| `senro.DurationHistory` | How long each unit took last time, which is what `Partition` balances by | [Partitioning](/docs/monorepo/partition/) |
| `senro.Flusher`, `senro.Reporter` | Optional interfaces a `Sink` may also implement | [Trace exporter](/docs/extend/exporter/) |
| `notify.ResponseReader` | A `Requester` that needs to read the response it got | [Notifier](/docs/extend/notifier/) |

## What is deliberately not a seam

- **The executor.** Local, container, Kubernetes and ssh are the four, and `senro.ExecutorTarget`
  is closed. See [Executors](/docs/executors/).
- **`trigger.Option`.** Its method is unexported and `trigger.Matcher` is the way in, because the
  set of questions a trigger can ask has to stay the set senro can render into a run's record.
- **`api.Remedy`.** An analyzer's remedy comes from a closed vocabulary of one, so the most an
  unsupervised run can do is retry a step. See [Failure analyzer](/docs/extend/analyzer/).

## Where to go next

- **[The event stream](/docs/reference/event-stream/)**: what a `Sink` and a notifier are reading.
- **[Embedding](/docs/reference/embedding/)**: running senro from your own program, where most of
  these get wired.
- **[The `api` package](/docs/reference/api/)**: the wire contract every extension shares.
