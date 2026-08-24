---
layout: ../../../layouts/DocsLayout.astro
title: Writing a trace exporter
---

# Writing a trace exporter

A `senro.Sink` receives every event the engine appends to a run's ledger, in ledger order. Reach for
one to fold that stream into something else: OpenTelemetry spans, metrics, a database row per step.

senro carries W3C Trace Context through the stream but ships no OpenTelemetry exporter, so this page
uses spans as the example. The split is deliberate: senro's job is trace context that is **correct**,
and turning that into spans is arithmetic over data anybody can read, against the
`go.opentelemetry.io/otel` version you already have.

## The interface

```go
package senro

type Sink interface {
	Emit(api.Event)
}

func WithSink(s Sink) Option
```

That is the whole surface, and it is the same one [notifications](/docs/notifications/) are built
on. Two optional interfaces are under [the contract](#what-senro-guarantees-you).

## The smallest one that works

```go
func (e *Exporter) Emit(ev api.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch ev.Type {
	case api.StepStarted:
		var b api.StepStartedBody
		if ev.Decode(&b) != nil || b.SpanID == "" {
			return
		}
		e.open[ev.Step] = &Span{
			TraceID: ev.TraceID, SpanID: b.SpanID, Parent: b.ParentSpanID,
			Links: b.LinkedSpanIDs, Name: ev.Step, Start: ev.TS,
		}

	case api.StepFinished:
		var b api.StepFinishedBody
		if ev.Decode(&b) != nil {
			return
		}
		e.close(ev.Step, ev.TS, b.State.Failed())
	}
}
```

That produces a span per step and misses three cases, which are
[below](#the-four-events-that-end-a-span).

## What is in the stream

Every event carries the trace. The span structure is in the payloads, because unlike the trace ID it
is not constant.

| Where | Field | What it is |
| --- | --- | --- |
| every event | `trace_id` | 32 lowercase hex characters, identical on every event of the run |
| `run.started` | `span_id` | the run's own span, the root of everything this run emits |
| `run.started` | `parent_span_id` | the inbound span, absent when senro started the trace |
| `run.started` | `trace_flags` | the W3C flags byte, two lowercase hex characters |
| `run.started` | `tracestate` | the inbound vendor state, verbatim |
| `run.finished` | `span_id` | the run span again, so the last event closes it on its own |
| `step.started` | `span_id` | this **attempt's** span |
| `step.started` | `parent_span_id` | from the graph, never from the clock |
| `step.started` | `linked_span_ids` | the needs that could not be the parent |
| `step.finished` | `span_id` | the span to close |
| `step.finished` | `parent_span_id` | present only when this event opened the span |
| `handler.*` | `span_id`, `parent_span_id` | one handler run, parented on the attempt that triggered it |

Everything above is `omitempty`, and every field senro adds within protocol v1 will be. Ignore what
you do not recognise.

## The span model

- **One span per run.** Its parent is whatever started senro, when something did.
- **One span per attempt, not per step.** A step retried three times produces three `step.started`
  events with three span IDs. One merged span would report a step that took the sum of every attempt
  and succeeded, which did not happen.
- **Parentage comes from the graph, never from the clock.** A step's span hangs off the first of its
  `Needs` in plan order; a step with no needs hangs off the run. Two steps that ran back to back only
  because `MaxParallel` was 1 are **siblings**, since nesting them would report a pipeline with no
  parallelism at all, which is exactly what somebody opened the trace to find out about.
- **A span has one parent; a step may wait on many.** The needs that could not be the parent are in
  `linked_span_ids`, which is what OpenTelemetry links are for: causality without containment.

### The four events that end a span

Three are easy to miss, and each one missed leaves a span that never closes or work absent from the
trace.

- **`step.finished` closes the attempt.** The ordinary case.
- **`step.retried` also closes an attempt.** An attempt that will be retried emits no `step.finished`
  at all. Close the step's currently open span here; no span ID is carried and none is needed,
  because a step has at most one attempt in flight.
- **`step.finished` sometimes opens the span too.** A step restored from cache emits `cache.hit`,
  `ws.restored` and `step.finished` with no `step.started` anywhere, and a step skipped by a false
  `When` condition or a failed upstream is the same shape. Their finish event carries
  `parent_span_id` and `linked_span_ids` precisely so the span can exist, and the start time is the
  event's timestamp less the `duration_ns` it reports.
- **`handler.succeeded` and `handler.failed` close a handler.** Handlers emit **no**
  `step.log.appended` markers, so anything that models a run by walking what steps logged misses them
  completely.

## The contract

### What you must guarantee

**`Emit` must not block.** It is called inline on the engine's goroutine, holding the lock that makes
a ledger append and its delivery a single atomic unit, so a slow `Emit` slows the whole run and a
wedged one stops it. Do only what is cheap: touch a map and return, and hand each finished span to a
batching span processor.

### What senro guarantees you

- Every event the run appends, in ledger order, exactly once. `WithSink` is repeatable and composes
  with `senro.WithAttach`: a run given both feeds the attach socket and every sink from one stream.
- **`senro.Flusher`** gives you a bounded chance to finish after the run's stream is sealed, on a
  context derived with `context.WithoutCancel`, so a **cancelled** run still exports its trace. That
  is the run whose trace somebody actually wants. Shut your span processor down here.
- **`senro.Reporter`** hands you an appender for recording your own outcomes in the run's ledger. An
  exporter rarely needs it; a notifier does.

### What happens on error

There is nowhere to return one, deliberately: an observer must not be able to end a build. A panic in
`Emit` is recovered and the event dropped. It is still a bug in your sink.

## Wire it into a run

```go
err := senro.Run(ctx, pipeline, senro.WithSink(otelspan.New(os.Stdout)))
```

### Continue an inbound trace

This is the highest-value part and needs no code from you. senro reads `TRACEPARENT` and `TRACESTATE`
from its own environment, both spellings, which every CI system and deploy tool that has a trace
exports. Such a run carries the upstream trace ID on every event and the upstream span as
`run.started`'s `parent_span_id`. From an embedder that already holds a span:

```go
sc := trace.SpanContextFromContext(ctx)
err := senro.Run(ctx, pipeline, senro.WithSink(exporter),
	senro.WithTraceContext(
		fmt.Sprintf("00-%s-%s-%02x", sc.TraceID(), sc.SpanID(), sc.TraceFlags()),
		sc.TraceState().String()))
```

`WithTraceContext` wins over the environment, including when given empty strings, which is how a
caller says "this run is a root, ignore the ambient variables".

> A **malformed** traceparent is ignored and the run starts a fresh trace. It is never salvaged and
> never a reason to refuse to run: half a broken trace ID joined to a fresh span would be a link to a
> trace that does not exist.

### The trace continues outward

Every step's command is launched with `TRACEPARENT` set to **that attempt's own span**, so a tool
that reads it becomes a child of the step it ran in. Your exporter does nothing to get this.

- **The attempt's span, not the run's**: one shared parent would flatten the trace into a list.
  Handlers too, from the span `handler.started` published, and `TRACESTATE` travels with it.
- **On every executor**: local, container, Kubernetes and ssh. On Kubernetes it is ordinary pod env,
  readable by anybody who can read the pod, which is fine for two random identifiers and is exactly
  why secrets are files there instead.
- **A step that declares its own `TRACEPARENT` (or `traceparent`) wins**: senro leaves it alone and
  exports no tracestate beside it, because vendor state belongs to the trace its traceparent named.
- **It never enters the cache key.** The key's env component digests only the names a step declared
  in [`CacheEnv`](/docs/data/caching/), built from the step's declared environment rather than what
  the command is finally launched with. A value that changes every run would otherwise mean a pure
  step never hits again.

## The worked example

[`examples/extensions/otelspan`](https://github.com/xavidop/senro/tree/main/examples/extensions/otelspan)
handles all four closing events and imports `github.com/xavidop/senro/api` and the standard library
and nothing else from senro, which the test suite checks mechanically.

Run it against a real pipeline with `go run ./examples/otelexport`, whose pipeline is deliberately
awkward: `lint` and `test` in
parallel off `fetch`, `test` failing once and recovering, `audit` skipped by a false condition,
`package` waiting on two things, `deploy` with an `Always` handler. Six steps, nine spans:

```
trace 254ace9a1767055bbbec4d35943b14ba
    release                     cd2d9cef6e314db2  1.419s senro.status=succeeded_with_recovery
      fetch                     663ca0a6d8208e8b    11ms senro.attempt=1 senro.state=succeeded
        audit                   65431cefc7ffe417      0s senro.state=skipped_condition
!       test                    01ef82c82a778c69    21ms senro.attempt=1 senro.state=retried
        lint                    170842380fc784dd   229ms senro.attempt=1 senro.state=succeeded
          package               d314461326c97a28    16ms link=ed641a4c5d0081c8
            deploy              d370199f0c116957    12ms senro.state=succeeded
              deploy/always/release-lock  ae63ed6490deb3d6  13ms senro.handler_kind=always
        test                    ed641a4c5d0081c8    16ms senro.attempt=2 senro.state=recovered
```

Both attempts at `test` are children of `fetch`, not of each other; `lint` and `test` are siblings
even though one finished first; `audit` is present with a zero duration and a reason rather than
absent; `package` links the attempt at `test` that actually succeeded.

For unit tests, feed it events and assert on the spans: you need no engine and no collector. For the
end-to-end shape, run a real pipeline with `senro.WithSink(exp)` and check that every span walks up
to a single root with no dangling parent and no cycle. That one assertion catches every parentage
mistake worth catching.

## Where to go next

- **[The event stream](/docs/reference/event-stream/)**: every event type and payload, which is what you are folding.
- **[Writing a notifier](/docs/notifications/custom/)**: another extension point built on the same sink.
- **[Run options and outcomes](/docs/reference/run-options/)**: where `WithTraceContext` earns its keep.
