// Package otelspan turns a senro run's event stream into spans. It is
// written the way one in somebody else's repository would be: it imports
// github.com/xavidop/senro and .../api and nothing else from this module
// (extension_static_test.go checks that, and extension_e2e_test.go drives it
// through a real senro.Run). It is not in senro itself because that would
// put otel's dependency graph into a library other people embed.
//
// senro carries correct W3C Trace Context in the stream (api.Event.TraceID,
// span structure in the payloads); folding that into spans is arithmetic
// over a stream any Sink can read. This one prints; every Span below has an
// exact OpenTelemetry counterpart, so swapping in a real SDK is small.
//
// Using it:
//
//	exp := otelspan.New(os.Stdout)
//	err := senro.Run(ctx, pipeline, senro.WithSink(exp))
//
// senro guarantees, so this package does not have to: a valid trace ID,
// identical across the run and unique per run; span IDs unique within the
// run; parentage that mirrors the dependency graph; and an inbound
// traceparent already continued, which is why the printed tree can have a
// root whose parent is named but absent.
package otelspan

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
)

// Kind separates the three things a senro span can be. It maps to nothing
// in OpenTelemetry (SpanKind there is client/server/producer); it is carried
// as an ordinary attribute.
type Kind string

// The three kinds; senro emits spans for only three things.
const (
	KindRun     Kind = "run"
	KindStep    Kind = "step"
	KindHandler Kind = "handler"
)

// Span is one finished span, in the shape an OpenTelemetry SDK wants: every
// field has a counterpart there, and nothing here is senro-shaped except the
// contents of Attrs, which carry a senro. prefix for that reason.
type Span struct {
	TraceID string
	SpanID  string
	// Parent is empty only for a run that started its own trace; a run
	// continuing an inbound one has a parent this run never emitted.
	Parent string
	// Links are the causal edges that could not be parentage: a step that
	// waited on five things has one parent and four links.
	Links []string
	Name  string
	Kind  Kind
	Start time.Time
	End   time.Time
	// Error is set from the step's terminal state, never inferred from an
	// exit code alone: a retried-into-success step is not an error, and one
	// cancelled by a shutdown did not fail on its own account.
	Error bool
	Attrs map[string]string
}

// Duration is End less Start, real even for spans whose start was never
// announced; see Emit on a step restored from cache.
func (s Span) Duration() time.Duration { return s.End.Sub(s.Start) }

// Exporter folds a senro event stream into spans; it satisfies senro.Sink
// and senro.Flusher. Emit runs inline on the engine's goroutine, so it only
// touches maps; everything that talks to the outside world happens in Flush,
// which senro calls after the stream is sealed, on a context detached from
// the run's own so a cancelled run still gets its trace exported. A real
// exporter hands spans to a batching processor from Emit, the same division:
// cheap in Emit, slow somewhere else.
type Exporter struct {
	w io.Writer

	mu sync.Mutex
	// open holds spans started and not yet ended, keyed by spanKey. A step
	// has at most one attempt in flight, which makes "the open span for this
	// step" unambiguous and is why step.retried carries no span ID.
	open map[string]*Span
	// done holds finished spans in completion order.
	done []Span
	// traceID is the run's, learned from the first event and unchanging.
	traceID string
}

// New returns an Exporter that writes its span tree to w when the run ends.
func New(w io.Writer) *Exporter {
	return &Exporter{w: w, open: map[string]*Span{}}
}

// spanKey namespaces a handler's key away from a step's. A handler's
// synthetic Step ("parent/kind/handler") and a real step ID may both contain
// slashes, so keying on Step alone would let the two collide and close each
// other's spans; Kind is what tells them apart.
func spanKey(kind Kind, step string) string { return string(kind) + "\x00" + step }

// Emit folds one event; the whole span model is in this switch:
//
//   - run.started and run.finished bracket the run span.
//   - step.started opens an attempt's span; step.finished closes it.
//   - step.retried closes the attempt that did NOT finish; a retried
//     attempt emits no step.finished, so without this case every failed
//     attempt of a retried step would be an unterminated span.
//   - A cached or skipped step emits no step.started; its step.finished
//     carries the parentage, and the start time is the event's timestamp
//     less the duration it reports.
//   - handler.started and its completion bracket a handler.
func (e *Exporter) Emit(ev api.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ev.TraceID != "" {
		e.traceID = ev.TraceID
	}

	switch ev.Type {
	case api.RunStarted:
		var b api.RunStartedBody
		if ev.Decode(&b) != nil || b.SpanID == "" {
			return
		}
		name := b.Pipeline
		if name == "" {
			name = "run"
		}
		e.open[spanKey(KindRun, "")] = &Span{
			TraceID: ev.TraceID, SpanID: b.SpanID, Parent: b.ParentSpanID,
			Name: name, Kind: KindRun, Start: b.StartedAt,
			Attrs: attrs(
				"senro.run", ev.Run,
				"senro.plan_digest", b.PlanDigest,
				"senro.trace_flags", b.TraceFlags,
				"senro.tracestate", b.TraceState,
				// Whether this run joined somebody else's trace is not
				// derivable from anything else here.
				"senro.trace_continued", strconv.FormatBool(b.ParentSpanID != ""),
			),
		}

	case api.RunFinished:
		var b api.RunFinishedBody
		if ev.Decode(&b) != nil {
			return
		}
		e.close(spanKey(KindRun, ""), ev.TS, map[string]string{
			"senro.status": string(b.Status),
		}, b.Status != api.RunSucceeded && b.Status != api.RunSucceededWithRecovery)

	case api.StepStarted:
		var b api.StepStartedBody
		if ev.Decode(&b) != nil || b.SpanID == "" {
			return
		}
		e.open[spanKey(KindStep, ev.Step)] = &Span{
			TraceID: ev.TraceID, SpanID: b.SpanID, Parent: b.ParentSpanID,
			Links: b.LinkedSpanIDs, Name: ev.Step, Kind: KindStep, Start: ev.TS,
			Attrs: attrs(
				"senro.step", ev.Step,
				"senro.attempt", strconv.Itoa(ev.Attempt),
				"senro.group", ev.Group,
				"senro.executor_class", b.ExecutorClass,
				"senro.platform", b.Platform,
			),
		}

	case api.StepFinished:
		var b api.StepFinishedBody
		if ev.Decode(&b) != nil {
			return
		}
		key := spanKey(KindStep, ev.Step)
		if _, live := e.open[key]; !live {
			// Never started: cached or skipped. The finish event states the
			// parentage and reports the duration, so the span can exist.
			e.open[key] = &Span{
				TraceID: ev.TraceID, SpanID: b.SpanID, Parent: b.ParentSpanID,
				Links: b.LinkedSpanIDs, Name: ev.Step, Kind: KindStep,
				Start: ev.TS.Add(-b.Duration),
				Attrs: attrs(
					"senro.step", ev.Step,
					"senro.attempt", strconv.Itoa(ev.Attempt),
					"senro.group", ev.Group,
				),
			}
		}
		a := map[string]string{"senro.state": string(b.State)}
		if b.ExitCode != 0 {
			a["senro.exit_code"] = strconv.Itoa(b.ExitCode)
		}
		if b.Cached {
			a["senro.cached"] = "true"
		}
		if b.Error != "" {
			a["senro.error"] = b.Error
		}
		if b.Reason != "" {
			a["senro.reason"] = b.Reason
		}
		// State.Failed(), not "the exit code was non-zero": a step retried
		// into success is not an error, and one cancelled by a shutdown did
		// not fail on its own account. senro publishes the terminal state so
		// an exporter never reverse-engineers it from a number.
		e.close(key, ev.TS, a, b.State.Failed())

	case api.StepRetried:
		var b api.StepRetriedBody
		if ev.Decode(&b) != nil {
			return
		}
		// b.Attempt is the attempt about to start; the one being closed is
		// the one before it, and a step has one attempt in flight.
		e.close(spanKey(KindStep, ev.Step), ev.TS, map[string]string{
			"senro.state":     "retried",
			"senro.reason":    b.Reason,
			"senro.predicate": b.Predicate,
		}, true)

	case api.HandlerStarted:
		var b api.HandlerBody
		if ev.Decode(&b) != nil || b.SpanID == "" {
			return
		}
		e.open[spanKey(KindHandler, ev.Step)] = &Span{
			TraceID: ev.TraceID, SpanID: b.SpanID, Parent: b.ParentSpanID,
			Name: ev.Step, Kind: KindHandler, Start: ev.TS,
			Attrs: attrs(
				"senro.handler_kind", b.Kind,
				"senro.parent_step", b.Parent,
			),
		}

	case api.HandlerSucceeded, api.HandlerFailed:
		var b api.HandlerBody
		if ev.Decode(&b) != nil {
			return
		}
		a := map[string]string{}
		if b.Error != "" {
			a["senro.error"] = b.Error
		}
		if b.Panicked {
			a["senro.panicked"] = "true"
		}
		e.close(spanKey(KindHandler, ev.Step), ev.TS, a, ev.Type == api.HandlerFailed)

	case api.StepLogAppended:
		var b api.StepLogAppendedBody
		if ev.Decode(&b) != nil {
			return
		}
		if s, live := e.open[spanKey(KindStep, ev.Step)]; live {
			n, _ := strconv.ParseInt(s.Attrs["senro.log_bytes"], 10, 64)
			s.Attrs["senro.log_bytes"] = strconv.FormatInt(n+b.Len, 10)
		}
	}
}

// close ends an open span, merging attributes in. A key with nothing open is
// a no-op rather than an error: a client that joined a run already in
// progress legitimately sees ends whose starts it missed.
func (e *Exporter) close(key string, end time.Time, extra map[string]string, isErr bool) {
	s, live := e.open[key]
	if !live {
		return
	}
	delete(e.open, key)
	s.End = end
	if isErr {
		s.Error = true
	}
	for k, v := range extra {
		if v != "" {
			s.Attrs[k] = v
		}
	}
	e.done = append(e.done, *s)
}

// Spans returns every span that finished, in completion order. Spans still
// open are excluded: one left open after Flush is a fact about the run, not
// a span to invent an end time for.
func (e *Exporter) Spans() []Span {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Span(nil), e.done...)
}

// Flush writes the span tree. It satisfies senro.Flusher, called after
// run.finished seals the stream, with a context detached from the run's own.
// A real exporter shuts down its batch processor here instead.
func (e *Exporter) Flush(context.Context) error {
	for _, line := range e.Report() {
		if _, err := fmt.Fprintln(e.w, line); err != nil {
			return err
		}
	}
	return nil
}

// Report renders the tree as lines, so a test can assert on it without
// capturing a writer.
func (e *Exporter) Report() []string {
	spans := e.Spans()

	byParent := map[string][]Span{}
	known := map[string]bool{}
	for _, s := range spans {
		known[s.SpanID] = true
	}
	var roots []Span
	for _, s := range spans {
		// A span whose parent is not one of ours is a root of what WE
		// emitted: the ordinary case for a continued inbound traceparent.
		if s.Parent == "" || !known[s.Parent] {
			roots = append(roots, s)
			continue
		}
		byParent[s.Parent] = append(byParent[s.Parent], s)
	}

	out := []string{fmt.Sprintf("trace %s", e.traceID)}
	var walk func(s Span, depth int)
	walk = func(s Span, depth int) {
		out = append(out, render(s, depth))
		kids := byParent[s.SpanID]
		// By start time, so the printout reads as a timeline; this only
		// orders siblings.
		sort.SliceStable(kids, func(i, j int) bool { return kids[i].Start.Before(kids[j].Start) })
		for _, k := range kids {
			walk(k, depth+1)
		}
	}
	sort.SliceStable(roots, func(i, j int) bool { return roots[i].Start.Before(roots[j].Start) })
	for _, r := range roots {
		if r.Parent != "" {
			out = append(out, fmt.Sprintf("  (parent %s, a span this run did not emit: an inbound trace)", r.Parent))
		}
		walk(r, 1)
	}
	return out
}

func render(s Span, depth int) string {
	mark := " "
	if s.Error {
		mark = "!"
	}
	// The indent is part of the name column, so the columns after it stay in
	// line however deep the tree goes.
	line := fmt.Sprintf("%s %-44s %-16s %9s",
		mark, indent(depth)+s.Name, s.SpanID, s.Duration().Round(time.Millisecond))

	keys := make([]string, 0, len(s.Attrs))
	for k := range s.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		line += fmt.Sprintf(" %s=%s", k, s.Attrs[k])
	}
	for _, l := range s.Links {
		line += fmt.Sprintf(" link=%s", l)
	}
	return line
}

func indent(depth int) string {
	s := ""
	for range depth {
		s += "  "
	}
	return s
}

// attrs builds an attribute map from alternating key/value pairs, dropping
// empty values so a span never carries a key whose answer is "nothing".
func attrs(kv ...string) map[string]string {
	m := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] != "" {
			m[kv[i]] = kv[i+1]
		}
	}
	return m
}
