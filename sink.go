package senro

import (
	"context"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
)

// Sink observes a run. Every event the engine appends to the run's ledger is
// handed to Emit, in ledger order, on the engine's own goroutine.
//
// One method, not the engine's internal Sink interface: driving a run
// (Control) is a bigger contract than watching one and already has an owner
// in the attach package. Adding control later stays additive.
//
// Emit must NOT block: the engine calls it while holding the lock that makes
// an append and its delivery one atomic unit, so a slow Emit slows the run
// and a wedged one stops it. An implementation that talks to anything must
// hand the event off and return; see the notify package's sinks.
//
// A panic in Emit does not kill the run: Run recovers it and drops the
// event, because an observer must not be able to end a build. It is still a
// bug in the sink.
//
// A Sink may also implement Reporter (record its own events in the ledger)
// and Flusher (a bounded chance to finish before Run returns); both are
// optional.
type Sink interface {
	Emit(api.Event)
}

// SinkFunc adapts an ordinary function to Sink.
//
//	senro.Run(ctx, p, senro.WithSink(senro.SinkFunc(func(e api.Event) {
//		log.Printf("%d %s %s", e.Seq, e.Type, e.Step)
//	})))
type SinkFunc func(api.Event)

// Emit calls f.
func (f SinkFunc) Emit(e api.Event) { f(e) }

// Appender appends one event to the run's ledger and reports whether it
// landed there.
//
// False means it did not and never will: the event is not one a Sink may
// append (only api.NotifyDelivered, api.NotifyFailed and api.NotifyDropped
// are), or the run's stream is already sealed. Sealed is the interesting
// one: run.finished is appended and the stream sealed in one critical
// section, so an outcome only known after run.finished (the outcome of
// delivering it always is) has no ledger left to go in, and this return
// value says so at the moment it happens. notify.Notifier writes those
// outcomes to standard error at shutdown.
//
// An alias, not a defined type, so a Sink written against this package and
// the same Sink seen through the engine's internal interface are the same
// method.
type Appender = func(api.Event) bool

// Reporter is an optional interface a Sink may implement to record its own
// events in the run's ledger, which is the only place an event is real.
//
// Run calls SetAppender once, before the run's first event, and only on a
// Sink that implements it. The Appender may be called from any goroutine,
// including after the run has ended, where it returns false.
//
// It must NOT be called from inside Emit: Emit runs under the engine's
// append lock, and an Appender call from there would deadlock the run.
// Report from the goroutine that does the work.
//
// The events a Sink may append are restricted to the notify.* set: an
// observer is authoritative about its own behaviour and about nothing else.
type Reporter interface {
	SetAppender(Appender)
}

// Flusher is an optional interface a Sink may implement to be given a
// bounded chance to finish its work before Run returns.
//
// Run calls Flush after the engine has emitted run.finished and closed the
// ledger, with a context derived via context.WithoutCancel: a cancelled run
// still wants its "cancelled" notification to go out, and that is precisely
// the run whose context is already dead. A Flusher must bound its own wait,
// since Run's context may have no deadline.
//
// The error is not propagated: a run that did everything it was asked did
// not fail because a webhook was down. A Flusher with something to say says
// it on a channel it chose; notify.Notifier writes to standard error.
type Flusher interface {
	Flush(context.Context) error
}

// WithSink hands Run a sink of the caller's own, which receives every event
// the run appends to its ledger, in order.
//
// Repeatable. Each call adds a sink, and every one of them sees every event:
//
//	senro.Run(ctx, p,
//		senro.WithSink(notifier),
//		senro.WithSink(senro.SinkFunc(func(e api.Event) { ... })),
//	)
//
// It composes with WithAttach rather than competing with it. A run given
// both feeds the attach server and every sink here from the same stream.
//
// Read Sink's own doc before writing one: Emit must not block, because the
// engine calls it inline.
func WithSink(s Sink) Option {
	return func(c *runConfig) { c.sinks = append(c.sinks, s) }
}

// externalSink adapts a caller's Sink to the engine's own interface: the
// engine needs a Control channel (there is none, control belongs to attach)
// and may want an Appender.
//
// It is also the boundary where foreign code enters the emit path, which is
// why Emit recovers: an observer must not kill a run. sink.Multi's recovery
// only covers sinks reached through a Multi, and a single WithSink on a run
// with no other observer deliberately is not, so it is not double-queued
// behind a bounded queue whose drops it could never account for. Recovering
// here keeps the policy without the second queue.
type externalSink struct{ s Sink }

func (e externalSink) Emit(ev api.Event) {
	defer func() { _ = recover() }()
	e.s.Emit(ev)
}

// Control returns nil: see Sink's own doc on why a public sink observes but
// does not drive.
func (e externalSink) Control() <-chan sink.ControlRequest { return nil }

// SetAppender forwards to the wrapped sink when it wants one, and does
// nothing otherwise. Unconditional on this type, so the engine's own check
// (does this sink implement sink.Reporter?) has one answer for every
// externalSink and the decision is made in exactly one place: here.
func (e externalSink) SetAppender(a sink.Appender) {
	if r, ok := e.s.(Reporter); ok {
		r.SetAppender(a)
	}
}

// teeSink hands each event to several observers in turn, on the caller's own
// goroutine, adding no queue and no goroutine of its own.
//
// Deliberately not sink.Multi: its bounded queue would sit in front of
// observers that each do their own queueing and overflow accounting (the
// attach hub's api.StreamEndOverflowed, notify.Notifier's notify.dropped),
// and events dropped before them would vanish from an accounting they never
// see. The fan-out stays a tee, and "Emit must not block" stays each sink's
// own promise.
type teeSink []sink.Sink

// collapse returns the cheapest sink for what is actually in the list: none
// at all costs nothing (and starts nothing, see
// TestRunWithNoOptionsStartsNoGoroutines), and one observer is that observer
// rather than a one-element loop around it.
func (t teeSink) collapse() sink.Sink {
	switch len(t) {
	case 0:
		return sink.Nop()
	case 1:
		return t[0]
	default:
		return t
	}
}

func (t teeSink) Emit(e api.Event) {
	for _, s := range t {
		s.Emit(e)
	}
}

// Control returns the first non-nil control channel, matching
// internal/sink.Multi's rule exactly: the engine reads one control channel,
// and two observers driving one run is a conflict, not a fan-out.
func (t teeSink) Control() <-chan sink.ControlRequest {
	for _, s := range t {
		if c := s.Control(); c != nil {
			return c
		}
	}
	return nil
}

// Shells returns the first non-nil interactive-session channel, matching
// Control's rule for the identical reason: two observers each handing the
// engine sessions for one run is a conflict, not a fan-out. An observer that
// hosts none (every sink but an attach hub) is skipped.
func (t teeSink) Shells() <-chan sink.ShellRequest {
	for _, s := range t {
		if h, ok := s.(sink.ShellHost); ok {
			if c := h.Shells(); c != nil {
				return c
			}
		}
	}
	return nil
}

// SetAppender reaches every observer that wants one, not the first.
// Reporting has none of the conflict Control has: two notifiers each
// recording their own delivery outcomes is the ordinary case, and first one
// wins would silently discard the second one's.
func (t teeSink) SetAppender(a sink.Appender) {
	for _, s := range t {
		if r, ok := s.(sink.Reporter); ok {
			r.SetAppender(a)
		}
	}
}
