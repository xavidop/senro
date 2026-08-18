// Package notify sends a run's events somewhere outside the process: a
// webhook, a Slack channel, anything that speaks HTTP.
//
// A notifier is a senro.Sink:
//
//	n := notify.New(
//		notify.Slack(os.Getenv("SLACK_WEBHOOK_URL")),
//		notify.Webhook("https://ci.example.com/hooks/senro",
//			notify.Sign(os.Getenv("SENRO_HOOK_SECRET")),
//			notify.On(api.RunStarted, api.StepFinished, api.RunFinished)),
//	)
//	defer func() { _ = n.Close() }()
//
//	err := senro.Run(ctx, pipeline, senro.WithSink(n))
//
// Run flushes the notifier before it returns. Close is needed only for a
// notifier never handed to Run, and is harmless after a flush.
//
// Guarantees: delivery never blocks the run. Each destination has a bounded
// queue and its own goroutine; a full queue drops the event, and drops are
// counted and recorded as api.NotifyDropped. Delivery is at-least-once, with
// an X-Senro-Delivery header (unique per event per destination) as the key a
// receiver should deduplicate on. Every outcome is itself a ledger event
// (api.NotifyDelivered, api.NotifyFailed, api.NotifyDropped); a notifier
// skips its own notify.* events, which is what prevents the feedback loop.
// Events arrive already redacted by the engine, upstream of every sink (see
// senro.WithSecrets), so this package does no secret filtering of its own.
//
// One outcome cannot be an event: run.finished is appended and the ledger
// sealed in a single critical section, so by the time the outcome of
// delivering run.finished is known, the stream it would join has closed.
// Those outcomes, and only those, are written to standard error as the run
// shuts down. The split is exact, not a heuristic: whatever the engine's
// appender rejects is what gets printed. See senro.Appender.
//
// Webhook and Slack are ordinary destinations, not privileged ones: Webhook
// is To(url, EventJSON(), Named("webhook")) and Slack is To(url, SlackText(),
// Named("slack"), On(api.RunFinished)). To plus a Renderer is the public way
// to add a destination kind, inheriting the queue, retries, signing, outcome
// events and flush. See Renderer for the contract third-party rendering code
// runs under.
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
)

// DefaultGrace bounds how long Flush waits for queued notifications to go
// out once the run has ended. Long enough for a retry or two against a
// healthy endpoint, short enough that a wedged one cannot hold a CI job
// open.
const DefaultGrace = 10 * time.Second

// settleGrace bounds the second wait in Flush: how long an already cancelled
// destination gets to actually stop. senro's own delivery path settles the
// instant it is cancelled, so this expires only for code senro did not write
// (a Renderer that blocks forever, a transport that ignores cancellation).
// A destination still running when it expires is abandoned: its goroutine is
// left parked and the destination is named in the shutdown report.
const settleGrace = time.Second

// ownTypes are the events a notifier produces. Emit skips them; delivering
// them would produce outcomes that are themselves delivered, without end.
var ownTypes = map[api.Type]bool{
	api.NotifyDelivered: true,
	api.NotifyFailed:    true,
	api.NotifyDropped:   true,
}

// Option configures a Notifier. A *Destination is one, so destinations and
// settings go in the same call:
//
//	notify.New(notify.WithGrace(time.Minute), notify.Slack(url))
type Option interface {
	applyNotifier(*Notifier)
}

type optionFunc func(*Notifier)

func (f optionFunc) applyNotifier(n *Notifier) { f(n) }

// WithGrace overrides how long Flush waits for queued notifications before
// giving up on them and reporting what did not go out. See DefaultGrace.
func WithGrace(d time.Duration) Option {
	return optionFunc(func(n *Notifier) { n.grace = d })
}

// WithReportWriter redirects the shutdown report (outcomes that arrived
// after the run's event stream closed; see the package doc) away from
// standard error.
func WithReportWriter(w io.Writer) Option {
	return optionFunc(func(n *Notifier) { n.reportw = w })
}

// bound is one destination plus the queue in front of it.
//
// The queue is a sink.Multi of exactly one sink: Multi is this repo's
// bounded drop-rather-than-block queue, with drop accounting, a worker
// goroutine and panic isolation already tested. One queue per destination
// rather than one shared fanout, because filtering must happen BEFORE the
// queue: a destination that only wants run.finished must not have its queue
// filled by events it will discard, or the one event it exists for is the
// one that gets dropped.
type bound struct {
	d *Destination
	q sink.Fanout

	// dropped is this destination's running loss count, reported in each
	// api.NotifyDropped so a slow endpoint reads as a climbing number.
	dropped atomic.Int64
}

// Notifier delivers a run's events to one or more destinations. It is a
// senro.Sink; hand it to senro.WithSink.
//
// One Notifier serves one run. It takes the run's ledger appender at the
// start and reports what it could not record at the end, both of which are
// specific to that run, and Flush stops its queues for good.
type Notifier struct {
	bs      []*bound
	grace   time.Duration
	reportw io.Writer

	// reports hands outcomes to the one goroutine allowed to record them.
	// Emit runs inside the engine's append critical section, so appending
	// from there would deadlock the run against itself; the queue is what
	// makes reporting a drop safe from inside Emit.
	reports sink.Fanout

	// ctx is cancelled when Flush runs out of grace, which is what makes an
	// in-flight request and a retry wait interruptible. Deliberately rooted
	// at Background rather than at the run's context: the run whose
	// notification matters most is often the one that was just cancelled.
	ctx   context.Context
	abort context.CancelFunc

	mu         sync.Mutex
	appender   func(api.Event) bool
	unrecorded []string

	flushOnce sync.Once
	flushErr  error
}

// New returns a Notifier delivering to the given destinations. A Notifier
// with no destination is valid and does nothing, so a caller can build one
// from configuration without special-casing the empty case.
//
// It starts one goroutine per destination, plus one for recording outcomes.
// Flush or Close stops them.
func New(opts ...Option) *Notifier {
	n := &Notifier{grace: DefaultGrace, reportw: os.Stderr}
	n.ctx, n.abort = context.WithCancel(context.Background())
	for _, o := range opts {
		o.applyNotifier(n)
	}
	for _, b := range n.bs {
		b.q = sink.Multi(sink.FuncSink(func(e api.Event) { n.deliver(b, e) }))
	}
	n.reports = sink.Multi(sink.FuncSink(n.record))
	return n
}

// Emit queues one event for every destination that wants it, and returns.
// It never blocks and never fails: the engine calls this inline, holding the
// lock that makes an append and its delivery atomic.
func (n *Notifier) Emit(e api.Event) {
	if ownTypes[e.Type] {
		return
	}
	for _, b := range n.bs {
		if !b.d.wants(e.Type) {
			continue
		}
		// Emit on a full queue is a silent no-op, so a drop is only visible
		// by comparing DroppedTotal around it. DroppedTotal, not the map
		// Dropped allocates: this runs once per event per destination.
		before := b.q.DroppedTotal()
		b.q.Emit(e)
		if lost := b.q.DroppedTotal() - before; lost > 0 {
			total := b.dropped.Add(int64(lost))
			n.report(outcome{
				dest: b.d, about: e, typ: api.NotifyDropped, dropped: int(total),
			})
		}
	}
}

// SetAppender receives the run's ledger appender. See senro.Reporter.
func (n *Notifier) SetAppender(a func(api.Event) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.appender = a
}

// Flush delivers what is still queued, records what outcomes it can, and
// reports the ones it cannot. Run calls it; a caller driving a Notifier
// itself calls Close.
//
// Bounded by ctx and by the notifier's own grace (see WithGrace), whichever
// comes first, so a wedged endpoint delays a process by at most the grace.
// A cancelled destination that still does not stop gets one further short
// window and is then abandoned and named in the shutdown report; see
// settleGrace.
//
// Idempotent: the first call does the work, later calls return the same
// error.
func (n *Notifier) Flush(ctx context.Context) error {
	n.flushOnce.Do(func() { n.flushErr = n.flush(ctx) })
	return n.flushErr
}

// Close is Flush with no deadline of the caller's own, so the notifier's own
// grace is the only bound. Suitable for a defer.
func (n *Notifier) Close() error { return n.Flush(context.Background()) }

func (n *Notifier) flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, n.grace)
	defer cancel()
	defer n.abort()

	// Closing a queue drains it before Close returns; that drain is rule
	// four (run.finished is delivered before Run returns). One closer
	// goroutine per destination, so a slow one does not delay the rest and
	// so the wait below can name the destination that would not stop.
	done := make([]chan struct{}, len(n.bs))
	for i, b := range n.bs {
		ch := make(chan struct{})
		done[i] = ch
		go func(b *bound, ch chan struct{}) {
			defer close(ch)
			_ = b.q.Close()
		}(b, ch)
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for _, ch := range done {
			<-ch
		}
	}()

	var abandoned []string
	select {
	case <-drained:
	case <-ctx.Done():
		// Out of grace. Cancelling n.ctx aborts the in-flight request and
		// any retry wait, so the workers settle promptly and still produce
		// their outcomes. Third-party code (a Renderer, a transport) may
		// ignore the cancellation, so the second wait is bounded too; see
		// settleGrace.
		n.abort()
		t := time.NewTimer(settleGrace)
		defer t.Stop()
		select {
		case <-drained:
		case <-t.C:
			for i, ch := range done {
				select {
				case <-ch:
				default:
					abandoned = append(abandoned, n.bs[i].d.name)
				}
			}
		}
	}

	// Every delivery that was going to settle has settled, so every outcome
	// is in the report queue; draining it is its last chance at the ledger.
	_ = n.reports.Close()

	return n.writeReport(abandoned)
}

// outcome is one delivery's result, the single form both the ledger event
// and the operator-facing line are built from.
type outcome struct {
	dest     *Destination
	about    api.Event
	typ      api.Type
	attempts int
	status   int
	dur      time.Duration
	err      error
	dropped  int
	// skip marks an event a stateful destination had nothing to say about
	// (see Requester). Neither a delivery nor a failure, and recorded as
	// neither: no request was ever made.
	skip bool
}

// event renders the outcome as the api.Event it will be recorded as.
func (o outcome) event() api.Event {
	body := api.NotifyBody{
		Destination: o.dest.name,
		Event:       o.about.Type,
		Seq:         o.about.Seq,
		Attempts:    o.attempts,
		Status:      o.status,
		Duration:    o.dur,
		Dropped:     o.dropped,
	}
	if o.err != nil {
		body.Error = o.dest.sanitize(o.err)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		// api.NotifyBody is plain scalars, so this cannot fail; if it ever
		// does, the envelope alone is still a truthful event.
		raw = nil
	}
	return api.Event{
		Type:    o.typ,
		Step:    o.about.Step,
		Attempt: o.about.Attempt,
		Payload: raw,
	}
}

// lineFor renders one outcome for standard error, used only when the ledger
// could not take the event. Deliberately derived from the event's payload
// rather than from the outcome, so the ledger entry and this line are the
// same claim by construction.
func lineFor(e api.Event) string {
	var body api.NotifyBody
	if err := e.Decode(&body); err != nil {
		return fmt.Sprintf("  %s (payload unreadable: %v)", e.Type, err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  %s: %s ", body.Destination, body.Event)
	switch e.Type {
	case api.NotifyDelivered:
		fmt.Fprintf(&b, "delivered, HTTP %d, %s in %s",
			body.Status, plural(body.Attempts, "attempt"), readable(body.Duration))
	case api.NotifyDropped:
		fmt.Fprintf(&b, "dropped without being sent, %d lost to a full queue so far", body.Dropped)
	default:
		fmt.Fprintf(&b, "NOT delivered after %s in %s: %s",
			plural(body.Attempts, "attempt"), readable(body.Duration), body.Error)
	}
	return b.String()
}

// readable rounds a duration to a precision worth reading at its own scale;
// a flat millisecond rounding would report a local delivery as "0s".
func readable(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(time.Millisecond)
	default:
		return d.Round(time.Microsecond)
	}
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// report hands an outcome to the recording goroutine. Non-blocking, because
// its callers are the engine's own emit path (a drop, from Emit) and a
// destination worker, and neither may wait on the other.
func (n *Notifier) report(o outcome) {
	if o.skip {
		return
	}
	n.reports.Emit(o.event())
}

// record is the one goroutine that touches the ledger. It runs on the report
// queue's worker, never on the engine's, which is what keeps an append from
// deadlocking against the append that produced the event. The appender is
// read under the lock and called outside it: calling under n.mu while
// blocked on the engine's append lock would deadlock against Emit.
func (n *Notifier) record(e api.Event) {
	n.mu.Lock()
	ap := n.appender
	n.mu.Unlock()

	if ap != nil && ap(e) {
		return
	}
	// The ledger would not take it: no ledger at all, or the stream is
	// sealed and this is the outcome of delivering the event that sealed
	// it. Keep it for the shutdown report; see the package doc.
	n.mu.Lock()
	n.unrecorded = append(n.unrecorded, lineFor(e))
	n.mu.Unlock()
}

// writeReport says out loud what the ledger could not take, and which
// destinations never stopped (empty unless third-party code ignored
// cancellation; see settleGrace).
func (n *Notifier) writeReport(abandoned []string) error {
	n.mu.Lock()
	lines := append([]string(nil), n.unrecorded...)
	n.mu.Unlock()

	lost := n.reports.DroppedTotal()
	if len(lines) == 0 && lost == 0 && len(abandoned) == 0 {
		return nil
	}
	var b strings.Builder
	if len(lines) > 0 || lost > 0 {
		fmt.Fprintf(&b, "senro notify: %s arrived after this run's event stream closed, so %s reported here instead of in the ledger:\n",
			plural(len(lines), "delivery outcome"), pronoun(len(lines)))
		for _, l := range lines {
			b.WriteString(l)
			b.WriteString("\n")
		}
		if lost > 0 {
			fmt.Fprintf(&b, "  and %d further outcome(s) were lost before they could be recorded at all\n", lost)
		}
	}
	if len(abandoned) > 0 {
		fmt.Fprintf(&b, "senro notify: %s still running when this run's grace ran out, did not stop when cancelled, and %s abandoned; whatever %s was delivering may not have arrived:\n",
			plural(len(abandoned), "destination"), pronoun2(len(abandoned)), pronoun3(len(abandoned)))
		for _, name := range abandoned {
			fmt.Fprintf(&b, "  %s\n", name)
		}
	}
	if _, err := io.WriteString(n.reportw, b.String()); err != nil {
		return err
	}
	if len(lines)+lost == 0 {
		return fmt.Errorf("notify: %s did not stop when this run's grace ran out", plural(len(abandoned), "destination"))
	}
	err := fmt.Errorf("notify: %s could not be recorded in the run's event stream", plural(len(lines)+lost, "delivery outcome"))
	if len(abandoned) > 0 {
		return fmt.Errorf("%w, and %s did not stop when this run's grace ran out", err, plural(len(abandoned), "destination"))
	}
	return err
}

func pronoun(n int) string {
	if n == 1 {
		return "it is"
	}
	return "they are"
}

func pronoun2(n int) string {
	if n == 1 {
		return "was"
	}
	return "were"
}

func pronoun3(n int) string {
	if n == 1 {
		return "it"
	}
	return "they"
}
