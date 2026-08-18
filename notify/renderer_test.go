package notify_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/notify"
)

// capture is an endpoint that keeps every request it was handed, so a test
// can assert on the bytes a Renderer produced rather than on the Renderer
// being called.
type capture struct {
	srv *httptest.Server

	mu   sync.Mutex
	reqs []captured
}

type captured struct {
	header http.Header
	body   string
}

func newCapture(t *testing.T) *capture {
	t.Helper()
	c := &capture{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		n, _ := r.Body.Read(b)
		c.mu.Lock()
		c.reqs = append(c.reqs, captured{header: r.Header.Clone(), body: string(b[:n])})
		c.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *capture) url() string { return c.srv.URL }

func (c *capture) all() []captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]captured(nil), c.reqs...)
}

func (c *capture) one(t *testing.T) captured {
	t.Helper()
	got := c.all()
	if len(got) != 1 {
		t.Fatalf("the endpoint received %d requests, want exactly 1", len(got))
	}
	return got[0]
}

// ─────────────────────────────────────────────────────────────────────────────
// The public extension point: a Renderer somebody else wrote.
// ─────────────────────────────────────────────────────────────────────────────

// TestToDeliversWhatTheRendererProduced is the whole point of the surface: a
// body this package has never heard of goes out unchanged, with the headers
// every destination carries, and lands as a notify.delivered in the ledger.
func TestToDeliversWhatTheRendererProduced(t *testing.T) {
	c := newCapture(t)
	l := &ledger{}

	n := notify.New(notify.To(c.url(),
		notify.RendererFunc(func(e api.Event) ([]byte, error) {
			return []byte("event=" + string(e.Type) + " seq=" + strconv.FormatUint(e.Seq, 10)), nil
		}),
		notify.Named("pager"),
		notify.ContentType("text/plain"),
		notify.Header("X-Api-Key", "routing-key"),
	))
	n.SetAppender(l.append)
	n.Emit(event(7, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := c.one(t)
	if got.body != "event=run.finished seq=7" {
		t.Errorf("body = %q, want the renderer's own bytes", got.body)
	}
	if ct := got.header.Get("Content-Type"); ct != "text/plain" {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if k := got.header.Get("X-Api-Key"); k != "routing-key" {
		t.Errorf("X-Api-Key = %q, want the header the destination was given", k)
	}
	// The generic headers a third-party destination inherits without asking.
	if d := got.header.Get(notify.HeaderDelivery); d != "01TESTRUN/7" {
		t.Errorf("%s = %q, want the run/seq deduplication key", notify.HeaderDelivery, d)
	}
	if ty := got.header.Get(notify.HeaderEvent); ty != "run.finished" {
		t.Errorf("%s = %q", notify.HeaderEvent, ty)
	}

	delivered := l.ofType(api.NotifyDelivered)
	if len(delivered) != 1 {
		t.Fatalf("ledger has %d notify.delivered events, want 1: %v", len(delivered), l.all())
	}
	if b := bodyOf(t, delivered[0]); b.Destination != "pager" {
		t.Errorf("notify.delivered names destination %q, want pager", b.Destination)
	}
}

// TestAThirdPartyRendererInheritsSigningAndRetry proves the surface is a
// renderer and not a destination kind: the signature covers the bytes the
// renderer produced, and a 500 is retried, with neither written by the
// third party.
func TestAThirdPartyRendererInheritsSigningAndRetry(t *testing.T) {
	var mu sync.Mutex
	var sigs []string
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		n, _ := r.Body.Read(b)
		mu.Lock()
		sigs = append(sigs, r.Header.Get(notify.HeaderSignature))
		bodies = append(bodies, string(b[:n]))
		attempt := len(bodies)
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l := &ledger{}
	n := notify.New(notify.To(srv.URL,
		notify.RendererFunc(func(api.Event) ([]byte, error) { return []byte(`{"custom":true}`), nil }),
		notify.Named("custom"),
		notify.Sign("shh"),
		notify.Retry(3, time.Millisecond),
	))
	n.SetAppender(l.append)
	n.Emit(event(1, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("the endpoint saw %d requests, want 2 (a 500 then a retry)", len(bodies))
	}
	for i, s := range sigs {
		if !strings.HasPrefix(s, "v1=") {
			t.Errorf("request %d carried signature %q, want a v1= HMAC over the renderer's body", i+1, s)
		}
	}
	if len(l.ofType(api.NotifyDelivered)) != 1 {
		t.Errorf("ledger = %v, want one notify.delivered", l.all())
	}
}

// TestARendererThatPanicsIsAFailedDeliveryNotADeadRun: sink.Multi would
// swallow the panic silently, losing the outcome with it, so a panicking
// renderer must instead be an ordinary failed delivery naming what happened.
func TestARendererThatPanicsIsAFailedDeliveryNotADeadRun(t *testing.T) {
	c := newCapture(t)
	l := &ledger{}

	n := notify.New(notify.To(c.url(),
		notify.RendererFunc(func(e api.Event) ([]byte, error) {
			if e.Seq == 1 {
				panic("a third party's renderer dereferenced nil")
			}
			return []byte("fine"), nil
		}),
		notify.Named("panicky"),
	))
	n.SetAppender(l.append)
	n.Emit(event(1, api.StepFinished))
	n.Emit(event(2, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	failed := l.ofType(api.NotifyFailed)
	if len(failed) != 1 {
		t.Fatalf("ledger has %d notify.failed events, want 1: %v", len(failed), l.all())
	}
	b := bodyOf(t, failed[0])
	if !strings.Contains(b.Error, "renderer panicked") {
		t.Errorf("notify.failed error = %q, want it to name the RENDERER as what panicked, "+
			"which is the difference between a bug a reader can find and one they cannot", b.Error)
	}
	if !strings.Contains(b.Error, "dereferenced nil") {
		t.Errorf("notify.failed error = %q, want the panic value in it", b.Error)
	}
	// And the destination is still alive: the next event went out.
	if got := c.one(t); got.body != "fine" {
		t.Errorf("after the panic the endpoint received %q, want the next event delivered", got.body)
	}
	if len(l.ofType(api.NotifyDelivered)) != 1 {
		t.Errorf("ledger = %v, want the event after the panic to be delivered", l.all())
	}
}

// panicTransport is an http.RoundTripper that panics: the other piece of
// foreign code on a destination's worker goroutine (see notify.Client).
type panicTransport struct{}

func (panicTransport) RoundTrip(*http.Request) (*http.Response, error) {
	panic("a custom transport blew up")
}

// TestAPanicAnywhereOnADestinationsWorkerStillProducesAnOutcome is the
// backstop: the queue's worker already recovers, so without send's own
// recover this event would simply have no outcome at all.
func TestAPanicAnywhereOnADestinationsWorkerStillProducesAnOutcome(t *testing.T) {
	l := &ledger{}
	n := notify.New(notify.To("https://example.invalid/hook",
		notify.RendererFunc(func(api.Event) ([]byte, error) { return []byte("ok"), nil }),
		notify.Named("exploding-transport"),
		notify.Client(&http.Client{Transport: panicTransport{}}),
	))
	n.SetAppender(l.append)
	n.Emit(event(1, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	failed := l.ofType(api.NotifyFailed)
	if len(failed) != 1 {
		t.Fatalf("ledger has %d notify.failed events, want 1: %v", len(failed), l.all())
	}
	if b := bodyOf(t, failed[0]); !strings.Contains(b.Error, "panicked") {
		t.Errorf("notify.failed error = %q, want it to say the delivery panicked", b.Error)
	}
}

// TestARendererThatReturnsAnErrorIsAFailedDelivery: a renderer that cannot
// render says so, and that is a failure of this delivery and nothing else.
func TestARendererThatReturnsAnErrorIsAFailedDelivery(t *testing.T) {
	c := newCapture(t)
	l := &ledger{}

	n := notify.New(notify.To(c.url(),
		notify.RendererFunc(func(api.Event) ([]byte, error) {
			return nil, errRenderTest
		}),
		notify.Named("broken"),
	))
	n.SetAppender(l.append)
	n.Emit(event(1, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if reqs := c.all(); len(reqs) != 0 {
		t.Errorf("the endpoint received %d requests, want none: a body that would not render is not a request", len(reqs))
	}
	failed := l.ofType(api.NotifyFailed)
	if len(failed) != 1 {
		t.Fatalf("ledger has %d notify.failed events, want 1: %v", len(failed), l.all())
	}
	if b := bodyOf(t, failed[0]); !strings.Contains(b.Error, "no PagerDuty routing key") {
		t.Errorf("notify.failed error = %q, want the renderer's own message", b.Error)
	}
}

// TestADestinationWithNoRendererSaysSo: To(url, nil) is a wiring mistake
// that must report itself rather than deliver empty bodies forever.
func TestADestinationWithNoRendererSaysSo(t *testing.T) {
	c := newCapture(t)
	l := &ledger{}

	n := notify.New(notify.To(c.url(), nil, notify.Named("nothing")))
	n.SetAppender(l.append)
	n.Emit(event(1, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if reqs := c.all(); len(reqs) != 0 {
		t.Errorf("the endpoint received %d requests, want none", len(reqs))
	}
	failed := l.ofType(api.NotifyFailed)
	if len(failed) != 1 {
		t.Fatalf("ledger has %d notify.failed events, want 1: %v", len(failed), l.all())
	}
	if b := bodyOf(t, failed[0]); !strings.Contains(b.Error, "renderer") {
		t.Errorf("notify.failed error = %q, want it to name the missing renderer", b.Error)
	}
}

// TestASlowRendererDoesNotDelayEmit is rule one held against third-party
// code: a Renderer runs on the destination's own worker, never on the
// engine's goroutine.
func TestASlowRendererDoesNotDelayEmit(t *testing.T) {
	c := newCapture(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	n := notify.New(
		notify.WithGrace(100*time.Millisecond),
		notify.To(c.url(), notify.RendererFunc(func(api.Event) ([]byte, error) {
			<-release
			return []byte("never"), nil
		}), notify.Named("slow")),
	)
	defer func() { _ = n.Close() }()

	start := time.Now()
	for i := 1; i <= 200; i++ {
		n.Emit(event(uint64(i), api.StepFinished))
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("200 Emit calls took %s with a renderer that never returns; rendering is happening inline", elapsed)
	}
}

// TestARendererThatNeverReturnsCannotHoldTheProcessOpen: senro's own path
// settles the instant Flush cancels it, but a Renderer that blocks forever
// is under no such obligation, so it is abandoned and named in the shutdown
// report while the process leaves.
func TestARendererThatNeverReturnsCannotHoldTheProcessOpen(t *testing.T) {
	c := newCapture(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	var report strings.Builder
	n := notify.New(
		notify.WithReportWriter(&report),
		notify.WithGrace(50*time.Millisecond),
		notify.To(c.url(), notify.RendererFunc(func(api.Event) ([]byte, error) {
			<-block
			return nil, nil
		}), notify.Named("wedged")),
	)
	n.Emit(event(1, api.RunFinished))

	start := time.Now()
	err := n.Close()
	elapsed := time.Since(start)

	if elapsed > 30*time.Second {
		t.Fatalf("Close took %s against a renderer that never returns", elapsed)
	}
	if err == nil {
		t.Error("Close reported no error for a destination it had to abandon")
	}
	if out := report.String(); !strings.Contains(out, "wedged") {
		t.Errorf("the shutdown report does not name the abandoned destination:\n%s", out)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The built-ins are the extension point, not a private shortcut past it.
// ─────────────────────────────────────────────────────────────────────────────

// TestWebhookIsToWithEventJSON and TestSlackIsToWithSlackText hold the two
// built-ins to the public path, byte-for-byte on the request: the moment a
// built-in diverges from the public constructor, the public one is the
// untested one.
func TestWebhookIsToWithEventJSON(t *testing.T) {
	builtin := newCapture(t)
	byHand := newCapture(t)

	l := &ledger{}
	n := notify.New(
		notify.Webhook(builtin.url()),
		notify.To(byHand.url(), notify.EventJSON()),
	)
	n.SetAppender(l.append)
	n.Emit(event(3, api.StepFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	a, b := builtin.one(t), byHand.one(t)
	if a.body != b.body {
		t.Errorf("Webhook posted %q, To(url, EventJSON()) posted %q: they must be the same request", a.body, b.body)
	}
	if a.header.Get("Content-Type") != b.header.Get("Content-Type") {
		t.Errorf("Content-Type differs: %q vs %q", a.header.Get("Content-Type"), b.header.Get("Content-Type"))
	}
	var got api.Event
	if err := json.Unmarshal([]byte(a.body), &got); err != nil {
		t.Fatalf("a webhook body must be the api.Event itself: %v", err)
	}
	if got.Type != api.StepFinished || got.Seq != 3 {
		t.Errorf("webhook body decoded to %+v, want the event it was given", got)
	}
}

func TestSlackIsToWithSlackText(t *testing.T) {
	builtin := newCapture(t)
	byHand := newCapture(t)

	l := &ledger{}
	n := notify.New(
		notify.Slack(builtin.url()),
		notify.To(byHand.url(), notify.SlackText(), notify.On(api.RunFinished)),
	)
	n.SetAppender(l.append)
	n.Emit(event(4, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	a, b := builtin.one(t), byHand.one(t)
	if a.body != b.body {
		t.Errorf("Slack posted %q, To(url, SlackText()) posted %q: they must be the same request", a.body, b.body)
	}
	if !strings.Contains(a.body, `"text"`) {
		t.Errorf("a Slack body must carry a text field, got %q", a.body)
	}
}

// TestSlackKeepsItsDefaultsWhenBuiltThroughTo: routing the built-ins through
// To must not have moved their defaults (name and event filter) nor stopped
// an explicit option overriding them.
func TestSlackKeepsItsDefaultsWhenBuiltThroughTo(t *testing.T) {
	c := newCapture(t)
	l := &ledger{}

	n := notify.New(notify.Slack(c.url()))
	n.SetAppender(l.append)
	n.Emit(event(1, api.StepFinished)) // filtered out by the default
	n.Emit(event(2, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := c.all(); len(got) != 1 {
		t.Fatalf("Slack delivered %d events, want only run.finished", len(got))
	}
	delivered := l.ofType(api.NotifyDelivered)
	if len(delivered) != 1 {
		t.Fatalf("ledger = %v, want one notify.delivered", l.all())
	}
	if b := bodyOf(t, delivered[0]); b.Destination != "slack" {
		t.Errorf("destination = %q, want slack", b.Destination)
	}
}

func TestSlackDefaultsStillYieldToExplicitOptions(t *testing.T) {
	c := newCapture(t)
	l := &ledger{}

	n := notify.New(notify.Slack(c.url(), notify.On(api.StepFinished), notify.Named("chatops")))
	n.SetAppender(l.append)
	n.Emit(event(1, api.StepFinished))
	n.Emit(event(2, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := c.all(); len(got) != 1 {
		t.Fatalf("delivered %d events, want only the one On named", len(got))
	}
	delivered := l.ofType(api.NotifyDelivered)
	if len(delivered) != 1 {
		t.Fatalf("ledger = %v, want one notify.delivered", l.all())
	}
	if b := bodyOf(t, delivered[0]); b.Destination != "chatops" {
		t.Errorf("destination = %q, want the name Named gave it", b.Destination)
	}
}

// TestSenrosOwnHeadersWinOverACustomOne. A destination may add headers, but
// not forge the ones a receiver deduplicates and routes on.
func TestSenrosOwnHeadersWinOverACustomOne(t *testing.T) {
	c := newCapture(t)

	l := &ledger{}
	n := notify.New(notify.To(c.url(),
		notify.RendererFunc(func(api.Event) ([]byte, error) { return []byte("x"), nil }),
		notify.Header(notify.HeaderDelivery, "forged"),
		notify.Header(notify.HeaderEvent, "forged"),
	))
	n.SetAppender(l.append)
	n.Emit(event(9, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := c.one(t)
	if d := got.header.Get(notify.HeaderDelivery); d != "01TESTRUN/9" {
		t.Errorf("%s = %q, want senro's own value", notify.HeaderDelivery, d)
	}
	if e := got.header.Get(notify.HeaderEvent); e != "run.finished" {
		t.Errorf("%s = %q, want senro's own value", notify.HeaderEvent, e)
	}
}

// errRenderTest is the renderer failure the test above asserts reaches the
// ledger verbatim.
var errRenderTest = errRender{}

type errRender struct{}

func (errRender) Error() string { return "no PagerDuty routing key was configured" }
