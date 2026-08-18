package notify_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/notify"
)

// ledger is a stand-in for the run's own ledger: the function the engine
// hands a Reporter (see senro.Appender). It records what a notifier appends,
// and can be sealed, which is what the engine's real one does the instant
// run.finished is written.
type ledger struct {
	mu     sync.Mutex
	sealed bool
	events []api.Event
	seq    uint64
}

func (l *ledger) append(e api.Event) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sealed {
		return false
	}
	l.seq++
	e.Seq = l.seq
	l.events = append(l.events, e)
	return true
}

func (l *ledger) seal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sealed = true
}

func (l *ledger) all() []api.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]api.Event(nil), l.events...)
}

func (l *ledger) ofType(t api.Type) []api.Event {
	var out []api.Event
	for _, e := range l.all() {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

func bodyOf(t *testing.T, e api.Event) api.NotifyBody {
	t.Helper()
	var b api.NotifyBody
	if err := e.Decode(&b); err != nil {
		t.Fatalf("decoding a %s payload: %v", e.Type, err)
	}
	return b
}

// event builds a plausible non-notify event to deliver.
func event(seq uint64, ty api.Type) api.Event {
	return api.Event{V: 1, Seq: seq, TS: time.Now().UTC(), Type: ty, Run: "01TESTRUN"}
}

// wedgedServer never answers: requests pile up until their contexts are
// cancelled.
func wedgedServer(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

// ─────────────────────────────────────────────────────────────────────────────
// Rule 1: delivery never blocks the run, and what it loses it accounts for.
// ─────────────────────────────────────────────────────────────────────────────

// TestAWedgedEndpointDoesNotDelayEmit is rule one at the seam the engine
// touches: Emit runs inline under the engine's append lock, so an Emit that
// waited on HTTP would stop the build. A wall-clock assertion, because a
// synchronous implementation fails only this kind of test.
func TestAWedgedEndpointDoesNotDelayEmit(t *testing.T) {
	srv := wedgedServer(t)

	l := &ledger{}
	n := notify.New(
		notify.WithGrace(200*time.Millisecond),
		notify.Webhook(srv.URL, notify.Timeout(time.Minute)),
	)
	n.SetAppender(l.append)
	defer func() { _ = n.Close() }()

	start := time.Now()
	for i := 1; i <= 200; i++ {
		n.Emit(event(uint64(i), api.StepFinished))
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("200 Emit calls took %s against an endpoint that never answers; Emit is delivering inline", elapsed)
	}
}

// TestAFullQueueDropsCountsAndReports: when the queue in front of a wedged
// destination fills, the drop must be an event in the run's stream carrying
// a running total, not a silent loss.
func TestAFullQueueDropsCountsAndReports(t *testing.T) {
	srv := wedgedServer(t)

	l := &ledger{}
	n := notify.New(
		notify.WithGrace(100*time.Millisecond),
		notify.Webhook(srv.URL, notify.Timeout(time.Minute)),
	)
	n.SetAppender(l.append)
	defer func() { _ = n.Close() }()

	// Comfortably past the queue depth, so the drop branch is entered many
	// times.
	const emitted = 9000
	for i := 1; i <= emitted; i++ {
		n.Emit(event(uint64(i), api.StepFinished))
	}

	// The reports queue drains on its own goroutine; give it a moment.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(l.ofType(api.NotifyDropped)) == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	drops := l.ofType(api.NotifyDropped)
	if len(drops) == 0 {
		t.Fatal("emitting 9000 events into a wedged destination produced no notify.dropped: the drop is not being accounted for")
	}
	first := bodyOf(t, drops[0])
	last := bodyOf(t, drops[len(drops)-1])
	if first.Destination != "webhook" {
		t.Errorf("notify.dropped names destination %q, want webhook", first.Destination)
	}
	if first.Event != api.StepFinished {
		t.Errorf("notify.dropped names event %q, want step.finished: the body must describe the event that was lost", first.Event)
	}
	if last.Dropped <= first.Dropped {
		t.Errorf("Dropped went %d then %d, want a climbing running total", first.Dropped, last.Dropped)
	}
	if last.Dropped > emitted {
		t.Errorf("Dropped = %d after %d events, which is more than were ever emitted", last.Dropped, emitted)
	}
}

// TestFilteringHappensBeforeTheQueue is the reason each destination has its
// own queue: behind a shared one, a Slack destination's queue would fill
// with events it was going to discard, and the one event it exists for would
// be the one dropped. Nine thousand unwanted events, then the wanted one,
// and the wanted one has to arrive.
func TestFilteringHappensBeforeTheQueue(t *testing.T) {
	var got atomic.Int64
	seen := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Add(1)
		seen <- r.Header.Get(notify.HeaderEvent)
	}))
	defer srv.Close()

	l := &ledger{}
	n := notify.New(notify.Slack(srv.URL))
	n.SetAppender(l.append)

	for i := 1; i <= 9000; i++ {
		n.Emit(event(uint64(i), api.StepLogAppended))
	}
	n.Emit(event(9001, api.RunFinished))

	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := got.Load(); n != 1 {
		t.Fatalf("the endpoint received %d requests, want exactly 1: the filter is not being applied before the queue", n)
	}
	select {
	case ty := <-seen:
		if ty != string(api.RunFinished) {
			t.Errorf("the one delivered event was %q, want run.finished", ty)
		}
	default:
		t.Fatal("no event was delivered at all")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Rule 2: at-least-once, with retry and jitter, and every outcome is an event.
// ─────────────────────────────────────────────────────────────────────────────

// TestATransientFailureIsRetriedAndThenRecorded proves both halves of rule
// two at once: the delivery survives two failures, and the fact that it took
// three attempts is in the run's stream rather than in nobody's log.
func TestATransientFailureIsRetriedAndThenRecorded(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	l := &ledger{}
	n := notify.New(notify.Webhook(srv.URL, notify.Retry(3, time.Millisecond)))
	n.SetAppender(l.append)

	n.Emit(event(7, api.RunStarted))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	delivered := l.ofType(api.NotifyDelivered)
	if len(delivered) != 1 {
		t.Fatalf("notify.delivered count = %d, want 1 (ledger: %v)", len(delivered), l.all())
	}
	b := bodyOf(t, delivered[0])
	if b.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3: two failures were not retried", b.Attempts)
	}
	if b.Status != http.StatusNoContent {
		t.Errorf("Status = %d, want %d", b.Status, http.StatusNoContent)
	}
	if b.Event != api.RunStarted || b.Seq != 7 {
		t.Errorf("body names event %s seq %d, want run.started seq 7", b.Event, b.Seq)
	}
	if len(l.ofType(api.NotifyFailed)) != 0 {
		t.Error("a delivery that eventually succeeded also reported notify.failed")
	}
}

// TestAPermanentRefusalIsNotRetried. A 400 will be a 400 next time too, and
// retrying it only delays the report of a misconfiguration. Distinguishing
// unavailable from unwilling is the whole of the retry policy.
func TestAPermanentRefusalIsNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	l := &ledger{}
	n := notify.New(notify.Webhook(srv.URL, notify.Retry(5, time.Millisecond)))
	n.SetAppender(l.append)

	n.Emit(event(1, api.RunStarted))
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := hits.Load(); got != 1 {
		t.Errorf("the endpoint was called %d times for a 400, want 1", got)
	}
	failed := l.ofType(api.NotifyFailed)
	if len(failed) != 1 {
		t.Fatalf("notify.failed count = %d, want 1", len(failed))
	}
	b := bodyOf(t, failed[0])
	if b.Status != http.StatusBadRequest {
		t.Errorf("Status = %d, want 400", b.Status)
	}
	if b.Error == "" {
		t.Error("notify.failed carries no error text, so nobody reading the stream learns why")
	}
}

// TestARetriedRequestCarriesTheSameDeliveryKey. At-least-once means a
// receiver will see the same event twice, and the only way it can act once is
// if the two requests are recognisably the same delivery.
func TestARetriedRequestCarriesTheSameDeliveryKey(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		keys = append(keys, r.Header.Get(notify.HeaderDelivery))
		first := hits == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := notify.New(notify.Webhook(srv.URL, notify.Retry(2, time.Millisecond)))
	n.Emit(event(42, api.StepFinished))
	if err := n.Close(); err != nil && !strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 {
		t.Fatalf("got %d requests, want 2", len(keys))
	}
	if keys[0] != keys[1] {
		t.Errorf("delivery keys differ across a retry: %q then %q", keys[0], keys[1])
	}
	if want := "01TESTRUN/42"; keys[0] != want {
		t.Errorf("delivery key = %q, want %q (run and sequence number identify an event permanently)", keys[0], want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Signing, and what must never appear in an outcome.
// ─────────────────────────────────────────────────────────────────────────────

// TestASignedRequestIsVerifiableWithoutThisPackage recomputes the signature
// from raw stdlib primitives, the way a receiver would; a helper shared with
// the signer would agree with itself no matter what it computed.
func TestASignedRequestIsVerifiableWithoutThisPackage(t *testing.T) {
	const secret = "shared-hook-secret"
	type received struct {
		body []byte
		ts   string
		sig  string
	}
	got := make(chan received, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- received{body: b, ts: r.Header.Get(notify.HeaderTimestamp), sig: r.Header.Get(notify.HeaderSignature)}
	}))
	defer srv.Close()

	n := notify.New(notify.Webhook(srv.URL, notify.Sign(secret)))
	n.Emit(event(3, api.RunFinished))
	if err := n.Close(); err != nil && !strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("Close: %v", err)
	}

	var r received
	select {
	case r = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no request arrived")
	}
	if r.ts == "" {
		t.Fatal("a signed request carries no timestamp, so a captured one can be replayed forever")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(r.ts))
	mac.Write([]byte("."))
	mac.Write(r.body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(r.sig), []byte(want)) {
		t.Errorf("signature = %q, want %q", r.sig, want)
	}

	// Change one byte of the body and the same computation must stop
	// agreeing.
	mac2 := hmac.New(sha256.New, []byte(secret))
	mac2.Write([]byte(r.ts))
	mac2.Write([]byte("."))
	mac2.Write(append(append([]byte(nil), r.body...), ' '))
	if hmac.Equal(mac2.Sum(nil), mac.Sum(nil)) {
		t.Error("the signature does not depend on the body")
	}
}

// TestAnUnsignedRequestCarriesNoSignatureHeader: a receiver that requires
// the header needs the unsigned case to be unambiguous.
func TestAnUnsignedRequestCarriesNoSignatureHeader(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get(notify.HeaderSignature)
	}))
	defer srv.Close()

	n := notify.New(notify.Webhook(srv.URL))
	n.Emit(event(1, api.RunFinished))
	_ = n.Close()

	select {
	case sig := <-got:
		if sig != "" {
			t.Errorf("an unsigned destination sent %s: %q", notify.HeaderSignature, sig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no request arrived")
	}
}

// TestTheDestinationURLNeverReachesAnOutcome: a Slack webhook URL is a
// credential, net/http puts the URL into every error, and the run's redactor
// cannot help because the URL is configuration, not a registered secret.
func TestTheDestinationURLNeverReachesAnOutcome(t *testing.T) {
	// A port nothing listens on, with the shape of a Slack webhook URL: the
	// path is the part that is secret.
	const token = "T00000000-B11111111-kkkkkkkkkkkkkkkkkkkkkkkk"
	dead := "http://127.0.0.1:1/services/" + token

	l := &ledger{}
	var report bytes.Buffer
	n := notify.New(
		notify.WithReportWriter(&report),
		notify.Slack(dead, notify.Retry(1, time.Millisecond), notify.Timeout(time.Second)),
	)
	n.SetAppender(l.append)

	n.Emit(event(1, api.RunFinished))
	_ = n.Close()

	failed := l.ofType(api.NotifyFailed)
	if len(failed) != 1 {
		t.Fatalf("notify.failed count = %d, want 1", len(failed))
	}
	b := bodyOf(t, failed[0])
	if b.Error == "" {
		t.Fatal("notify.failed carries no error, so this test proves nothing about what an error contains")
	}
	if strings.Contains(b.Error, token) {
		t.Errorf("the destination URL leaked into the run's ledger: %q", b.Error)
	}
	if strings.Contains(report.String(), token) {
		t.Errorf("the destination URL leaked into the shutdown report: %q", report.String())
	}
	if raw, err := json.Marshal(failed[0]); err == nil && bytes.Contains(raw, []byte(token)) {
		t.Errorf("the destination URL leaked into the notify.failed event: %s", raw)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The feedback loop, and the sealed ledger.
// ─────────────────────────────────────────────────────────────────────────────

// TestANotifierNeverNotifiesAboutItsOwnEvents: every outcome is an event
// that reaches every sink, including the notifier that produced it, and
// delivering those would loop forever.
func TestANotifierNeverNotifiesAboutItsOwnEvents(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l := &ledger{}
	n := notify.New(notify.Webhook(srv.URL))
	// The appender feeds every appended event straight back into the
	// notifier, exactly as the engine does.
	n.SetAppender(func(e api.Event) bool {
		ok := l.append(e)
		if ok {
			n.Emit(e)
		}
		return ok
	})

	n.Emit(event(1, api.RunStarted))

	// Wait for the first delivery, then keep watching; closing here would
	// stop a runaway before it could be seen.
	deadline := time.Now().Add(5 * time.Second)
	for hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if hits.Load() == 0 {
		t.Fatal("the one event was never delivered, so this test proves nothing")
	}
	// Long enough for several trips around a would-be loop.
	time.Sleep(300 * time.Millisecond)

	if got := hits.Load(); got != 1 {
		t.Fatalf("the endpoint received %d requests for one event, want 1: outcomes are being notified about", got)
	}
	if got := len(l.all()); got != 1 {
		t.Fatalf("the ledger holds %d events for one delivery, want 1", got)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestAnOutcomeTheLedgerRefusesIsReportedOnStandardError is the
// sealed-ledger case in the small: the outcome of delivering run.finished is
// decided after the stream has closed, the appender returns false, and the
// outcome must land in the report instead.
func TestAnOutcomeTheLedgerRefusesIsReportedOnStandardError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l := &ledger{}
	var report bytes.Buffer
	n := notify.New(notify.WithReportWriter(&report), notify.Slack(srv.URL))
	n.SetAppender(l.append)

	// Exactly what the engine does: the stream closes behind run.finished
	// before its delivery has been decided.
	n.Emit(event(99, api.RunFinished))
	l.seal()

	err := n.Close()
	if err == nil {
		t.Error("Flush reported no error for an outcome it could not record")
	}

	out := report.String()
	if out == "" {
		t.Fatal("nothing was written to the report writer: the outcome of the final delivery was dropped silently")
	}
	for _, want := range []string{"slack", "run.finished", "delivered"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not mention %q:\n%s", want, out)
		}
	}
	if got := len(l.ofType(api.NotifyDelivered)); got != 0 {
		t.Errorf("%d notify.delivered reached a sealed ledger, which is supposed to be impossible", got)
	}
}

// TestAnOutcomeBeforeTheSealIsAnOrdinaryEvent is the other half: everything
// that is not the final delivery goes in the ledger.
func TestAnOutcomeBeforeTheSealIsAnOrdinaryEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l := &ledger{}
	var report bytes.Buffer
	n := notify.New(notify.WithReportWriter(&report), notify.Webhook(srv.URL))
	n.SetAppender(l.append)

	n.Emit(event(1, api.StepFinished))
	if err := n.Close(); err != nil {
		t.Fatalf("Close reported trouble for an outcome that had a ledger to go in: %v", err)
	}

	if got := len(l.ofType(api.NotifyDelivered)); got != 1 {
		t.Fatalf("notify.delivered in the ledger = %d, want 1", got)
	}
	if report.Len() != 0 {
		t.Errorf("an outcome that was recorded was also printed:\n%s", report.String())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Rule 4, at this level: what is queued when the run ends still goes out.
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushDeliversWhatIsStillQueued: Emit only enqueues, so without the
// flush the last event of a run (usually run.finished) is in a channel when
// the process exits.
func TestFlushDeliversWhatIsStillQueued(t *testing.T) {
	got := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow enough that the delivery is still in flight when Flush is
		// called, so this fails without the drain.
		time.Sleep(150 * time.Millisecond)
		got <- r.Header.Get(notify.HeaderEvent)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := notify.New(notify.Webhook(srv.URL))
	n.Emit(event(1, api.RunFinished))

	if err := n.Flush(context.Background()); err != nil && !strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("Flush: %v", err)
	}
	select {
	case ty := <-got:
		if ty != string(api.RunFinished) {
			t.Errorf("delivered %q, want run.finished", ty)
		}
	default:
		t.Fatal("Flush returned before run.finished had been delivered")
	}
}

// TestFlushIsBoundedByItsGrace: waiting for a delivery must not become
// waiting forever, or a wedged endpoint holds a CI job open.
func TestFlushIsBoundedByItsGrace(t *testing.T) {
	srv := wedgedServer(t)

	var report bytes.Buffer
	n := notify.New(
		notify.WithGrace(250*time.Millisecond),
		notify.WithReportWriter(&report),
		notify.Webhook(srv.URL, notify.Timeout(time.Minute), notify.Retry(10, time.Second)),
	)
	n.Emit(event(1, api.RunFinished))

	start := time.Now()
	err := n.Flush(context.Background())
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("Flush took %s against an endpoint that never answers, with a 250ms grace", elapsed)
	}
	if err == nil {
		t.Error("Flush reported success for a notification that never went out")
	}
	if !strings.Contains(report.String(), "NOT delivered") {
		t.Errorf("the report does not say the notification failed:\n%s", report.String())
	}
}

// TestFlushIsIdempotent: Run flushes, and a caller's `defer n.Close()` then
// flushes again; the second call must not re-report, block, or panic.
func TestFlushIsIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l := &ledger{}
	var report bytes.Buffer
	n := notify.New(notify.WithReportWriter(&report), notify.Webhook(srv.URL))
	n.SetAppender(l.append)

	n.Emit(event(1, api.RunFinished))
	first := n.Flush(context.Background())
	after := report.String()
	second := n.Close()

	if first != nil {
		t.Fatalf("first flush: %v", first)
	}
	if second != first {
		t.Errorf("second flush returned %v, want the first call's own result %v", second, first)
	}
	if report.String() != after {
		t.Errorf("the second flush wrote the report again:\n%s", report.String())
	}
	// Emitting after a flush must be harmless: the engine can still be
	// unwinding when Run returns.
	n.Emit(event(2, api.StepFinished))
}

// TestANotifierWithNoDestinationsDoesNothing: configuration that turned
// everything off must not need a special case at the call site.
func TestANotifierWithNoDestinationsDoesNothing(t *testing.T) {
	n := notify.New()
	n.Emit(event(1, api.RunFinished))
	if err := n.Close(); err != nil {
		t.Errorf("Close on an empty notifier: %v", err)
	}
}

// TestOnWithNoTypesDeliversNothing: On() must mean none, not fall back to
// the default of all.
func TestOnWithNoTypesDeliversNothing(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()

	n := notify.New(notify.Webhook(srv.URL, notify.On()))
	for _, ty := range []api.Type{api.RunStarted, api.StepFinished, api.RunFinished} {
		n.Emit(event(1, ty))
	}
	_ = n.Close()

	if got := hits.Load(); got != 0 {
		t.Errorf("On() with no types delivered %d events, want 0", got)
	}
}

// TestTheWebhookBodyIsTheEventItself: a receiver decodes it with the
// published schema, which is only true if the body is the event and not a
// wrapper around it.
func TestTheWebhookBodyIsTheEventItself(t *testing.T) {
	got := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- b
	}))
	defer srv.Close()

	want := event(11, api.StepFinished)
	want.Step = "build"
	want.Payload = json.RawMessage(`{"state":"succeeded","duration_ns":5}`)

	n := notify.New(notify.Webhook(srv.URL))
	n.Emit(want)
	_ = n.Close()

	select {
	case raw := <-got:
		var back api.Event
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("the body is not an api.Event: %v (%s)", err, raw)
		}
		if back.Seq != want.Seq || back.Type != want.Type || back.Step != want.Step {
			t.Errorf("decoded %+v, want seq/type/step from %+v", back, want)
		}
		var body api.StepFinishedBody
		if err := back.Decode(&body); err != nil || body.State != api.StateSucceeded {
			t.Errorf("the payload did not survive the round trip: %v %+v", err, body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no request arrived")
	}
}

// TestTheSlackBodyIsReadableText: Slack's incoming webhook takes a text
// field read by people.
func TestTheSlackBodyIsReadableText(t *testing.T) {
	got := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- b
	}))
	defer srv.Close()

	e := event(5, api.RunFinished)
	e.Payload = json.RawMessage(fmt.Sprintf(
		`{"status":"failed","steps":{"succeeded":2,"failed":1},"duration_ns":%d}`, 1500*time.Millisecond))

	n := notify.New(notify.Slack(srv.URL))
	n.Emit(e)
	_ = n.Close()

	select {
	case raw := <-got:
		var body struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("the Slack body is not JSON with a text field: %v (%s)", err, raw)
		}
		for _, want := range []string{"01TESTRUN", "failed", "2 succeeded", "1 failed", "1.5s"} {
			if !strings.Contains(body.Text, want) {
				t.Errorf("the message does not mention %q: %q", want, body.Text)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no request arrived")
	}
}
