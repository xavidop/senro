package senro_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/source"
	"github.com/xavidop/senro/notify"
)

// The three interfaces WithSink knows about, asserted against the one type
// in this repo that implements all of them. notify does not depend on this
// package, so nothing but these lines stops the two sides drifting into
// signatures that merely look alike; the drift would fail silently, with
// SetAppender simply never called.
var (
	_ senro.Sink     = (*notify.Notifier)(nil)
	_ senro.Reporter = (*notify.Notifier)(nil)
	_ senro.Flusher  = (*notify.Notifier)(nil)
)

// notifyGate lets a step block until the test watching the run says it may
// finish, and notifySecret hands the leaking step below its value.
//
// Pointers a test replaces, not values: a function name registers once for
// the whole binary and the gate runs under `-count=2`, so a single channel
// closed by the first iteration would already be closed for the second, and
// the test would pass on the first run and fail on the second.
var (
	notifyGate   atomic.Pointer[chan struct{}]
	notifySecret atomic.Pointer[string]
)

// newGate installs a fresh gate for one test and returns the function that
// opens it, safe to call from several goroutines and more than once.
func newGate(t *testing.T) func() {
	t.Helper()
	ch := make(chan struct{})
	notifyGate.Store(&ch)
	t.Cleanup(func() { notifyGate.Store(nil) })
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func init() {
	senro.RegisterFunc("notifytest/wait", func(ctx senro.Ctx, p struct{}) error {
		g := notifyGate.Load()
		if g == nil {
			return errors.New("no gate was installed for this test")
		}
		select {
		case <-*g:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return errors.New("the gate never opened")
		}
	})
	// Returns an error carrying a secret's value. A func's returned error is
	// the workload's own verdict and lands verbatim in step.finished's Error
	// (see internal/engine/funcstep.go's invoke), which makes it the shortest
	// path from a credential to an event payload that anyone can write.
	senro.RegisterFunc("notifytest/leak", func(ctx senro.Ctx, p struct{}) error {
		v := notifySecret.Load()
		if v == nil {
			return errors.New("no secret was set for this test")
		}
		return errors.New("connecting with " + *v + " failed")
	})
}

// readLedger returns every event a finished run wrote to disk. The ledger,
// not a sink's memory of it: an event is only real once it is there, and the
// whole question these tests ask is which notify outcomes made it.
func readLedger(t *testing.T, dir string) []api.Event {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("opening the run's ledger: %v", err)
	}
	defer func() { _ = f.Close() }()

	var out []api.Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<22)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e api.Event
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("a line of events.jsonl is not an api.Event: %v (%s)", err, line)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("reading the run's ledger: %v", err)
	}
	return out
}

func eventsOfType(events []api.Event, ty api.Type) []api.Event {
	var out []api.Event
	for _, e := range events {
		if e.Type == ty {
			out = append(out, e)
		}
	}
	return out
}

func notifyBodyOf(t *testing.T, e api.Event) api.NotifyBody {
	t.Helper()
	var b api.NotifyBody
	if err := e.Decode(&b); err != nil {
		t.Fatalf("decoding a %s payload: %v", e.Type, err)
	}
	return b
}

// runDirs gives a run its own directory and cache root, so nothing here
// depends on or disturbs the operator's real ones.
func runDirs(t *testing.T) (dir string, opts []senro.Option) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "run")
	return dir, []senro.Option{
		senro.WithDir(dir),
		senro.WithRunID("01NOTIFYTEST"),
		senro.WithCacheDir(t.TempDir()),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WithSink itself.
// ─────────────────────────────────────────────────────────────────────────────

// TestWithSinkReceivesTheWholeStream. The gap this closes: before it, a
// program embedding senro could not receive the event stream at all, and
// every other option on Run was useless to anyone who wanted to watch what
// happened without a terminal attached to it.
func TestWithSinkReceivesTheWholeStream(t *testing.T) {
	var mu sync.Mutex
	var seen []api.Event
	sinkFn := senro.SinkFunc(func(e api.Event) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, e)
	})

	dir, opts := runDirs(t)
	p := senro.New("with-sink")
	p.Workflow("main").Step("hello", exec.Command("true"))

	if err := senro.Run(context.Background(), p, append(opts, senro.WithSink(sinkFn))...); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("the sink received nothing")
	}
	if seen[0].Type != api.RunStarted {
		t.Errorf("first event = %s, want run.started", seen[0].Type)
	}
	if last := seen[len(seen)-1]; last.Type != api.RunFinished {
		t.Errorf("last event = %s, want run.finished", last.Type)
	}
	// The sink saw exactly the ledger, in the ledger's own order. Anything
	// else means an observer and the run's source of truth disagree about
	// what happened.
	ledger := readLedger(t, dir)
	if len(seen) != len(ledger) {
		t.Fatalf("the sink saw %d events, the ledger holds %d", len(seen), len(ledger))
	}
	for i := range ledger {
		if seen[i].Seq != ledger[i].Seq || seen[i].Type != ledger[i].Type {
			t.Fatalf("event %d: sink saw %d/%s, ledger holds %d/%s",
				i, seen[i].Seq, seen[i].Type, ledger[i].Seq, ledger[i].Type)
		}
	}
}

// TestWithSinkIsRepeatable. Two observers of one run is the ordinary case: a
// notifier and something of the caller's own. A "last one wins" option would
// silently drop the first.
func TestWithSinkIsRepeatable(t *testing.T) {
	var a, b atomic.Int64
	_, opts := runDirs(t)
	p := senro.New("two-sinks")
	p.Workflow("main").Step("hello", exec.Command("true"))

	err := senro.Run(context.Background(), p, append(opts,
		senro.WithSink(senro.SinkFunc(func(api.Event) { a.Add(1) })),
		senro.WithSink(senro.SinkFunc(func(api.Event) { b.Add(1) })),
	)...)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if a.Load() == 0 || a.Load() != b.Load() {
		t.Errorf("sinks saw %d and %d events, want the same non-zero count", a.Load(), b.Load())
	}
}

// TestWithSinkDoesNotTakeControlAwayFromAttach: adding a sink turns the
// run's observer into a fan-out, which must still hand the engine the ONE
// control channel; getting it wrong would leave `senro attach` visibly
// connected to a run it cannot drive, only for runs that also passed
// WithSink. Proven by cancelling a thirty-second run over the real attach
// socket with a sink alongside.
func TestWithSinkDoesNotTakeControlAwayFromAttach(t *testing.T) {
	isolateAttachRegistry(t)

	att, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket,
		Dir:  filepath.Join(t.TempDir(), "run"),
	})
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	defer func() { _ = att.Close() }()

	var seen atomic.Int64
	started := make(chan struct{})
	var once sync.Once
	watch := senro.SinkFunc(func(e api.Event) {
		seen.Add(1)
		if e.Type == api.StepStarted {
			once.Do(func() { close(started) })
		}
	})

	p := senro.New("attach-plus-sink")
	p.Workflow("main").Step("slow", exec.Command("sh", "-c", "sleep 30"))

	done := make(chan error, 1)
	go func() {
		done <- senro.Run(context.Background(), p,
			senro.WithAttach(att), senro.WithSink(watch), senro.WithCacheDir(t.TempDir()))
	}()

	select {
	case <-started:
	case err := <-done:
		t.Fatalf("the run ended before its step started: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the step never started")
	}

	ls, err := source.Dial(context.Background(), att.Addr())
	if err != nil {
		t.Fatalf("dialling the attach socket: %v", err)
	}
	defer func() { _ = ls.Close() }()

	res, err := ls.Control(context.Background(),
		api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel})
	if err != nil {
		t.Fatalf("run.cancel over attach: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("run.cancel was refused: %s", res.Error)
	}

	select {
	case err := <-done:
		var runErr *senro.RunError
		if !errors.As(err, &runErr) || runErr.Status != api.RunCancelled {
			t.Fatalf("Run returned %v, want a cancelled RunError", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the run was not cancelled: control did not reach the engine")
	}
	if seen.Load() == 0 {
		t.Error("the sink saw nothing, so this test says nothing about a run with both")
	}
}

// TestAPanickingSinkDoesNotKillTheRun. An observer is arbitrary caller code
// on the engine's own emit path. internal/sink.Multi states the policy for
// the sinks it fans out to, and a sink handed straight to WithSink is not
// behind one, deliberately, so the policy has to hold here too.
func TestAPanickingSinkDoesNotKillTheRun(t *testing.T) {
	dir, opts := runDirs(t)
	p := senro.New("panicking-sink")
	p.Workflow("main").Step("hello", exec.Command("true"))

	err := senro.Run(context.Background(), p, append(opts,
		senro.WithSink(senro.SinkFunc(func(api.Event) { panic("an observer had a bad day") })),
	)...)
	if err != nil {
		t.Fatalf("a panicking observer failed the run: %v", err)
	}
	if got := eventsOfType(readLedger(t, dir), api.RunFinished); len(got) != 1 {
		t.Errorf("run.finished count = %d, want 1: the run did not complete", len(got))
	}
}

// appendingSink is a Reporter that appends whatever it is told to, which is
// what an observer with a ledger appender could do if nothing stopped it.
type appendingSink struct {
	mu       sync.Mutex
	appender func(api.Event) bool
	results  map[api.Type]bool
	done     chan struct{}
	once     sync.Once
}

func (s *appendingSink) Emit(e api.Event) {
	if e.Type != api.RunStarted {
		return
	}
	// Not from Emit: an append from inside Emit deadlocks against the very
	// append that produced this event. Another goroutine, as the contract
	// says.
	go func() {
		s.mu.Lock()
		ap := s.appender
		s.mu.Unlock()
		for _, ty := range []api.Type{api.StepFinished, api.RunFinished, api.NotifyDelivered} {
			ok := ap(api.Event{Type: ty, Step: "forged", Payload: json.RawMessage(`{"destination":"x"}`)})
			s.mu.Lock()
			s.results[ty] = ok
			s.mu.Unlock()
		}
		s.once.Do(func() { close(s.done) })
	}()
}

func (s *appendingSink) SetAppender(a func(api.Event) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appender = a
}

// TestASinkMayOnlyAppendItsOwnEvents. Reporter hands an observer a way to
// write to the run's own source of truth, and an observer is arbitrary
// caller code. Unrestricted, it could append a step.finished for a step that
// never ran, and every reader downstream (the fold, the TUI, replay, the
// golden fixtures) would believe it, because the ledger is what they believe
// by definition. An observer is authoritative about its own behaviour and
// nothing else.
func TestASinkMayOnlyAppendItsOwnEvents(t *testing.T) {
	s := &appendingSink{results: map[api.Type]bool{}, done: make(chan struct{})}

	dir, opts := runDirs(t)
	p := senro.New("forging-sink")
	p.Workflow("main").Step("hello", exec.Command("sh", "-c", "sleep 0.3"))

	if err := senro.Run(context.Background(), p, append(opts, senro.WithSink(s))...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the sink never finished trying to append")
	}

	s.mu.Lock()
	results := s.results
	s.mu.Unlock()
	if results[api.StepFinished] {
		t.Error("a sink was allowed to append step.finished")
	}
	if results[api.RunFinished] {
		t.Error("a sink was allowed to append run.finished")
	}
	if !results[api.NotifyDelivered] {
		t.Error("a sink was refused notify.delivered, which is exactly what this path exists for")
	}

	for _, e := range readLedger(t, dir) {
		if e.Step == "forged" && e.Type != api.NotifyDelivered {
			t.Errorf("a forged %s reached the ledger", e.Type)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The four rules, through the public entry point, against a real HTTP server.
// ─────────────────────────────────────────────────────────────────────────────

// receiver is a webhook endpoint that remembers every body it was given.
type receiver struct {
	mu     sync.Mutex
	bodies [][]byte
	types  []string
	delay  time.Duration
	srv    *httptest.Server
}

func newReceiver(t *testing.T, delay time.Duration) *receiver {
	t.Helper()
	r := &receiver{delay: delay}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body := make([]byte, 0, 1024)
		buf := make([]byte, 4096)
		for {
			n, err := req.Body.Read(buf)
			body = append(body, buf[:n]...)
			if err != nil {
				break
			}
		}
		if r.delay > 0 {
			time.Sleep(r.delay)
		}
		r.mu.Lock()
		r.bodies = append(r.bodies, body)
		r.types = append(r.types, req.Header.Get(notify.HeaderEvent))
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) got() ([][]byte, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.bodies...), append([]string(nil), r.types...)
}

// TestRunFinishedIsDeliveredBeforeRunReturns is rule four end to end.
//
// Emit only enqueues. Without a flush inside the shutdown window, the one
// notification anybody actually reads is the one still sitting in a channel
// when the process exits, because run.finished is emitted at the exact moment
// everything begins shutting down. The receiver is deliberately slow, so a
// run that returned without waiting would leave nothing to find here rather
// than usually finding it.
func TestRunFinishedIsDeliveredBeforeRunReturns(t *testing.T) {
	rcv := newReceiver(t, 200*time.Millisecond)

	n := notify.New(
		// The shutdown report goes to standard error by default, which in a
		// test run is somebody else's terminal. This test is not about the
		// report; the two that are set a buffer and read it.
		notify.WithReportWriter(io.Discard),
		notify.Webhook(rcv.srv.URL, notify.On(api.RunFinished)),
	)
	defer func() { _ = n.Close() }()

	_, opts := runDirs(t)
	p := senro.New("rule-four")
	p.Workflow("main").Step("hello", exec.Command("true"))

	if err := senro.Run(context.Background(), p, append(opts, senro.WithSink(n))...); err != nil {
		t.Fatalf("Run: %v", err)
	}

	bodies, types := rcv.got()
	if len(bodies) != 1 {
		t.Fatalf("the endpoint received %d notifications by the time Run returned, want 1", len(bodies))
	}
	if types[0] != string(api.RunFinished) {
		t.Errorf("delivered %q, want run.finished", types[0])
	}
	var e api.Event
	if err := json.Unmarshal(bodies[0], &e); err != nil {
		t.Fatalf("the delivered body is not an api.Event: %v", err)
	}
	var body api.RunFinishedBody
	if err := e.Decode(&body); err != nil {
		t.Fatalf("decoding run.finished: %v", err)
	}
	if body.Status != api.RunSucceeded {
		t.Errorf("the delivered run.finished says %q, want succeeded", body.Status)
	}
}

// TestARunFinishesPromptlyDespiteAWedgedEndpoint is rule one end to end: a
// build must not slow down because Slack is having an afternoon.
//
// A wall-clock assertion against a server that never answers. The bound is
// generous on purpose: what it rules out is not a slow run but a run that
// waits for the endpoint, which with a five second per-request timeout and
// three attempts would take at least fifteen seconds even before the flush.
func TestARunFinishesPromptlyDespiteAWedgedEndpoint(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer func() {
		close(block)
		srv.Close()
	}()

	var report bytes.Buffer
	n := notify.New(
		notify.WithGrace(500*time.Millisecond),
		notify.WithReportWriter(&report),
		notify.Webhook(srv.URL, notify.Timeout(time.Minute)),
	)
	defer func() { _ = n.Close() }()

	dir, opts := runDirs(t)
	p := senro.New("rule-one")
	p.Workflow("main").Step("hello", exec.Command("true"))

	start := time.Now()
	if err := senro.Run(context.Background(), p, append(opts, senro.WithSink(n))...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 10*time.Second {
		t.Fatalf("the run took %s with a wedged notification endpoint", elapsed)
	}
	// And it really did run: an assertion about speed is worthless if the
	// run did nothing.
	if got := eventsOfType(readLedger(t, dir), api.RunFinished); len(got) != 1 {
		t.Fatalf("run.finished count = %d, want 1", len(got))
	}
	if !strings.Contains(report.String(), "NOT delivered") {
		t.Errorf("nothing said the notification failed:\n%s", report.String())
	}
}

// TestADeliveryOutcomeForAnEarlierEventIsInTheLedger is the half of the
// sealed-ledger ruling that has to be real for the ruling to be honest:
// everything except the final delivery is an ordinary event, in the run's own
// stream, with a sequence number before run.finished.
//
// Deterministic, not raced: the second step blocks until a watching sink sees
// the notify.delivered land, so the run cannot finish before the outcome has
// been recorded. Without that gate this test would pass or fail on timing.
func TestADeliveryOutcomeForAnEarlierEventIsInTheLedger(t *testing.T) {
	rcv := newReceiver(t, 0)

	n := notify.New(
		notify.WithReportWriter(io.Discard),
		notify.Webhook(rcv.srv.URL, notify.On(api.StepFinished)),
	)
	defer func() { _ = n.Close() }()

	open := newGate(t)
	watcher := senro.SinkFunc(func(e api.Event) {
		if e.Type == api.NotifyDelivered {
			open()
		}
	})

	dir, opts := runDirs(t)
	p := senro.New("outcome-in-ledger")
	w := p.Workflow("main")
	w.Step("first", exec.Command("true"))
	w.Step("second", senro.Func("notifytest/wait", struct{}{})).Needs("first")

	if err := senro.Run(context.Background(), p,
		append(opts, senro.WithSink(n), senro.WithSink(watcher))...); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := readLedger(t, dir)
	delivered := eventsOfType(events, api.NotifyDelivered)
	if len(delivered) == 0 {
		t.Fatal("no notify.delivered reached the ledger: outcomes for ordinary events are not being recorded")
	}
	finished := eventsOfType(events, api.RunFinished)
	if len(finished) != 1 {
		t.Fatalf("run.finished count = %d, want 1", len(finished))
	}
	if delivered[0].Seq >= finished[0].Seq {
		t.Errorf("notify.delivered has seq %d, run.finished has %d: an outcome after the seal cannot exist",
			delivered[0].Seq, finished[0].Seq)
	}
	b := notifyBodyOf(t, delivered[0])
	if b.Event != api.StepFinished {
		t.Errorf("the recorded outcome is about %q, want step.finished", b.Event)
	}
	if b.Destination != "webhook" {
		t.Errorf("the recorded outcome names destination %q, want webhook", b.Destination)
	}
	// run.finished must still be the last event in the ledger. An observer
	// that can append is an observer that could have appended past the seal.
	if last := events[len(events)-1]; last.Type != api.RunFinished {
		t.Errorf("the last event in the ledger is %s, want run.finished", last.Type)
	}
}

// TestTheFinalDeliverysOutcomeIsReportedOnStandardError is the other half,
// and the answer to the problem this feature could not route around.
//
// run.finished is appended and the stream sealed in one critical section, so
// the outcome of delivering run.finished has no ledger left to go in. That
// outcome is the single most interesting one an operator has: it says whether
// the notification they are waiting for actually went out. It is reported in
// text, at shutdown, rather than dropped.
func TestTheFinalDeliverysOutcomeIsReportedOnStandardError(t *testing.T) {
	rcv := newReceiver(t, 0)

	var report bytes.Buffer
	n := notify.New(
		notify.WithReportWriter(&report),
		notify.Webhook(rcv.srv.URL, notify.On(api.RunFinished)),
	)
	defer func() { _ = n.Close() }()

	dir, opts := runDirs(t)
	p := senro.New("sealed-ledger")
	p.Workflow("main").Step("hello", exec.Command("true"))

	if err := senro.Run(context.Background(), p, append(opts, senro.WithSink(n))...); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// It went out.
	if bodies, _ := rcv.got(); len(bodies) != 1 {
		t.Fatalf("the endpoint received %d notifications, want 1", len(bodies))
	}
	// It is not, and cannot be, in the ledger.
	if got := eventsOfType(readLedger(t, dir), api.NotifyDelivered); len(got) != 0 {
		t.Errorf("%d notify.delivered events reached a sealed ledger", len(got))
	}
	// So it is here instead, saying which destination, which event, and what
	// happened.
	out := report.String()
	if out == "" {
		t.Fatal("the outcome of the run.finished delivery was reported nowhere at all")
	}
	for _, want := range []string{"webhook", "run.finished", "delivered", "HTTP 200"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not mention %q:\n%s", want, out)
		}
	}
}

// TestASecretInAnEventPayloadNeverReachesTheWebhookBody is rule three, proven
// by observation over the bytes the endpoint actually received rather than by
// reading the code that is supposed to prevent it.
//
// The vector is real: a func step's returned error is the workload's verdict
// and lands verbatim in step.finished's Error, and a webhook body is that
// event. What stops it is upstream of every sink: the engine redacts each
// payload in runCore.append, before the ledger and before Sink.Emit, so a
// notifier receives an event with nothing left to redact. This test would
// also fail if a notifier were rebuilding the body from somewhere less
// careful.
func TestASecretInAnEventPayloadNeverReachesTheWebhookBody(t *testing.T) {
	const value = "s3cr3t-notify-token-aaaa"
	v := value
	notifySecret.Store(&v)

	type Config struct {
		RegistryToken secret.String `source:"fake://ci/ghcr#token"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/ghcr#token", value)

	ctx := context.Background()
	cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	rcv := newReceiver(t, 0)
	n := notify.New(notify.WithReportWriter(io.Discard), notify.Webhook(rcv.srv.URL))
	defer func() { _ = n.Close() }()

	dir, opts := runDirs(t)
	p := senro.New("redaction")
	p.Workflow("main").Step("leak", senro.Func("notifytest/leak", struct{}{}))

	runErr := senro.Run(ctx, p, append(opts, senro.WithSink(n), senro.WithSecrets(cfg))...)
	if runErr == nil {
		t.Fatal("the leaking step was supposed to fail; this test proves nothing if it did not run")
	}

	bodies, _ := rcv.got()
	if len(bodies) == 0 {
		t.Fatal("the endpoint received nothing at all")
	}
	all := bytes.Join(bodies, []byte("\n"))

	// The bytes that would carry the secret really were delivered: without
	// this the search below would pass against an endpoint that got nothing
	// interesting.
	if !bytes.Contains(all, []byte("connecting with")) {
		t.Fatalf("the step's own error text never reached the webhook, so this search proves nothing:\n%s", all)
	}
	if !bytes.Contains(all, []byte(redact.Placeholder)) {
		t.Errorf("no %s in the delivered bodies: nothing was redacted", redact.Placeholder)
	}
	if bytes.Contains(all, []byte(value)) {
		t.Errorf("the secret's value reached the webhook body")
	}
	// And the same claim over the ledger, which is where the event came from.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	if bytes.Contains(raw, []byte(value)) {
		t.Error("the secret's value reached events.jsonl")
	}
}
