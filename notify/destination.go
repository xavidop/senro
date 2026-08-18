package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
)

// Defaults for a destination; every one is overridable.
const (
	// DefaultAttempts is the total number of HTTP requests one event gets,
	// not the number of retries after the first.
	DefaultAttempts = 3

	// DefaultRetryBase is the first backoff interval; each further wait
	// doubles it, jittered. See backoff.
	DefaultRetryBase = 250 * time.Millisecond

	// DefaultTimeout bounds one request. The whole delivery is bounded
	// separately by the notifier's grace; see Notifier.Flush.
	DefaultTimeout = 5 * time.Second

	// maxBackoff caps the doubling: the shutdown grace is measured in
	// seconds, so a longer wait could only be interrupted, never served.
	maxBackoff = 5 * time.Second
)

// Header names every request carries, whatever the destination kind.
const (
	// HeaderEvent is the event's type, so a receiver can route without
	// parsing the body.
	HeaderEvent = "X-Senro-Event"
	// HeaderRun is the run ID.
	HeaderRun = "X-Senro-Run"
	// HeaderSeq is the event's sequence number within that run.
	HeaderSeq = "X-Senro-Seq"
	// HeaderDelivery is the deduplication key: run plus sequence number.
	// Delivery is at-least-once; a receiver that must act exactly once acts
	// on this.
	HeaderDelivery = "X-Senro-Delivery"
	// HeaderTimestamp is the Unix second the request was signed at, present
	// only on a signed request and covered by the signature, so a captured
	// request cannot be replayed indefinitely.
	HeaderTimestamp = "X-Senro-Timestamp"
	// HeaderSignature is the signature; see Sign for its construction.
	HeaderSignature = "X-Senro-Signature"
)

// Destination is one place a run's events go.
//
// Constructors differ only in body shape, name, and default event filter;
// the queue, drop policy, retries, signing, headers and outcome events are
// shared code. Webhook and Slack are To with a Renderer, the same call a
// third-party destination makes; only a Renderer can render a body here.
type Destination struct {
	name string
	url  string

	// types is the filter. nil means every event, which is different from
	// an empty map, meaning none.
	types map[api.Type]bool

	// renderer turns one event into a request body; see Renderer.
	renderer Renderer
	// requester, when set, decides this destination's method, URL and body
	// per event instead of renderer and url. See Requester.
	requester   Requester
	contentType string
	headers     http.Header

	secret   string
	client   *http.Client
	attempts int
	base     time.Duration
	timeout  time.Duration
}

// DestinationOption configures a Destination. Every one applies to every
// destination kind.
type DestinationOption func(*Destination)

// applyNotifier makes a Destination usable directly as a Notifier option, so
// New reads as the list of places a run reports to.
func (d *Destination) applyNotifier(n *Notifier) { n.bs = append(n.bs, &bound{d: d}) }

// withDefaults puts a built-in's defaults in front of the caller's options.
// Order matters: defaults are ordinary options and the last one wins, which
// is what lets a caller's Named or On override them.
func withDefaults(defaults []DestinationOption, opts []DestinationOption) []DestinationOption {
	out := make([]DestinationOption, 0, len(defaults)+len(opts))
	out = append(out, defaults...)
	return append(out, opts...)
}

// Webhook posts each event to url as JSON: the api.Event itself, exactly as
// api/schema/event.schema.json describes it, so a receiver decodes it with
// the published schema. Every event by default (narrow with On); a program
// can drop what it does not want, a person in a channel cannot, which is why
// Slack's default is the opposite. It is To(rawURL, EventJSON(),
// Named("webhook")) and nothing more.
func Webhook(rawURL string, opts ...DestinationOption) *Destination {
	return To(rawURL, EventJSON(), withDefaults([]DestinationOption{Named("webhook")}, opts)...)
}

// Slack posts to a Slack incoming webhook URL, as a short line of text a
// person can read in a channel. Only api.RunFinished by default; widen with
// On, remembering that a fan-out of two hundred steps is two hundred
// messages. The URL is itself the credential and is never put in an event,
// the shutdown report, or an error message; see Destination.sanitize. It is
// To(rawURL, SlackText(), Named("slack"), On(api.RunFinished)) and nothing
// more.
func Slack(rawURL string, opts ...DestinationOption) *Destination {
	return To(rawURL, SlackText(), withDefaults(
		[]DestinationOption{Named("slack"), On(api.RunFinished)}, opts)...)
}

// On restricts a destination to the given event types. On() with no
// arguments is a destination that receives nothing, a legitimate way to turn
// one off from configuration without removing it.
func On(types ...api.Type) DestinationOption {
	return func(d *Destination) {
		d.types = make(map[api.Type]bool, len(types))
		for _, t := range types {
			d.types[t] = true
		}
	}
}

// Named overrides the destination's name, which identifies it in
// api.NotifyBody.Destination and the shutdown report. Worth setting when a
// run has two webhooks.
func Named(name string) DestinationOption {
	return func(d *Destination) { d.name = name }
}

// Sign adds an HMAC-SHA256 signature to every request. The construction, so
// a receiver can verify it without this package:
//
//	X-Senro-Timestamp: <unix seconds>
//	X-Senro-Signature: v1=<hex(HMAC_SHA256(secret, timestamp + "." + body))>
//
// A receiver recomputes the HMAC over the raw body, compares with hmac.Equal
// (never ==, which leaks timing), and rejects a timestamp too far from its
// own clock to stop replays.
func Sign(secret string) DestinationOption {
	return func(d *Destination) { d.secret = secret }
}

// Client supplies the http.Client to deliver with, for a caller that needs
// its own transport: a proxy, a pinned CA, a tighter dial timeout.
func Client(c *http.Client) DestinationOption {
	return func(d *Destination) {
		if c != nil {
			d.client = c
		}
	}
}

// Retry sets how many requests one event gets in total (not extra ones after
// the first) and the first backoff interval. attempts below 1 is treated as
// 1: an event is always tried once. See DefaultAttempts and backoff.
func Retry(attempts int, base time.Duration) DestinationOption {
	return func(d *Destination) {
		if attempts < 1 {
			attempts = 1
		}
		d.attempts = attempts
		d.base = base
	}
}

// Timeout bounds one request. See DefaultTimeout.
func Timeout(t time.Duration) DestinationOption {
	return func(d *Destination) { d.timeout = t }
}

// wants reports whether this destination is interested in a type. Checked
// before the queue, never after; see bound for why.
func (d *Destination) wants(t api.Type) bool {
	if d.types == nil {
		return true
	}
	return d.types[t]
}

// deliver runs on a destination's own worker goroutine, one event at a time,
// and turns the result into an outcome for the ledger.
func (n *Notifier) deliver(b *bound, e api.Event) { n.report(b.d.send(n.ctx, e)) }

// send makes up to attempts requests and reports what happened.
//
// Retry policy for every destination kind: no response, 429, or 5xx is
// retried (unavailable rather than unwilling); any other 4xx is not, since
// it will be the same next time and only delays the misconfiguration report.
//
// The outer recover backstops the non-senro code on this goroutine (a
// Renderer, a caller's http.Client). Without it the queue's worker would
// swallow the panic and the event would have no outcome at all, and silence
// is the one answer a notifier must never give.
func (d *Destination) send(ctx context.Context, e api.Event) (o outcome) {
	start := time.Now()
	o = outcome{dest: d, about: e, typ: api.NotifyFailed}
	defer func() {
		if r := recover(); r != nil {
			o = outcome{
				dest: d, about: e, typ: api.NotifyFailed,
				err: fmt.Errorf("delivering to this destination panicked: %v", r),
				dur: time.Since(start),
			}
		}
	}()

	if d.url == "" && d.requester == nil {
		o.err = errors.New("no URL is configured for this destination")
		return o
	}
	req, err := d.buildRequest(e)
	if err != nil {
		o.err = fmt.Errorf("building the request: %w", err)
		o.dur = time.Since(start)
		return o
	}
	if req == nil {
		// The destination decided it has nothing to say about this event.
		// Neither delivered nor failed; see outcome.skip.
		o.skip = true
		return o
	}
	if req.URL == "" {
		o.err = errors.New("this destination built a request with no URL")
		o.dur = time.Since(start)
		return o
	}

	for attempt := 1; attempt <= d.attempts; attempt++ {
		o.attempts = attempt
		o.status, o.err = d.do(ctx, e, req)
		if o.err == nil {
			o.typ = api.NotifyDelivered
			o.dur = time.Since(start)
			return o
		}
		if attempt == d.attempts || !retryable(o.status) {
			break
		}
		if !wait(ctx, backoff(attempt, d.base)) {
			break
		}
	}
	o.dur = time.Since(start)
	return o
}

// render calls the destination's Renderer, the one place third-party code
// runs on this goroutine. A missing renderer and a panicking one both come
// back as an ordinary error, which send turns into an api.NotifyFailed;
// neither ends the run, stops the destination, nor is retried. See Renderer
// for the contract this enforces.
func (d *Destination) render(e api.Event) (body []byte, err error) {
	if d.renderer == nil {
		return nil, errors.New("no renderer is configured for this destination")
	}
	defer func() {
		if r := recover(); r != nil {
			body, err = nil, fmt.Errorf("this destination's renderer panicked: %v", r)
		}
	}()
	return d.renderer.Render(e)
}

// do makes one request. A zero status means no response was received at all,
// which is what tells retryable apart from a refusal.
func (d *Destination) do(ctx context.Context, e api.Event, r *Request) (int, error) {
	rctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, r.Method, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return 0, err
	}
	d.setHeaders(req, e, r.Body)

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	// Read a bounded amount so the connection can be reused and a Requester
	// that needs the response can have it; bounded because a receiver's
	// error page is not something to pull into memory whole.
	body := drainAndClose(resp.Body)

	if resp.StatusCode/100 != 2 {
		return resp.StatusCode, errors.New(strings.TrimSpace(resp.Status))
	}
	// A ResponseReader is how a stateful destination learns the id of the
	// resource it just created. Only on success: a 4xx body is an error
	// page, not a resource.
	if rr, ok := d.requester.(ResponseReader); ok {
		d.readResponse(rr, e, body)
	}
	return resp.StatusCode, nil
}

// setHeaders is the same for every destination kind. The destination's own
// headers go on first so senro's overwrite them: a destination may add to a
// request but may not forge the envelope a receiver routes on.
func (d *Destination) setHeaders(req *http.Request, e api.Event, body []byte) {
	for name, values := range d.headers {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	req.Header.Set("Content-Type", d.contentType)
	req.Header.Set("User-Agent", "senro-notify")
	req.Header.Set(HeaderEvent, string(e.Type))
	req.Header.Set(HeaderRun, e.Run)
	req.Header.Set(HeaderSeq, strconv.FormatUint(e.Seq, 10))
	req.Header.Set(HeaderDelivery, e.Run+"/"+strconv.FormatUint(e.Seq, 10))
	if d.secret == "" {
		return
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(d.secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	req.Header.Set(HeaderTimestamp, ts)
	req.Header.Set(HeaderSignature, "v1="+hex.EncodeToString(mac.Sum(nil)))
}

// sanitize renders an error without the destination's URL in it. Not
// cosmetic: a Slack webhook URL is a credential, net/http puts the URL into
// every error it returns, and the run's redactor cannot help because the URL
// is configuration, not a registered secret.
func (d *Destination) sanitize(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	var ue *url.Error
	if errors.As(err, &ue) {
		// url.Error renders as `Op "the-url": inner`. Keep the operation and
		// the cause, drop the URL.
		msg = ue.Op + ": " + ue.Err.Error()
	}
	if d.url != "" {
		msg = strings.ReplaceAll(msg, d.url, "<"+d.name+" url>")
	}
	return msg
}

// retryable reports whether a status is worth another request. A zero status
// means the request never got an answer (a dial failure, a timeout, a reset),
// which is the most retryable case there is.
func retryable(status int) bool {
	return status == 0 || status == http.StatusTooManyRequests || status/100 == 5
}

// backoff is how long to wait after a failed attempt: the base doubled once
// per attempt, capped, then jittered into [d/2, d). Half-jitter rather than
// full: a draw near zero would hammer an endpoint that just said it was
// overloaded, while the top half still spreads a fleet out.
func backoff(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for range attempt - 1 {
		d *= 2
		if d >= maxBackoff {
			d = maxBackoff
			break
		}
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)))
}

// wait sleeps, and reports false if the context ended first: the notifier is
// shutting down and there is no time left to retry in.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// slackBody renders one event as the text field Slack's incoming webhook
// needs. A summary, not the event: a channel is read by people. Everything
// here comes from the event's already-redacted payload.
func slackBody(e api.Event) ([]byte, error) {
	return json.Marshal(struct {
		Text string `json:"text"`
	}{Text: slackText(e)})
}

func slackText(e api.Event) string {
	switch e.Type {
	case api.RunStarted:
		var b api.RunStartedBody
		_ = e.Decode(&b)
		if b.Pipeline != "" {
			return fmt.Sprintf("senro: pipeline %s started (run %s)", b.Pipeline, e.Run)
		}
		return fmt.Sprintf("senro: run %s started", e.Run)
	case api.RunFinished:
		var b api.RunFinishedBody
		_ = e.Decode(&b)
		return fmt.Sprintf("senro: run %s %s in %s (%s)",
			e.Run, b.Status, b.Duration.Round(time.Millisecond), steps(b.Steps))
	case api.StepFinished:
		var b api.StepFinishedBody
		_ = e.Decode(&b)
		line := fmt.Sprintf("senro: step %s %s (run %s)", e.Step, b.State, e.Run)
		if b.Error != "" {
			line += ": " + b.Error
		}
		return line
	default:
		if e.Step != "" {
			return fmt.Sprintf("senro: %s on step %s (run %s)", e.Type, e.Step, e.Run)
		}
		return fmt.Sprintf("senro: %s (run %s)", e.Type, e.Run)
	}
}

// steps renders a run's state histogram in a stable order, so two runs with
// the same outcome read identically.
func steps(h map[api.State]int) string {
	order := []api.State{
		api.StateSucceeded, api.StateFailed, api.StateTimedOut, api.StatePanicked,
		api.StateCancelled, api.StateRecovered, api.StateCached,
		api.StateSkippedCondition, api.StateSkippedUpstreamFailed,
	}
	var parts []string
	for _, st := range order {
		if n := h[st]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, st))
		}
	}
	if len(parts) == 0 {
		return "no steps"
	}
	return strings.Join(parts, ", ")
}
