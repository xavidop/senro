// Command notify runs a short pipeline that reports to a webhook, and to a
// stand-in for Slack, without either of them existing anywhere but this
// process.
//
// Run it:
//
//	go run ./examples/notify
//
// It starts two HTTP endpoints of its own, points a senro notifier at them,
// runs a three-step pipeline whose middle step fails, and prints what each
// endpoint received. Nothing leaves the machine; swap the two URLs for real
// ones and the rest of the program is unchanged.
//
// What it shows: the webhook receives every event as the same JSON in the
// run's events.jsonl; Slack receives one message, for run.finished; every
// request is signed and verified here the way a receiver in any language
// would; each delivery's outcome is itself an event in the run's stream;
// and the outcome of the LAST delivery cannot be an event and arrives on
// standard error at shutdown instead (see package notify for why).
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/notify"
)

// hookSecret would come from configuration in a real program, and would be
// shared with whoever is receiving.
const hookSecret = "example-shared-secret"

func main() {
	ctx := context.Background()

	webhook := newEndpoint("webhook")
	defer webhook.Close()
	slack := newEndpoint("slack")
	defer slack.Close()

	n := notify.New(
		// Everything, signed. A webhook receiver is a program: it can drop
		// what it does not want.
		notify.Webhook(webhook.URL, notify.Sign(hookSecret),
			notify.On(api.RunStarted, api.StepFinished, api.RunFinished)),
		// run.finished only, which is Slack's default and needs no On at
		// all. Spelled out here to make the contrast visible.
		notify.Slack(slack.URL, notify.Sign(hookSecret), notify.On(api.RunFinished)),
	)
	defer func() { _ = n.Close() }()

	p := senro.New("notify-demo")
	ci := p.Workflow("ci")
	ci.Step("fetch", exec.Command("echo", "fetching dependencies"))
	ci.Step("test", exec.Command("sh", "-c", "echo running tests; exit 1")).Needs("fetch")
	ci.Step("publish", exec.Command("echo", "publishing")).Needs("test")

	// A second sink alongside the notifier: a delivery outcome is an event
	// like any other, so every observer sees the notify.delivered lines.
	watch := senro.SinkFunc(func(e api.Event) {
		switch e.Type {
		case api.NotifyDelivered, api.NotifyFailed, api.NotifyDropped:
			var b api.NotifyBody
			_ = e.Decode(&b)
			fmt.Printf("  event %-18s %s <- %s\n", e.Type, b.Destination, b.Event)
		case api.StepFinished, api.RunFinished:
			fmt.Printf("  event %-18s %s\n", e.Type, e.Step)
		}
	})

	fmt.Println("running the pipeline")
	runErr := senro.Run(ctx, p, senro.WithSink(n), senro.WithSink(watch))

	// Run has already flushed the notifier; this only makes the ordering of
	// this program's own output obvious.
	_ = n.Close()

	fmt.Printf("\nthe webhook endpoint received %d requests:\n", webhook.count())
	for _, r := range webhook.received() {
		fmt.Printf("  %-18s seq %-3d signature %s\n", r.event, r.seq, r.signature)
	}
	fmt.Printf("\nthe Slack endpoint received %d requests:\n", slack.count())
	for _, r := range slack.received() {
		fmt.Printf("  %s\n", r.text)
	}

	if runErr != nil {
		fmt.Printf("\n%v\n", runErr)
		os.Exit(1)
	}
}

// endpoint is a receiving server: it verifies the signature the way any
// receiver would, and remembers what it was sent.
type endpoint struct {
	*httptest.Server
	name string

	mu   sync.Mutex
	reqs []request
}

type request struct {
	event     string
	seq       uint64
	signature string
	text      string
}

func newEndpoint(name string) *endpoint {
	e := &endpoint{name: name}
	e.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := verify(hookSecret, r)
		if err != nil {
			log.Printf("%s: rejecting a request: %v", name, err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		seq, _ := strconv.ParseUint(r.Header.Get(notify.HeaderSeq), 10, 64)
		req := request{
			event:     r.Header.Get(notify.HeaderEvent),
			seq:       seq,
			signature: r.Header.Get(notify.HeaderSignature)[:11] + "...",
		}
		// A Slack body is a text message; a webhook body is the event
		// itself. Decode whichever this is for the summary at the end.
		var slackBody struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(body, &slackBody); err == nil && slackBody.Text != "" {
			req.text = slackBody.Text
		}
		e.mu.Lock()
		e.reqs = append(e.reqs, req)
		e.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return e
}

func (e *endpoint) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.reqs)
}

func (e *endpoint) received() []request {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]request(nil), e.reqs...)
}

// verify is what a receiver has to implement, in any language: recompute the
// HMAC over the exact bytes received, compare it in constant time, and refuse
// a timestamp too far from now so a captured request cannot be replayed
// forever.
func verify(secret string, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	ts := r.Header.Get(notify.HeaderTimestamp)
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, errors.New("no usable timestamp")
	}
	if age := time.Since(time.Unix(secs, 0)); age > 5*time.Minute || age < -5*time.Minute {
		return nil, fmt.Errorf("timestamp is %s away from now", age)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(r.Header.Get(notify.HeaderSignature)), []byte(want)) {
		return nil, errors.New("bad signature")
	}
	return body, nil
}
