package notify

import (
	"encoding/json"
	"net/http"

	"github.com/xavidop/senro/api"
)

// Renderer turns one event into the body of the request that carries it,
// and is all a third party writes to add a destination kind; the queue,
// drops, retries, timeout, signing, headers, outcome events and flush are
// inherited through To.
//
//	type pagerduty struct{ key string }
//
//	func (p pagerduty) Render(e api.Event) ([]byte, error) {
//		return json.Marshal(map[string]any{"routing_key": p.key, ...})
//	}
//
//	notify.New(notify.To("https://events.pagerduty.com/v2/enqueue", pagerduty{key},
//		notify.Named("pagerduty"), notify.On(api.RunFinished)))
//
// The contract: Render is called once per event per destination, on that
// destination's own worker goroutine, never on the engine's, so it may be
// slow but must return eventually (the notifier's grace bounds shutdown; see
// Notifier.Flush). It must not retain or mutate the event, which every other
// observer also receives. It should be deterministic per event: retries
// reuse the rendered body, so a Renderer counts events, not requests. An
// error means no request for this event, recorded as api.NotifyFailed and
// not retried. A panic becomes the same failed delivery and stops neither
// the run nor the destination. Events arrive already redacted; a credential
// the renderer itself holds goes in the body or a Header, which senro's
// redactor never sees.
type Renderer interface {
	Render(api.Event) ([]byte, error)
}

// RendererFunc adapts an ordinary function to Renderer.
//
//	notify.To(url, notify.RendererFunc(func(e api.Event) ([]byte, error) {
//		return json.Marshal(myShape{Type: string(e.Type), Run: e.Run})
//	}))
type RendererFunc func(api.Event) ([]byte, error)

// Render calls f.
func (f RendererFunc) Render(e api.Event) ([]byte, error) { return f(e) }

// To returns a destination that posts every event it wants to rawURL, with
// the body r renders. It is the constructor Webhook and Slack are written in
// terms of; see Renderer.
//
// The name is "destination" unless Named says otherwise (two unnamed
// destinations are two report lines nobody can tell apart); the content type
// is application/json unless ContentType says otherwise. A nil Renderer is
// neither a panic nor a silent empty body: every event is reported as a
// failed delivery naming the missing renderer.
func To(rawURL string, r Renderer, opts ...DestinationOption) *Destination {
	d := &Destination{
		name:        "destination",
		url:         rawURL,
		renderer:    r,
		contentType: "application/json",
		client:      &http.Client{},
		attempts:    DefaultAttempts,
		base:        DefaultRetryBase,
		timeout:     DefaultTimeout,
	}
	for _, o := range opts {
		if o == nil {
			continue
		}
		o(d)
	}
	return d
}

// EventJSON renders the api.Event itself as JSON, exactly as
// api/schema/event.schema.json describes it. It is what Webhook posts, and
// the useful default for anything that speaks JSON.
func EventJSON() Renderer {
	return RendererFunc(func(e api.Event) ([]byte, error) { return json.Marshal(e) })
}

// SlackText renders one event as the short line of text a Slack incoming
// webhook shows in a channel. It is what Slack posts; see slackBody.
func SlackText() Renderer { return RendererFunc(slackBody) }

// ContentType sets the Content-Type header of every request this destination
// makes. The default is application/json, which is what both built-ins send.
func ContentType(ct string) DestinationOption {
	return func(d *Destination) {
		if ct != "" {
			d.contentType = ct
		}
	}
}

// Header adds one header to every request this destination makes: an API
// key, a tenant identifier.
//
//	notify.To(url, r, notify.Header("DD-API-KEY", os.Getenv("DD_API_KEY")))
//
// Repeating one name replaces the value rather than appending. senro's own
// headers are set after these and win: a destination may add to a request
// but may not forge the envelope.
func Header(name, value string) DestinationOption {
	return func(d *Destination) {
		if name == "" {
			return
		}
		if d.headers == nil {
			d.headers = make(http.Header)
		}
		d.headers.Set(name, value)
	}
}
