// Package pagerduty is a senro notifier for PagerDuty's Events API v2,
// written the way one in somebody else's repository would be: it imports
// github.com/xavidop/senro/api and .../notify and nothing else of senro's
// (extension_static_test.go checks that; extension_e2e_test.go drives it
// through a real run).
//
// Using it:
//
//	n := notify.New(pagerduty.Destination(os.Getenv("PD_ROUTING_KEY"), "ci.example.com"))
//	defer func() { _ = n.Close() }()
//
//	err := senro.Run(ctx, pipeline, senro.WithSink(n))
//
// The queue, drop accounting, retries, timeout, headers, outcome events and
// flush are all senro's; what is here is one method that turns an event into
// some bytes.
package pagerduty

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/notify"
)

// EventsAPI is PagerDuty's Events API v2 enqueue endpoint, which is the same
// URL for everybody: the routing key in the body is what selects a service.
const EventsAPI = "https://events.pagerduty.com/v2/enqueue"

// Renderer turns a senro event into a PagerDuty Events API v2 alert. It is
// a notify.Renderer.
type Renderer struct {
	// RoutingKey is the integration key of the PagerDuty service to alert.
	// Required: a renderer without one reports itself.
	RoutingKey string

	// Source is what PagerDuty shows as the origin of the alert, usually the
	// host or the pipeline's name. Defaults to "senro".
	Source string
}

// alert is the Events API v2 request body, trimmed to the fields this
// example sends.
type alert struct {
	RoutingKey  string       `json:"routing_key"`
	EventAction string       `json:"event_action"`
	DedupKey    string       `json:"dedup_key"`
	Payload     alertPayload `json:"payload"`
}

type alertPayload struct {
	Summary       string            `json:"summary"`
	Source        string            `json:"source"`
	Severity      string            `json:"severity"`
	Timestamp     string            `json:"timestamp"`
	Component     string            `json:"component,omitempty"`
	CustomDetails map[string]string `json:"custom_details,omitempty"`
}

// Render turns one event into one alert. The dedup key is the run's own ID,
// which makes a failed run and its recovery one incident and makes senro's
// at-least-once delivery safe. Returning an error means no request; senro
// records it as an api.NotifyFailed carrying this message.
func (r Renderer) Render(e api.Event) ([]byte, error) {
	if r.RoutingKey == "" {
		return nil, errors.New("no PagerDuty routing key is configured for this destination")
	}
	source := r.Source
	if source == "" {
		source = "senro"
	}

	var body api.RunFinishedBody
	if err := e.Decode(&body); err != nil {
		return nil, fmt.Errorf("reading the run.finished payload: %w", err)
	}

	action, severity := "resolve", "info"
	if failed(body.Status) {
		action, severity = "trigger", "error"
	}
	return json.Marshal(alert{
		RoutingKey:  r.RoutingKey,
		EventAction: action,
		DedupKey:    "senro/" + e.Run,
		Payload: alertPayload{
			Summary: fmt.Sprintf("senro run %s %s in %s",
				e.Run, body.Status, body.Duration.Round(time.Millisecond)),
			Source:    source,
			Severity:  severity,
			Timestamp: e.TS.UTC().Format(time.RFC3339),
			Component: "senro",
			CustomDetails: map[string]string{
				"run":    e.Run,
				"status": string(body.Status),
			},
		},
	})
}

// failed reports whether a run's status is one worth waking somebody for. A
// partial run counts: some of it did not happen.
func failed(s api.RunStatus) bool {
	switch s {
	case api.RunFailed, api.RunCancelled, api.RunPartial:
		return true
	default:
		return false
	}
}

// Destination is the whole wiring, so using this package is one line. Only
// api.RunFinished, because an alert per step is an alert nobody reads; any
// notify.DestinationOption still applies and takes precedence:
//
//	pagerduty.Destination(key, "ci.example.com", notify.Retry(5, time.Second))
func Destination(routingKey, source string, opts ...notify.DestinationOption) *notify.Destination {
	defaults := []notify.DestinationOption{
		notify.Named("pagerduty"),
		notify.On(api.RunFinished),
	}
	return notify.To(EventsAPI, Renderer{RoutingKey: routingKey, Source: source},
		append(defaults, opts...)...)
}
