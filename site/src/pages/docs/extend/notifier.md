---
layout: ../../../layouts/DocsLayout.astro
title: Writing a notifier
---

# Writing a notifier

A notification destination is a URL plus a `notify.Renderer`: one method turning one event into
the bytes of one request body. Reach for one when you want a run's result somewhere the
[built-in destinations](/docs/notifications/) do not go: PagerDuty, Datadog, Discord, an internal
incident bus.

## The interface

```go
package notify

type Renderer interface {
	Render(api.Event) ([]byte, error)
}

func To(rawURL string, r Renderer, opts ...DestinationOption) *Destination
```

`notify.RendererFunc` adapts a plain function if you have no state to carry.

## The smallest one that works

```go
d := notify.To("https://example.com/incidents",
	notify.RendererFunc(func(e api.Event) ([]byte, error) {
		var b api.RunFinishedBody
		if err := e.Decode(&b); err != nil {
			return nil, fmt.Errorf("reading the run.finished payload: %w", err)
		}
		return json.Marshal(map[string]string{
			"run":    e.Run,
			"status": string(b.Status),
		})
	}),
	notify.Named("incidents"),
	notify.On(api.RunFinished),
)
```

That inherits the queue, the retries, the signing, the outcome events and the flush. The only
thing you wrote is the body.

> Set `Named`. Unnamed destinations are all called `destination`, in the ledger and in the
> shutdown report.

## The contract

### What you must guarantee

- **`Render` must return eventually.** One that blocks forever gets a short further window after
  the shutdown grace expires and the notifier is cancelled, then is abandoned and named in the
  shutdown report so the process can exit.
- **It must not retain or mutate the event.** The same `api.Event` goes to every other observer.
- **It should be deterministic for one event.** The body is rendered once and reused across
  retries, so a renderer that counts calls is counting events, not requests.
- **Filter with `On`, not inside `Render`.** Filtering happens before the queue. A destination that
  only wants `run.finished` must not have its queue filled with events it will discard, or the one
  event it exists for is the one that gets dropped.

### What senro guarantees you

- **The queue**: bounded, per destination, on its own goroutine. `Render` runs there, one event at
  a time, never on the goroutine running the build. It may be slow: that delays only this
  destination and fills its queue. `Emit` never blocks the engine, and a full queue drops rather
  than waits, with the loss counted as `notify.dropped`.
- **Retry**: up to `Retry(attempts, base)` requests per event, doubling and jittered, retrying only
  what is worth retrying (no answer, a 429, any 5xx) and never a 400 that will be a 400 again.
- **Timeouts**: `Timeout(d)` bounds one request; the notifier's grace bounds the whole shutdown.
- **Signing and headers**: `Sign(secret)` HMAC-signs the exact bytes you rendered, and
  `X-Senro-Event`, `X-Senro-Run`, `X-Senro-Seq` and the `X-Senro-Delivery` deduplication key go on
  every request. senro's own headers are set **after** yours and win: a destination may add to a
  request, but may not forge the key a receiver deduplicates on.
- **Outcome events**: every delivery becomes `notify.delivered`, `notify.failed` or
  `notify.dropped` in the run's ledger, naming your destination.
- **The flush**: `senro.Run` flushes before it returns, so `run.finished` is on its way out before
  your process is.
- **Redaction**: the engine redacts every event payload before the event exists, upstream of every
  sink. Your renderer has nothing left to do about it. See [Secrets](/docs/secrets/).

### What happens on error

An error from `Render` means **no request for this event**. It is recorded as `notify.failed` with
your error's text and not retried: a body that would not render will not render differently a
second later.

A panic is caught and becomes the same failed delivery. It does not fail the run and does not stop
the destination, which delivers the next event normally. It is still a bug.

## Wire it into a run

```go
n := notify.New(pagerduty.Destination(os.Getenv("PD_ROUTING_KEY"), "ci.example.com"))
defer func() { _ = n.Close() }()

err := senro.Run(ctx, pipeline, senro.WithSink(n))
```

Ship a constructor, so using your package is one line:

```go
func Destination(routingKey, source string, opts ...notify.DestinationOption) *notify.Destination {
	defaults := []notify.DestinationOption{
		notify.Named("pagerduty"),
		notify.On(api.RunFinished),
	}
	return notify.To(EventsAPI, Renderer{RoutingKey: routingKey, Source: source},
		append(defaults, opts...)...)
}
```

**Put your defaults first and the caller's options after.** `append(defaults, opts...)` means the
last option wins, so a caller's `notify.Retry(5, time.Second)` overrides you without you deciding
field by field what they may change.

[Every option](/docs/notifications/#options) applies to a destination built with `To`, exactly as
to `Webhook` and `Slack`. `ContentType` and `Header` exist for destinations that are not senro's
own:

```go
notify.To("https://http-intake.logs.datadoghq.com/api/v2/logs", myRenderer{},
	notify.Named("datadog"),
	notify.ContentType("application/x-ndjson"),
	notify.Header("DD-API-KEY", os.Getenv("DD_API_KEY")),
)
```

## When the endpoint moves: `Requester`

A destination that creates a resource on one event and updates it on the next does not fit
`Renderer`, which renders a body for a fixed URL. Implement `notify.Requester` instead:

```go
type Request struct {
	Method string // defaults to POST when empty
	URL    string // required
	Body   []byte // may be nil for a method that has none
}

type Requester interface {
	Request(api.Event) (*Request, error)
}

notify.To("", nil, notify.WithRequester(myRequester), notify.Named("mydest"))
```

- A destination with a Requester uses it **instead of** its Renderer; its configured URL becomes a
  base the Requester may use or ignore.
- Returning a nil `*Request` with a nil error means "nothing to send for this event", recorded as
  neither delivered nor failed.
- `Request` has no headers field on purpose: headers are declared once with `Header`, and senro's
  envelope headers still win.
- It is called on the delivery goroutine, one event at a time per destination, so you may keep
  state without a lock. It must not block for long, or the bounded queue behind it drops.
- A Requester that needs to **see the response** (the id of the resource it just created)
  implements `notify.ResponseReader`, called only on a 2xx.

The built-in worked example is GitHub Checks (`notify/githubchecks.go`): a check run POSTed once
and PATCHed thereafter, with the id learned from the create's response.

## Where your credential lives

A routing key in a request body and an API key passed to `Header` go through no redactor, which
only ever sees event payloads. **senro redacts the run's secrets out of the events you render
from, and takes no responsibility for a credential your renderer holds.** Keep it out of the
strings you put in the body.

The destination URL is treated as a credential regardless, since for a Slack incoming webhook it
is the whole of one: `notify` strips it out of every error it records or prints, for your
destination as much as for its own.

## The worked example

[`examples/extensions/pagerduty`](https://github.com/xavidop/senro/tree/main/examples/extensions/pagerduty)
is a full destination for PagerDuty's Events API v2, commented and driven end to end by senro's own
tests.

It maps a run's status to a `trigger` or `resolve` action, and uses the run's own ID as the
PagerDuty dedup key, so an at-least-once delivery is one incident rather than several and a later
resolve closes the one it opened.

Test yours the way senro tests those: an `httptest.Server` standing in for the endpoint, and
assertions on the bytes that arrived. The one thing a test substitutes is the URL, which is why
`To` takes it. For the outcome events as well as the request, hand the notifier an appender with
`n.SetAppender(...)`, which is what `senro.Run` does.

## Where to go next

- **[Notifications](/docs/notifications/)**: the built-in destinations, every option, and what a receiver sees.
- **[The event stream](/docs/reference/event-stream/)**: what an `api.Event` contains, which is what you are rendering.
- **[Writing a trace exporter](/docs/extend/exporter/)**: the same sink, folded into spans instead.
