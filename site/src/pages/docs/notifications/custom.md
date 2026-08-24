---
layout: ../../../layouts/DocsLayout.astro
title: Write a destination
---

# Write a destination

A destination is a URL plus one method that turns an event into a request body. Everything else
(the queue, retries, signing, headers, outcome events, the end-of-run flush) you inherit.

Reach for this when you want a run's result somewhere senro does not ship: PagerDuty, Datadog,
Discord, an internal incident bus.

```go
type Renderer interface {
	Render(api.Event) ([]byte, error)
}

func To(rawURL string, r Renderer, opts ...DestinationOption) *Destination
```

## Build one in three steps

### 1. Render the body

`notify.RendererFunc` adapts a plain function, which is enough when you have no state to carry:

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

> **Always set `Named`.** Unnamed destinations are all called `destination`, in the ledger and in
> the shutdown report, so two of them are two lines nobody can tell apart.

### 2. Filter with `On`, not inside `Render`

Filtering happens **before** the queue. A destination that only wants `run.finished` must not have
its queue filled with events it is going to discard, or the one event it exists for is the one
that gets dropped.

```go
notify.On(api.RunFinished)                                  // yes
// if e.Type != api.RunFinished { return nil, nil }         // no
```

### 3. Wire it in

```go
n := notify.New(notify.Slack(slackURL), d)
defer func() { _ = n.Close() }()

err := senro.Run(ctx, pipeline, senro.WithSink(n))
```

## Ship it as a package

If other people will use your destination, give them a constructor so it is one line:

```go
package pagerduty

const EventsAPI = "https://events.pagerduty.com/v2/enqueue"

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
last option wins, so a caller's `notify.Retry(5, time.Second)` overrides yours without you having
to decide field by field what they may change. Every built-in destination is written this way.

## The rules

### What you must do

| | |
|---|---|
| **Return eventually** | A `Render` that blocks forever gets a short window after the shutdown grace expires, then is abandoned and named in the shutdown report so the process can exit. |
| **Do not retain or mutate the event** | The same `api.Event` goes to every other observer. |
| **Be deterministic for one event** | The body is rendered once and reused across retries, so a renderer that counts calls is counting events, not requests. |
| **Filter with `On`** | See above. |

### What you get for free

| | |
|---|---|
| **A queue** | Bounded, per destination, on its own goroutine. `Render` runs there, one event at a time, never on the goroutine running the build. Being slow delays only this destination; a full queue drops rather than waits, counted as `notify.dropped`. |
| **Retries** | Up to `Retry(attempts, base)` requests per event, doubling and jittered, retrying only what is worth retrying (no answer, a 429, any 5xx) and never a 400 that will be a 400 again. |
| **Timeouts** | `Timeout(d)` bounds one request; the notifier's grace bounds the whole shutdown. |
| **Signing and headers** | `Sign(secret)` HMAC-signs the exact bytes you rendered, and `X-Senro-Event`, `X-Senro-Run`, `X-Senro-Seq` and `X-Senro-Delivery` go on every request. senro's headers are set after yours and win. |
| **Outcome events** | Every delivery becomes `notify.delivered`, `notify.failed` or `notify.dropped` in the run's ledger, naming your destination. |
| **The flush** | `senro.Run` flushes before it returns, so `run.finished` is on its way out before your process is. |
| **Redaction** | The engine redacts every event payload before the event exists, upstream of every sink. Your renderer has nothing left to do about it. See [Secrets](/docs/secrets/). |

Every [option](/docs/notifications/#options) applies to a destination built with `To`, exactly as
it does to `Webhook` and `Slack`.

### What happens on error

An error from `Render` means **no request for this event**. It is recorded as `notify.failed` with
your error's text, and not retried: a body that would not render will not render differently a
second later.

A panic is caught and becomes the same failed delivery. It does not fail the run and does not stop
the destination, which delivers the next event normally. It is still a bug.

## When the URL changes per event: `Requester`

A destination that **creates** a resource on one event and **updates** it on the next does not fit
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
  neither delivered nor failed. That is how a Requester filters.
- `Request` has no headers field: headers are declared once with `Header`, and senro's envelope
  headers still win.
- It is called on the delivery goroutine, one event at a time per destination, so you may keep
  state without a lock. It must not block for long, or the bounded queue behind it drops.
- A Requester that needs to **read the response** (the id of the resource it just created)
  implements `notify.ResponseReader`, called only on a 2xx.

[GitHub Checks](/docs/notifications/github-checks/) is the built-in worked example: a check run
POSTed once and PATCHed thereafter, with the id learned from the create's response
(`notify/githubchecks.go`).

## Your credential is yours

A routing key in a request body and an API key passed to `Header` go through **no redactor**,
which only ever sees event payloads. senro redacts the run's secrets out of the events you render
from, and takes no responsibility for a credential your renderer holds. Keep it out of the strings
you put in the body.

The destination URL is treated as a credential regardless, since for a Slack incoming webhook it
is the whole of one: `notify` strips it out of every error it records or prints, for your
destination as much as for its own.

## Testing

Stand up an `httptest.Server`, point the destination at it, and assert on the bytes that arrived.
The one thing a test substitutes is the URL, which is why `To` takes it:

```go
func TestBodyShape(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()

	n := notify.New(notify.To(srv.URL, Renderer{}, notify.On(api.RunFinished)))
	// ... emit an event, then n.Flush(ctx) ...

	if !strings.Contains(string(got), `"status":"failed"`) {
		t.Errorf("body %s", got)
	}
}
```

For the outcome events as well as the request, hand the notifier an appender with
`n.SetAppender(...)`, which is what `senro.Run` does.

## The worked example

[`examples/extensions/pagerduty`](https://github.com/xavidop/senro/tree/main/examples/extensions/pagerduty)
is a full destination for PagerDuty's Events API v2, commented and driven end to end by senro's
own tests.

It maps a run's status to a `trigger` or `resolve` action, and uses the run's own ID as the
PagerDuty dedup key, so an at-least-once delivery is one incident rather than several, and a later
resolve closes the one it opened.

## Where to go next

- **[Notifications](/docs/notifications/)**: the options, the headers, the failure reporting.
- **[The event stream](/docs/run/event-stream/)**: what an `api.Event` contains, which is
  what you are rendering.
- **[Writing a trace exporter](/docs/extend/exporter/)**: the same sink, folded into spans instead.
