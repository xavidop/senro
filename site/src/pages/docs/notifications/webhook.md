---
layout: ../../../layouts/DocsLayout.astro
title: Webhook
---

# Webhook

Posts each event to an HTTP endpoint as JSON: the `api.Event` itself, exactly as
[`event.schema.json`](/docs/reference/api/) describes it. A receiver needs no senro-specific
knowledge, just the schema.

```go
import "github.com/xavidop/senro/notify"

n := notify.New(
	notify.Webhook("https://ci.example.com/hooks/senro",
		notify.Sign(os.Getenv("SENRO_HOOK_SECRET"))),
)
defer func() { _ = n.Close() }()

err := senro.Run(ctx, p, senro.WithSink(n))
```

**A webhook receives every event by default.** That is the opposite of Slack's default, because a
program can drop what it does not want. Narrow it when you know what you need:

```go
notify.Webhook(url, notify.On(api.RunStarted, api.StepFinished, api.RunFinished))
```

## What a request looks like

```http
POST /hooks/senro HTTP/1.1
Content-Type: application/json
X-Senro-Event: step.finished
X-Senro-Run: 20260807T101503-a1b2c3
X-Senro-Seq: 42
X-Senro-Delivery: 20260807T101503-a1b2c3/42
X-Senro-Timestamp: 1786000000
X-Senro-Signature: v1=9f0c...

{"v":1,"seq":42,"ts":"2026-08-07T10:15:41Z","run":"20260807T101503-a1b2c3",
 "type":"step.finished","step":"build",
 "payload":{"state":"failed","exit_code":2,"duration":"12.4s"}}
```

Route on `X-Senro-Event` without parsing the body. Deduplicate on `X-Senro-Delivery`, which
identifies one event permanently: [delivery is
at-least-once](/docs/notifications/#what-every-request-carries).

## Verify a signed request

`Sign(secret)` adds two headers, computed over the raw request body:

```
X-Senro-Timestamp: <unix seconds>
X-Senro-Signature: v1=<hex(HMAC_SHA256(secret, timestamp + "." + body))>
```

To verify, recompute the same HMAC over the exact bytes you received:

```go
func verify(r *http.Request, secret []byte) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	ts := r.Header.Get("X-Senro-Timestamp")
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, errors.New("no usable timestamp")
	}
	// Reject anything far from your own clock, so a captured request cannot
	// be replayed forever.
	if d := time.Since(time.Unix(secs, 0)); d > 5*time.Minute || d < -5*time.Minute {
		return nil, errors.New("timestamp outside the accepted window")
	}

	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%s.%s", ts, body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))

	// Constant-time. Never ==.
	if !hmac.Equal([]byte(want), []byte(r.Header.Get("X-Senro-Signature"))) {
		return nil, errors.New("signature mismatch")
	}
	return body, nil
}
```

Two things people get wrong: reading the body twice (the signature is over the **raw** bytes, so
verify before decoding), and comparing with `==` instead of `hmac.Equal`.

## Sending somewhere that is not senro-shaped

`Header` and `ContentType` cover an endpoint that wants its own envelope, an API key, or a
different media type:

```go
notify.Webhook("https://http-intake.logs.datadoghq.com/api/v2/logs",
	notify.Named("datadog"),
	notify.ContentType("application/x-ndjson"),
	notify.Header("DD-API-KEY", os.Getenv("DD_API_KEY")),
)
```

If the **body** has to change shape, that is a renderer, not an option. See
[Write your own](/docs/notifications/custom/).

> senro's own headers are set **after** yours and win. A destination may add to a request, but may
> not forge the key a receiver deduplicates on.

## Options

Every [option](/docs/notifications/#options) works here. The ones that matter most for a webhook:

| | |
|---|---|
| `On(types...)` | Which events are sent. Default: all of them. |
| `Sign(secret)` | HMAC-sign every request. Off by default. |
| `Retry(attempts, base)` | Total requests per event, and the first backoff. Default: 3, 250ms. |
| `Timeout(d)` | Bounds one request. Default: 5s. |
| `Client(c)` | Your own `*http.Client`, for a proxy or a pinned CA. |

## Where to go next

- **[The `api` package](/docs/reference/api/)**: the schema your receiver decodes.
- **[The event stream](/docs/reference/event-stream/)**: every event type, and what its payload
  holds.
- **[Notifications](/docs/notifications/)**: retries, drops, and how a failed delivery is reported.
