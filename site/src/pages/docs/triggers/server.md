---
layout: ../../../layouts/DocsLayout.astro
title: Run it as a server
---

# Run it as a server

Your pipeline binary can be the webhook endpoint. One line changes:

```go
ev, err := trigger.FromRequest(r, trigger.Secret(hookSecret))  // instead of LoadEvent(path)
```

No event file, no dispatcher, no second binary. Everything after that line is the same pipeline
and the same triggers.

Runnable: [`examples/server`](https://github.com/xavidop/senro/tree/main/examples/server).

## The handler

```go
func (s *server) webhook(w http.ResponseWriter, r *http.Request) {
	ev, err := trigger.FromRequest(r, trigger.Secret(s.secret))
	if err != nil {
		httpError(w, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)

	// Not r.Context(): it is cancelled when the response is written.
	go senro.Run(context.Background(), pipeline(),
		senro.WithTrigger(ev, trigger.OnPush(trigger.Branches("main"))))
}
```

`FromRequest` does three things: works out which source sent the delivery, verifies its signature,
and parses the body into the same [`trigger.Event`](/docs/triggers/) a file would have produced.

Two things it does not do: manage [concurrency](#one-run-at-a-time), and wait for the run.

> **Reply before the run finishes.** A webhook sender times out in seconds; a pipeline takes
> minutes. Reply `202` and run in the background.

> **Never pass `r.Context()` to `senro.Run`.** It is cancelled the moment the response is written,
> which kills the pipeline the instant it starts.

## Verifying each provider

The four sources authenticate differently. `trigger.Secret` covers the difference; you pass the
same secret you configured on the webhook.

> **Verification is not optional.** `FromRequest` with neither `Secret` nor `Unverified` is an
> error, because an endpoint that verifies nothing runs your pipeline for anybody who can reach
> it. Passing both is an error too, not a precedence rule.

### GitHub

```go
trigger.FromRequest(r, trigger.Secret(os.Getenv("SENRO_HOOK_SECRET")))
```

HMAC-SHA256 over the raw body, in `X-Hub-Signature-256`. Set the same secret in the repository's
webhook settings.

### GitLab

```go
trigger.FromRequest(r, trigger.Secret(os.Getenv("SENRO_HOOK_SECRET")))
```

GitLab sends the secret **itself** in `X-Gitlab-Token`, unsigned. That is only as good as the
transport, so serve this over HTTPS.

### Gitea

```go
trigger.FromRequest(r, trigger.Secret(os.Getenv("SENRO_HOOK_SECRET")))
```

Same HMAC as GitHub, in `X-Gitea-Signature`. Newer builds also send `X-Hub-Signature-256`; either
is accepted.

### Bitbucket

```go
trigger.FromRequest(r, trigger.Unverified())
```

**Bitbucket Cloud signs nothing and sends no token.** There is nothing for a secret to check, so
senro says so instead of pretending. Restrict the endpoint by network: Bitbucket publishes its
egress ranges.

`Unverified()` is also the answer when a proxy in front of you already verified the delivery.

## Status codes

```go
func httpError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, trigger.ErrUnsigned):
		http.Error(w, "unauthorized\n", http.StatusUnauthorized)
	case errors.Is(err, trigger.ErrUnknownSource):
		http.Error(w, "unrecognised delivery\n", http.StatusBadRequest)
	default:
		http.Error(w, "could not read the delivery\n", http.StatusBadRequest)
	}
}
```

| Error | Means | Answer |
|---|---|---|
| `trigger.ErrUnsigned` | Signature absent, malformed **or** wrong | `401` |
| `trigger.ErrUnknownSource` | No header naming a source senro knows | `400` |
| anything else | Body unreadable, too large, unparseable | `400` |

`ErrUnsigned` covers all three failure modes on purpose: which one it was is not something to
confirm to whoever sent it.

## One run at a time

senro has no opinion here. Pick one and write it:

```go
func (s *server) take() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	return true
}
```

```go
if !s.take() {
	http.Error(w, "a run is already in progress\n", http.StatusConflict)
	return
}
```

`409` says "busy, nothing queued, retrying immediately will not help". That is reject-don't-queue,
the same choice [contrib/dispatcher](#two-processes-instead) makes. A queue has a backlog, an
eviction policy and a memory: worth having on purpose, not by accident.

## A custom provider

A [source of your own](/docs/triggers/custom/) has no header `FromRequest` could recognise, so say
what to parse it as:

```go
ev, err := trigger.FromRequest(r,
	trigger.As("deploy-bus", r.Header.Get("X-Bus-Event")),
	trigger.WithProviders(deploybus.Provider{}),
	trigger.Unverified())   // senro knows no signature scheme for your bus
```

Verify the delivery yourself before calling, since `Unverified()` means senro checks nothing.

`As` also pins a built-in, for a proxy that rewrites headers or an endpoint that only ever
receives one source.

## Without `FromRequest`

If you already hold the pieces separately:

```go
func Parse(provider, event string, payload []byte, providers ...Provider) (*Event, error)
```

```go
ev, err := trigger.Parse("github", r.Header.Get("X-GitHub-Event"), body)
```

No signature check, no header sniffing, no body read: those are yours. It is the primitive
`FromRequest` is built on.

## Two processes instead

Keep the pipeline a one-shot binary and put a receiver in front of it. senro ships one at
[`contrib/dispatcher`](https://github.com/xavidop/senro/tree/main/contrib/dispatcher), which adds
a per-group lock (a file lock, or a Kubernetes `Lease` with `-namespace`):

```sh
dispatcher -addr :8080 -secret-file /etc/senro/webhook-secret \
           -pipeline ./ci -group ci-main
```

**A raw webhook body is not an event file.** No GitHub, GitLab, Bitbucket or Gitea body says which
event it is, which is why [the file format](/docs/triggers/events/#the-format) is an envelope.
Writing the body verbatim gets `the event names no provider` from the pipeline, at the far end of
an exec where nobody is looking.

Two helpers build one, and are what the dispatcher uses:

```go
provider, event, ok := trigger.SourceOf(r.Header)   // from the delivery's headers
file, err := trigger.Envelope(provider, event, body)
```

### Which shape to pick

| | A server | A dispatcher |
|---|---|---|
| Processes | One | Two |
| Concurrency | Yours to write | A lock, `-cancel-in-progress` |
| A crash takes down | Endpoint **and** run | The run only |
| Replicas exclude each other | You arrange it | `-namespace`, over a `Lease` |
| Pipelines per endpoint | One | One dispatcher each |

Start with a server for one pipeline and one thing to deploy. Move to a dispatcher when a crashed
run should not take the endpoint with it.

## Where to go next

- **[Triggers](/docs/triggers/)**: the matchers and the three outcomes.
- **[The event file](/docs/triggers/events/)**: the envelope, for the two-process shape.
- **[Write your own](/docs/triggers/custom/)**: a source that is not one of the four.
- **[GitHub Checks](/docs/notifications/github-checks/)**: reporting the result back on the commit.
