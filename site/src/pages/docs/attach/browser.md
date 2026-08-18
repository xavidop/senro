---
layout: ../../../layouts/DocsLayout.astro
title: The browser UI
---

# The browser UI

`senro ui` serves a view of a **live** run in a browser, with controls. Run it beside the
pipeline; it prints a link that works once.

```console
$ senro ui
http://127.0.0.1:52413/h/kQ8x...
senro ui: serving a view of this run, with controls. The link above works once. Press Ctrl-C to stop.
```

Open the link. The page it lands on carries no credential in its URL.

## What it shows

Steps in the fold's own creation order, so the page and the terminal lay a run out identically.
Each step shows its state, its elapsed time and its dependencies, and expansion children are
indented under their parent, which carries the fold's group summary.

Selecting a step shows its detail (kind, attempt, exit code, error, any handlers that ran and how
they ended) and tails its output.

## Controls

The page offers the control operations [the TUI](/docs/attach/tui/) does, bar one, and decides
which to show from the folded state rather than from a fixed list:

| Scope | Offered | When |
| --- | --- | --- |
| Run | Pause / Resume | While the run is live. Follows a pause applied from anywhere, including `senro attach` or the CLI |
| Run | Cancel run | While the run is live. Asks first |
| Step | Release | The step is held at a breakpoint |
| Step | Retry, Rerun from here | The step has finished. `Rerun from here` asks first |
| Step | Break before, Skip | The step has not started. `Skip` asks first |

A finished run offers nothing, and neither does a running step: an operation the engine would
refuse is not drawn, because a button that produces a refusal teaches an operator to distrust it.

The one exception is [`ws.snapshot`](/docs/attach/control-ops/#forcing-a-snapshot). The UI server
forwards it, so it is not withheld from the browser, but no button draws it: whether a step mounts
a workspace at all is not in the folded state these decisions are made from, so the page cannot
tell a step the engine would accept it for from one it would answer `no_workspace`. The TUI's `w`
key, which shows the engine's refusal in its footer, is where that operation lives today.

Nothing is applied optimistically. A control request's answer is an event in the stream, so the
page changes when the run does, in the same sequence the TUI shows.

### What is not forwarded, and why

`POST /api/shell` has no route on the UI server and no handler in it. Use
[`senro shell`](/docs/attach/shell/) from `senro attach` instead.

A page that can steer a run is reasonable for whoever holds the one-time link; a page that can run
arbitrary commands is not, and the boundary is routing rather than a check somebody could forget
on the next endpoint.

Control is also held to a check the read routes are not: a `POST` must carry an `Origin` header
that exactly matches the UI server's own, and a request without one is refused.

That is not redundant with the `SameSite=Strict` cookie. A site does not include the port, so a
page served by any other process on `127.0.0.1` is same-site with this server and its fetches do
carry the cookie. The origin is what tells them apart.

## Where the token goes

The run's bearer credential ([Security](/docs/attach/security/)) never reaches the browser.
`senro ui` holds it in its own process and adds it to the routes it forwards.

A token in a URL is in browser history, `Referer` headers, and screenshots. A token in
`localStorage` or a readable cookie is one injected script away from being somebody else's.
Neither applies to a credential the page never has.

What the browser holds instead is a session cookie for the UI server alone:

- **HttpOnly**: neither the page's scripts nor the WebAssembly module can read it.
- **SameSite=Strict**: no other page can cause it to be sent.
- **Session-scoped**, with no `Expires`: it never reaches disk, and it is meaningless once
  `senro ui` exits anyway.

The cookie is minted by the one-time nonce in the printed link. That nonce is the single place a
credential touches a URL: it is in the terminal scrollback, and it may be in the browser's
history, because some browsers record a redirect chain.

It is not reusable. Spent on first use, the response redirects to `/`, and a second attempt gets
the same fixed 404 an unknown path does.

## What it binds

Loopback, always, with no flag to widen it. A browser UI on a routable address would put a live
build's view, and the session cookie that opens it, on the network in plaintext. The attach
server already refuses a non-loopback bind without TLS, with no opt-out; this is the same rule.

The `Host` header of every request is checked against the loopback names the server actually
bound under. That defends against DNS rebinding, where an attacker's domain resolves to
`127.0.0.1` after their page loaded and the browser then treats it as same-origin. The `Host`
header is the one thing that arrangement cannot change.

To watch a run on another machine, forward the port (`ssh -L`) and point a local `senro ui --addr`
at the forward, with the run's token in `$SENRO_ATTACH_TOKEN`. Both the credential and the traffic
are then protected by something.

## Resuming

The client speaks the protocol every other client speaks: `GET /api/state` for a snapshot, then
`GET /api/stream?from=<state.seq+1>` to tail it.

It also handles the retained ring passing the resume point. Whether that arrives as a `410 Gone`
before the stream opens or as a terminal `overflowed` marker mid-stream, the client takes a fresh
snapshot and subscribes from it.

The snapshot **replaces** the state rather than merging, because the events in between are gone
and merging would render a run that never happened.

A stream that ends without saying why (the connection died before the server could write its
marker) is reconnected from where it got to, paced, and bounded: a peer that answers every
subscription and immediately closes it is reported rather than looped on forever.

## What it does not do

A run that has already finished has no attach server, and this client speaks HTTP to one. Read a
finished run with `senro attach --run <id> --follow`, which tails the run's own files from disk.

It does not open a shell; see [What is not forwarded, and why](#what-is-not-forwarded-and-why).

## Why the client is Go and not JavaScript

The page is a Go client compiled to WebAssembly, and it folds the run's events with
`api.RunState.Apply`, the same fold the TUI uses, the attach server keeps its own state with, and
offline replay runs. `Apply` is a few hundred lines of rules, most of them not obvious:

- A new attempt clears the previous one's terminal state, so a retried step shows as pending, not
  as the failure it is recovering from.
- A new attempt drops the previous attempt's log high-water marks, because the next attempt
  writes a different file starting at byte zero.
- A log high-water mark comes from the marker's own offset, not from accumulation, so replaying
  an already-folded event cannot inflate the count.
- A handler is not a step: it has no dependencies, nothing depends on it, and counting it as one
  makes every step count wrong.
- An out-of-order sequence number is an error; a repeat is idempotent; a forward gap is accepted.

A JavaScript reimplementation would be correct until it was not, and the failure would be silent:
the browser reporting a pass while the terminal reports a fail, on the same run. That is why
[`api`](/docs/reference/api/) is standard-library only.

A test enforces it: no package the client is built from may name an event type, and the fold is
called from exactly one file.

## What it costs

The client is **4.0MB** of WebAssembly, **1.12MB gzipped**, which is what the `senro` binary
embeds and what a browser downloads. Most of that is unavoidable: a hello-world `js/wasm` binary
is already 1.9MB.

The one large avoidable cost was `net/http`, which on `js/wasm` links the whole HTTP client and
server tree to reach a wrapper around `fetch`; linking it took the client to **10.9MB (2.9MB
gzipped)**. So the client calls `fetch` directly through `syscall/js`, and gets its HTTP
semantics from a transport-agnostic package tested on the host against a real attach server.

It is embedded gzipped rather than compressed on the way out, so the binary grows by the
compressed figure: **9.1MB to 10.2MB**, all but 28KB of it the client. Everything served carries
an `ETag`, so a reload is a conditional request answered with `304` and no body.

The compiled client is not committed. `make wasm` builds it and stages it where the binary embeds
it from, and `make all` and the release build both run it. A tree that has not run it produces a
`senro` where every command works except this one, which refuses and says exactly what to run.
