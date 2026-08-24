---
layout: ../../../layouts/DocsLayout.astro
title: The browser UI
---

# The browser UI

`senro ui` shows a **live** run in a browser, with controls. Run it alongside the pipeline. It
prints a link that works once.

```console
$ senro ui
http://127.0.0.1:52413/h/kQ8x...
senro ui: serving a view of this run, with controls. The link above works once. Press Ctrl-C to stop.
```

Open the link. The page it lands on carries no credential in its URL.

## What it shows

Steps appear in the same order as the terminal UI, so the two match. Each step shows its state,
elapsed time, and dependencies. Steps expanded from a fold are indented under their parent, which
shows the group's summary.

Selecting a step shows its detail (kind, attempt, exit code, error, any handlers that ran and how
they ended) and tails its output.

## Controls

The page offers the same control operations [the TUI](/docs/attach/tui/) does, except one. It
decides which to show based on each step's current state, not a fixed list:

| Scope | Offered | When |
| --- | --- | --- |
| Run | Pause / Resume | While the run is live. Follows a pause applied from anywhere, including `senro attach` or the CLI |
| Run | Cancel run | While the run is live. Asks first |
| Step | Release | The step is held at a breakpoint |
| Step | Retry, Rerun from here | The step has finished. `Rerun from here` asks first |
| Step | Break before, Skip | The step has not started. `Skip` asks first |

A finished run offers no controls, and neither does a step that's currently running. If the engine
would refuse an operation, the button for it isn't shown at all. A button that just produces a
refusal trains people to ignore it.

The one exception is [`ws.snapshot`](/docs/attach/control-ops/#forcing-a-snapshot). The UI server
can forward this operation, but no button triggers it, because the page can't tell in advance
whether a step has a workspace to snapshot. Use the TUI's `w` key for this instead: it shows the
engine's refusal if there's nothing to capture.

Nothing is applied optimistically. A control request's answer arrives as an event in the stream,
so the page updates exactly when the run does, and in the same order the TUI shows.

### What is not forwarded, and why

`POST /api/shell` has no route on the UI server and no handler in it. Use
[`senro shell`](/docs/attach/shell/) from `senro attach` instead.

A page that can steer a run is a reasonable thing to hand to whoever holds the one-time link. A
page that can run arbitrary commands is not. That boundary is enforced by routing: the shell
endpoint simply doesn't exist here, rather than by a check that could be forgotten later.

Control requests are held to a check the read routes aren't: a `POST` must carry an `Origin`
header that exactly matches the UI server's own. A request without one is refused.

This isn't redundant with the `SameSite=Strict` cookie. A "site" doesn't include the port, so a
page served by any other process on `127.0.0.1` counts as same-site with this server, and its
requests would still carry the cookie. The `Origin` check is what tells them apart.

## Where the token goes

The run's bearer credential ([Security](/docs/attach/security/)) never reaches the browser.
`senro ui` holds it in its own process and adds it to the routes it forwards.

A token in a URL ends up in browser history, `Referer` headers, and screenshots. A token in
`localStorage` or a readable cookie is one injected script away from being stolen. Neither risk
applies here, because the page never has the credential at all.

What the browser holds instead is a session cookie for the UI server alone:

- **HttpOnly**: neither the page's scripts nor the WebAssembly module can read it.
- **SameSite=Strict**: no other page can cause it to be sent.
- **Session-scoped**, with no `Expires`: it never reaches disk, and it is meaningless once
  `senro ui` exits anyway.

The cookie is minted from the one-time nonce in the printed link. That nonce is the only place a
credential ever touches a URL: it ends up in your terminal scrollback, and possibly in browser
history, since some browsers record redirect chains.

It's not reusable. The first use redirects to `/` and spends it; a second attempt gets the same
404 as any unknown path.

## What it binds

Always loopback. There's no flag to widen it. A browser UI on a routable address would put a live
build's view, and the session cookie that opens it, on the network in plaintext. The attach server
already refuses a non-loopback bind without TLS; this follows the same rule.

Every request's `Host` header is checked against the loopback names the server actually bound to.
That defends against DNS rebinding, where an attacker's domain resolves to `127.0.0.1` after the
page loads, tricking the browser into treating it as same-origin. The `Host` header is the one
thing that trick can't fake.

To watch a run on another machine, forward the port (`ssh -L`) and point a local
`senro ui --addr` at the forward, with the run's token in `$SENRO_ATTACH_TOKEN`. That way both the
credential and the traffic are protected.

## Resuming

The client speaks the protocol every other client speaks: `GET /api/state` for a snapshot, then
`GET /api/stream?from=<state.seq+1>` to tail it.

It also handles the case where the resume point has fallen out of the retained event buffer.
Whether that shows up as a `410 Gone` before the stream opens or as a terminal `overflowed` marker
mid-stream, the client just takes a fresh snapshot and subscribes from there.

The new snapshot **replaces** the old state instead of merging with it: the events in between are
gone, and merging would show a run that never actually happened.

If a stream ends without saying why (the connection died before the server could write its
marker), the client reconnects from where it left off, with pacing and a limit, so it won't loop
forever against a server that immediately closes every subscription.

## What it does not do

A finished run has no attach server, and this client only speaks HTTP to one. To read a finished
run, use `senro attach --run <id> --follow`, which tails the run's own files from disk.

It does not open a shell; see [What is not forwarded, and why](#what-is-not-forwarded-and-why).

## Why the client is Go and not JavaScript

The browser page is a Go client compiled to WebAssembly. It folds the run's events using the exact
same code the TUI, the attach server, and offline replay all use (`api.RunState.Apply`). That's
deliberate: a separate JavaScript implementation of that logic could drift out of sync, and the
failure would be silent: the browser showing a pass while the terminal shows a fail, for the same
run. Using one shared implementation rules that out.

## What it costs

The client is about **4.0MB** of WebAssembly (**1.1MB gzipped**), embedded in the `senro` binary
and downloaded by the browser once per session. Most of that size is unavoidable: a minimal Go
WebAssembly binary already starts around 2MB. The binary embeds the client gzipped, and everything
it serves carries an `ETag`, so a reload after the first load is a fast conditional request with no
body.
