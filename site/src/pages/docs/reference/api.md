---
layout: ../../../layouts/DocsLayout.astro
title: "api: the wire contract"
---

# `api`: the wire contract

`github.com/xavidop/senro/api` is a Go package with the types you need to read a run from the
outside: the event envelope, the attach protocol's request/response frames, the run and step
status enums, and a function that turns a stream of events into live state. Reach for it when
you're writing your own client or tool instead of using the shipped TUI or browser UI.

It's part of the main `senro` module, so `go get github.com/xavidop/senro` is all you need.

## What's in it

- **`Event`** and **`Type`**: the envelope every stream entry uses, and the full list of event
  types. See [The event stream](/docs/reference/event-stream/).
- **`Frame`**, **`Kind`**, and the `Op*` constants: the attach wire protocol. See
  [Control operations](/docs/attach/control-ops/).
- **`State`** and **`RunStatus`**: a step's final state and a run's overall outcome, plus
  `RollUp([]State) RunStatus`, the function that computes one from the other. See
  [Step states](/docs/steps/states/).
- **`RunState`** and **`(*RunState).Apply(Event) error`**: turns a stream of events into a live
  picture of a run. The attach server, the TUI, the browser UI, and offline replay all use this
  same function, so they always agree on what a run's state is.
- **`Version`**, **`VersionMinor`**, **`CheckVersion`**: for checking that a client and the engine
  speak compatible versions of the protocol. See [Control operations](/docs/attach/control-ops/).

## Small and dependency-free

`api` imports nothing beyond the Go standard library. That keeps it easy to depend on: a Slack bot
that posts on `run.finished`, a status-page poller, or your own TUI only needs `api`'s types, not
the rest of senro. See [A Func step off the coordinator](/docs/executors/func-remote/) for another
place this matters: it bounds what has to be shipped to a remote host.

## Stability

Changes to `api` are additive: a type is never renamed, removed, or repurposed within the same
major protocol version (the one `api.Version` reports).

As a consumer, the rule for you is: **ignore event types and struct fields you don't recognize.**
`encoding/json` already skips unknown struct fields, and `Type.Known()` skips unrecognized event
types rather than erroring. That's what lets the protocol add new things later without breaking
your client.

## Where to go next

- **[The event stream](/docs/reference/event-stream/)**: the envelope, in depth.
- **[Attach](/docs/attach/)**: the protocol built on these types.
- **[Step states](/docs/steps/states/)**: what `State` and `RunStatus` mean.
