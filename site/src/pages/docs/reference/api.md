---
layout: ../../../layouts/DocsLayout.astro
title: "api: the wire contract"
---

# `api`: the wire contract

`github.com/xavidop/senro/api` is the wire contract: the event envelope written to disk, the
attach frame protocol, the run and step state enums, and the function that turns an event stream
into `RunState`. You need it when you write your own client or tooling that reads a run.

It is a plain package of the one `senro` module, `github.com/xavidop/senro`: no per-package
version, no separate release tag.

## What is in it

- **`Event`** and **`Type`**: the envelope every stream entry uses, and the full set of event
  types, split into what this build declares (`DeclaredTypes()`) and what is reserved for later.
  See [The event stream](/docs/reference/event-stream/).
- **`Frame`**, **`Kind`**, and the `Op*` constants: the attach wire protocol. See
  [Control operations](/docs/attach/control-ops/).
- **`State`** and **`RunStatus`**: a step's terminal state and a run's rolled-up outcome, plus
  `RollUp([]State) RunStatus`, the pure function that reduces one to the other. See
  [Step states](/docs/steps/states/).
- **`RunState`** and **`(*RunState).Apply(Event) error`**: one function, used by the live attach
  server, the TUI, the browser UI and offline replay alike, so they cannot disagree about what a
  stream means.
- **`Version`**, **`VersionMinor`**, **`CheckVersion`**: protocol version negotiation. An equal
  major version is required to interoperate at all; a minor mismatch only warns. See
  [Control operations](/docs/attach/control-ops/).

## No dependency of its own

`api`'s own code imports nothing beyond the standard library, and `api/nodeps_test.go` enforces
that in code, not just in a comment: a build that adds a dependency to this package tree fails its
own test suite.

That keeps third-party clients small. A Slack bot posting on `run.finished`, a status-page poller,
or a completely different TUI needs nothing but `api`'s types, and should never have to write, or
trust, more code than the protocol requires, however senro's own dependency graph grows.

It also bounds what a `Func` step running on an SSH host has to carry across; see
[A Func step off the coordinator](/docs/executors/func-remote/).

> **What this does not mean:** `api` ships inside the one `senro` module, so a client that runs
> `go get github.com/xavidop/senro` and imports only `api` still resolves the whole module's
> `go.mod` (bubbletea, the executor packages, everything else), even though none of it is imported
> by the client's own code. Only the package's source is dependency-free; the module it ships in
> is not.

## Stability

Within a major **protocol** version, the one `api.Version` reports, changes to `api` are additive
only: a type is never renamed, removed, or repurposed. That is a promise about the protocol, not
about a module tag; no tagged release of senro has been cut yet.

The flip side is a rule for you as a consumer: **ignore event types and struct fields you do not
recognize.** `encoding/json` already does this for unknown struct fields, and `Type.Known()` and
the `RunState` fold both skip an unrecognized `Type` rather than erroring. A newer engine will
eventually emit both; that is the point of the protocol being versioned.

## Released with the rest of senro

One `vX.Y.Z` tag covers `api` and everything else in this repository: no separate version number,
nothing to keep in sync between two tags. Releases are automated from Conventional Commits landing
on `main`; see
[CONTRIBUTING.md](https://github.com/xavidop/senro/blob/main/CONTRIBUTING.md#releases) on GitHub.

## Where to go next

- **[The event stream](/docs/reference/event-stream/)**: the envelope, in depth.
- **[Attach](/docs/attach/)**: the protocol built on these types.
- **[Step states](/docs/steps/states/)**: what `State` and `RunStatus` mean.
