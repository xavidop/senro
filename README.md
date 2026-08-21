<div align="center">

<img src="site/public/favicon.svg" width="56" height="56" alt="senro: a horizontal transit line with four stations along it, and a spur branching off partway to a fifth station" />

# 線路 &nbsp;senro

### A pipeline engine, defined in Go

*Define a pipeline as ordinary Go code, run it locally, in containers, on Kubernetes or over SSH,
and attach to it live from a second terminal or a browser while it runs.*

[![Go Reference](https://pkg.go.dev/badge/github.com/xavidop/senro.svg)](https://pkg.go.dev/github.com/xavidop/senro)
[![CI](https://github.com/xavidop/senro/actions/workflows/ci.yml/badge.svg)](https://github.com/xavidop/senro/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/xavidop/senro)](https://goreportcard.com/report/github.com/xavidop/senro)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

---

`senro` (線路, Japanese for *railway track*) is a pipeline engine you program instead of
configure. Your Go code builds an immutable graph of workflows and steps, `senro` resolves it into
a plan, executes it, and exposes one append-only event stream that a terminal UI, a browser page,
or a script can attach to, live, to watch and steer the run.

CI/CD is the most familiar thing to build with it: a pipeline that tests, builds and deploys reads
exactly like a CI job. It is not the boundary. A data pipeline, a batch job, an infrastructure
rollout, or a release script is built the same way: steps with dependencies, retries, failure
handlers, and one event stream.

## Why

CI systems ask you to describe real programming (dependency graphs, retries, branching, recovery)
in YAML, which has no functions, no types, no tests, and no debugger. So the logic ends up in
shell scripts the YAML merely invokes.

`senro` starts from the other end: the pipeline *is* the Go program. For the thing that decides
whether your build ships, you get a real language's tooling: the compiler, types, `go test`, and a
debugger.

## Features

- **Pipelines as code.** `New`, `Workflow`, `Step`, `Needs`, `Retry`, `Timeout`, `OnFailure`,
  `Always`: typed, composable, testable. See [Pipelines](site/src/pages/docs/steps/index.md).
- **Definition, plan, execution.** `Build()` resolves your code into an immutable, validated
  `Plan`; the engine executes the plan, never your code directly. That is what makes a run
  inspectable, replayable and attachable. See [Concepts](site/src/pages/docs/concepts.md).
- **Live attach.** A terminal UI (`senro attach`), a browser UI (`senro ui`, a Go client compiled
  to WebAssembly), or plain streaming lines, all folding the same event stream, live over a socket
  or replayed from disk with the same client. See [Attach](site/src/pages/docs/attach/index.md).
- **Control, not just watch.** Cancel, pause and resume a run; retry, skip and re-run from a step;
  set breakpoints; open an interactive shell inside a live step's sandbox, with a real terminal
  where the executor can host one. See [Control operations](site/src/pages/docs/attach/control-ops.md)
  and [The shell](site/src/pages/docs/attach/shell.md).
- **Four executors.** Local processes, containers on a local Docker daemon, one pod per step on
  Kubernetes, and processes on a remote host over your own SSH configuration. Chosen per workflow
  with `senro.On(...)`. See [Kubernetes](site/src/pages/docs/executors/kubernetes.md) and
  [SSH](site/src/pages/docs/executors/ssh.md).
- **Two step kinds.** `exec.Command` runs a command on any executor; `senro.Func` runs a
  registered Go function, on the coordinator, on an SSH host, or in a container, by staging the
  pipeline binary there and re-entering it. See
  [Func steps off the coordinator](site/src/pages/docs/executors/func-remote.md).
- **Fan-out for monorepos.** `Expand` builds one step per unit over eight unit graphs (`glob`,
  `gowork`, `cargo`, `jswork`, `maven`, `gradle`, `pyproject`, `bazel`), with per-unit edges
  (`NeedsEach`), duration-balanced sharding (`Partition`), and change-scoped runs (`Affected`).
  See [Fan-out](site/src/pages/docs/monorepo/fan-out.md) and [Monorepos](site/src/pages/docs/monorepo/index.md).
- **Caching that tells the truth.** Named workspaces snapshotted between steps (and, with bounds,
  between runs), an opt-in `Pure()` action cache keyed off declared inputs, a best-effort scratch
  cache, and a shared remote tier over an S3-compatible bucket or an OCI registry so a fleet
  starts warm. `senro verify --recheck-pure` audits purity claims empirically. See
  [Shared cache](site/src/pages/docs/data/shared-cache.md).
- **Secrets that stay out of logs.** Values are delivered as files, never argv or environment
  values; output is redacted, unsafe channels are refused outright before anything runs. Built on
  [mamori](https://github.com/xavidop/mamori). See [Secrets](site/src/pages/docs/secrets/index.md).
- **Triggers.** The pipeline binary is its own matcher: hand it a GitHub, GitLab, Bitbucket or
  Gitea webhook event (or your own provider) and it decides whether to run, with exit code 78 for
  "not my business". Or be the endpoint yourself: `trigger.FromRequest` verifies and parses a
  delivery straight off the wire, per source, with no event file in between.
  See [Triggers](site/src/pages/docs/triggers/index.md) and
  [Run it as a server](site/src/pages/docs/triggers/server.md).
- **Notifications and export.** Slack, GitHub Checks, webhooks, or your own destination through a
  small `Renderer`/`Requester` seam; traces exported from the event stream through a `Sink`. See
  [Notifications](site/src/pages/docs/notifications.md).
- **Failure analysis, gated.** Hand a failed step to your own analyzer (your SDK, your model, no
  provider dependency in senro); its proposal applies only when a human or an explicit policy
  accepts it. `contrib/genkitanalyzer` is one you can install, in its own module so senro's graph
  stays free of it. See [Writing a failure analyzer](site/src/pages/docs/analyzers/custom.md) and
  [An AI analyzer](site/src/pages/docs/analyzers/genkit.md).
- **Honest failure states.** A step ends in one of ten states, never a boolean; `recovered` (flaky
  but passed on retry) is not `succeeded`, and `retry.OnInfra()` retries broken infrastructure but
  never a failing test. See [Failure handling](site/src/pages/docs/steps/states.md).
- **Embeddable.** senro is a library first: no global state, no `os.Exit`, no reading your argv;
  attach is one opt-in call. The wire contract lives in `github.com/xavidop/senro/api` with no
  dependency beyond the standard library. See [Embedding](site/src/pages/docs/reference/embedding.md).

## Install

The CLI:

```bash
brew install xavidop/tap/senro
# or
go install github.com/xavidop/senro/cmd/senro@latest
```

The library, which is all a pipeline needs:

```bash
go get github.com/xavidop/senro
```

Released binaries for linux and darwin on amd64 and arm64, with checksums, SBOMs and SLSA
provenance, are on the [releases page](https://github.com/xavidop/senro/releases). See
[Install](site/src/pages/docs/install.md) for verifying a download.

Requires Go 1.26+ on Linux or macOS. Windows is deliberately unsupported; see
[Platform support](#platform-support).

## Quick start

A pipeline is a Go package with a `main` that builds the pipeline and calls `senro.Run`. This one
runs `go test` and, once that passes, `go build`, retrying only infrastructure failures, never a
failing test:

```go
// Command ci is a two-step pipeline: test, then build.
package main

import (
	"context"
	"log"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/retry"
)

func main() {
	ctx := context.Background()

	p := senro.New("ci")

	verify := p.Workflow("verify")
	verify.Step("test", exec.Command("go", "test", "./..."))
	verify.Step("build", exec.Command("go", "build", "./...")).
		Needs("test").
		Retry(3, retry.OnInfra()) // retries a dropped connection, never a failing test

	// Opens a unix socket a second terminal can attach to while this runs.
	att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket})
	if err != nil {
		log.Fatal(err)
	}
	defer att.Close()

	log.Printf("attach with: senro attach --pid %d", os.Getpid())

	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
```

Run it with the CLI, which builds the package, execs it, and attaches automatically (a TUI on a
TTY, plain lines otherwise):

```bash
senro run ./ci
```

Or run the binary yourself and attach from a second terminal, or open the browser view:

```bash
go run ./ci &
senro attach     # terminal UI
senro ui         # browser UI, prints a one-time link
```

Every run writes `runs/<id>/` with `events.jsonl` and per-step logs.
`senro attach --run <id>` reopens a finished run from disk over the exact same protocol; there is
no separate offline mode. From here, the
[getting started guide](site/src/pages/docs/quickstart.md) continues.

## Documentation

The full documentation lives under [`site/`](site/src/pages/docs/), an Astro site (`make site-dev`
serves it locally with Node 22 via nvm). It is the primary reference for what is implemented:

- **Start:** [Introduction](site/src/pages/docs/index.md),
  [Getting started](site/src/pages/docs/quickstart.md),
  [Concepts](site/src/pages/docs/concepts.md)
- **Build:** [Pipelines](site/src/pages/docs/steps/index.md),
  [Fan-out](site/src/pages/docs/monorepo/fan-out.md), [Monorepos](site/src/pages/docs/monorepo/index.md),
  [Failure handling](site/src/pages/docs/steps/states.md),
  [Secrets](site/src/pages/docs/secrets/index.md), [Kubernetes](site/src/pages/docs/executors/kubernetes.md),
  [SSH](site/src/pages/docs/executors/ssh.md)
- **Attach:** [The protocol](site/src/pages/docs/attach/index.md),
  [TUI](site/src/pages/docs/attach/tui.md), [Browser UI](site/src/pages/docs/attach/browser.md),
  [Security](site/src/pages/docs/attach/security.md)
- **Use:** [Embedding](site/src/pages/docs/reference/embedding.md),
  [Triggers](site/src/pages/docs/triggers/index.md),
  [Notifications](site/src/pages/docs/notifications.md),
  [Shared cache](site/src/pages/docs/data/shared-cache.md), [CLI](site/src/pages/docs/cli/index.md),
  [Reading a failed run](site/src/pages/docs/reference/debugging.md)
- **Extend:** [Write a unit graph](site/src/pages/docs/monorepo/unit-graphs/custom.md),
  [a trigger source](site/src/pages/docs/triggers/custom.md),
  [a notifier](site/src/pages/docs/notifications/custom.md),
  [a trace exporter](site/src/pages/docs/extend/exporter.md),
  [a failure analyzer](site/src/pages/docs/analyzers/custom.md)

Runnable examples live in [examples/](examples/), each a small `main` package with a doc comment
saying what it demonstrates. An [agent skill](skills/senro/) ships in-repo for AI coding tools.

## Platform support

`senro` targets **Linux and macOS**. Windows is not supported, deliberately: attach's security
boundary is a kernel peer-credential check with no Windows equivalent implemented, and senro fails
closed rather than advertising a feature it cannot secure. See
[Attach security](site/src/pages/docs/attach/security.md#platform-support) for the full reasoning.

## Contributing

Contributions are welcome. `make all` runs formatting, `go vet`, the linters and the test suite;
run it before sending a change. See [CONTRIBUTING.md](CONTRIBUTING.md) for the repository layout,
what the container, Kubernetes and SSH executors' tests need, and how releases are cut.

## License

[MIT](LICENSE) © Xavier Portilla Edo

## About

`senro` is built by [Xavier Portilla Edo](https://github.com/xavidop), and mirrors the structure,
tooling and release process of his other Go project,
[`mamori`](https://github.com/xavidop/mamori) (typed, watchable configuration and secrets for Go),
which senro's own secrets support is built directly on top of.
