---
layout: ../../layouts/DocsLayout.astro
title: Quickstart
---

# Quickstart

From nothing to a running, attachable pipeline. [Install](/docs/install/) first.

## 1. Write the pipeline

A pipeline is a Go package with a `main` that builds a `senro.Pipeline`, adds a `Workflow` and its
`Step`s, and calls `senro.Run`. Put this in `ci/main.go`:

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

	// Run builds p first, so a dangling Needs, a duplicate id, or an empty
	// command surfaces here, before anything executes.
	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		os.Exit(1)
	}
}
```

Four things are doing the work:

- `exec.Command` wraps any command as a step. [Steps](/docs/steps/) has the rest.
- `.Needs("test")` makes `build` wait for `test`. [Ordering](/docs/steps/ordering/).
- `.Retry(3, retry.OnInfra())` allows up to three attempts, but only when the environment failed
  the step (a dropped SSH connection, a killed process), never when the command simply exited
  non-zero. [Retries](/docs/steps/retries/).
- `attach.Listen` plus `senro.WithAttach` is the one addition that makes the run observable from
  another process. Without it, `senro.Run` costs exactly what the engine costs: no attach server,
  no extra goroutine. [Attach](/docs/attach/).

## 2. Run it

```bash
senro run ./ci
```

The CLI builds the package, execs it, and attaches automatically: a terminal UI on a TTY, plain
streaming lines otherwise. Or run the binary yourself and attach from a second terminal:

```bash
go run ./ci &
senro attach
```

## 3. Watch and steer it

Both routes end in the same place: an interactive view of `test` and `build` running.

| Key | What it does |
|---|---|
| `enter` | Focus a step and follow its log |
| `r` | Retry the focused step |
| `c` | Cancel the run |

[The TUI](/docs/attach/tui/) has the full key list and the other renderers;
[Control operations](/docs/attach/control-ops/) covers what each key asks the engine to do.

## 4. Reopen the run after it finished

Every run's events land on disk, so the process does not need to be alive:

```bash
senro attach --run <run-id>
```

This is the same client over the same protocol, reading recorded events and step logs instead of a
live socket. When a run fails, that directory is where the answer is:
[Reading a failed run](/docs/reference/debugging/) covers every file in `runs/<id>/` and where a
step's stdout and stderr live.

## Where to go next

- [Concepts](/docs/concepts/): pipeline, plan, execution, and the event stream underneath all of
  this.
- [Steps](/docs/steps/): `Env`, `WorkDir`, `Timeout`, `ContinueOnError`, and the two step kinds.
- [Handlers](/docs/steps/handlers/): `OnFailure` and `Always` when a step fails anyway.
- [Executors](/docs/executors/): the same steps in a container, on a pod, or over SSH.
- [Secrets](/docs/secrets/): a credential without ever putting it in argv, an env value, or a log.
- [CLI](/docs/cli/): every flag `senro run` and `senro attach` accept.
