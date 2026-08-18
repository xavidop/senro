---
layout: ../../../layouts/DocsLayout.astro
title: Embedding
---

# Embedding

There is no separate embedding API. `senro run` is convenience wrapped around the same three calls
any Go `main` makes to become an attachable pipeline: `attach.Listen`, `senro.Run`,
`senro.WithAttach`. This page is the run options and how to handle a run's outcome.

```go
package main

import (
	"context"
	"log"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
)

func main() {
	ctx := context.Background()

	p := senro.New("ci")
	p.Workflow("verify").Step("test", exec.Command("go", "test", "./..."))

	att, err := attach.Listen(ctx, attach.Options{
		Bind:          attach.AutoUnixSocket,
		Pipeline:      p.Name(), // names this run in `senro attach`'s listing
		WaitForClient: false,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer att.Close()

	// Run builds p first, so a dangling Needs or an empty command
	// surfaces here, before anything executes.
	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		log.Fatal(err) // or recover *senro.RunError, as shown below
	}
}
```

That is what `senro run ./ci` builds and execs on your behalf, and exactly what a plain
`go run ./ci` runs directly. The only difference is who does the `go build`.

## `Run` and `RunPlan`

```go
func Run(ctx context.Context, p *Pipeline, opts ...Option) error
func RunPlan(ctx context.Context, p *Plan, opts ...Option) error
```

`Run` takes the `Pipeline` itself and calls `Build()` internally, returning the build error
directly if the pipeline does not validate. This is the ordinary path.

`RunPlan` executes an already-built `Plan` without building again: reach for it when the plan that
runs must be exactly the one you already inspected, not a fresh `Build()` of whatever the pipeline
looks like by then.

> **A plan is a snapshot.** `Build()` copies every field it reads, including `Needs` slices, so a
> later builder call cannot mutate a `Plan` that has already been built. That is the whole reason
> `RunPlan` exists beside `Run`.

## Run options

Both take the same options:

| Option | What it does |
| --- | --- |
| `WithAttach(att)` | Fans every event out to `att`'s socket, and adopts its run directory and run ID unless overridden, so the attach server and the engine agree on exactly one run. |
| `WithSink(s)` | Adds an observer of your own, which receives every event the run appends to its ledger, in order. Repeatable, and composes with `WithAttach`. It is how an embedding program watches a run without a terminal, and what the outbound notifiers are built on; see [Notifications](/docs/notifications/). |
| `WithDir(dir)` | Pins the run's on-disk directory. Unset, the run adopts `att.Dir()` under `WithAttach`, or generates one under `runs/<id>`. Set it only to pin a specific, known path. |
| `WithRunID(id)` | The same, for the run's identifier. |
| `WithSecrets(cfg)` | Hands `Run` the resolved configuration struct [mamori](https://github.com/xavidop/mamori)'s `Load` returned, so steps reach their credentials with `SecretEnv`. See [Secrets](/docs/secrets/) for what this does and does not protect. |
| `WithParams(p)` | The run's parameters, a flat `map[string]string` such as `{"branch": "main"}`. Never recorded in an event or a cache key. `senro.When`'s `Branch` and `ParamIs` conditions read them; see [Conditions](/docs/steps/conditions/). |
| `WithCacheDir(dir)` | Where the content-addressed store, the action cache and the scratch cache live. Unset: `$SENRO_CACHE_DIR`, or the platform's cache directory. Unlike `WithDir` this is not per run; a cache root is shared by every run on the machine. |
| `WithLocalClass(class)` | Overrides the local executor's cache equivalence class (the default is a bare `"local/darwin/arm64"`-shaped string), so a caller can fingerprint a toolchain version too. Most pipelines never need this. |
| `WithTrigger(ev, ts...)` | Gates the run on the event that started it. No trigger matching the event means no run starts, and `Run` returns `trigger.ErrNoMatch` rather than an empty success. See [Triggers](/docs/triggers/). |
| `WithRemoteCache(rc)` | Tiers the content store and the action cache over an S3-compatible bucket or an OCI registry repository. `senro.RemoteCacheFromEnv()` reads the same settings from `SENRO_REMOTE_CACHE` and its companions; an explicit option wins. See [Shared cache](/docs/data/shared-cache/). |
| `WithFuncBuild(pkg)` | Names the Go package this program was built from, so a `Func` step on a target of another platform can be cross-compiled for it. `senro run` sets it for you. See [A Func step off the coordinator](/docs/executors/func-remote/). |
| `WithTraceContext(tp, ts)` | Adopts an inbound W3C trace, so the run's events land under the CI job or webhook delivery that started it rather than beginning a trace of their own. See [Writing a trace exporter](/docs/extend/exporter/). |
| `WithAnalyzer(a, opts...)` | Hands `Run` an analyzer of your own, offered every step that fails. Nothing it proposes is applied without a human approving it. See [Writing an analyzer](/docs/extend/analyzer/). |

`Run` with no options costs exactly what the engine costs: **no attach server, and not one extra
goroutine.** A run directory and ID still exist either way, generated the same way `attach.Listen`
generates them, so even an unattached `Run` produces a real, inspectable run on disk.

## Handling the outcome

```go
if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		// A real run outcome: failed, partial, or cancelled.
		log.Printf("run ended: %s", runErr.Status)
		for _, s := range runErr.Steps {
			log.Printf("  %s %s (exit %d)", s.ID, s.State, s.ExitCode)
		}
		if runErr.StepsOmitted > 0 {
			log.Printf("  and %d more", runErr.StepsOmitted)
		}
		log.Printf("  evidence: %s/events.jsonl", runErr.Dir)
	} else {
		// An engine-level failure: an invalid plan, a disk write failure.
		log.Fatal(err)
	}
	os.Exit(1)
}
```

`Run` keeps a single `error` return. When you need to know *why* a run failed, `errors.As`
recovers a `*senro.RunError`. `RunError` carries four exported fields, everything a failure report
needs without re-reading the event log:

- **`Status`**: the run's rolled-up `api.RunStatus`, one of `succeeded`, `succeeded_with_recovery`,
  `partial`, `failed` or `cancelled`. See [Step states](/docs/steps/states/) for how steps roll up
  into it.
- **`Dir`**: the run directory. `events.jsonl`, `plan.json` and every step's per-attempt logs live
  under it. See [Reading a failed run](/docs/reference/debugging/).
- **`Steps`**: up to three `RunErrorStep` values naming the steps behind `Status`, each with `ID`,
  `State` and `ExitCode`. `ExitCode` is meaningful only when `State` is `failed`; a step that never
  reached a process reports `0`.
- **`StepsOmitted`**: how many further qualifying steps exist beyond `Steps`.

**Two of the five statuses never produce a `RunError` at all.** `succeeded` and
`succeeded_with_recovery` both return `nil`: a run that failed a step and passed it on retry is a
success with a note, not an error to handle. `cmd/senro`'s own exit-code mapping is built exactly
this way; see [CLI](/docs/cli/).

## Where to go next

- **[Attach](/docs/attach/)**: what `attach.Listen`'s options configure.
- **[CLI](/docs/cli/)**: the wrapper `senro run` puts around this exact pattern.
- **[Steps](/docs/steps/)**: building the `Pipeline` this page assumes exists.
- **[Secrets](/docs/secrets/)**: `senro.WithSecrets`, the other option most pipelines reach for.
- **[Notifications](/docs/notifications/)**: the webhook and Slack sinks built on `WithSink`.
- **[Triggers](/docs/triggers/)**: `senro.WithTrigger`, for a pipeline that decides for itself
  whether an event is its business.
