---
layout: ../../../layouts/DocsLayout.astro
title: "Run options and outcomes"
---

# `senro.Run`: options and outcomes

Every senro pipeline is a Go program that calls `senro.Run` (see [Quickstart](/docs/quickstart/)
for the basic pattern). This page is the reference for that call: every option `Run` accepts, and
how to read what it returns when a run fails.

## `Run` and `RunPlan`

```go
func Run(ctx context.Context, p *Pipeline, opts ...Option) error
func RunPlan(ctx context.Context, p *Plan, opts ...Option) error
```

`Run` takes your `Pipeline`, builds it, and runs it. If the pipeline doesn't validate, you get the
build error back. Use `Run` for the ordinary case.

`RunPlan` runs a `Plan` you already built, without building it again. Use it when you need to
inspect the plan first and guarantee that exact plan is what runs, not a fresh build of whatever
the pipeline looks like by the time it executes.

> **A plan is a snapshot.** Once you `Build()` a pipeline, the resulting `Plan` can't be changed by
> anything you do to the pipeline afterward. That's why `RunPlan` is useful: it guarantees the plan
> that runs is the one you inspected.

## Run options

Both `Run` and `RunPlan` take the same options:

| Option | What it does |
| --- | --- |
| `WithAttach(att)` | Sends every event to `att`'s socket, and uses its run directory and run ID (unless you override them), so the attach server and the engine agree on one run. |
| `WithSink(s)` | Adds your own observer, which gets every event the run produces, in order. You can pass this more than once, and it works alongside `WithAttach`. This is how a program watches a run without a terminal; it's also what the notifiers use. See [Notifications](/docs/notifications/). |
| `WithDir(dir)` | Pins the run's directory on disk. If you don't set it, the run uses `att.Dir()` under `WithAttach`, or generates a new `runs/<id>` directory. |
| `WithRunID(id)` | Same idea, for the run's ID. |
| `WithSecrets(cfg)` | Passes the resolved secrets config from [mamori](https://github.com/xavidop/mamori)'s `Load`, so steps can read credentials with `SecretEnv`. See [Secrets](/docs/secrets/) for what this protects and what it doesn't. |
| `WithParams(p)` | The run's parameters: a flat `map[string]string`, like `{"branch": "main"}`. Never recorded in an event or a cache key. `senro.When`'s `Branch` and `ParamIs` conditions read these. See [Conditions](/docs/steps/conditions/). |
| `WithCacheDir(dir)` | Where the content store, action cache, and scratch cache live. Defaults to `$SENRO_CACHE_DIR`, or your platform's standard cache directory. Unlike `WithDir`, this is shared across every run on the machine, not per run. |
| `WithLocalClass(class)` | Overrides the cache key senro uses to identify "this machine" (normally something like `"local/darwin/arm64"`). Most pipelines don't need this. |
| `WithTrigger(ev, ts...)` | Gates the run on the event that started it. If nothing matches, `Run` returns `trigger.ErrNoMatch` instead of running. See [Triggers](/docs/triggers/). |
| `WithRemoteCache(rc)` | Backs the content store and action cache with an S3-compatible bucket or an OCI registry, so runs on different machines can share a cache. `senro.RemoteCacheFromEnv()` reads the same settings from `SENRO_REMOTE_CACHE`. See [Shared cache](/docs/data/shared-cache/). |
| `WithFuncBuild(pkg)` | Names the Go package this program was built from, so a `Func` step can be cross-compiled to run on a different platform. `senro run` sets this for you automatically. See [A Func step off the coordinator](/docs/executors/func-remote/). |
| `WithTraceContext(tp, ts)` | Continues an inbound W3C trace, so the run's events show up under the CI job or webhook delivery that started it. See [Writing a trace exporter](/docs/extend/exporter/). |
| `WithAnalyzer(a, opts...)` | Adds your own analyzer, which gets a chance to look at every step that fails. It can only propose a fix; nothing happens without a human approving it. See [Writing an analyzer](/docs/analyzers/custom/). |

Calling `Run` with no options costs nothing extra: no attach server, no background work. A run
directory and ID still get created either way, so even an unattached run leaves a real, inspectable
record on disk.

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

`Run` returns a single `error`. To find out *why* a run failed, use `errors.As` to recover a
`*senro.RunError`. It carries everything a failure report needs, without re-reading the event log:

- **`Status`**: the run's overall outcome, one of `succeeded`, `succeeded_with_recovery`,
  `partial`, `failed`, or `cancelled`. See [Step states](/docs/steps/states/) for how steps roll up
  into this.
- **`Dir`**: the run directory, containing `events.jsonl`, `plan.json`, and every step's logs. See
  [Reading a failed run](/docs/reference/debugging/).
- **`Steps`**: up to three steps behind the failure, each with `ID`, `State`, and `ExitCode`.
  `ExitCode` only means something when `State` is `failed`; otherwise it's `0`.
- **`StepsOmitted`**: how many more failing steps exist beyond those three.

**`succeeded` and `succeeded_with_recovery` never produce a `RunError`.** Both return `nil`. A
run that failed a step but passed on retry counts as a success, not an error.

## Where to go next

- **[Quickstart](/docs/quickstart/)**: the same `senro.Run` call, in context.
- **[Attach](/docs/attach/)**: what `attach.Listen`'s options configure.
- **[CLI](/docs/cli/)**: the wrapper `senro run` puts around this exact pattern.
- **[Steps](/docs/steps/)**: building the `Pipeline` this page assumes exists.
- **[Secrets](/docs/secrets/)**: `senro.WithSecrets`, the other option most pipelines reach for.
- **[Notifications](/docs/notifications/)**: the webhook and Slack sinks built on `WithSink`.
- **[Triggers](/docs/triggers/)**: `senro.WithTrigger`, for a pipeline that decides for itself
  whether an event is its business.
