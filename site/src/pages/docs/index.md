---
layout: ../../layouts/DocsLayout.astro
title: Introduction
---

# senro

`senro` (線路, "railway track") defines pipelines in Go, executes them, and exposes a live attach
protocol so a second process can watch and debug a run in progress. Reach for it when you want a
pipeline as real, typed, testable Go code instead of YAML.

It is a pipeline engine first, and nothing in the API is CI-specific. CI/CD is the most familiar
thing to build on it, but a data pipeline, a batch job, an infrastructure rollout or a release
script is built the same way: steps with dependencies, retries and failure handlers, one event
stream you can attach to.

> The railway metaphor lives in prose and error messages, never in identifiers. A step is a
> station, a workflow is a line, the resolved execution graph is a timetable; the Go API says
> `Pipeline`, `Workflow`, `Step` and `Plan`.

## Start

| Page | What it gives you |
|---|---|
| [Install](/docs/install/) | `go get`, requirements, platform support |
| [Quickstart](/docs/quickstart/) | A pipeline, a run, and a terminal attached to it |
| [Concepts](/docs/concepts/) | Pipeline, workflow, step, plan, event stream |

## Steps: the building blocks

| Page | What it covers |
|---|---|
| [Steps](/docs/steps/) | What a step is, `exec.Command`, the two step kinds |
| [Ordering](/docs/steps/ordering/) | `Needs` on a step, `senro.Needs` on a workflow |
| [Settings](/docs/steps/settings/) | `Env`, `WorkDir`, `Timeout`, `ContinueOnError` |
| [Retries](/docs/steps/retries/) | `Retry`, `RetryPolicy`, the predicates, backoff |
| [Handlers](/docs/steps/handlers/) | `OnFailure`, `Always`, and what a handler inherits |
| [End states](/docs/steps/states/) | The ten end states, and how they propagate |
| [Conditions](/docs/steps/conditions/) | `When`, `Branch`, `ParamIs`, `EnvIs` |
| [Func steps](/docs/steps/functions/) | `RegisterFunc`, `senro.Func`, `senro.Ctx` |

## Executors: where steps run

| Page | What it covers |
|---|---|
| [Executors](/docs/executors/) | The four targets, `senro.On`, read-only enforcement |
| [Containers](/docs/executors/containers/) | `container.Image`, its five properties, `container.User` |
| [Kubernetes](/docs/executors/kubernetes/) | A workflow on a pod: setup, behavior, refusals |
| [SSH](/docs/executors/ssh/) | A workflow on a remote host: setup, behavior, refusals |
| [Func off the coordinator](/docs/executors/func-remote/) | A `Func` step over SSH or in a container |

## Data: workspaces and caching

| Page | What it covers |
|---|---|
| [Workspaces](/docs/data/workspaces/) | `senro.Workspace`, `Mount`, `ScopeRun`, snapshots |
| [Persistent workspaces](/docs/data/persistent/) | `ScopePersistent` and its four rules |
| [Scratch cache](/docs/data/scratch/) | `ScratchCache`, `Key`, `RestoreKeys` |
| [Caching a step](/docs/data/caching/) | `Pure()`, `Inputs`, `Outputs`, `CacheEnv` |
| [Cache keys](/docs/data/cache-keys/) | What enters a key, and `cache explain` |
| [Shared cache](/docs/data/shared-cache/) | The S3 and OCI tier, config, degradation |
| [Archiving](/docs/data/archiving/) | Archiving a run, and `logs fetch` |

## Monorepos

| Page | What it covers |
|---|---|
| [Monorepos](/docs/monorepo/) | The problem, and which tool solves which part |
| [Fan-out](/docs/monorepo/fan-out/) | `Expand`, `Template`, `MaxParallel`, `MaxNodes` |
| [Per-unit ordering](/docs/monorepo/needs-each/) | Per-unit edges versus the workflow barrier |
| [Partitioning](/docs/monorepo/partition/) | `Partition`, `TemplateShard`, the duration history |
| [Affected](/docs/monorepo/affected/) | Running only what a change affects |

## Unit graphs

| Page | What it covers |
|---|---|
| [Choosing a graph](/docs/monorepo/unit-graphs/) | The eight graphs, and which support `Affected` |
| [`glob`](/docs/monorepo/unit-graphs/glob/) | One unit per directory matching a pattern |
| [`gowork`](/docs/monorepo/unit-graphs/gowork/) | Go modules and packages, via `go list` |
| [`cargo`](/docs/monorepo/unit-graphs/cargo/) | Rust crates, from the manifests |
| [`jswork`](/docs/monorepo/unit-graphs/jswork/) | npm, pnpm, Yarn and Bun workspace packages |
| [`maven`](/docs/monorepo/unit-graphs/maven/) | Maven reactor projects |
| [`gradle`](/docs/monorepo/unit-graphs/gradle/) | Gradle projects, from the declarative subset |
| [`pyproject`](/docs/monorepo/unit-graphs/pyproject/) | Python distributions, and why `Affected` is refused |
| [`bazel`](/docs/monorepo/unit-graphs/bazel/) | Bazel packages, with and without running bazel |
| [Write your own](/docs/monorepo/unit-graphs/custom/) | `UnitGraph` and `UnitAffector` |

## Secrets

| Page | What it covers |
|---|---|
| [Secrets](/docs/secrets/) | Declaring and using a secret; the file-path rule |
| [Channels](/docs/secrets/channels/) | Safe, redacted, refused; per-executor delivery |

## Watch and control a run

| Page | What it covers |
|---|---|
| [Attach](/docs/attach/) | What attach is; one client, two sources; `attach.Listen` |
| [The TUI](/docs/attach/tui/) | The terminal UI and its keys |
| [The browser UI](/docs/attach/browser/) | `senro ui` |
| [Control operations](/docs/attach/control-ops/) | The eleven operations, and the refusal codes |
| [The shell](/docs/attach/shell/) | `senro shell` on a live step, `--tty` |
| [Security](/docs/attach/security/) | The boundary, tokens, TLS, platform support |

## Triggers

| Page | What it covers |
|---|---|
| [Running on an event](/docs/triggers/) | Wiring `WithTrigger`, the matchers, the three outcomes |
| [Run it as a server](/docs/triggers/server/) | Your binary as the webhook endpoint, verified per source |
| [The event file](/docs/triggers/events/) | The envelope every source is delivered through |
| [GitHub](/docs/triggers/github/) | `push`, `pull_request`, and tags arriving as pushes |
| [GitLab](/docs/triggers/gitlab/) | Push, tag push, merge request, and GitLab's own action words |
| [Bitbucket](/docs/triggers/bitbucket/) | `repo:push`, `pullrequest:*`, and the missing file lists |
| [Gitea](/docs/triggers/gitea/) | `push`, `pull_request`, `create`, and the double tag |
| [Schedule & manual](/docs/triggers/manual/) | The neutral shape, for cron and for a button |
| [Write your own](/docs/triggers/custom/) | `trigger.Provider` and `trigger.Matcher` |

## Notifications

| Page | What it covers |
|---|---|
| [Sending a result out](/docs/notifications/) | The destinations, every option, delivery and retries |
| [Slack](/docs/notifications/slack/) | A line in a channel, and what widening it costs |
| [Webhook](/docs/notifications/webhook/) | Raw events as JSON, and verifying a signature |
| [GitHub Checks](/docs/notifications/github-checks/) | A check run on the commit, with annotations |
| [Write your own](/docs/notifications/custom/) | `notify.Renderer` and `notify.Requester` |

## Failure analyzers

| Page | What it covers |
|---|---|
| [What an analyzer does](/docs/analyzers/) | Explaining a failed step, and the approval gate |
| [The AI analyzer](/docs/analyzers/genkit/) | `contrib/genkitanalyzer`, with the model of your choice |
| [Write your own](/docs/analyzers/custom/) | `Analyzer`, `api.Failure` and `api.Proposal` |

## Extend

| Page | What it covers |
|---|---|
| [Extension points](/docs/extend/) | Every extension point, one line each |
| [A trace exporter](/docs/extend/exporter/) | A trace exporter as a `Sink` |
| [A unit graph](/docs/monorepo/unit-graphs/custom/) | Implement `UnitGraph` / `UnitAffector` |
| [A trigger source](/docs/triggers/custom/) | Implement `trigger.Provider` |
| [A notifier](/docs/notifications/custom/) | Implement a `Renderer` / `Requester` |
| [A failure analyzer](/docs/analyzers/custom/) | Implement `Analyzer`, and the proposal gate |

## Reference

| Page | What it covers |
|---|---|
| [CLI](/docs/cli/) | Every command in one table, plus exit codes |
| [Running and watching](/docs/cli/run/) | `senro run`, `attach`, `shell`, `ui` |
| [Cache commands](/docs/cli/cache/) | `cache gc`, `cache explain`, `verify` |
| [Workspace commands](/docs/cli/workspaces/) | `ws ls/pull/diff`, `logs fetch`, `func check` |
| [The event stream](/docs/reference/event-stream/) | The event envelope, and the fold |
| [`api`](/docs/reference/api/) | The `api` package as a wire contract |
| [Embedding](/docs/reference/embedding/) | Embedding senro in your own program |
| [Reading a failed run](/docs/reference/debugging/) | The run directory, file by file |
| [Agent skill](/docs/reference/skill/) | The skill that teaches an AI agent senro |
