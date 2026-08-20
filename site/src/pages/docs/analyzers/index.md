---
layout: ../../../layouts/DocsLayout.astro
title: Failure analyzers
---

# Failure analyzers

When a step fails, senro can hand that failure to a program you supply, which answers with one
line saying what broke. That line lands in the run's event stream, so it shows up in the TUI, in
`events.jsonl` and in anything else watching the run.

That program is an **analyzer**. It can be a model, a `strings.Contains` over the log, or a lookup
in your team's runbook. senro does not care which.

```
  ✗ fetch      failed      exit 1
    proposed   fetch failed on the network, not on its own work
               press a to retry this step, A to dismiss
```

## What you get out of it

Nothing runs an analyzer for you by default. You add one option to `senro.Run`:

```go
senro.Run(ctx, p, senro.WithAnalyzer(myAnalyzer, senro.AnalyzerName("runbook")))
```

From then on, every failed step gets:

- **An explanation** in the run's ledger, as an `analysis.proposed` event.
- **Optionally, one offered action**: retry this step. That is the whole vocabulary, and it is
  offered, not taken. See [the gate](#a-proposal-never-applies-itself).

A step that was skipped because something upstream failed is never sent to your analyzer. Only
steps that actually broke are.

## Your two options

| | |
|---|---|
| **[The AI analyzer](/docs/analyzers/genkit/)** | `contrib/genkitanalyzer`, ready to install. You give it a [Genkit](https://genkit.dev) instance, it explains failures with the model of your choice: Gemini, OpenAI, Anthropic, Ollama, anything Genkit has a plugin for. |
| **[Write your own](/docs/analyzers/custom/)** | One method, `Analyze(ctx, api.Failure) (api.Proposal, error)`. Reach for it when the answer is in your logs or your runbook rather than in a model. |

senro itself holds no API key and depends on no AI SDK. The Genkit analyzer is a separate module
you install on purpose.

## A proposal never applies itself

An explanation is a suggestion. It becomes an action only when somebody decides:

- **You decide**, in [the TUI](/docs/attach/tui/): `a` accepts the focused step's proposal, `A`
  dismisses it. Accepting a retry runs the same code path `r` (retry) already does, refusals
  included.
- **A policy you wrote decides**, for a run nobody is watching:

  ```go
  senro.WithAnalyzer(a, senro.AcceptWithoutHumanApproval(
      func(f api.Failure, p api.Proposal) bool {
          return p.Remedy == api.RemedyRetry && f.Attempt == 1
      }))
  ```

  Anything a policy applies is recorded with `policy: true`, so you can tell from the ledger alone
  that no person was involved. A policy applies at most once per step per run.

With neither, a proposal stays a proposal: an explanation you read, and nothing more.

## What it costs the run

Nothing. `Analyze` runs off the engine's goroutine, one call is bounded by
`senro.AnalyzeTimeout` (30s by default), and a slow analyzer never delays a step. An analyzer that
errors, panics or has no answer is treated as "no comment": the run's own result is unchanged.

## Where to go next

- **[The AI analyzer](/docs/analyzers/genkit/)**: install `contrib/genkitanalyzer` and wire it up.
- **[Write your own](/docs/analyzers/custom/)**: the interface, with a worked example.
- **[The TUI](/docs/attach/tui/)**: `a` and `A`, and what the footer shows before you press one.
- **[Reading a failed run](/docs/reference/debugging/)**: what senro tells you with no analyzer at
  all.
