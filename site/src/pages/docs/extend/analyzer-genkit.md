---
layout: ../../../layouts/DocsLayout.astro
title: A Genkit analyzer
---

# A Genkit analyzer

`contrib/genkitanalyzer` is a real [failure analyzer](/docs/extend/analyzer/) backed by
[Genkit](https://genkit.dev), Google's open-source AI framework for Go. It explains a failed step
with one generation call, using the Genkit instance you already configured.

Read [Writing a failure analyzer](/docs/extend/analyzer/) first for the interface and the contract.
This page is the provider binding.

## Install

```bash
go get github.com/xavidop/senro/contrib/genkitanalyzer
```

It is a **nested module**, with its own `go.mod`. senro itself takes no dependency on Genkit, so a
client that imports only `github.com/xavidop/senro/api` still pulls in nothing but the standard
library. The edge runs one way: this module depends on senro, senro never depends on it.

## Use it

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/contrib/genkitanalyzer"
)

func main() {
	ctx := context.Background()

	g := genkit.Init(ctx, genkit.WithPlugins(&googlegenai.GoogleAI{}))

	p := senro.New("ci")
	// ... workflows and steps ...

	err := senro.Run(ctx, p,
		senro.WithAnalyzer(
			genkitanalyzer.New(g, genkitanalyzer.Model("googleai/gemini-2.5-flash")),
			senro.AnalyzerName("genkit"),
			senro.AnalyzeTimeout(20*time.Second)))
	if err != nil {
		log.Fatal(err)
	}
}
```

`AnalyzerName` is what the ledger records, so it names the analyzer rather than the model: the name
is persisted, streamed and pasted into bug reports. `AnalyzeTimeout` bounds one call and defaults to
`senro.DefaultAnalyzeTimeout` (30 seconds).

## The Genkit instance is yours

`New` takes the `*genkit.Genkit` you built. The package never calls `genkit.Init`, never registers a
plugin, never reads an API key and never falls back to a provider. Which model answers, where its
credential comes from and where its telemetry goes stay decisions your program makes.

With no `Model` option, no model name is sent and Genkit resolves the default you set with
`genkit.WithDefaultModel`.

## Options

| Option | What it does |
| --- | --- |
| `Model(name)` | The model, in Genkit's `provider/model` spelling. Its provider half comes from a plugin you registered. |
| `Prompt(fn)` | Replaces the prompt built per failure. `DefaultPrompt` is exported, so wrap it rather than start over. |
| `Remedy(fn)` | Replaces the policy below. It is handed the `api.Failure`, never the model's answer. |

```go
a := genkitanalyzer.New(g,
	genkitanalyzer.Model("googleai/gemini-2.5-flash"),
	genkitanalyzer.Prompt(func(f api.Failure) string {
		return genkitanalyzer.DefaultPrompt(f) + "\nThis pipeline builds a Go module.\n"
	}))
```

## What leaves the machine

The prompt is built from [`api.Failure`](/docs/extend/analyzer/#what-you-are-handed) and nothing
else. Every string on that struct has already been through the run's redactor, and there is no
second redactor on this path.

So a `Prompt` of your own must build its text from `f` alone. A field read from an environment
variable, a file or the workspace has been through no redactor at all, and whatever the prompt
contains is sent to your provider.

## The remedy is not the model's to choose

`api.Proposal.Remedy` is decided from the failure senro recorded, never parsed out of the answer. A
model writing "just retry" is not evidence that retrying is safe.

```go
func DefaultRemedy(f api.Failure) api.Remedy {
	if f.State != api.StateFailed {
		return api.RemedyNone
	}
	if f.Error != "" && f.ExitCode == 0 {
		return api.RemedyRetry
	}
	return api.RemedyNone
}
```

Only infrastructure failure earns a retry. A non-zero exit is the workload's own verdict, and running
it again until it passes deletes what it just told you. That is senro's stance everywhere, and it is
why [`retry.OnInfra`](/docs/steps/retries/) is the policy the documentation reaches for first.

The three exclusions are each deliberate:

- **Anything but `StateFailed`.** A timed-out step hit a budget the pipeline author wrote down, and
  proposing a retry asks a person to overrule it. A panicked step is a bug in your Go code, which
  senro's own retry loop does not reconsider either.
- **An empty `Error`.** `api.Failure` sets it only for infrastructure failure.
- **A non-zero `ExitCode` alongside an `Error`.** The two fields are separate so a verdict is never
  confused with a broken substrate.

`Remedy(fn)` replaces the policy for a caller who knows their own infrastructure. What it cannot do
is widen what a proposal may ask for: `api.Remedy` is a closed vocabulary with one applicable member.

## No proposal is a real answer

When the model returns nothing usable, `Analyze` returns an error wrapping `ErrNoAnswer` rather than
a proposal with an empty summary. senro reads an error as "no proposal", counts it in the run's
shutdown report, and appends nothing to the ledger.

`Summary` is the one field on `api.Proposal` with no `omitempty`, because it is what a person reads
first. A proposal carrying an empty one would still occupy the gate and still be offered for
approval, saying nothing.

`ErrNoAnswer` is exported so a caller composing analyzers can tell "the model had nothing to say"
apart from "the provider was unreachable", and fall back to a local classifier for the first without
retrying the second.

## What a proposal does, and does not, do

A proposal never applies itself. It becomes an action only when an attached client accepts it (`a` in
the TUI, `analysis.accept` on the wire), or when you configured a policy in so many words:

```go
senro.AcceptWithoutHumanApproval(func(f api.Failure, p api.Proposal) bool {
	return p.Remedy == api.RemedyRetry && f.Attempt == 1
})
```

A run nobody is watching applies nothing by default. Being slow costs the build nothing either:
`Analyze` runs off the engine's goroutine, offers go onto a bounded queue that drops rather than
waits, and no scheduling decision blocks on a model.

## Testing yours

The package's own tests define a model in-process with `genkit.DefineModel` and reach no network, so
a well-formed answer, an unusable one, the remedy policy and the deadline are all covered with no
credential. Do the same for an analyzer of your own: a test that needs an API key cannot run in CI
and cannot run on a fork.

## Where to go next

- [Writing a failure analyzer](/docs/extend/analyzer/): the interface, the contract and the gate.
- [Control operations](/docs/attach/control-ops/): `analysis.accept` and `analysis.reject`.
- [Extension points](/docs/extend/): the other four seams.
