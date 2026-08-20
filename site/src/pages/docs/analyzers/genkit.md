---
layout: ../../../layouts/DocsLayout.astro
title: The AI analyzer
---

# The AI analyzer

`contrib/genkitanalyzer` is a ready-made [failure analyzer](/docs/analyzers/) that asks a model
what broke. You supply the model; senro supplies the failure.

It is built on [Genkit](https://genkit.dev), Google's open-source AI framework for Go, so the
provider is whatever Genkit has a plugin for: Gemini, Vertex AI, OpenAI, Anthropic, Ollama and the
rest.

## Install

```bash
go get github.com/xavidop/senro/contrib/genkitanalyzer
```

It is a **nested module with its own `go.mod`**, so senro itself never pulls in an AI SDK. If you
never install this, your dependency graph never hears about Genkit.

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

	// Your Genkit instance: your plugin, your credential, your telemetry.
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

That is the whole integration. When a step fails, its explanation shows up in the TUI and in the
run's event stream, exactly as [Failure analyzers](/docs/analyzers/) describes.

`New` takes the `*genkit.Genkit` **you** built. The package never calls `genkit.Init`, never
registers a plugin, never reads an API key and never falls back to a provider of its own.

## Options

| Option | What it does |
| --- | --- |
| `Model(name)` | The model, in Genkit's `provider/model` spelling. Leave it out and Genkit uses whatever you set with `genkit.WithDefaultModel`. |
| `Prompt(fn)` | Replaces the prompt built per failure. `DefaultPrompt` is exported, so add to it rather than starting over. |
| `Remedy(fn)` | Replaces the retry policy below. It is handed the `api.Failure`, never the model's answer. |

```go
a := genkitanalyzer.New(g,
	genkitanalyzer.Model("googleai/gemini-2.5-flash"),
	genkitanalyzer.Prompt(func(f api.Failure) string {
		return genkitanalyzer.DefaultPrompt(f) + "\nThis pipeline builds a Go module.\n"
	}))
```

## What leaves your machine

The prompt is built from [`api.Failure`](/docs/analyzers/custom/#what-you-are-handed-apifailure)
and nothing else. Every string on that struct has already been through the run's redactor.

If you replace the prompt, **build it from `f` alone**. A value you read from an environment
variable, a file or the workspace has been through no redactor at all, and everything in the
prompt is sent to your provider.

## The model does not choose the remedy

Whether a proposal offers "retry this step" is decided from the failure senro recorded, never
parsed out of the model's answer. A model writing "just retry it" is not evidence that retrying is
safe.

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

In words: **only a step whose process failed to start or run at all is offered a retry.** A
non-zero exit code is the workload's own verdict, and running it again until it passes deletes
what it just told you. A `timed_out` step hit a budget you wrote down; a `panicked` step is a bug
in your Go code.

`Remedy(fn)` swaps that policy out if you know your own infrastructure better. What it cannot do
is widen what a proposal may ask for: the vocabulary stays [one word
long](/docs/analyzers/custom/#the-remedy-vocabulary-is-one-word-long).

## When the model has nothing useful to say

`Analyze` returns an error wrapping `ErrNoAnswer` rather than a proposal with an empty summary,
and senro reads that as "no comment": nothing is appended to the ledger, and the run's own result
is unchanged.

`ErrNoAnswer` is exported, so an analyzer composing several sources can tell "the model had
nothing to say" apart from "the provider was unreachable", and fall back to a local classifier for
the first without retrying the second.

## Testing

The package's own tests define a model in-process with `genkit.DefineModel` and reach no network,
which is worth copying: a test that needs an API key cannot run in CI and cannot run on a fork.

## Where to go next

- **[Failure analyzers](/docs/analyzers/)**: the gate, and who turns a proposal into an action.
- **[Write your own](/docs/analyzers/custom/)**: the interface, when a model is not the right
  answer.
- **[Control operations](/docs/attach/control-ops/)**: `analysis.accept` and `analysis.reject`.
