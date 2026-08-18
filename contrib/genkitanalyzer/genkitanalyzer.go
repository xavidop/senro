// Package genkitanalyzer explains a failed senro step with one Genkit
// generation call.
//
//	g := genkit.Init(ctx, genkit.WithPlugins(&googlegenai.GoogleAI{}))
//
//	err := senro.Run(ctx, pipe,
//		senro.WithAnalyzer(
//			genkitanalyzer.New(g, genkitanalyzer.Model("googleai/gemini-2.5-flash")),
//			senro.AnalyzerName("genkit")))
//
// # The Genkit instance is yours
//
// New takes the *genkit.Genkit you already configured. This package never
// calls genkit.Init, never reads an API key, never registers a plugin and
// never defaults to a provider: which model answers, where its credential
// comes from and where its telemetry goes are decisions that belong to your
// program, not to a library senro ships. Model names the model, and with no
// Model the caller's own genkit.WithDefaultModel decides.
//
// # Why this is a separate module
//
// senro is one Go module whose api package carries nothing beyond the
// standard library, enforced by a test. Genkit in senro's own go.mod would
// put the Google AI SDK stack in the dependency graph of everyone who
// imports senro, including a client that wanted only api. So the edge points
// one way: this module depends on senro, senro never depends on this module,
// and Go leaves a nested module out of the parent's ./... on its own. Nothing
// here needs to be in senro to work, which is the point of the Analyzer seam.
//
// It does import github.com/xavidop/senro rather than api alone, unlike
// examples/extensions/fakeanalyzer, because New returns senro.Analyzer: that
// is a compile-time proof the constructor satisfies the interface, and it
// costs a caller nothing, since anyone calling New is writing
// senro.WithAnalyzer on the next line.
//
// # What leaves the machine
//
// Whatever the prompt contains is sent to your model provider. It is built
// from the fields of api.Failure and nothing else, and every string on
// api.Failure has already been through the run's redactor. There is no second
// redactor on this path, so a Prompt of your own must not add a field read
// from anywhere else: an environment variable, a file, the workspace. Read
// api.Failure's doc comment for the rule it is a fixed list to enforce.
//
// # The remedy is not the model's to choose
//
// api.Proposal.Remedy is decided by DefaultRemedy from the api.Failure senro
// recorded, never parsed out of the model's answer. See DefaultRemedy.
//
// # Contract
//
// Analyze is bounded by the context senro hands it (senro.AnalyzeTimeout) and
// passes that context straight through to Genkit. It returns an error rather
// than an empty proposal when the model says nothing usable; senro reads an
// error as "no proposal", which is a correct and common answer, and counts it
// in the run's shutdown report. It writes nothing to standard output or
// standard error: reporting is the engine's job.
package genkitanalyzer

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
)

// ErrNoAnswer is what Analyze returns when the call succeeded but produced
// nothing that could become a proposal: an empty response, or one that is
// only whitespace.
//
// Exported so a caller composing analyzers can tell "the model had nothing to
// say" apart from "the provider was unreachable", and fall back to a local
// classifier for the first without retrying the second. senro itself does not
// distinguish them: both are simply no proposal.
var ErrNoAnswer = errors.New("genkitanalyzer: the model returned no usable answer")

// maxSummary bounds api.Proposal.Summary, in runes.
//
// Summary is one line rendered in the TUI's footer, in a CI log and in the
// ledger, and a model asked for one sentence sometimes writes a paragraph.
// Nothing is lost when it fires: the full first line is kept as the head of
// Detail, so the truncation costs a reader a glance rather than the text.
const maxSummary = 200

// Option configures an analyzer. See New.
type Option func(*analyzer)

// Model names the model to generate with, in Genkit's "provider/model"
// spelling ("googleai/gemini-2.5-flash", "ollama/llama3.2"). The provider
// half has to come from a plugin registered on the *genkit.Genkit passed to
// New; this package registers none.
//
// Unset, no model name is sent and Genkit resolves the default the caller
// configured with genkit.WithDefaultModel. There is deliberately no fallback
// here: a library picking somebody's model, and so somebody's bill, is not a
// default this package is entitled to choose.
func Model(name string) Option {
	return func(a *analyzer) { a.model = name }
}

// Prompt replaces the prompt built for each failure. See DefaultPrompt, which
// is what it replaces, and which is exported so a caller can wrap rather than
// rewrite it:
//
//	genkitanalyzer.Prompt(func(f api.Failure) string {
//		return genkitanalyzer.DefaultPrompt(f) + "\nThis pipeline builds a Go module.\n"
//	})
//
// fn is called on the analyzer's own goroutine, once per failure, and must
// build its text from f alone: everything on an api.Failure has been through
// the run's redactor and nothing added from elsewhere has. A prompt that
// comes back empty is an error rather than a call to the provider.
//
// A nil fn is ignored, so the option is safe to build from a variable that
// may be unset.
func Prompt(fn func(api.Failure) string) Option {
	return func(a *analyzer) {
		if fn != nil {
			a.prompt = fn
		}
	}
}

// Remedy replaces the policy that decides api.Proposal.Remedy. See
// DefaultRemedy, which is what it replaces.
//
// fn is handed the api.Failure and NOT the model's answer, and that is the
// whole design of this seam rather than an omission: the remedy is the one
// part of a proposal that can cause work to happen, so it is decided from
// what senro recorded. A model writing "just retry" is not evidence that
// retrying is safe.
//
// A nil fn is ignored.
func Remedy(fn func(api.Failure) api.Remedy) Option {
	return func(a *analyzer) {
		if fn != nil {
			a.remedy = fn
		}
	}
}

// analyzer is unexported: New's return type is the interface senro asks for,
// and a concrete type here would be a second surface to keep compatible for
// no gain. Options are the way to configure one.
type analyzer struct {
	g      *genkit.Genkit
	model  string
	prompt func(api.Failure) string
	remedy func(api.Failure) api.Remedy
}

// New returns an analyzer that explains a failed step with one call to g.
//
// g is the Genkit instance the caller built, with the caller's plugins,
// credentials and telemetry; see the package doc. A nil g is not a panic:
// Analyze reports it as an error, so a wiring mistake shows up in the run's
// shutdown report rather than as an analyzer that silently never answers.
func New(g *genkit.Genkit, opts ...Option) senro.Analyzer {
	a := &analyzer{
		g:      g,
		prompt: DefaultPrompt,
		remedy: DefaultRemedy,
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Analyze implements senro.Analyzer.
//
// ctx carries senro's own deadline (senro.AnalyzeTimeout) and is passed
// straight to Genkit, so an over-running model is cancelled rather than left
// to outlive the run that asked.
func (a *analyzer) Analyze(ctx context.Context, f api.Failure) (api.Proposal, error) {
	if a.g == nil {
		return api.Proposal{}, errors.New(
			"genkitanalyzer: nil *genkit.Genkit: New needs the instance you configured")
	}
	// The deadline may already have passed while this failure sat in the
	// engine's queue. Asking anyway would bill somebody for an answer that
	// arrives after the only code that could read it has given up.
	if err := ctx.Err(); err != nil {
		return api.Proposal{}, err
	}

	prompt := a.prompt(f)
	if strings.TrimSpace(prompt) == "" {
		return api.Proposal{}, fmt.Errorf("genkitanalyzer: the prompt for step %q was empty", f.Step)
	}

	// WithPromptFn rather than WithPrompt: ai.WithPrompt runs its text through
	// fmt.Sprintf even when given no arguments, so a log tail containing %s or
	// %d would reach the model as %!s(MISSING). A prompt built from somebody's
	// build output is exactly the string that trips that.
	opts := []ai.GenerateOption{
		ai.WithPromptFn(func(context.Context, any) (string, error) { return prompt, nil }),
	}
	if a.model != "" {
		opts = append(opts, ai.WithModelName(a.model))
	}

	resp, err := genkit.Generate(ctx, a.g, opts...)
	if err != nil {
		return api.Proposal{}, fmt.Errorf("genkitanalyzer: generate: %w", err)
	}
	if resp == nil {
		return api.Proposal{}, fmt.Errorf("%w (no response)", ErrNoAnswer)
	}

	summary, detail := split(resp.Text())
	if summary == "" {
		// No summary, no proposal. api.Proposal.Summary is the one field with
		// no omitempty because it is what a person reads first, and a proposal
		// carrying an empty one would occupy the gate, be offered for approval
		// and say nothing. An error is the honest answer, and senro already
		// has a word for it.
		return api.Proposal{}, fmt.Errorf("%w for step %q", ErrNoAnswer, f.Step)
	}

	return api.Proposal{
		Summary: summary,
		Detail:  detail,
		Remedy:  a.remedy(f),
	}, nil
}
