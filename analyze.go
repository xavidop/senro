package senro

import (
	"context"
	"io"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
)

// Analyzer explains a failed step.
//
// senro takes no dependency on any model provider, holds no API key, and
// does not know which model anybody uses: describing a failure is senro's
// job, explaining it is somebody else's program against their own SDK. The
// same split the trace exporter is built on; see /docs/writing-an-exporter/.
// An implementation needs to import github.com/xavidop/senro/api and nothing
// else of senro's, checked mechanically: see examples/extensions/fakeanalyzer
// and TestAnExtensionImportsOnlySenrosPublicSurface.
//
// It is handed api.Failure and no handle to read more: everything on that
// struct is a field senro has decided may leave the machine, reviewable
// precisely because it is a fixed list. See api.Failure for the redaction
// rule.
//
// Analyze runs within a timeout (AnalyzeTimeout) on a goroutine that is not
// the engine's: it is never called from the append path, never holds the
// ledger lock, and a run whose analyzer hangs still finishes, reporting
// unexplained failures on standard error after a bounded grace. Failures are
// offered through a bounded queue; an analyzer that cannot keep up loses
// offers rather than slowing the run. An error means no proposal: it is
// counted in the shutdown report, never appended to the ledger, because a
// run did not fail because somebody's API was down. A panic is recovered and
// treated as an error.
//
// A proposal causes nothing on its own: it becomes an action only when an
// attached client accepts it, or when the caller configured a policy in so
// many words. See WithAnalyzer and AcceptWithoutHumanApproval.
type Analyzer interface {
	Analyze(context.Context, api.Failure) (api.Proposal, error)
}

// AnalyzerFunc adapts an ordinary function to Analyzer.
//
//	senro.WithAnalyzer(senro.AnalyzerFunc(
//		func(ctx context.Context, f api.Failure) (api.Proposal, error) {
//			return api.Proposal{Summary: "flaky: " + f.Step}, nil
//		}))
type AnalyzerFunc func(context.Context, api.Failure) (api.Proposal, error)

// Analyze calls f.
func (f AnalyzerFunc) Analyze(ctx context.Context, fail api.Failure) (api.Proposal, error) {
	return f(ctx, fail)
}

// DefaultAnalyzeTimeout bounds one call to Analyze. Long enough for a model
// to answer, short enough that a hung one is not the reason a queue backs up.
// Override with AnalyzeTimeout.
const DefaultAnalyzeTimeout = 30 * time.Second

// DefaultAnalyzeGrace bounds how long a run waits at shutdown for answers
// that have not arrived yet. Without the wait, the last failure of a run,
// usually the interesting one, would be the one whose explanation never
// landed in the ledger. It bounds SHUTDOWN and nothing else: no scheduling
// decision ever waits on an analyzer.
const DefaultAnalyzeGrace = 10 * time.Second

// AnalyzeOption configures WithAnalyzer.
type AnalyzeOption interface{ applyAnalyze(*engine.AnalyzeOptions) }

type analyzeOptionFunc func(*engine.AnalyzeOptions)

func (f analyzeOptionFunc) applyAnalyze(o *engine.AnalyzeOptions) { f(o) }

// WithAnalyzer hands Run an analyzer of the caller's own. Every step that
// settles in a failed terminal state is offered to it, and what it proposes
// becomes an analysis.proposed event in the run's ledger.
//
//	err := senro.Run(ctx, pipe,
//		senro.WithAnalyzer(myAnalyzer,
//			senro.AnalyzerName("claude"),
//			senro.AnalyzeTimeout(20*time.Second)))
//
// Not repeatable: a later call replaces an earlier one. Two analyzers would
// mean two proposals per failure competing for one gate; a caller who wants
// two writes an Analyzer that consults both and returns one answer.
//
// A run given no analyzer emits no analysis events, starts no goroutine and
// costs nothing.
//
// Nothing is applied without a human, by default: a proposal sits in the run
// until an attached client accepts it (api.OpAnalysisAccept, the TUI's 'a'
// key) or rejects it, and a run nobody was watching applies nothing.
// AcceptWithoutHumanApproval is the one way to change that.
func WithAnalyzer(a Analyzer, opts ...AnalyzeOption) Option {
	return func(c *runConfig) {
		if a == nil {
			c.analyze = nil
			return
		}
		o := &engine.AnalyzeOptions{
			Analyzer: a,
			Name:     "analyzer",
			Timeout:  DefaultAnalyzeTimeout,
			Grace:    DefaultAnalyzeGrace,
		}
		for _, opt := range opts {
			opt.applyAnalyze(o)
		}
		c.analyze = o
	}
}

// AnalyzerName is the caller's own name for this analyzer, recorded on every
// analysis.proposed. A name, never a model, an endpoint or a key: it is
// persisted, streamed and routinely pasted into bug reports, the same rule
// notify.Named follows.
func AnalyzerName(name string) AnalyzeOption {
	return analyzeOptionFunc(func(o *engine.AnalyzeOptions) { o.Name = name })
}

// AnalyzeTimeout bounds one call to Analyze. See DefaultAnalyzeTimeout.
//
// The context handed to Analyze carries this deadline, derived with
// context.WithoutCancel from the run's own: a cancelled run is frequently
// the one whose failure most wants explaining, and an inherited cancellation
// would return nothing for exactly those failures.
func AnalyzeTimeout(d time.Duration) AnalyzeOption {
	return analyzeOptionFunc(func(o *engine.AnalyzeOptions) { o.Timeout = d })
}

// AnalyzeGrace bounds how long shutdown waits for outstanding answers. See
// DefaultAnalyzeGrace.
func AnalyzeGrace(d time.Duration) AnalyzeOption {
	return analyzeOptionFunc(func(o *engine.AnalyzeOptions) { o.Grace = d })
}

// AnalyzeReportWriter redirects the shutdown report (the proposals that could
// not be recorded, the failures never analyzed, the analyzers that errored)
// away from standard error. Mostly for a caller that already has somewhere
// better to put operator-facing text, and for senro's own tests.
func AnalyzeReportWriter(w io.Writer) AnalyzeOption {
	return analyzeOptionFunc(func(o *engine.AnalyzeOptions) { o.Report = w })
}

// AcceptWithoutHumanApproval lets a proposal be applied with nobody
// watching: the one way to defeat the gate, spelled out at the call site on
// purpose.
//
//	senro.WithAnalyzer(a, senro.AcceptWithoutHumanApproval(
//		func(f api.Failure, p api.Proposal) bool {
//			return p.Remedy == api.RemedyRetry && f.Attempt == 1
//		}))
//
// policy is called once per proposal, on the analyzer's own goroutine, and
// only for a proposal whose remedy this build can apply (an advisory
// proposal is never offered). Returning false leaves the proposal waiting
// for a person, the default.
//
// It cannot widen what "applied" means: the policy chooses whether to apply
// a remedy, not the remedy, and api.Remedy is closed with one member. The
// most an unsupervised run can do on an analyzer's say-so is retry a step.
//
// Every proposal applied this way is recorded with
// api.AnalysisDecisionBody's Policy set, so a run nobody watched can be
// identified afterwards from the ledger alone. A nil policy restores the
// default.
func AcceptWithoutHumanApproval(policy func(api.Failure, api.Proposal) bool) AnalyzeOption {
	return analyzeOptionFunc(func(o *engine.AnalyzeOptions) { o.Policy = policy })
}
