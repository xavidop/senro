// Package fakeanalyzer is a senro failure analyzer, written the way one in
// somebody else's repository would be: it imports github.com/xavidop/senro/api
// and nothing else of senro's (extension_static_test.go checks that;
// analyze_e2e_test.go drives it through a real run).
//
// The answers are canned because the thing demonstrated is the wiring: a
// fully populated api.Failure arriving on a goroutine that is not the
// engine's, and an api.Proposal going back. Substituting a model is one
// function body; Analyze marks the exact line where the API call would go.
//
// Using it:
//
//	a := fakeanalyzer.New()
//	err := senro.Run(ctx, pipe,
//		senro.WithAnalyzer(a, senro.AnalyzerName("fake")))
//
// senro redacts (LogTail has already been through the run's redactor),
// bounds (LogTail is the last few kilobytes, not everything), isolates (own
// goroutine; an error becomes a report line, a panic is recovered), and
// gates (a Remedy is a request a person must approve, never an instruction).
package fakeanalyzer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/xavidop/senro/api"
)

// Analyzer explains a failed step from its exit code and the tail of its
// log. A real one would send the same two things to a model; this one
// matches substrings, so a test and a demo can both assert on it.
type Analyzer struct {
	mu   sync.Mutex
	seen []api.Failure
}

// New returns an analyzer ready to use. The zero value works too; this exists
// so calling code reads like it does with any other extension.
func New() *Analyzer { return &Analyzer{} }

// Seen is every failure this analyzer was handed, in order. Not something a
// real analyzer needs: it lets a test assert on what senro actually
// published to a third party (TestAnAnalyzerNeverSeesASecret reads this).
func (a *Analyzer) Seen() []api.Failure {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]api.Failure(nil), a.seen...)
}

// Analyze implements senro.Analyzer. The signature takes a context and
// returns an error because a real implementation makes a network call; senro
// gives the context a deadline (senro.AnalyzeTimeout) and treats an error as
// "no proposal", never as a reason the run failed.
func (a *Analyzer) Analyze(ctx context.Context, f api.Failure) (api.Proposal, error) {
	a.mu.Lock()
	a.seen = append(a.seen, f)
	a.mu.Unlock()

	// A real analyzer replaces everything from here to the return with one
	// call to its provider, built from exactly the fields below: your SDK,
	// your model, your key.
	//
	//	prompt := fmt.Sprintf("Step %q ran %v and failed with exit %d.\n\n%s",
	//		f.Step, f.Cmd, f.ExitCode, f.LogTail)
	//	answer, err := client.Complete(ctx, prompt)
	//	if err != nil {
	//		return api.Proposal{}, err
	//	}
	//	return parse(answer)
	if err := ctx.Err(); err != nil {
		return api.Proposal{}, err
	}

	switch {
	case f.State == api.StateTimedOut:
		// No remedy on purpose: the deadline was the pipeline author's
		// verdict, and proposing a retry would ask a person to overrule it
		// on the word of a program that cannot see why it was set.
		return api.Proposal{
			Summary: fmt.Sprintf("%s ran out of its own deadline", f.Step),
			Detail: "The step did not finish inside the timeout the pipeline " +
				"declared for it. Nothing here says the work was wrong, only that " +
				"it was slower than the budget. Raising the budget or making the " +
				"step smaller are both decisions for a person.",
		}, nil

	case strings.Contains(f.LogTail, "i/o timeout"),
		strings.Contains(f.LogTail, "connection refused"),
		strings.Contains(f.LogTail, "TLS handshake timeout"):
		return api.Proposal{
			Summary: fmt.Sprintf("%s failed on the network, not on its own work", f.Step),
			Detail: "The tail of the log is a transport error rather than anything " +
				"the step computed. That is the shape of a failure a second attempt " +
				"usually survives.",
			Remedy: api.RemedyRetry,
		}, nil

	case strings.Contains(f.LogTail, "no space left on device"):
		// Deliberately no remedy: retrying a full disk fails identically.
		return api.Proposal{
			Summary: fmt.Sprintf("%s filled the disk", f.Step),
			Detail: "Retrying will fail identically until something frees space. " +
				"This needs a person, not another attempt.",
		}, nil

	case f.ExitCode == 1 && strings.Contains(f.LogTail, "FAIL"):
		return api.Proposal{
			Summary: fmt.Sprintf("%s has a genuinely failing test", f.Step),
			Detail: "The output names a failing assertion rather than an " +
				"infrastructure problem. A retry would run the same code against " +
				"the same inputs.",
		}, nil
	}

	return api.Proposal{
		Summary: fmt.Sprintf("%s failed with exit %d and nothing recognisable in its log", f.Step, f.ExitCode),
	}, nil
}
