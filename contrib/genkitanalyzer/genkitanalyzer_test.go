// The tests define their model in-process with genkit.DefineModel, so the
// whole package is exercised through the real genkit.Generate path with no
// plugin, no credential and no network. A test that needs an API key is not a
// test: it cannot run in CI, it cannot run on a fork, and what it proves
// about this package is nothing that a model returning a fixed string does
// not prove more cheaply.
package genkitanalyzer_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/contrib/genkitanalyzer"
)

// New returns senro.Analyzer, so the compiler checks this already; it is
// written down because satisfying that interface is the whole reason this
// module imports the root package rather than api alone.
var _ senro.Analyzer = genkitanalyzer.New(nil)

// model is one in-process model: what it was asked, and what it answers.
type model struct {
	mu      sync.Mutex
	prompts []string

	// reply is the model itself. Returning an error is a provider that
	// failed; returning "" is a provider that answered with nothing.
	reply func(ctx context.Context, prompt string) (string, error)
}

// seen is every prompt this model was handed, in order.
func (m *model) seen() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.prompts...)
}

// define registers m under name on g. The name needs a provider half because
// that is Genkit's spelling for a model reference; "fake" is a provider no
// plugin supplies, which is exactly why nothing here can reach a network.
func define(g *genkit.Genkit, name string, reply func(context.Context, string) (string, error)) *model {
	m := &model{reply: reply}
	genkit.DefineModel(g, name, &ai.ModelOptions{Label: name},
		func(ctx context.Context, req *ai.ModelRequest, _ ai.ModelStreamCallback) (*ai.ModelResponse, error) {
			prompt := promptText(req)
			m.mu.Lock()
			m.prompts = append(m.prompts, prompt)
			m.mu.Unlock()

			text, err := m.reply(ctx, prompt)
			if err != nil {
				return nil, err
			}
			return &ai.ModelResponse{
				FinishReason: ai.FinishReasonStop,
				Message: &ai.Message{
					Role:    ai.RoleModel,
					Content: []*ai.Part{ai.NewTextPart(text)},
				},
			}, nil
		})
	return m
}

// promptText flattens a request back into the text the analyzer built.
func promptText(req *ai.ModelRequest) string {
	var b strings.Builder
	for _, msg := range req.Messages {
		for _, part := range msg.Content {
			if part.IsText() {
				b.WriteString(part.Text)
			}
		}
	}
	return b.String()
}

// answers replies with a fixed string, ignoring the prompt.
func answers(text string) func(context.Context, string) (string, error) {
	return func(context.Context, string) (string, error) { return text, nil }
}

func newGenkit(t *testing.T) *genkit.Genkit {
	t.Helper()
	return genkit.Init(t.Context())
}

// failure is a plausible non-zero-exit failure. Tests that care about a
// specific field set it on the copy they take.
func failure() api.Failure {
	return api.Failure{
		RunID:    "r-1",
		Pipeline: "ci",
		Step:     "test",
		Attempt:  1,
		State:    api.StateFailed,
		ExitCode: 1,
		Duration: 3 * time.Second,
		Cmd:      []string{"go", "test", "./..."},
		Needs:    []string{"build"},
		LogTail:  "--- FAIL: TestThing (0.00s)\n    thing_test.go:12: want 3, got 4\nFAIL\n",
	}
}

func TestAWellFormedAnswerBecomesAProposal(t *testing.T) {
	g := newGenkit(t)
	define(g, "fake/ok", answers(
		"TestThing asserts on a value the code no longer produces.\n\n"+
			"The tail names one failing assertion at thing_test.go:12 and nothing\n"+
			"about the machine it ran on."))

	a := genkitanalyzer.New(g, genkitanalyzer.Model("fake/ok"))

	p, err := a.Analyze(t.Context(), failure())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if want := "TestThing asserts on a value the code no longer produces."; p.Summary != want {
		t.Errorf("Summary = %q, want %q", p.Summary, want)
	}
	if !strings.HasPrefix(p.Detail, "The tail names one failing assertion") {
		t.Errorf("Detail = %q, want the lines after the first", p.Detail)
	}
	if strings.Contains(p.Detail, p.Summary) {
		t.Errorf("Detail repeats the summary: %q", p.Detail)
	}
	// A non-zero exit is the workload's verdict, whatever the prose said.
	if p.Remedy != api.RemedyNone {
		t.Errorf("Remedy = %q, want none for a non-zero exit", p.Remedy)
	}
}

func TestAOneLineAnswerHasNoDetail(t *testing.T) {
	g := newGenkit(t)
	define(g, "fake/terse", answers("  the linter found an unused import  "))

	p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/terse")).Analyze(t.Context(), failure())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if p.Summary != "the linter found an unused import" {
		t.Errorf("Summary = %q", p.Summary)
	}
	if p.Detail != "" {
		t.Errorf("Detail = %q, want empty", p.Detail)
	}
}

func TestMarkdownAModelWasAskedNotToWriteIsStrippedFromTheSummary(t *testing.T) {
	g := newGenkit(t)
	define(g, "fake/md", answers("**the build ran out of disk**\nRetrying will fail identically."))

	p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/md")).Analyze(t.Context(), failure())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if p.Summary != "the build ran out of disk" {
		t.Errorf("Summary = %q, want the emphasis markers gone", p.Summary)
	}
}

func TestAnUnusableAnswerIsAnErrorNotAnEmptyProposal(t *testing.T) {
	// api.Proposal.Summary is the one field with no omitempty. A proposal
	// without one would still occupy the gate and still be offered to a
	// person for approval, saying nothing; senro reads an error as "no
	// proposal", which is what actually happened.
	for _, tc := range []struct {
		name  string
		reply string
	}{
		{"empty", ""},
		{"whitespace", "   \n\n\t\n  "},
		{"markers only", "###\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGenkit(t)
			define(g, "fake/empty", answers(tc.reply))

			p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/empty")).Analyze(t.Context(), failure())
			if !errors.Is(err, genkitanalyzer.ErrNoAnswer) {
				t.Fatalf("err = %v, want ErrNoAnswer", err)
			}
			if p != (api.Proposal{}) {
				t.Errorf("Proposal = %+v, want the zero value alongside an error", p)
			}
		})
	}
}

func TestAProviderErrorIsAnErrorAndNotAProposal(t *testing.T) {
	boom := errors.New("429 rate limited")

	g := newGenkit(t)
	define(g, "fake/broken", func(context.Context, string) (string, error) { return "", boom })

	p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/broken")).Analyze(t.Context(), failure())
	if err == nil {
		t.Fatal("Analyze returned no error for a provider that failed")
	}
	if !strings.Contains(err.Error(), "429 rate limited") {
		t.Errorf("err = %v, want the provider's own message kept", err)
	}
	if p != (api.Proposal{}) {
		t.Errorf("Proposal = %+v, want the zero value", p)
	}
}

func TestANilGenkitIsAnErrorNotAPanic(t *testing.T) {
	// senro recovers a panic and reports it as an error anyway, so the
	// difference is only whether the operator can read what went wrong.
	p, err := genkitanalyzer.New(nil).Analyze(t.Context(), failure())
	if err == nil {
		t.Fatal("Analyze returned no error for a nil *genkit.Genkit")
	}
	if p != (api.Proposal{}) {
		t.Errorf("Proposal = %+v, want the zero value", p)
	}
}

// TestTheRemedyComesFromTheFailureNotTheModel: the model insists on a retry
// every time, and only the failure senro recorded decides whether it gets one.
func TestTheRemedyComesFromTheFailureNotTheModel(t *testing.T) {
	const insists = "whatever this is, just retry it, definitely retry it\nretry it again"

	infra := failure()
	infra.ExitCode = 0
	infra.Error = "create container: dial unix /var/run/docker.sock: connect: connection refused"

	timedOut := infra
	timedOut.State = api.StateTimedOut

	panicked := infra
	panicked.State = api.StatePanicked

	both := infra
	both.ExitCode = 3

	for _, tc := range []struct {
		name string
		f    api.Failure
		want api.Remedy
	}{
		{"infrastructure failure", infra, api.RemedyRetry},
		{"non-zero exit", failure(), api.RemedyNone},
		{"timed out", timedOut, api.RemedyNone},
		{"panicked", panicked, api.RemedyNone},
		{"infrastructure error with a verdict too", both, api.RemedyNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGenkit(t)
			define(g, "fake/eager", answers(insists))

			p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/eager")).Analyze(t.Context(), tc.f)
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if p.Remedy != tc.want {
				t.Errorf("Remedy = %q, want %q", p.Remedy, tc.want)
			}
			if got := genkitanalyzer.DefaultRemedy(tc.f); got != tc.want {
				t.Errorf("DefaultRemedy = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOnlyRemedyRetryIsEverProposed(t *testing.T) {
	// The vocabulary is closed, and this build applies exactly one member.
	for _, f := range []api.Failure{failure(), {State: api.StateFailed, Error: "infra"}, {}} {
		if r := genkitanalyzer.DefaultRemedy(f); r != api.RemedyNone && r != api.RemedyRetry {
			t.Errorf("DefaultRemedy(%+v) = %q, outside the closed vocabulary", f, r)
		}
	}
}

func TestTheDeadlineIsHonoured(t *testing.T) {
	var gotDeadline bool

	g := newGenkit(t)
	define(g, "fake/slow", func(ctx context.Context, _ string) (string, error) {
		_, gotDeadline = ctx.Deadline()
		<-ctx.Done() // an over-running model, cancelled rather than waited on
		return "", ctx.Err()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/slow")).Analyze(ctx, failure())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Analyze returned no error for a model that outran the deadline")
	}
	if p != (api.Proposal{}) {
		t.Errorf("Proposal = %+v, want the zero value", p)
	}
	if !gotDeadline {
		t.Error("the model was handed a context with no deadline: senro's timeout did not reach it")
	}
	if elapsed > 5*time.Second {
		t.Errorf("Analyze took %v: the deadline was not what ended it", elapsed)
	}
}

func TestAnAlreadyExpiredDeadlineIsNotSentToTheProvider(t *testing.T) {
	g := newGenkit(t)
	m := define(g, "fake/never", answers("should never be asked"))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/never")).Analyze(ctx, failure()); err == nil {
		t.Fatal("Analyze returned no error for a cancelled context")
	}
	if n := len(m.seen()); n != 0 {
		t.Errorf("the model was called %d time(s) for an answer nobody could read", n)
	}
}

func TestModelSelectsWhichModelAnswers(t *testing.T) {
	g := newGenkit(t)
	define(g, "fake/one", answers("model one answered"))
	two := define(g, "fake/two", answers("model two answered"))

	p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/two")).Analyze(t.Context(), failure())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if p.Summary != "model two answered" {
		t.Errorf("Summary = %q, want the model named by the option", p.Summary)
	}
	if len(two.seen()) != 1 {
		t.Errorf("fake/two was asked %d times, want 1", len(two.seen()))
	}
}

func TestWithNoModelOptionGenkitsOwnDefaultAnswers(t *testing.T) {
	// This package never picks a provider. Unset, the model is whatever the
	// caller configured on their own Genkit instance.
	ctx := t.Context()
	g := genkit.Init(ctx, genkit.WithDefaultModel("fake/default"))
	define(g, "fake/default", answers("the caller's default model answered"))

	p, err := genkitanalyzer.New(g).Analyze(ctx, failure())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if p.Summary != "the caller's default model answered" {
		t.Errorf("Summary = %q", p.Summary)
	}
}

func TestPromptReplacesWhatIsSent(t *testing.T) {
	g := newGenkit(t)
	m := define(g, "fake/echo", answers("fine"))

	a := genkitanalyzer.New(g,
		genkitanalyzer.Model("fake/echo"),
		genkitanalyzer.Prompt(func(f api.Failure) string {
			return "only this, about " + f.Step
		}))

	if _, err := a.Analyze(t.Context(), failure()); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	seen := m.seen()
	if len(seen) != 1 {
		t.Fatalf("model asked %d times, want 1", len(seen))
	}
	if seen[0] != "only this, about test" {
		t.Errorf("prompt = %q, want the caller's own", seen[0])
	}
}

func TestAnEmptyPromptIsRefusedBeforeTheProviderIsCalled(t *testing.T) {
	g := newGenkit(t)
	m := define(g, "fake/echo", answers("fine"))

	a := genkitanalyzer.New(g,
		genkitanalyzer.Model("fake/echo"),
		genkitanalyzer.Prompt(func(api.Failure) string { return "  \n " }))

	if _, err := a.Analyze(t.Context(), failure()); err == nil {
		t.Fatal("Analyze returned no error for an empty prompt")
	}
	if n := len(m.seen()); n != 0 {
		t.Errorf("the model was called %d time(s) with nothing to answer", n)
	}
}

func TestRemedyReplacesThePolicy(t *testing.T) {
	g := newGenkit(t)
	define(g, "fake/echo", answers("something broke"))

	var got api.Failure
	a := genkitanalyzer.New(g,
		genkitanalyzer.Model("fake/echo"),
		genkitanalyzer.Remedy(func(f api.Failure) api.Remedy {
			got = f
			return api.RemedyRetry
		}))

	f := failure() // a non-zero exit, which DefaultRemedy would refuse
	p, err := a.Analyze(t.Context(), f)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if p.Remedy != api.RemedyRetry {
		t.Errorf("Remedy = %q, want the caller's policy to win", p.Remedy)
	}
	if got.Step != f.Step || got.ExitCode != f.ExitCode {
		t.Errorf("the policy was handed %+v, want the failure senro recorded", got)
	}
}

func TestANilOptionArgumentIsIgnoredRatherThanFatal(t *testing.T) {
	g := newGenkit(t)
	define(g, "fake/echo", answers("still works"))

	a := genkitanalyzer.New(g,
		genkitanalyzer.Model("fake/echo"),
		genkitanalyzer.Prompt(nil),
		genkitanalyzer.Remedy(nil))

	if _, err := a.Analyze(t.Context(), failure()); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
}

func TestALongSummaryIsShortenedAndTheSentenceIsKept(t *testing.T) {
	long := strings.TrimSpace(strings.Repeat("the compiler reported a type error in a generated file ", 12))

	g := newGenkit(t)
	define(g, "fake/verbose", answers(long+"\n\nand here is why"))

	p, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/verbose")).Analyze(t.Context(), failure())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if n := len([]rune(p.Summary)); n > 210 {
		t.Errorf("Summary is %d runes: the one line a person reads first is a paragraph", n)
	}
	if !strings.HasSuffix(p.Summary, "...") {
		t.Errorf("Summary = %q, want it to say it was cut", p.Summary)
	}
	if !strings.Contains(p.Detail, long) {
		t.Error("the shortened sentence was dropped rather than moved into Detail")
	}
	if !strings.Contains(p.Detail, "and here is why") {
		t.Error("the rest of the answer was lost")
	}
}
