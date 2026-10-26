package notify

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/xavidop/senro/api"
)

// GitHub Checks: a trigger delivery starts a run (see package trigger), and
// this puts the result back on the commit that caused it, as a check run
// with a conclusion and per-step annotations. It is a Requester rather than
// a Renderer because a check run is a small state machine over two
// endpoints: POSTed at run start, PATCHed at run end. See request.go.
//
// It needs a token with `checks:write` and the commit SHA; senro discovers
// neither, both are arguments. Annotations are batched into the completion
// rather than sent per step, because GitHub's secondary rate limits are
// real.

// MaxAnnotations is how many step failures one check run reports. GitHub
// accepts 50 annotations per request; the summary says how many were left
// out so the list is never quietly partial.
const MaxAnnotations = 50

// MaxTestAnnotationMessage caps one test annotation's message. A failing
// test's output can be a whole stack dump; GitHub limits a message's size
// and the list is for locating a failure, not for reading the log.
const MaxTestAnnotationMessage = 2000

// GitHubChecks returns a destination that maintains one check run on a
// commit. owner and repo name the repository, sha is the commit, token needs
// `checks:write`, and name is the check's UI name (empty means "senro").
//
//	senro.WithSink(notify.New(
//		notify.GitHubChecks("acme", "web", sha, os.Getenv("GITHUB_TOKEN"), ""),
//	))
//
// Options are applied last, so Named, Client and a retry policy all work;
// senro's own headers are still set after them and win. senro's redactor
// never sees a Header value (see Renderer): the token is held and sent by
// this process, nothing else.
func GitHubChecks(owner, repo, sha, token, name string, opts ...DestinationOption) *Destination {
	if name == "" {
		name = "senro"
	}
	r := &GitHubChecksRequester{
		owner: owner, repo: repo, sha: sha, name: name,
		api:   "https://api.github.com",
		state: api.NewRunState(),
	}
	base := []DestinationOption{
		Named("github-checks"),
		WithRequester(r),
		Header("Authorization", "Bearer "+token),
		Header("Accept", "application/vnd.github+json"),
		Header("X-GitHub-Api-Version", "2022-11-28"),
	}
	// The URL is empty on purpose: the requester decides every URL, and a
	// base here would be a second place the API host is configured. Use
	// GitHubChecksAPI for GitHub Enterprise.
	return To("", nil, append(base, opts...)...)
}

// GitHubChecksAPI points a GitHub Checks destination at a different GitHub,
// for GitHub Enterprise. The default is https://api.github.com.
func GitHubChecksAPI(base string) DestinationOption {
	return func(d *Destination) {
		if g, ok := d.requester.(*GitHubChecksRequester); ok && base != "" {
			g.api = strings.TrimRight(base, "/")
		}
	}
}

// GitHubChecksRequester is the state machine behind GitHubChecks, exported
// so the type is nameable and GitHubChecksAPI can reach it. Its methods run
// one event at a time on the notifier's delivery goroutine; the mutex only
// guards against a requester mistakenly shared between two destinations.
type GitHubChecksRequester struct {
	owner, repo, sha, name string
	api                    string

	mu sync.Mutex
	// id is the check run GitHub assigned, learned from the create's
	// response. Zero until then, which is what tells Request whether the
	// next event is a create or an update.
	id int64
	// steps accumulate one annotation per failed step.finished, in arrival
	// order, sent with the completion.
	steps []stepAnnotation
	// state folds test.finished and step.finished through api.Apply, the
	// one fold every client uses. Tests are read from it at completion, not
	// annotated per event: a test that failed on attempt 1 and passed on
	// attempt 2 is recovered, and one that failed on three attempts is one
	// failure, and only the fold knows either.
	state *api.RunState
	// dropped counts annotations past MaxAnnotations, for the summary. Set by
	// complete, which is where the budget is spent.
	dropped int
	// done and failed count finished steps, for the summary.
	done, failed int
}

// stepAnnotation is a failed step's annotation and the step it is about, so
// complete can put the step's failed tests ahead of it.
type stepAnnotation struct {
	step string
	ann  ghAnnotation
}

// Request builds the create, the updates and the completion. Three event
// types matter; the rest return nil (see Requester). Filtering here rather
// than with a destination type filter keeps the rule in one place.
func (g *GitHubChecksRequester) Request(e api.Event) (*Request, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	switch e.Type {
	case api.RunStarted:
		if g.id != 0 {
			// A second create would leave the first check in progress
			// forever.
			return nil, nil
		}
		return g.create(e)

	case api.TestFinished:
		// Same rule as steps: accumulated, not sent. Folded rather than
		// annotated here, so retries settle before anything is reported. The
		// error is ignored like every other best-effort decode in this file:
		// an event the fold refuses is one fewer annotation, not a stuck
		// check.
		_ = g.state.Apply(e)
		return nil, nil

	case api.StepFinished:
		// Folded too, so the state's step order and its tests stay coherent
		// with the steps this check reports.
		_ = g.state.Apply(e)
		g.record(e)
		// Not sent per step (rate limits); accumulated and sent with the
		// run's completion.
		return nil, nil

	case api.RunFinished:
		if g.id == 0 {
			return nil, nil
		}
		return g.complete(e)
	}
	return nil, nil
}

// ReadResponse learns the check run's id from the create; GitHub assigns it
// and it appears only in the create's response body.
func (g *GitHubChecksRequester) ReadResponse(e api.Event, body []byte) {
	if e.Type != api.RunStarted {
		return
	}
	var res struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(body, &res); err != nil || res.ID == 0 {
		// Left at zero, so every later event is a no-op rather than a
		// request to a URL with an empty id. The failed create is already a
		// delivery outcome in the ledger.
		return
	}
	g.mu.Lock()
	g.id = res.ID
	g.mu.Unlock()
}

func (g *GitHubChecksRequester) checkRunsURL() string {
	return fmt.Sprintf("%s/repos/%s/%s/check-runs",
		g.api, url.PathEscape(g.owner), url.PathEscape(g.repo))
}

func (g *GitHubChecksRequester) create(e api.Event) (*Request, error) {
	body, err := json.Marshal(ghCheckRun{
		Name:       g.name,
		HeadSHA:    g.sha,
		Status:     "in_progress",
		StartedAt:  ghTime(e.TS),
		DetailsURL: "",
		Output: &ghOutput{
			Title:   "Running",
			Summary: "senro run " + e.Run + " is running.",
		},
	})
	if err != nil {
		return nil, err
	}
	return &Request{Method: "POST", URL: g.checkRunsURL(), Body: body}, nil
}

func (g *GitHubChecksRequester) complete(e api.Event) (*Request, error) {
	var b api.RunFinishedBody
	// Best-effort decode: a run.finished that will not decode is still a
	// run that ended, and conclusionFor handles the zero value. Leaving the
	// check in progress forever would be worse.
	_ = e.Decode(&b)

	ann, failedTests := g.annotations()
	body, err := json.Marshal(ghCheckRun{
		Name:        g.name,
		HeadSHA:     g.sha,
		Status:      "completed",
		Conclusion:  conclusionFor(b.Status),
		CompletedAt: ghTime(e.TS),
		Output: &ghOutput{
			Title:       titleFor(b.Status, g.failed, failedTests),
			Summary:     g.summary(b.Status, failedTests),
			Annotations: ann,
		},
	})
	if err != nil {
		return nil, err
	}
	return &Request{
		Method: "PATCH",
		URL:    fmt.Sprintf("%s/%d", g.checkRunsURL(), g.id),
		Body:   body,
	}, nil
}

// record folds one step.finished into the check's state.
func (g *GitHubChecksRequester) record(e api.Event) {
	var b api.StepFinishedBody
	if err := e.Decode(&b); err != nil {
		return
	}
	g.done++
	if !b.State.Failed() {
		return
	}
	g.failed++
	// Path is required by the API and a step is not a line of source;
	// ".senro" rather than a real path, so the failure is not read as being
	// about some unrelated file.
	g.steps = append(g.steps, stepAnnotation{step: e.Step, ann: ghAnnotation{
		Path: ".senro", StartLine: 1, EndLine: 1,
		AnnotationLevel: "failure",
		Title:           "step " + e.Step + " " + string(b.State),
		Message:         annotationMessage(e.Step, b),
	}})
}

// annotations builds the completion's annotation list and counts the failed
// tests, spending the MaxAnnotations budget and setting dropped.
//
// Steps are walked in the fold's order, and each step's failed tests go
// ahead of its own annotation: the test is the more specific location, and
// the step's annotation only says the step went red. Only tests the fold
// left failed are annotated. Recovered ones passed in the end, and passes and
// skips would bury the failures in the budget. A step annotation the fold
// never placed (its event refused) still goes out, last.
func (g *GitHubChecksRequester) annotations() ([]ghAnnotation, int) {
	var out []ghAnnotation
	g.dropped = 0
	add := func(a ghAnnotation) {
		if len(out) >= MaxAnnotations {
			g.dropped++
			return
		}
		out = append(out, a)
	}
	byStep := make(map[string][]int, len(g.steps))
	for i, s := range g.steps {
		byStep[s.step] = append(byStep[s.step], i)
	}
	failedTests := 0
	placed := make([]bool, len(g.steps))
	for _, id := range g.state.Order {
		if st := g.state.Steps[id]; st != nil {
			for _, t := range st.Tests {
				if t.Outcome == api.TestFailed {
					failedTests++
					add(testAnnotation(t))
				}
			}
		}
		for _, i := range byStep[id] {
			placed[i] = true
			add(g.steps[i].ann)
		}
	}
	for i, s := range g.steps {
		if !placed[i] {
			add(s.ann)
		}
	}
	return out, failedTests
}

// testAnnotation is one failed test's annotation.
func testAnnotation(b *api.TestResult) ghAnnotation {
	// A test without a source location is annotated on ".senro" for the
	// same reason a step is: Path is required and must not point at an
	// unrelated file.
	path := b.File
	if path == "" {
		path = ".senro"
	}
	line := b.Line
	if line <= 0 {
		line = 1
	}
	name := b.Name
	if b.Suite != "" {
		name = b.Suite + "." + b.Name
	}
	title := "test " + name + " failed"
	msg := b.Output
	if len(msg) > MaxTestAnnotationMessage {
		// Back the cut up to a rune start: a multi-byte character split by
		// the byte count would reach GitHub as U+FFFD.
		cut := MaxTestAnnotationMessage
		for cut > 0 && !utf8.RuneStart(msg[cut]) {
			cut--
		}
		msg = msg[:cut] + "\n[truncated]"
	}
	if msg == "" {
		// GitHub rejects an annotation with no message.
		msg = title
	}
	return ghAnnotation{
		Path: path, StartLine: line, EndLine: line,
		AnnotationLevel: "failure",
		Title:           title,
		Message:         msg,
	}
}

func annotationMessage(step string, b api.StepFinishedBody) string {
	var sb strings.Builder
	sb.WriteString("Step ")
	sb.WriteString(step)
	sb.WriteString(" ")
	sb.WriteString(string(b.State))
	if b.ExitCode != 0 {
		fmt.Fprintf(&sb, " (exit %d)", b.ExitCode)
	}
	if b.Error != "" {
		sb.WriteString(": ")
		sb.WriteString(b.Error)
	}
	return sb.String()
}

func (g *GitHubChecksRequester) summary(status api.RunStatus, failedTests int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "senro finished %s: %d step(s) completed", status, g.done)
	if g.failed > 0 {
		fmt.Fprintf(&sb, ", %d failed", g.failed)
	}
	if failedTests > 0 {
		fmt.Fprintf(&sb, ", %d test(s) failed", failedTests)
	}
	sb.WriteString(".")
	if g.dropped > 0 {
		// Said out loud: a reader who cannot tell a complete list from a
		// truncated one will act on the wrong one.
		fmt.Fprintf(&sb, "\n\n%d further failure(s) are not annotated here; GitHub accepts at most %d.",
			g.dropped, MaxAnnotations)
	}
	return sb.String()
}

// conclusionFor maps a run's own verdict onto GitHub's fixed set. An
// unrecognised status becomes "neutral" rather than "failure": a newer
// engine's status is not a failure and should not block a merge.
func conclusionFor(s api.RunStatus) string {
	switch s {
	case api.RunSucceeded:
		return "success"
	case api.RunSucceededWithRecovery:
		// Green: a handler recovered it, so the workload succeeded; the
		// recovery is visible in the summary and the run's stream.
		return "success"
	case api.RunFailed:
		return "failure"
	case api.RunPartial:
		// Red: steps that never ran because something upstream failed are
		// not a passing build.
		return "failure"
	case api.RunCancelled:
		return "cancelled"
	default:
		return "neutral"
	}
}

// titleFor is the check's one-line headline. Failed tests lead because they
// are the more specific news; failed steps are still named alongside them,
// since a step can fail for reasons no test reports (a build error, a
// timeout) and leaving it out would understate the run.
func titleFor(s api.RunStatus, failed, failedTests int) string {
	if failedTests > 0 && failed > 0 {
		return fmt.Sprintf("%s, %d test(s) failed, %d step(s) failed", s, failedTests, failed)
	}
	if failedTests > 0 {
		return fmt.Sprintf("%s, %d test(s) failed", s, failedTests)
	}
	if failed > 0 {
		return fmt.Sprintf("%s, %d step(s) failed", s, failed)
	}
	return string(s)
}

// ghTime is RFC3339, which is what the Checks API takes. The zero time
// marshals as an empty string and is omitted rather than sent as year one.
func ghTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

type ghCheckRun struct {
	Name        string    `json:"name"`
	HeadSHA     string    `json:"head_sha"`
	Status      string    `json:"status,omitempty"`
	Conclusion  string    `json:"conclusion,omitempty"`
	StartedAt   string    `json:"started_at,omitempty"`
	CompletedAt string    `json:"completed_at,omitempty"`
	DetailsURL  string    `json:"details_url,omitempty"`
	Output      *ghOutput `json:"output,omitempty"`
}

type ghOutput struct {
	Title       string         `json:"title"`
	Summary     string         `json:"summary"`
	Annotations []ghAnnotation `json:"annotations,omitempty"`
}

type ghAnnotation struct {
	Path            string `json:"path"`
	StartLine       int    `json:"start_line"`
	EndLine         int    `json:"end_line"`
	AnnotationLevel string `json:"annotation_level"`
	Title           string `json:"title,omitempty"`
	Message         string `json:"message"`
}
