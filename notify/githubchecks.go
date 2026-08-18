package notify

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

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
		api: "https://api.github.com",
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
	// annotations accumulate from failed steps, sent with the completion.
	annotations []ghAnnotation
	// dropped counts annotations past MaxAnnotations, for the summary.
	dropped int
	// done and failed count finished steps, for the summary.
	done, failed int
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

	case api.StepFinished:
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

	ann := g.annotations
	if len(ann) > MaxAnnotations {
		ann = ann[:MaxAnnotations]
	}
	body, err := json.Marshal(ghCheckRun{
		Name:        g.name,
		HeadSHA:     g.sha,
		Status:      "completed",
		Conclusion:  conclusionFor(b.Status),
		CompletedAt: ghTime(e.TS),
		Output: &ghOutput{
			Title:       titleFor(b.Status, g.failed),
			Summary:     g.summary(b.Status),
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
	if len(g.annotations) >= MaxAnnotations {
		g.dropped++
		return
	}
	// Path is required by the API and a step is not a line of source;
	// ".senro" rather than a real path, so the failure is not read as being
	// about some unrelated file.
	g.annotations = append(g.annotations, ghAnnotation{
		Path: ".senro", StartLine: 1, EndLine: 1,
		AnnotationLevel: "failure",
		Title:           "step " + e.Step + " " + string(b.State),
		Message:         annotationMessage(e.Step, b),
	})
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

func (g *GitHubChecksRequester) summary(status api.RunStatus) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "senro finished %s: %d step(s) completed", status, g.done)
	if g.failed > 0 {
		fmt.Fprintf(&sb, ", %d failed", g.failed)
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

func titleFor(s api.RunStatus, failed int) string {
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
