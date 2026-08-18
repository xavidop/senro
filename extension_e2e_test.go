package senro_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/examples/extensions/gitlabcomment"
	"github.com/xavidop/senro/examples/extensions/otelspan"
	"github.com/xavidop/senro/examples/extensions/pagerduty"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/notify"
	"github.com/xavidop/senro/retry"
	"github.com/xavidop/senro/trigger"
)

// This file is the proof the extension work exists for. Everything it drives
// comes from examples/extensions, whose two packages import only senro's
// public surface (asserted mechanically by
// TestAnExtensionImportsOnlySenrosPublicSurface), and it drives them through
// senro.Run rather than through either package's own seams. If a third party
// could not do this, the documentation is a wish.

// gitlabPush is a GitLab push webhook wrapped in the envelope trigger's file
// format documents, naming the EXAMPLE's provider rather than the built-in
// one. That is the interesting half: the example does not parse a push
// itself, it delegates to trigger.GitLab() and attaches the author on the way
// past, so this payload exercises a third party's provider standing on top of
// a built-in.
const gitlabPush = `{
  "provider": "gitlab-comment",
  "event": "Push Hook",
  "payload": {
    "object_kind": "push",
    "ref": "refs/heads/main",
    "before": "9d3a1b0f0000000000000000000000000000aaaa",
    "after": "1c2d3e4f0000000000000000000000000000bbbb",
    "user_username": "ada",
    "project": {"path_with_namespace": "acme/app", "default_branch": "main"},
    "commits": [
      {"added": ["services/api/main.go"], "modified": ["README.md"], "removed": []}
    ],
    "total_commits_count": 1
  }
}`

// gitlabComment is the event only the example can read: somebody asking a
// merge request to build again. Trimmed from the "Comment on a merge request"
// payload example in gitlab-org/gitlab,
// doc/user/project/integrations/webhook_events.md.
const gitlabComment = `{
  "provider": "gitlab-comment",
  "event": "Note Hook",
  "payload": {
    "object_kind": "note",
    "user": {"username": "ada"},
    "project": {"path_with_namespace": "acme/app", "default_branch": "main"},
    "object_attributes": {
      "noteable_type": "MergeRequest",
      "action": "create",
      "note": "/retest please, the runner flaked"
    },
    "merge_request": {
      "iid": 7,
      "target_branch": "main",
      "source_branch": "feat",
      "last_commit": {"id": "562e173be03b8ff2efb05345d12df18815438a4b"}
    }
  }
}`

// pagerDuty stands in for events.pagerduty.com and keeps what it was sent.
type pagerDuty struct {
	srv *httptest.Server

	mu     sync.Mutex
	alerts []map[string]any
}

func newPagerDuty(t *testing.T) *pagerDuty {
	t.Helper()
	p := &pagerDuty{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var alert map[string]any
		if err := json.NewDecoder(r.Body).Decode(&alert); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.alerts = append(p.alerts, alert)
		p.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *pagerDuty) all() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]map[string]any(nil), p.alerts...)
}

// destination is the example's own wiring, pointed at the test server. The
// URL is the one thing a test has to substitute, which is why notify.To
// takes it and the option list carries everything else.
func (p *pagerDuty) destination(opts ...notify.DestinationOption) *notify.Destination {
	defaults := []notify.DestinationOption{
		notify.Named("pagerduty"),
		notify.On(api.RunFinished),
	}
	return notify.To(p.srv.URL,
		pagerduty.Renderer{RoutingKey: "R0UT1NG", Source: "ci.example.com"},
		append(defaults, opts...)...)
}

// TestAThirdPartyTriggerSourceAndNotifierDriveARealRun. One run, gated by an
// event shape senro cannot parse, reported to a destination senro cannot
// render, with neither of them touching anything under internal/.
func TestAThirdPartyTriggerSourceAndNotifierDriveARealRun(t *testing.T) {
	pd := newPagerDuty(t)

	ev, err := trigger.ReadEvent(strings.NewReader(gitlabPush), gitlabcomment.Provider{})
	if err != nil {
		t.Fatalf("ReadEvent through a third-party provider: %v", err)
	}
	if ev.Kind != trigger.Push || ev.Branch != "main" || ev.Repo != "acme/app" {
		t.Fatalf("event = %+v, want a push to main of acme/app", ev)
	}

	var report strings.Builder
	n := notify.New(notify.WithReportWriter(&report), pd.destination())
	defer func() { _ = n.Close() }()

	marker := filepath.Join(t.TempDir(), "ran")
	pipe := senro.New("extended")
	pipe.Workflow("w").Step("touch", exec.Command("sh", "-c", "echo ran > "+marker))

	// Two built-in matchers and one written outside this repository, on the
	// same trigger, against an event neither of them knows the shape of.
	err = senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithSink(n),
		senro.WithTrigger(ev, trigger.OnPush(
			trigger.Branches("main"),
			trigger.Paths("services/**"),
			gitlabcomment.Author("ada"),
		)),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Close reports that the outcome of delivering run.finished had no ledger
	// left to go in, which is the documented shape of this run and not a
	// failure; the assertion on the report below is what checks it landed
	// somewhere. See notify's package doc.
	if err := n.Close(); err != nil && !strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("Close: %v", err)
	}

	alerts := pd.all()
	if len(alerts) != 1 {
		t.Fatalf("PagerDuty received %d alerts, want exactly 1 (run.finished): %v", len(alerts), alerts)
	}
	a := alerts[0]
	if a["routing_key"] != "R0UT1NG" {
		t.Errorf("routing_key = %v, want the renderer's own", a["routing_key"])
	}
	if a["event_action"] != "resolve" {
		t.Errorf("event_action = %v, want resolve for a run that succeeded", a["event_action"])
	}
	dedup, _ := a["dedup_key"].(string)
	if !strings.HasPrefix(dedup, "senro/") || len(dedup) <= len("senro/") {
		t.Errorf("dedup_key = %q, want senro/<run id>: it is what makes an at-least-once "+
			"delivery one incident rather than several", dedup)
	}

	// The outcome of delivering run.finished cannot be an event (the stream
	// is sealed behind the event it describes), so it reaches the shutdown
	// report instead. A third-party destination inherits that whole
	// mechanism, name and all.
	if out := report.String(); !strings.Contains(out, "pagerduty") || !strings.Contains(out, "delivered") {
		t.Errorf("the shutdown report does not account for the third-party delivery:\n%s", out)
	}
}

// TestAThirdPartyProviderReadsAnEventNoBuiltInDoes is the half of the story
// the built-in cannot tell: senro parses GitLab's push, tag push and merge
// request, and a comment on a merge request is none of those. "/retest" in a
// comment is a real reason to build, it arrives as a Note Hook, and the
// example turns it into an ordinary pull request event that the built-in
// Branches matcher narrows and the example's own Command matcher selects.
func TestAThirdPartyProviderReadsAnEventNoBuiltInDoes(t *testing.T) {
	ev, err := trigger.ReadEvent(strings.NewReader(gitlabComment), gitlabcomment.Provider{})
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if ev.Kind != trigger.PullRequest || ev.Number != 7 || ev.Branch != "main" {
		t.Fatalf("event = %+v, want a pull request 7 targeting main", ev)
	}
	if ev.Provider != "gitlab-comment" {
		t.Errorf("provider = %q, want the example's own name, filled in for it", ev.Provider)
	}

	marker := filepath.Join(t.TempDir(), "ran")
	pipe := senro.New("extended")
	pipe.Workflow("w").Step("touch", exec.Command("sh", "-c", "echo ran > "+marker))

	trig := trigger.OnPullRequest(trigger.Branches("main"), gitlabcomment.Command("retest"))
	err = senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trig),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("the pipeline did not run for the comment that asked it to: %v", err)
	}
	// The matcher a third party wrote is in the trigger's own description,
	// which is what a run's provenance record and every error message quote.
	if got, want := trig.String(), "pull_request(branches=[main], command=[retest])"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	// A comment that asked for nothing is a no-match and not an error: the
	// event is real and well formed, it just wants nothing.
	chat := strings.Replace(gitlabComment, "/retest please, the runner flaked", "looks good to me", 1)
	other, err := trigger.ReadEvent(strings.NewReader(chat), gitlabcomment.Provider{})
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if _, err := trigger.Select(other, trig); !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select on an ordinary comment = %v, want ErrNoMatch", err)
	}

	// A comment on something that is not a merge request is neither: there is
	// nothing to build and nobody meant to wire this.
	onAnIssue := strings.Replace(gitlabComment, `"noteable_type": "MergeRequest"`,
		`"noteable_type": "Issue"`, 1)
	if _, err := trigger.ReadEvent(strings.NewReader(onAnIssue), gitlabcomment.Provider{}); err == nil {
		t.Error("ReadEvent accepted a comment on an issue")
	} else if errors.Is(err, trigger.ErrNoMatch) {
		t.Errorf("ReadEvent = %v, want an ordinary error rather than a no-match", err)
	}
}

// TestAThirdPartyRendererSeesAFailedRunAsAFailedRun. The interesting half of
// a notifier is the run that went wrong, and everything the renderer needs to
// tell them apart is in the event it was handed.
func TestAThirdPartyRendererSeesAFailedRunAsAFailedRun(t *testing.T) {
	pd := newPagerDuty(t)

	var report strings.Builder
	n := notify.New(notify.WithReportWriter(&report), pd.destination())
	defer func() { _ = n.Close() }()

	pipe := senro.New("extended")
	pipe.Workflow("w").Step("boom", exec.Command("sh", "-c", "exit 3"))

	err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithSink(n),
	)
	if err == nil {
		t.Fatal("Run reported success for a pipeline whose only step exits 3")
	}
	if err := n.Close(); err != nil && !strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("Close: %v", err)
	}

	alerts := pd.all()
	if len(alerts) != 1 {
		t.Fatalf("PagerDuty received %d alerts, want 1: %v", len(alerts), alerts)
	}
	if got := alerts[0]["event_action"]; got != "trigger" {
		t.Errorf("event_action = %v, want trigger for a failed run", got)
	}
	payload, _ := alerts[0]["payload"].(map[string]any)
	if payload["severity"] != "error" {
		t.Errorf("severity = %v, want error", payload["severity"])
	}
	if summary, _ := payload["summary"].(string); !strings.Contains(summary, "failed") {
		t.Errorf("summary = %q, want it to say the run failed", summary)
	}
}

// TestAThirdPartyProvidersEventStillGatesTheRun: a no-match through a custom
// provider is the same inert no-match a built-in one produces. No run
// directory, no events, no notification, and a sentinel a dispatcher can map
// to exit 78.
func TestAThirdPartyProvidersEventStillGatesTheRun(t *testing.T) {
	pd := newPagerDuty(t)

	ev, err := trigger.ReadEvent(strings.NewReader(
		strings.Replace(gitlabPush, "refs/heads/main", "refs/heads/wip", 1)), gitlabcomment.Provider{})
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}

	var report strings.Builder
	n := notify.New(notify.WithReportWriter(&report), pd.destination())
	defer func() { _ = n.Close() }()

	root := t.TempDir()
	marker := filepath.Join(t.TempDir(), "ran")
	pipe := senro.New("extended")
	pipe.Workflow("w").Step("touch", exec.Command("sh", "-c", "echo ran > "+marker))

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(root, "should-not-exist")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithSink(n),
		senro.WithTrigger(ev, trigger.OnPush(trigger.Branches("main"))),
	)
	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Run = %v, want trigger.ErrNoMatch", err)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := listDir(t, filepath.Join(root, "should-not-exist")); len(got) != 0 {
		t.Errorf("a no-match created %v", got)
	}
	if alerts := pd.all(); len(alerts) != 0 {
		t.Errorf("a no-match notified somebody: %v", alerts)
	}
}

// TestAThirdPartyMatcherThatCannotAnswerFailsTheRunLoudly.
// gitlabcomment.Author is a matcher written outside this repository, and it
// keeps senro's rule: a question the event carries no answer to is an error,
// never a silent no-match. The two are indistinguishable from a dispatcher's
// side and mean opposite things.
func TestAThirdPartyMatcherThatCannotAnswerFailsTheRunLoudly(t *testing.T) {
	// A merge request payload carries a username; strip it and the matcher
	// has nothing to answer with.
	const anonymous = `{
	  "provider": "gitlab-comment",
	  "event": "Merge Request Hook",
	  "payload": {
	    "object_kind": "merge_request",
	    "object_attributes": {"iid": 7, "action": "open", "target_branch": "main",
	                          "source_branch": "feat", "last_commit": {"id": "cafe"}},
	    "project": {"path_with_namespace": "acme/app", "default_branch": "main"}
	  }
	}`
	ev, err := trigger.ReadEvent(strings.NewReader(anonymous), gitlabcomment.Provider{})
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if ev.Kind != trigger.PullRequest || ev.Action != "open" || ev.Number != 7 {
		t.Fatalf("event = %+v, want a merge request open on main", ev)
	}

	pipe := senro.New("extended")
	pipe.Workflow("w").Step("touch", exec.Command("true"))

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trigger.OnPullRequest(gitlabcomment.Author("ada"))),
	)
	if err == nil {
		t.Fatal("Run accepted a matcher that could not answer")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Run = %v, want an ordinary error rather than a no-match", err)
	}
	if !strings.Contains(err.Error(), "no author") {
		t.Errorf("the error does not carry the matcher's own message:\n%v", err)
	}
}

// TestAThirdPartyRendererThatPanicsDoesNotFailTheRun holds the two together
// at the seam that matters most: the notifier's promise is that a build never
// fails because an observer did, and an extension point is exactly where an
// observer starts being somebody else's code.
//
// The panic is not merely survived, it is recorded. A run whose notification
// silently never happened is a run that lied about being observed, so the
// panic becomes a notify.failed in the run's own ledger, which this test
// reads through a second, ordinary sink.
func TestAThirdPartyRendererThatPanicsDoesNotFailTheRun(t *testing.T) {
	pd := newPagerDuty(t)

	var mu sync.Mutex
	var failures []api.NotifyBody
	watch := senro.SinkFunc(func(e api.Event) {
		if e.Type != api.NotifyFailed {
			return
		}
		var b api.NotifyBody
		if err := e.Decode(&b); err == nil {
			mu.Lock()
			failures = append(failures, b)
			mu.Unlock()
		}
	})

	var report strings.Builder
	n := notify.New(
		notify.WithReportWriter(&report),
		// The renderer that got it wrong in the worst available way.
		notify.To(pd.srv.URL, notify.RendererFunc(func(api.Event) ([]byte, error) {
			panic("somebody else's renderer dereferenced nil")
		}), notify.Named("explosive"), notify.On(api.StepFinished)),
	)
	defer func() { _ = n.Close() }()

	marker := filepath.Join(t.TempDir(), "ran")
	pipe := senro.New("extended")
	pipe.Workflow("w").Step("touch", exec.Command("sh", "-c", "echo ran > "+marker))

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithSink(n),
		senro.WithSink(watch),
	); err != nil {
		t.Fatalf("Run failed because an observer's renderer panicked: %v", err)
	}
	if err := n.Close(); err != nil && !strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the step did not run: %v", err)
	}
	if alerts := pd.all(); len(alerts) != 0 {
		t.Errorf("a renderer that panicked still sent something: %v", alerts)
	}

	// Where the outcome is recorded depends on whether the delivery settled
	// before the run's stream sealed, which is a matter of timing and not of
	// policy: the ledger if it did, the shutdown report if it did not. Both
	// are places somebody reads. Silence is the only wrong answer, so the
	// assertion is on the union of the two.
	mu.Lock()
	said := report.String()
	for _, f := range failures {
		said += " " + f.Destination + " " + f.Error
	}
	mu.Unlock()

	if !strings.Contains(said, "explosive") {
		t.Fatalf("nothing anywhere accounts for the notification that panicked, which is a run "+
			"that silently lied about being observed; ledger and report together said:\n%s", said)
	}
	if !strings.Contains(said, "renderer panicked") {
		t.Errorf("the outcome does not name the renderer as what panicked:\n%s", said)
	}
}

// TestAThirdPartyExporterBuildsATraceFromTheEventStreamAlone decides whether
// senro was right not to depend on go.opentelemetry.io/otel:
// examples/extensions/otelspan is a senro.Sink importing only senro/api and
// the standard library, wired through senro.Run exactly as a third party
// would. If the event stream did not carry enough to build a span tree, this
// is where it would show.
//
// The pipeline is the awkward shapes on purpose: two steps in parallel with
// no edge, a step that fails once and recovers, a step skipped by a
// condition (so no step.started at all), a fan-in, and a handler that emits
// no log markers.
func TestAThirdPartyExporterBuildsATraceFromTheEventStreamAlone(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "attempted")

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("true"))
	l.Step("lint", exec.Command("true")).Needs("fetch")
	l.Step("test", exec.Command("sh", "-c",
		"if [ -f "+marker+" ]; then exit 0; else touch "+marker+"; exit 1; fi")).
		Needs("fetch").
		Retry(2, retry.OnExitCode(1))
	l.Step("audit", exec.Command("true")).Needs("fetch").When(senro.Branch("release"))
	l.Step("package", exec.Command("true")).Needs("lint", "test")
	l.Step("deploy", exec.Command("true")).
		Needs("package").
		Always(senro.Handler("release-lock", exec.Command("true")))

	exp := otelspan.New(io.Discard)
	if err := senro.Run(t.Context(), pipe,
		senro.WithSink(exp),
		senro.WithDir(filepath.Join(dir, "run")),
		senro.WithParams(senro.Params{"branch": "main"}),
		senro.WithTraceContext(
			"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "congo=t61rcWkgMzE"),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spans := exp.Spans()
	byName := map[string][]otelspan.Span{}
	byID := map[string]otelspan.Span{}
	for _, s := range spans {
		byName[s.Name] = append(byName[s.Name], s)
		byID[s.SpanID] = s
	}

	// One run, six steps, one of them run twice, plus one handler.
	if len(spans) != 9 {
		names := make([]string, 0, len(spans))
		for _, s := range spans {
			names = append(names, s.Name)
		}
		t.Fatalf("built %d spans, want 9; got %v", len(spans), names)
	}

	one := func(name string) otelspan.Span {
		t.Helper()
		if got := byName[name]; len(got) != 1 {
			t.Fatalf("%s produced %d spans, want 1", name, len(got))
		}
		return byName[name][0]
	}

	run := one("release")
	if run.Parent != "00f067aa0ba902b7" {
		t.Errorf("the run span's parent = %q, want the inbound span: the trace was not continued", run.Parent)
	}
	if run.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace = %q, want the inbound one", run.TraceID)
	}
	if run.Attrs["senro.tracestate"] != "congo=t61rcWkgMzE" {
		t.Errorf("tracestate = %q, want it carried through verbatim", run.Attrs["senro.tracestate"])
	}

	fetch := one("fetch")
	if fetch.Parent != run.SpanID {
		t.Errorf("fetch hangs off %q, want the run %q", fetch.Parent, run.SpanID)
	}

	// The parallelism assertion. lint and test have no edge, so neither may
	// contain the other however the scheduler happened to order them.
	lint := one("lint")
	if lint.Parent != fetch.SpanID {
		t.Errorf("lint hangs off %q, want fetch %q", lint.Parent, fetch.SpanID)
	}
	tests := byName["test"]
	if len(tests) != 2 {
		t.Fatalf("test produced %d spans, want 2 (one per attempt)", len(tests))
	}
	if tests[0].SpanID == tests[1].SpanID {
		t.Error("both attempts at test share a span ID")
	}
	for _, s := range tests {
		if s.Parent != fetch.SpanID {
			t.Errorf("a test attempt hangs off %q, want fetch %q: lint is not test's parent whatever ran first",
				s.Parent, fetch.SpanID)
		}
		if s.Duration() <= 0 {
			t.Errorf("test attempt %s has duration %s, so it was never closed", s.SpanID, s.Duration())
		}
	}

	// The step that never started. Its span exists only because
	// step.finished carries the parentage step.started would have.
	audit := one("audit")
	if audit.Parent != fetch.SpanID {
		t.Errorf("the skipped audit step hangs off %q, want fetch %q", audit.Parent, fetch.SpanID)
	}
	if audit.Attrs["senro.state"] != string(api.StateSkippedCondition) {
		t.Errorf("audit state = %q, want skipped_condition", audit.Attrs["senro.state"])
	}

	// The fan-in. One parent, and the other need recorded as a link rather
	// than dropped.
	pkg := one("package")
	if len(pkg.Links) != 1 {
		t.Errorf("package has %d links, want 1: a step waiting on two things must say so", len(pkg.Links))
	}
	parents := map[string]bool{pkg.Parent: true}
	for _, l := range pkg.Links {
		parents[l] = true
	}
	if !parents[lint.SpanID] {
		t.Error("package neither parents nor links to lint, which it needs")
	}
	if !parents[tests[1].SpanID] {
		t.Error("package neither parents nor links to the attempt at test that actually succeeded")
	}

	// The handler, which emits no log markers and would be invisible to
	// anything modelling a run by what steps logged.
	handler := one("deploy/always/release-lock")
	if handler.Parent != one("deploy").SpanID {
		t.Errorf("the handler hangs off %q, want the deploy attempt %q", handler.Parent, one("deploy").SpanID)
	}

	// Nothing may dangle. Every span walks up to the inbound parent, which is
	// the one span in the trace this run did not emit.
	for _, s := range spans {
		hops := 0
		for cur := s; cur.Parent != "00f067aa0ba902b7"; hops++ {
			next, ok := byID[cur.Parent]
			if !ok {
				t.Fatalf("span %s (%s) has parent %q, which is neither a span of this run nor the inbound parent",
					s.SpanID, s.Name, cur.Parent)
			}
			if hops > len(spans) {
				t.Fatalf("span %s (%s) is in a parent cycle", s.SpanID, s.Name)
			}
			cur = next
		}
	}
}
