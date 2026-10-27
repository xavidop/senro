package notify_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/notify"
)

// ghStub stands in for GitHub's Checks API and records what reached it.
type ghStub struct {
	srv *httptest.Server

	mu   sync.Mutex
	reqs []ghReq
}

type ghReq struct {
	method string
	path   string
	body   map[string]any
}

func newGHStub(t *testing.T) *ghStub {
	t.Helper()
	s := &ghStub{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var body map[string]any
		_ = json.Unmarshal(b, &body)

		s.mu.Lock()
		s.reqs = append(s.reqs, ghReq{method: r.Method, path: r.URL.Path, body: body})
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		// The id only ever appears in a create's response, which is how the
		// requester learns where to send the update.
		_, _ = io.WriteString(w, `{"id":4242}`)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *ghStub) seen() []ghReq {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ghReq(nil), s.reqs...)
}

func ghEvent(t *testing.T, typ api.Type, seq uint64, step string, payload string) api.Event {
	t.Helper()
	e := api.Event{
		V: api.Version, Seq: seq, Type: typ, Run: "r1", Step: step,
		TS: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
	}
	if payload != "" {
		e.Payload = json.RawMessage(payload)
	}
	return e
}

// drive pushes events through a notifier and waits for delivery, which is
// what makes this a test of the destination rather than of the requester's
// methods called directly.
func drive(t *testing.T, d *notify.Destination, events ...api.Event) {
	t.Helper()
	n := notify.New(d)
	// A standalone Notifier has no engine to append outcomes to, and Flush
	// says so; that is not what these tests are about.
	for _, e := range events {
		n.Emit(e)
	}
	if err := n.Flush(context.Background()); err != nil &&
		!strings.Contains(err.Error(), "could not be recorded") {
		t.Fatalf("flush: %v", err)
	}
}

// The whole loop: a check run is created when the run starts and completed
// when it ends, at the URL GitHub's own response named.
func TestACheckRunIsCreatedThenCompleted(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL))

	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	)

	reqs := gh.seen()
	if len(reqs) != 2 {
		t.Fatalf("made %d requests, want 2: %+v", len(reqs), reqs)
	}
	if reqs[0].method != http.MethodPost || reqs[0].path != "/repos/acme/web/check-runs" {
		t.Errorf("create = %s %s, want POST /repos/acme/web/check-runs", reqs[0].method, reqs[0].path)
	}
	if reqs[0].body["head_sha"] != "deadbeef" {
		t.Errorf("create head_sha = %v, want deadbeef", reqs[0].body["head_sha"])
	}
	if reqs[0].body["status"] != "in_progress" {
		t.Errorf("create status = %v, want in_progress", reqs[0].body["status"])
	}

	// The id came from the create's RESPONSE, not from anything senro chose.
	if reqs[1].method != http.MethodPatch || reqs[1].path != "/repos/acme/web/check-runs/4242" {
		t.Errorf("complete = %s %s, want PATCH .../4242", reqs[1].method, reqs[1].path)
	}
	if reqs[1].body["conclusion"] != "success" {
		t.Errorf("conclusion = %v, want success", reqs[1].body["conclusion"])
	}
}

// Steps do not each send a request. A thousand-step run would otherwise be a
// thousand calls into GitHub's secondary rate limits.
func TestStepsDoNotEachCostARequest(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL))

	events := []api.Event{ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`)}
	for i := 0; i < 20; i++ {
		events = append(events, ghEvent(t, api.StepFinished, uint64(i+2), "s", `{"state":"succeeded"}`))
	}
	events = append(events, ghEvent(t, api.RunFinished, 99, "", `{"status":"succeeded"}`))

	drive(t, d, events...)

	if n := len(gh.seen()); n != 2 {
		t.Errorf("made %d requests for a 20-step run, want 2", n)
	}
}

// A failed step becomes an annotation on the check, carrying the state and
// the exit code, so a reviewer sees which step failed without opening senro.
func TestAFailedStepBecomesAnAnnotation(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL))

	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.StepFinished, 2, "build", `{"state":"succeeded"}`),
		ghEvent(t, api.StepFinished, 3, "test", `{"state":"failed","exit_code":2,"error":"boom"}`),
		ghEvent(t, api.RunFinished, 4, "", `{"status":"failed"}`),
	)

	reqs := gh.seen()
	if len(reqs) != 2 {
		t.Fatalf("made %d requests, want 2", len(reqs))
	}
	out, ok := reqs[1].body["output"].(map[string]any)
	if !ok {
		t.Fatalf("completion carries no output: %+v", reqs[1].body)
	}
	ann, ok := out["annotations"].([]any)
	if !ok || len(ann) != 1 {
		t.Fatalf("annotations = %v, want exactly the one failed step", out["annotations"])
	}
	a := ann[0].(map[string]any)
	if a["annotation_level"] != "failure" {
		t.Errorf("annotation level = %v, want failure", a["annotation_level"])
	}
	for _, want := range []string{"test", "failed"} {
		if !strings.Contains(a["title"].(string), want) {
			t.Errorf("annotation title %q does not mention %q", a["title"], want)
		}
	}
	if msg := a["message"].(string); !strings.Contains(msg, "boom") || !strings.Contains(msg, "exit 2") {
		t.Errorf("annotation message %q does not carry the error and the exit code", msg)
	}
	if reqs[1].body["conclusion"] != "failure" {
		t.Errorf("conclusion = %v, want failure", reqs[1].body["conclusion"])
	}
}

// Every run status this build declares maps onto a conclusion GitHub accepts.
// A status that fell through to the empty string would leave a check run
// permanently in progress.
func TestEveryRunStatusMapsToAConclusion(t *testing.T) {
	valid := map[string]bool{
		"success": true, "failure": true, "neutral": true,
		"cancelled": true, "timed_out": true, "action_required": true, "skipped": true,
	}
	for _, st := range []api.RunStatus{
		api.RunSucceeded, api.RunSucceededWithRecovery, api.RunPartial,
		api.RunFailed, api.RunCancelled,
		api.RunStatus("something-a-newer-engine-emits"),
	} {
		t.Run(string(st), func(t *testing.T) {
			gh := newGHStub(t)
			d := notify.GitHubChecks("acme", "web", "sha", "tok", "",
				notify.GitHubChecksAPI(gh.srv.URL))
			drive(t, d,
				ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
				ghEvent(t, api.RunFinished, 2, "", `{"status":"`+string(st)+`"}`),
			)
			reqs := gh.seen()
			if len(reqs) != 2 {
				t.Fatalf("made %d requests, want 2", len(reqs))
			}
			got, _ := reqs[1].body["conclusion"].(string)
			if !valid[got] {
				t.Errorf("status %q produced conclusion %q, which GitHub does not accept", st, got)
			}
			if reqs[1].body["status"] != "completed" {
				t.Errorf("check run was left %v rather than completed", reqs[1].body["status"])
			}
		})
	}
}

// More failures than GitHub will take are truncated, and the summary says so.
// A silently short list is one a reader cannot tell from a complete one.
func TestTooManyFailuresAreTruncatedAndSaidOutLoud(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "sha", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL))

	events := []api.Event{ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`)}
	const failures = notify.MaxAnnotations + 7
	for i := 0; i < failures; i++ {
		events = append(events, ghEvent(t, api.StepFinished, uint64(i+2), "s", `{"state":"failed"}`))
	}
	events = append(events, ghEvent(t, api.RunFinished, 999, "", `{"status":"failed"}`))
	drive(t, d, events...)

	reqs := gh.seen()
	out := reqs[len(reqs)-1].body["output"].(map[string]any)
	ann := out["annotations"].([]any)
	if len(ann) != notify.MaxAnnotations {
		t.Errorf("sent %d annotations, want the cap of %d", len(ann), notify.MaxAnnotations)
	}
	if s := out["summary"].(string); !strings.Contains(s, "7 further") {
		t.Errorf("summary %q does not say how many failures were left out", s)
	}
}

// A create answered without an id leaves the run with nothing to update; the
// completion must not be sent to a URL with an empty id in it.
func TestACreateWithNoIDDoesNotProduceABrokenUpdate(t *testing.T) {
	var paths []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		_, _ = io.WriteString(w, `{"not_an_id":1}`)
	}))
	defer srv.Close()

	d := notify.GitHubChecks("acme", "web", "sha", "tok", "", notify.GitHubChecksAPI(srv.URL))
	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	)

	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 {
		t.Fatalf("made %d requests, want only the create: %v", len(paths), paths)
	}
}

// completionOutput drives events through a fresh destination and returns
// the completion's output object.
func completionOutput(t *testing.T, events ...api.Event) map[string]any {
	t.Helper()
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL))
	drive(t, d, events...)
	reqs := gh.seen()
	if len(reqs) != 2 {
		t.Fatalf("made %d requests, want 2", len(reqs))
	}
	out, ok := reqs[1].body["output"].(map[string]any)
	if !ok {
		t.Fatalf("completion carries no output: %+v", reqs[1].body)
	}
	return out
}

// A failed test is annotated at its own file and line, ahead of its step's
// annotation, and the title counts it.
func TestAFailedTestBecomesAnAnnotationWithFileAndLine(t *testing.T) {
	out := completionOutput(t,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.TestFinished, 2, "t", `{"name":"TestX","suite":"pkg/a","outcome":"failed","output":"boom","file":"a_test.go","line":9}`),
		ghEvent(t, api.StepFinished, 3, "t", `{"state":"failed","exit_code":1}`),
		ghEvent(t, api.RunFinished, 4, "", `{"status":"failed"}`),
	)
	ann, _ := out["annotations"].([]any)
	if len(ann) != 2 {
		t.Fatalf("annotations = %v, want the test and its step", out["annotations"])
	}
	a := ann[0].(map[string]any)
	if a["path"] != "a_test.go" || a["start_line"] != float64(9) || a["end_line"] != float64(9) {
		t.Errorf("test annotation location = %v:%v-%v", a["path"], a["start_line"], a["end_line"])
	}
	if a["title"] != "test pkg/a.TestX failed" || a["message"] != "boom" || a["annotation_level"] != "failure" {
		t.Errorf("test annotation = %+v", a)
	}
	if !strings.Contains(ann[1].(map[string]any)["title"].(string), "step t") {
		t.Errorf("second annotation is not the step's: %+v", ann[1])
	}
	if !strings.Contains(out["title"].(string), "1 test(s) failed") {
		t.Errorf("title %q does not count the failed test", out["title"])
	}
	if !strings.Contains(out["summary"].(string), ", 1 test(s) failed") {
		t.Errorf("summary %q does not count the failed test", out["summary"])
	}
}

func TestAPassedTestIsNotAnnotated(t *testing.T) {
	out := completionOutput(t,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.TestFinished, 2, "t", `{"name":"TestOK","outcome":"passed"}`),
		ghEvent(t, api.TestFinished, 3, "t", `{"name":"TestSkip","outcome":"skipped"}`),
		ghEvent(t, api.StepFinished, 4, "t", `{"state":"succeeded"}`),
		ghEvent(t, api.RunFinished, 5, "", `{"status":"succeeded"}`),
	)
	if ann, _ := out["annotations"].([]any); len(ann) != 0 {
		t.Errorf("annotations = %v, want none", ann)
	}
	if out["title"] != "succeeded" {
		t.Errorf("title = %v", out["title"])
	}
}

// The cap is bytes, the output is text: a cut that lands inside a multi-byte
// rune moves back to its start rather than sending half a character.
func TestATestAnnotationMessageIsCutAtARuneBoundary(t *testing.T) {
	// One byte of padding puts the cap one byte into a three-byte rune.
	long := "x" + strings.Repeat("€", notify.MaxTestAnnotationMessage/3+5)
	out := completionOutput(t,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.TestFinished, 2, "t", `{"name":"A","outcome":"failed","output":"`+long+`"}`),
		ghEvent(t, api.RunFinished, 3, "", `{"status":"failed"}`),
	)
	ann, _ := out["annotations"].([]any)
	if len(ann) != 1 {
		t.Fatalf("annotations = %v", ann)
	}
	msg := ann[0].(map[string]any)["message"].(string)
	body, ok := strings.CutSuffix(msg, "\n[truncated]")
	if !ok || !utf8.ValidString(body) || !strings.HasPrefix(long, body) || len(body) > notify.MaxTestAnnotationMessage {
		t.Errorf("message is not a whole-rune prefix under the cap (len %d, valid %v)", len(body), utf8.ValidString(body))
	}
	if want := notify.MaxTestAnnotationMessage - 1; len(body) != want {
		t.Errorf("cut at %d bytes, want %d (the rune start before the cap)", len(body), want)
	}
}

func TestATestWithoutAFileAnnotatesDotSenro(t *testing.T) {
	long := strings.Repeat("x", notify.MaxTestAnnotationMessage+10)
	out := completionOutput(t,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.TestFinished, 2, "t", `{"name":"A","outcome":"failed","output":"`+long+`"}`),
		ghEvent(t, api.TestFinished, 3, "t", `{"name":"B","outcome":"failed"}`),
		ghEvent(t, api.RunFinished, 4, "", `{"status":"failed"}`),
	)
	ann, _ := out["annotations"].([]any)
	if len(ann) != 2 {
		t.Fatalf("annotations = %v", ann)
	}
	a := ann[0].(map[string]any)
	if a["path"] != ".senro" || a["start_line"] != float64(1) || a["end_line"] != float64(1) || a["title"] != "test A failed" {
		t.Errorf("fallback annotation = %+v", a)
	}
	want := strings.Repeat("x", notify.MaxTestAnnotationMessage) + "\n[truncated]"
	if a["message"] != want {
		t.Errorf("message not cut to the cap with a marker (len %d)", len(a["message"].(string)))
	}
	if b := ann[1].(map[string]any); b["message"] != "test B failed" {
		t.Errorf("empty output message = %v, want the title text", b["message"])
	}
}

// Test and step annotations share one budget, and the overflow is said out
// loud. The tests are distinct: the same name reported again is the same test
// (see TestATestFailedOnEveryAttemptIsOneAnnotation).
func TestTestAnnotationsShareTheBudgetWithSteps(t *testing.T) {
	events := []api.Event{ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`)}
	for i := 0; i < notify.MaxAnnotations; i++ {
		events = append(events, ghEvent(t, api.TestFinished, uint64(2+i), "t",
			fmt.Sprintf(`{"name":"T%d","outcome":"failed"}`, i)))
	}
	events = append(events,
		ghEvent(t, api.StepFinished, 100, "t", `{"state":"failed"}`),
		ghEvent(t, api.RunFinished, 101, "", `{"status":"failed"}`))
	out := completionOutput(t, events...)
	if ann, _ := out["annotations"].([]any); len(ann) != notify.MaxAnnotations {
		t.Errorf("annotations = %d, want %d", len(ann), notify.MaxAnnotations)
	}
	if !strings.Contains(out["summary"].(string), "1 further failure(s) are not annotated here") {
		t.Errorf("summary %q does not report the dropped step", out["summary"])
	}
}

// attemptEvent is ghEvent for a step-scoped event on a given attempt.
func attemptEvent(t *testing.T, typ api.Type, seq uint64, step string, attempt int, payload string) api.Event {
	t.Helper()
	e := ghEvent(t, typ, seq, step, payload)
	e.Attempt = attempt
	return e
}

// A test that failed on attempt 1 and passed on attempt 2 is recovered, not
// failed. The check must agree with every other client, which all fold, so it
// neither annotates the test nor counts it in the title.
func TestARecoveredTestIsNotAnnotated(t *testing.T) {
	out := completionOutput(t,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		attemptEvent(t, api.TestFinished, 2, "t", 1, `{"name":"TestX","outcome":"failed","output":"boom"}`),
		attemptEvent(t, api.StepRetried, 3, "t", 2, `{"attempt":2}`),
		attemptEvent(t, api.TestFinished, 4, "t", 2, `{"name":"TestX","outcome":"passed"}`),
		attemptEvent(t, api.StepFinished, 5, "t", 2, `{"state":"recovered"}`),
		ghEvent(t, api.RunFinished, 6, "", `{"status":"succeeded"}`),
	)
	if ann, _ := out["annotations"].([]any); len(ann) != 0 {
		t.Errorf("annotations = %v, want none for a recovered test", ann)
	}
	if title := out["title"].(string); strings.Contains(title, "test(s) failed") {
		t.Errorf("title %q counts a recovered test as failed", title)
	}
}

// A test that failed on all three attempts is one failing test, annotated
// once with its last attempt's output, not three.
func TestATestFailedOnEveryAttemptIsOneAnnotation(t *testing.T) {
	events := []api.Event{ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`)}
	seq := uint64(1)
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			seq++
			events = append(events, attemptEvent(t, api.StepRetried, seq, "t", attempt,
				fmt.Sprintf(`{"attempt":%d}`, attempt)))
		}
		seq++
		events = append(events, attemptEvent(t, api.TestFinished, seq, "t", attempt,
			fmt.Sprintf(`{"name":"TestX","outcome":"failed","output":"boom %d"}`, attempt)))
	}
	events = append(events,
		attemptEvent(t, api.StepFinished, seq+1, "t", 3, `{"state":"failed","exit_code":1}`),
		ghEvent(t, api.RunFinished, seq+2, "", `{"status":"failed"}`))
	out := completionOutput(t, events...)

	ann, _ := out["annotations"].([]any)
	var tests int
	for _, a := range ann {
		m := a.(map[string]any)
		if !strings.HasPrefix(m["title"].(string), "test ") {
			continue
		}
		tests++
		if m["message"] != "boom 3" {
			t.Errorf("test annotation message = %v, want the last attempt's output", m["message"])
		}
	}
	if tests != 1 {
		t.Errorf("test annotations = %d, want 1: %v", tests, ann)
	}
	if title := out["title"].(string); !strings.Contains(title, "1 test(s) failed") {
		t.Errorf("title %q, want it to count one failed test", title)
	}
}

// With both a failed test and a failed step that reported none (a build
// error, say), the title names both, so neither is understated.
func TestTheTitleCountsFailedTestsAndSteps(t *testing.T) {
	out := completionOutput(t,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.TestFinished, 2, "unit", `{"name":"TestX","outcome":"failed"}`),
		ghEvent(t, api.StepFinished, 3, "unit", `{"state":"failed","exit_code":1}`),
		ghEvent(t, api.StepFinished, 4, "build", `{"state":"failed","exit_code":2}`),
		ghEvent(t, api.RunFinished, 5, "", `{"status":"failed"}`),
	)
	if want := "failed, 1 test(s) failed, 2 step(s) failed"; out["title"] != want {
		t.Errorf("title = %q, want %q", out["title"], want)
	}
}

func TestExistingCheckRunPatchesInProgress(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL), notify.ExistingCheckRun(42))

	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	)

	reqs := gh.seen()
	if len(reqs) != 2 {
		t.Fatalf("made %d requests, want 2: %+v", len(reqs), reqs)
	}
	for i, r := range reqs {
		if r.method != http.MethodPatch || r.path != "/repos/acme/web/check-runs/42" {
			t.Errorf("request %d = %s %s, want PATCH .../42", i, r.method, r.path)
		}
	}
	if reqs[0].body["status"] != "in_progress" {
		t.Errorf("start status = %v, want in_progress", reqs[0].body["status"])
	}
	if reqs[0].body["started_at"] == nil {
		t.Errorf("start has no started_at: %v", reqs[0].body)
	}
	if _, ok := reqs[0].body["head_sha"]; ok {
		t.Errorf("start carries head_sha, which is for a create: %v", reqs[0].body)
	}
	if reqs[1].body["status"] != "completed" {
		t.Errorf("finish status = %v, want completed", reqs[1].body["status"])
	}
}

func TestDetailsURLIsRenderedOnEveryPatch(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL), notify.ExistingCheckRun(42),
		notify.DetailsURL("https://ci.example/runs/{run}"))
	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	)
	reqs := gh.seen()
	if len(reqs) != 2 {
		t.Fatalf("made %d requests, want 2", len(reqs))
	}
	for i, r := range reqs {
		if r.body["details_url"] != "https://ci.example/runs/r1" {
			t.Errorf("request %d details_url = %v", i, r.body["details_url"])
		}
	}

	gh2 := newGHStub(t)
	d2 := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh2.srv.URL), notify.ExistingCheckRun(42))
	drive(t, d2,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	)
	for i, r := range gh2.seen() {
		if _, ok := r.body["details_url"]; ok {
			t.Errorf("request %d has details_url without the option", i)
		}
	}
}

func setGHEnv(t *testing.T, apiURL string) {
	t.Helper()
	t.Setenv(notify.EnvGitHubToken, "tok")
	t.Setenv(notify.EnvGitHubAPI, apiURL)
	t.Setenv(notify.EnvGitHubRepo, "acme/web")
	t.Setenv(notify.EnvGitHubSHA, "deadbeef")
	t.Setenv(notify.EnvGitHubCheckRun, "42")
	t.Setenv(notify.EnvGitHubCheckName, "ci/senro")
	t.Setenv(notify.EnvGitHubDetailsURL, "https://ci.example/runs/{run}")
}

func TestGitHubChecksFromEnv(t *testing.T) {
	events := []api.Event{
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	}

	gh := newGHStub(t)
	setGHEnv(t, gh.srv.URL)
	d, ok := notify.GitHubChecksFromEnv()
	if !ok || d == nil {
		t.Fatalf("ok = %v, d = %v, want a destination", ok, d)
	}
	drive(t, d, events...)
	reqs := gh.seen()
	if len(reqs) != 2 {
		t.Fatalf("made %d requests, want 2", len(reqs))
	}
	if reqs[0].method != http.MethodPatch || reqs[0].path != "/repos/acme/web/check-runs/42" {
		t.Errorf("start = %s %s", reqs[0].method, reqs[0].path)
	}
	if reqs[1].body["name"] != "ci/senro" || reqs[1].body["head_sha"] != "deadbeef" {
		t.Errorf("completion name/sha = %v/%v", reqs[1].body["name"], reqs[1].body["head_sha"])
	}
	if reqs[1].body["details_url"] != "https://ci.example/runs/r1" {
		t.Errorf("details_url = %v", reqs[1].body["details_url"])
	}

	// No check run id: the requester creates, as GitHubChecks does.
	gh2 := newGHStub(t)
	setGHEnv(t, gh2.srv.URL)
	t.Setenv(notify.EnvGitHubCheckRun, "")
	d, ok = notify.GitHubChecksFromEnv()
	if !ok {
		t.Fatal("ok = false without a check run id")
	}
	drive(t, d, events...)
	if r := gh2.seen(); len(r) == 0 || r[0].method != http.MethodPost {
		t.Errorf("without an id the first request = %+v, want POST", r)
	}

	t.Setenv(notify.EnvGitHubCheckRun, "abc")
	if d, ok := notify.GitHubChecksFromEnv(); ok || d != nil {
		t.Errorf("non-numeric id: ok = %v, d = %v, want false, nil", ok, d)
	}

	t.Setenv(notify.EnvGitHubCheckRun, "42")
	t.Setenv(notify.EnvGitHubToken, "")
	if d, ok := notify.GitHubChecksFromEnv(); ok || d != nil {
		t.Errorf("empty token: ok = %v, d = %v, want false, nil", ok, d)
	}
}

// A run id is spliced into a URL path, so it is path-escaped: a run id
// holding a slash or a space must not change the URL's shape.
func TestDetailsURLPathEscapesTheRunID(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL), notify.ExistingCheckRun(42),
		notify.DetailsURL("https://ci.example/runs/{run}"))
	start := ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`)
	start.Run = "a b/c"
	drive(t, d, start)
	reqs := gh.seen()
	if len(reqs) != 1 {
		t.Fatalf("made %d requests, want 1", len(reqs))
	}
	if got, want := reqs[0].body["details_url"], "https://ci.example/runs/a%20b%2Fc"; got != want {
		t.Errorf("details_url = %v, want %v", got, want)
	}
}

func TestDetailsURLWithoutAPlaceholderIsSentVerbatim(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL), notify.ExistingCheckRun(42),
		notify.DetailsURL("https://ci.example/dashboard"))
	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunFinished, 2, "", `{"status":"succeeded"}`),
	)
	for i, r := range gh.seen() {
		if r.body["details_url"] != "https://ci.example/dashboard" {
			t.Errorf("request %d details_url = %v", i, r.body["details_url"])
		}
	}
}

// An adopted check is moved to in_progress once: a second run.started (a
// replayed or duplicated event) sends nothing.
func TestADuplicateAdoptedRunStartedSendsNothing(t *testing.T) {
	gh := newGHStub(t)
	d := notify.GitHubChecks("acme", "web", "deadbeef", "tok", "",
		notify.GitHubChecksAPI(gh.srv.URL), notify.ExistingCheckRun(42))
	drive(t, d,
		ghEvent(t, api.RunStarted, 1, "", `{"pipeline":"p"}`),
		ghEvent(t, api.RunStarted, 2, "", `{"pipeline":"p"}`),
	)
	if reqs := gh.seen(); len(reqs) != 1 {
		t.Fatalf("made %d requests, want 1: %+v", len(reqs), reqs)
	}
}

func TestGitHubChecksFromEnvRejectsARepoWithoutASlash(t *testing.T) {
	setGHEnv(t, "http://127.0.0.1:0")
	for _, repo := range []string{"acme", "acme/", "/web", ""} {
		t.Setenv(notify.EnvGitHubRepo, repo)
		if d, ok := notify.GitHubChecksFromEnv(); ok || d != nil {
			t.Errorf("repo %q: ok = %v, d = %v, want false, nil", repo, ok, d)
		}
	}
}
