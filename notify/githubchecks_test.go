package notify_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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
