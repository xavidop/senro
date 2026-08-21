package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/trigger"
)

// newDispatcher builds one backed by a file lock in a temp directory, with a
// pipeline that is a script the test writes.
func newDispatcher(t *testing.T, script string) *dispatcher {
	t.Helper()
	dir := t.TempDir()

	pipeline := filepath.Join(dir, "pipeline")
	if err := os.WriteFile(pipeline, []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
		t.Fatalf("writing the pipeline: %v", err)
	}

	st, err := persist.Open(filepath.Join(dir, "locks"))
	if err != nil {
		t.Fatalf("persist.Open: %v", err)
	}
	d := &dispatcher{
		secret:   "shh",
		pipeline: pipeline,
		group:    "test-group",
		locker:   persist.StoreLocker(st),
		timeout:  30 * time.Second,
	}
	// A delivery starts a goroutine that outlives serve, and t.TempDir is
	// removed when the test returns; without this the pipeline script is
	// deleted underneath a run still using it.
	t.Cleanup(func() {
		d.mu.Lock()
		stop := d.running
		d.mu.Unlock()
		if stop != nil {
			stop()
		}
		waitFor(t, d, true, "a run outlived its test")
	})
	return d
}

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// deliver posts a body the way a real webhook does: with the header naming
// which source sent it and what happened. Without one the delivery names no
// event source, which is a 400 before anything else is looked at.
func deliver(t *testing.T, d *dispatcher, body, sig string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	if sig != "" {
		req.Header.Set("X-Hub-Signature-256", sig)
	}
	w := httptest.NewRecorder()
	d.serve(w, req)
	return w
}

// Every way of getting the signature wrong is refused, and refused
// identically: absent, malformed or simply wrong is not something to confirm
// to whoever sent it.
func TestOnlyACorrectlySignedDeliveryIsAccepted(t *testing.T) {
	d := newDispatcher(t, "exit 0")
	const body = `{"ref":"refs/heads/main"}`

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Errorf("a correctly signed delivery got %d, want 202", got)
	}

	for _, tc := range []struct{ name, sig string }{
		{"no signature at all", ""},
		{"signed with the wrong secret", sign("not-the-secret", body)},
		{"signature of a different body", sign("shh", `{"ref":"refs/heads/other"}`)},
		{"not hex", "sha256=zzzz"},
		{"empty after the prefix", "sha256="},
		{"no prefix", strings.TrimPrefix(sign("shh", body), "sha256=")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := deliver(t, d, body, tc.sig)
			if w.Code != http.StatusForbidden {
				t.Errorf("got %d, want 403", w.Code)
			}
			if strings.Contains(w.Body.String(), "signature") {
				t.Errorf("the refusal explains itself: %q", w.Body.String())
			}
		})
	}
}

// The standing constraint: a LOCK, not a queue. A second delivery while one
// holds the group is refused with a reason, never buffered.
func TestASecondDeliveryIsRejectedRatherThanQueued(t *testing.T) {
	// A pipeline that stays up, so the group is genuinely held when the
	// second delivery lands.
	d := newDispatcher(t, "sleep 2")
	const body = `{}`

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Fatalf("the first delivery got %d, want 202", got)
	}
	// The run starts on its own goroutine; wait for it to actually hold the
	// lock rather than racing it.
	waitHeld(t, d)

	w := deliver(t, d, body, sign("shh", body))
	if w.Code != http.StatusConflict {
		t.Fatalf("the second delivery got %d, want 409", w.Code)
	}
	msg := w.Body.String()
	// It must say what holds it and that nothing is queued, or an operator
	// waits for a run that is never going to start.
	for _, want := range []string{"test-group", "rejects rather than queues"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the rejection does not mention %q: %q", want, msg)
		}
	}
}

// And once the first run finishes, the group is free again with no
// intervention: the lock is released by the run, not by a timer.
func TestTheGroupIsFreeAgainWhenTheRunEnds(t *testing.T) {
	d := newDispatcher(t, "exit 0")
	const body = `{}`

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Fatalf("first delivery: %d", got)
	}
	waitFree(t, d)

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Errorf("the group was still held after the run ended: got %d, want 202", got)
	}
}

// Exit 78 is "no trigger matched", the ordinary answer for most deliveries;
// what this pins is that a 78 still releases the group.
func TestANonMatchingDeliveryStillReleasesTheGroup(t *testing.T) {
	d := newDispatcher(t, "exit 78")
	const body = `{}`

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Fatalf("first delivery: %d", got)
	}
	waitFree(t, d)

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Errorf("a pipeline that matched nothing did not release the group: got %d", got)
	}
}

// With -cancel-in-progress the newest delivery wins: the run in progress is
// terminated and the new one takes the group. Still not a queue; the
// displaced run is gone, not deferred.
func TestCancelInProgressDisplacesTheRunningRun(t *testing.T) {
	d := newDispatcher(t, "sleep 5")
	d.cancel = true
	const body = `{}`

	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Fatalf("first delivery: %d", got)
	}
	waitHeld(t, d)

	w := deliver(t, d, body, sign("shh", body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("the second delivery got %d, want 202: %s", w.Code, w.Body.String())
	}
}

// A body past the limit is refused on size. This endpoint is reachable from
// the internet and an unbounded read is a way to exhaust the process.
func TestAnOversizedDeliveryIsRefused(t *testing.T) {
	d := newDispatcher(t, "exit 0")
	big := strings.Repeat("x", maxBody+1)
	w := deliver(t, d, big, sign("shh", big))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("got %d, want 413", w.Code)
	}
}

func TestOnlyPostIsServed(t *testing.T) {
	d := newDispatcher(t, "exit 0")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	d.serve(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET got %d, want 405", w.Code)
	}
}

// waitHeld blocks until the group is genuinely locked, so a test asserting a
// rejection is not racing the goroutine that takes the lock.
func waitHeld(t *testing.T, d *dispatcher) {
	t.Helper()
	waitFor(t, d, false, "the run never took the group")
}

// waitFree blocks until the group is available again.
func waitFree(t *testing.T, d *dispatcher) {
	t.Helper()
	waitFor(t, d, true, "the group was never released")
}

func waitFor(t *testing.T, d *dispatcher, free bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		u, err := d.locker.TryAcquire(context.Background(), d.group, "probe")
		if err == nil {
			_ = u.Release(context.Background())
			if free {
				return
			}
		} else if !free {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

// The assertion whose absence let a real bug ship: the pipeline is execed
// with --trigger-event <file>, and NOTHING here ever checked that the file
// is one trigger.LoadEvent can read.
//
// It could not be: the dispatcher wrote the raw webhook body, and a raw body
// is not the envelope format. No GitHub, GitLab, Bitbucket or Gitea body
// says which event it is, which is the whole reason the envelope exists. So
// every delivery reached the pipeline as "the event names no provider", at
// the far end of an exec, in a log nobody reads, while these tests went on
// asserting 202.
func TestThePipelineReceivesAnEventItCanActuallyLoad(t *testing.T) {
	dir := t.TempDir()
	captured := filepath.Join(dir, "captured.json")

	// The stub pipeline copies the file it was handed, so the test can read
	// exactly what a real pipeline binary would have been given.
	d := newDispatcher(t, "cp \"$2\" "+captured)

	body := string(rawGitHubPush(t))
	if got := deliver(t, d, body, sign("shh", body)).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d, want 202", got)
	}
	waitFor(t, d, true, "the run did not finish")

	ev, err := trigger.LoadEvent(captured)
	if err != nil {
		t.Fatalf("the pipeline was handed a file trigger.LoadEvent cannot read: %v", err)
	}
	if ev == nil {
		t.Fatal("the pipeline was handed no event at all")
	}
	if ev.Provider != "github" {
		t.Errorf("Provider = %q, want github from the X-GitHub-Event header", ev.Provider)
	}
	if ev.Kind != trigger.Push {
		t.Errorf("Kind = %q, want push", ev.Kind)
	}
	if ev.Branch != "master" {
		t.Errorf("Branch = %q, want the branch the payload names", ev.Branch)
	}
}

// A delivery carrying no source header names nothing this build can parse,
// so it is refused before the concurrency group is taken: a run must not be
// started for a body nobody can attribute.
func TestADeliveryWithNoSourceHeaderIsRefused(t *testing.T) {
	d := newDispatcher(t, "exit 0")
	const body = `{"ref":"refs/heads/main"}`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", sign("shh", body))
	w := httptest.NewRecorder()
	d.serve(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d, want 400 for a delivery naming no event source", w.Code)
	}
}

// rawGitHubPush is a real GitHub push body: the trigger package's own
// fixture, with senro's envelope stripped back off, which is what the wire
// actually carries.
func rawGitHubPush(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "trigger", "testdata", "github-push-branch.json"))
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	var env struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("reading the fixture envelope: %v", err)
	}
	return env.Payload
}
