package attachsrv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/stepid"
)

// These tests all dial a REAL unix socket (no in-process handler shortcut)
// because the whole point of these tests is the transport: a real
// net.Listener, a real http.Server, a real client on the other end. A test
// that called the handler functions directly would prove the JSON shapes
// are right and nothing about whether the socket, the mux routing or the
// streaming/flushing actually work.

func TestStateReturnsAFoldableSnapshotCarryingItsSeq(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	for _, e := range twoStepEvents() {
		ts.hub.Emit(e)
	}

	resp, err := ts.client.Get(ts.url("/api/state"))
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var st api.RunState
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if st.Seq != 8 {
		t.Errorf("Seq = %d, want 8 (twoStepEvents' last seq)", st.Seq)
	}
	if !st.Run.Done || st.Run.Status != api.RunSucceeded {
		t.Errorf("Run = %+v, want a finished succeeded run", st.Run)
	}
	if len(st.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(st.Steps))
	}

	// "Foldable": the whole reason /api/state exists is so a client can seed
	// its own RunState and keep applying live events onto it. Prove the JSON
	// round trip preserved everything api.RunState.Apply depends on (Seq,
	// the Steps/Order/Handlers maps and slices), not just that it decodes.
	if err := st.Apply(api.Event{V: 1, Seq: 9, Type: api.StepCreated, Step: "extra"}); err != nil {
		t.Fatalf("Apply on the decoded snapshot: %v", err)
	}
	if _, ok := st.Steps["extra"]; !ok {
		t.Error("decoded snapshot did not fold a subsequent event correctly — it is not the foldable snapshot the endpoint promises")
	}
}

func TestPlanReturnsThePlan(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Needs: []string{"build"}},
	}}
	writePlanFile(t, ts.dir, p)

	resp, err := ts.client.Get(ts.url("/api/plan"))
	if err != nil {
		t.Fatalf("GET /api/plan: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	got, err := plan.Unmarshal(b)
	if err != nil {
		t.Fatalf("response body does not round-trip through plan.Unmarshal: %v", err)
	}
	if got.Digest() != p.Digest() {
		t.Errorf("digest = %s, want %s — the served plan is not the one written to disk", got.Digest(), p.Digest())
	}
}

func TestPlanIsNotFoundBeforeItExists(t *testing.T) {
	ts := newTestServer(t, testServerOpts{}) // no plan.json written

	resp, err := ts.client.Get(ts.url("/api/plan"))
	if err != nil {
		t.Fatalf("GET /api/plan: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestLogsHonoursFrom(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	writeLogFile(t, ts.dir, "build", 1, api.StreamStdout, "0123456789")

	resp, err := ts.client.Get(ts.url("/api/logs/build?attempt=1&stream=stdout&from=4"))
	if err != nil {
		t.Fatalf("GET /api/logs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(b) != "456789" {
		t.Errorf("body = %q, want %q", b, "456789")
	}

	// from absent (or 0) serves the whole file: the range boundary must be
	// an offset, not a truncation of what's returned when unspecified.
	resp2, err := ts.client.Get(ts.url("/api/logs/build?attempt=1&stream=stdout"))
	if err != nil {
		t.Fatalf("GET /api/logs (no from): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	b2, _ := io.ReadAll(resp2.Body)
	if string(b2) != "0123456789" {
		t.Errorf("body with no from = %q, want the whole file", b2)
	}
}

func TestLogsAttemptAndStreamSelectTheRightFile(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	writeLogFile(t, ts.dir, "build", 1, api.StreamStdout, "attempt1-stdout")
	writeLogFile(t, ts.dir, "build", 2, api.StreamStdout, "attempt2-stdout")
	writeLogFile(t, ts.dir, "build", 1, api.StreamStderr, "attempt1-stderr")

	cases := map[string]string{
		"/api/logs/build?attempt=1&stream=stdout": "attempt1-stdout",
		"/api/logs/build?attempt=2&stream=stdout": "attempt2-stdout",
		"/api/logs/build?attempt=1&stream=stderr": "attempt1-stderr",
	}
	for path, want := range cases {
		resp, err := ts.client.Get(ts.url(path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(b) != want {
			t.Errorf("GET %s body = %q, want %q", path, b, want)
		}
	}
}

// Step IDs contain "/", which stepid.Encode percent-encodes into a single
// path segment so it survives as ONE segment on the wire.
//
// This sends a real %2F-encoded request at handleLogs's "{step}" wildcard
// route, worth pinning rather than assuming: several well-known Go routers
// have historically decoded %2F before matching, splitting a nested ID
// across segments. Go's stdlib ServeMux (1.22+) does not, so
// r.PathValue("step") comes back with "/" intact.
func TestLogsHandlesAStepIDContainingASlash(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	const step = "deploy/discover/apply-west"
	writeLogFile(t, ts.dir, step, 1, api.StreamStdout, "nested-step-output")

	encoded := stepid.Encode(step)
	if !strings.Contains(encoded, "%2F") {
		t.Fatalf("stepid.Encode(%q) = %q, want it to contain %%2F — this test is worthless if the request it sends does not actually exercise the encoded-slash path", step, encoded)
	}
	resp, err := ts.client.Get(ts.url("/api/logs/" + encoded + "?attempt=1&stream=stdout"))
	if err != nil {
		t.Fatalf("GET /api/logs/%s: %v", encoded, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "nested-step-output" {
		t.Errorf("body = %q, want %q", b, "nested-step-output")
	}
}

func TestLogsMissingFileIsNotFound(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	resp, err := ts.client.Get(ts.url("/api/logs/never-ran?attempt=1&stream=stdout"))
	if err != nil {
		t.Fatalf("GET /api/logs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// An unvalidated stream is a file-read primitive, not merely a bad
// parameter. This sends a relative traversal reaching a secret placed
// OUTSIDE the run directory, plus an absolute-path variant, against both a
// normal and a ReadOnly server (ReadOnly gates no file read, so it must
// stay refused either way). Every case must be refused before any file is
// opened, so the assertions check the status AND that the secret's
// contents never appear in the body.
func TestLogsRejectsPathTraversalViaStream(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("ReadOnly=%v", readOnly), func(t *testing.T) {
			ts := newTestServer(t, testServerOpts{ReadOnly: readOnly})
			writeLogFile(t, ts.dir, "build", 1, api.StreamStdout, "not-a-secret")

			secretDir := t.TempDir()
			secretPath := filepath.Join(secretDir, "secret.txt")
			if err := os.WriteFile(secretPath, []byte("TOP-SECRET"), 0o644); err != nil {
				t.Fatalf("write secret file: %v", err)
			}

			payloads := []string{
				// Relative traversal out of the run directory's logs/ tree.
				// This string, run through filepath.Join with enough ".."
				// to clear any plausible t.TempDir() nesting depth,
				// resolves to exactly secretPath.
				"../../../../../../../../../../.." + secretPath,
				// Not itself a traversal: filepath.Join does not treat an
				// embedded absolute-looking element as resetting the path.
				// But it is not "stdout" or "stderr" either, and the
				// allowlist must refuse it like any other garbage value.
				secretPath,
				"weird",
			}
			for _, stream := range payloads {
				u := ts.url("/api/logs/build?attempt=1&stream=" + url.QueryEscape(stream))
				resp, err := ts.client.Get(u)
				if err != nil {
					t.Fatalf("GET %s: %v", u, err)
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()

				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("stream=%q: status = %d, want 400", stream, resp.StatusCode)
				}
				if strings.Contains(string(body), "TOP-SECRET") {
					t.Fatalf("stream=%q: response body contains the secret file's contents: %q", stream, body)
				}
			}
		})
	}
}

// Validating "stream" alone is not enough: stepid.Encode is
// url.PathEscape, which leaves "." untouched, so a step id decoding to
// ".." reaches LogSet.Path as a literal ".." element and Clean cancels the
// "logs" segment LogSet.Path itself adds, landing one directory above
// logs/. Confined rather than a full escape, but the same bug shape as the
// stream case: filepath.Join(runDir, "logs", stepid.Encode(".."), "1",
// "stdout") == filepath.Join(runDir, "1", "stdout").
func TestLogsRejectsATraversalViaTheStepSegment(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	// Exactly where step="%2e%2e" (i.e. "..") resolves to: one level above
	// logs/, inside the run directory.
	outside := filepath.Join(ts.dir, "1", "stdout")
	if err := os.MkdirAll(filepath.Dir(outside), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(outside, []byte("outside-the-log-tree"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	resp, err := ts.client.Get(ts.url("/api/logs/%2e%2e?attempt=1&stream=stdout"))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK {
		t.Fatalf("status = 200 (body: %q) — the resolved path escaped the logs/ tree", body)
	}
	if strings.Contains(string(body), "outside-the-log-tree") {
		t.Fatalf("response body contains content from outside logs/: %q", body)
	}
}

// r.PathValue("step") is already fully percent-decoded, so handleLogs must
// not decode it again. plan.Validate imposes no character restrictions on
// node IDs, so a step ID containing a literal "%" is one plan away, and a
// second decode breaks two ways: an invalid escape errors outright (this
// test), and a literal "%2F" would silently become an unrelated step with
// a real "/".
func TestLogsHandlesAStepIDContainingAPercent(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	const step = "build/coverage[pct=90%]"
	writeLogFile(t, ts.dir, step, 1, api.StreamStdout, "percent-step-output")

	encoded := stepid.Encode(step)
	resp, err := ts.client.Get(ts.url("/api/logs/" + encoded + "?attempt=1&stream=stdout"))
	if err != nil {
		t.Fatalf("GET /api/logs/%s: %v", encoded, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s) — a step id containing \"%%\" must not error as an invalid URL escape", resp.StatusCode, body)
	}
	if string(body) != "percent-step-output" {
		t.Errorf("body = %q, want %q", body, "percent-step-output")
	}
}

func TestStreamDeliversFromRequestedSeqInOrder(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	events := twoStepEvents()
	for _, e := range events[:3] { // seq 1..3 already on the hub
		ts.hub.Emit(e)
	}

	resp, err := ts.client.Get(ts.url("/api/stream?from=2"))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "ndjson") {
		t.Errorf("Content-Type = %q, want an ndjson content type", ct)
	}

	stream := streamNDJSON(t, resp.Body)

	// Replay: seq 2 then 3, in order, never 1 (that's what from=2 excludes).
	e2 := recvEvent(t, stream, 3*time.Second)
	e3 := recvEvent(t, stream, 3*time.Second)
	if e2.Seq != 2 || e3.Seq != 3 {
		t.Fatalf("replay = [%d %d], want [2 3]", e2.Seq, e3.Seq)
	}

	// Live delivery continues in order past the replay.
	ts.hub.Emit(events[3]) // seq 4
	ts.hub.Emit(events[4]) // seq 5
	e4 := recvEvent(t, stream, 3*time.Second)
	e5 := recvEvent(t, stream, 3*time.Second)
	if e4.Seq != 4 || e5.Seq != 5 {
		t.Fatalf("live delivery = [%d %d], want [4 5]", e4.Seq, e5.Seq)
	}
}

// The hub's own ring guarantees events are never silently skipped: a
// subscriber either gets everything from fromSeq forward, or an error. This
// pins how the SERVER surfaces that error over HTTP: the requested range is
// gone for good (410), not a 200 with a truncated or renumbered stream, and
// the body names the remedy (re-snapshot via /api/state and resubscribe
// from state.seq+1) rather than leaving the client to guess.
func TestStreamRespondsGoneWhenFromSeqHasBeenEvicted(t *testing.T) {
	ts := newTestServer(t, testServerOpts{RingSize: 8})
	for i := 1; i <= 100; i++ {
		ts.hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	// Ring size 8: only seq 93..100 are still retained.

	resp, err := ts.client.Get(ts.url("/api/stream?from=1"))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusGone {
		t.Fatalf("status = %d, want %d (Gone)", resp.StatusCode, http.StatusGone)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(b), "lifecycle_overflow") {
		t.Errorf("body = %q, want it to name lifecycle_overflow so the client knows what happened", b)
	}
	if !strings.Contains(string(b), "state") {
		t.Errorf("body = %q, want it to point the client at /api/state as the remedy", b)
	}

	// The one pairing that must NEVER overflow: State().Seq+1.
	st := ts.hub.State()
	resp2, err := ts.client.Get(ts.url(fmt.Sprintf("/api/stream?from=%d", st.Seq+1)))
	if err != nil {
		t.Fatalf("GET /api/stream (state.seq+1): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status for the documented resume pairing = %d, want 200 (this pairing must never overflow)", resp2.StatusCode)
	}
}

// A client subscribes, disconnects, and resubscribes from last+1. It must
// see no gap (everything emitted while it was away) and no repeat (nothing
// it already saw, redelivered).
func TestResubscribeFromLastPlusOneSeesNoGapNoRepeat(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	for i := 1; i <= 5; i++ {
		ts.hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: fmt.Sprintf("s%d", i)})
	}

	resp1, err := ts.client.Get(ts.url("/api/stream?from=1"))
	if err != nil {
		t.Fatalf("GET /api/stream (first): %v", err)
	}
	stream1 := streamNDJSON(t, resp1.Body)

	var last uint64
	for i := 0; i < 3; i++ {
		e := recvEvent(t, stream1, 3*time.Second)
		if e.Seq != last+1 {
			t.Fatalf("first connection: got seq %d, want %d", e.Seq, last+1)
		}
		last = e.Seq
	}
	if err := resp1.Body.Close(); err != nil {
		t.Fatalf("close first stream: %v", err)
	}

	// Emit more while disconnected: this is exactly what a gap would lose.
	ts.hub.Emit(api.Event{V: 1, Seq: 6, Type: api.StepCreated, Step: "s6"})
	ts.hub.Emit(api.Event{V: 1, Seq: 7, Type: api.StepCreated, Step: "s7"})

	resp2, err := ts.client.Get(ts.url(fmt.Sprintf("/api/stream?from=%d", last+1)))
	if err != nil {
		t.Fatalf("GET /api/stream (resubscribe): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("resubscribe status = %d, want 200", resp2.StatusCode)
	}
	stream2 := streamNDJSON(t, resp2.Body)

	want := []uint64{4, 5, 6, 7}
	var got []uint64
	for range want {
		e := recvEvent(t, stream2, 3*time.Second)
		got = append(got, e.Seq)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v — gap or repeat at index %d", got, want, i)
		}
	}
}

func TestReadOnlyServerRejectsControl(t *testing.T) {
	ts := newTestServer(t, testServerOpts{ReadOnly: true})

	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}

	var res api.Frame
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode response frame: %v", err)
	}
	if res.Kind != api.KindRes {
		t.Errorf("Kind = %q, want %q", res.Kind, api.KindRes)
	}
	if res.ID != "c1" {
		t.Errorf("ID = %q, want %q (the response must correlate to the request)", res.ID, "c1")
	}
	if res.OK == nil || *res.OK {
		t.Errorf("OK = %v, want a non-nil false", res.OK)
	}
	if res.Error == "" {
		t.Error("Error is empty, want a reason a client could show a user")
	}
}

// Without a body-size bound, the read-only 403 path echoes req.ID verbatim
// into its own body, making an unbounded request a same-size reflection on
// the one endpoint meant to be inert. This sends a frame whose ID alone
// dwarfs any legitimate one, against a ReadOnly server specifically.
//
// A non-read-only server would not pin anything: with no consumer wired to
// Hub.Control() here, the request would sit until controlTimeout and
// return a small 503 whatever its size.
func TestControlRejectsAnOversizedBody(t *testing.T) {
	ts := newTestServer(t, testServerOpts{ReadOnly: true})

	huge := strings.Repeat("x", 2<<20) // 2MiB, far past maxControlBodyBytes
	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: huge, Type: api.OpRunCancel}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if len(body) > len(huge) {
		t.Fatalf("response body is %d bytes — as large as (or larger than) the oversized request, want it refused well before that", len(body))
	}
	if resp.StatusCode == http.StatusForbidden {
		t.Errorf("status = 403, want the request refused for its size before ever reaching the read-only check")
	}
}

// The engine's own scheduler (internal/engine/control.go) is the ordinary
// consumer of hub.Control() in production, wired to it separately from
// this transport layer. This test stands in for that consumer (read one
// request, reply) to prove the server's HTTP<->hub.Control() round trip
// actually works end to end, not just that a read-only server refuses to
// attempt it.
func TestControlIsForwardedAndTheResponseIsCorrelated(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	// gotOp captures what the hub-side consumer actually received, so the
	// test can assert on it after the HTTP round trip completes: reading
	// req.Op directly in the main goroutine below (rather than through
	// this channel) would race the goroutine that received it.
	gotOp := make(chan string, 1)
	go func() {
		req := <-ts.hub.Control()
		gotOp <- req.Op
		req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
	}()

	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "c2", Type: api.OpStepRetry}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body: %s)", resp.StatusCode, body)
	}
	var res api.Frame
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode response frame: %v", err)
	}

	// This is the part of control this handler owns: the Frame.Type ->
	// ControlRequest.Op mapping. Op-specific semantics (what run.cancel /
	// step.retry should DO) are out of scope; this only confirms the op
	// name itself survives the HTTP -> hub.Control() hop unchanged.
	select {
	case op := <-gotOp:
		if op != api.OpStepRetry {
			t.Errorf("hub.Control() received Op = %q, want %q — the Frame.Type -> ControlRequest.Op mapping is wrong", op, api.OpStepRetry)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the hub-side consumer to report the op it received")
	}

	if res.ID != "c2" {
		t.Errorf("ID = %q, want %q", res.ID, "c2")
	}
	if res.OK == nil || !*res.OK {
		t.Errorf("OK = %v, want a non-nil true", res.OK)
	}
}

// --- the Hub.Done() precheck ---

// The HTTP-layer half of the finished-run refusal (the engine-side durable
// refusal is internal/engine/control_test.go's): once the hub has folded
// run.finished, handleControl must answer immediately with a JSON
// api.Frame carrying sink.ReasonRunFinished and WITHOUT sending on the
// control channel, which nothing will ever read again. Proven directly: a
// consumer goroutine started before the request must receive nothing.
func TestControlAfterTheRunHasFinishedIsRefusedWithoutTouchingTheChannel(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	for _, e := range twoStepEvents() { // ends with run.finished; see its own doc
		ts.hub.Emit(e)
	}

	gotSomething := make(chan sink.ControlRequest, 1)
	go func() {
		req := <-ts.hub.Control()
		gotSomething <- req
	}()

	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "late1", Type: api.OpRunCancel}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}

	start := time.Now()
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body: %s)", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json — a refusal after the run finished must be a real Frame, not plain text", ct)
	}
	var res api.Frame
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("response is not a decodable api.Frame: %v", err)
	}
	if res.OK == nil || *res.OK {
		t.Errorf("OK = %v, want a non-nil false", res.OK)
	}
	if res.Error != sink.ReasonRunFinished {
		t.Errorf("Error = %q, want %q", res.Error, sink.ReasonRunFinished)
	}
	if res.ID != "late1" {
		t.Errorf("ID = %q, want %q", res.ID, "late1")
	}
	if elapsed > time.Second {
		t.Errorf("took %s to answer, want well under 1s — a slow answer means this went through the channel instead of the precheck", elapsed)
	}

	select {
	case req := <-gotSomething:
		t.Errorf("the hub's control channel received %+v — a request submitted after the run finished must never reach it", req)
	case <-time.After(200 * time.Millisecond):
		// Nothing arrived: the precheck answered without ever touching the
		// channel, exactly as required.
	}
}

// A control.applied event is permanent and broadcast, so its Args must
// never carry more than the op recognises (see controlArgAllowlist):
// run.cancel takes none, step.retry exactly "step". Both are refused at
// the HTTP layer, before the engine and so before the ledger;
// internal/engine/control_test.go covers the second, independent layer.
func TestControlRejectsAnUnrecognisedArgument(t *testing.T) {
	cases := []struct {
		name    string
		op      string
		payload map[string]string
	}{
		{"run.cancel with any argument at all", api.OpRunCancel, map[string]string{"reason": "operator request"}},
		{"run.cancel with a secret-shaped key", api.OpRunCancel, map[string]string{"aws_secret_access_key": "AKIAFAKEFAKEFAKEFAKE"}},
		{"step.retry with an extra key alongside step", api.OpStepRetry, map[string]string{"step": "build", "extra": "unexpected"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, testServerOpts{})

			// No consumer at all on ts.hub.Control(): if the request were
			// ever forwarded, POST would hang until controlTimeout and this
			// test would time out: itself a signal the refusal did not
			// happen at the HTTP layer as required.
			payload, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}
			reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "x", Type: tc.op, Payload: payload}
			b, err := json.Marshal(reqFrame)
			if err != nil {
				t.Fatalf("marshal request frame: %v", err)
			}
			resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
			if err != nil {
				t.Fatalf("POST /api/control: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 400 (body: %s)", resp.StatusCode, body)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			for k := range tc.payload {
				if strings.Contains(string(body), k) {
					t.Errorf("response body echoes the argument key %q — refusals must not reflect client-supplied content", k)
				}
			}
		})
	}
}

// maxControlArgsBytes: even well under maxControlBodyBytes, a payload
// carrying more than a step ID has no legitimate use, and an unbounded
// Args map is a per-request disk-exhaustion path into the permanent ledger
// via control.applied. Refused on size alone, before json.Unmarshal.
func TestControlRejectsAnOversizedArgsPayload(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	huge := map[string]string{"step": strings.Repeat("x", 2048)} // > maxControlArgsBytes once marshalled
	payload, err := json.Marshal(huge)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "x", Type: api.OpStepRetry, Payload: payload}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400 (body: %s)", resp.StatusCode, body)
	}
}

// handleControl must populate ControlRequest.ClientID from the accepted
// connection: two requests over the SAME http.Client (which reuses one
// keep-alive connection) carry the same id, a separate client a different
// one. Without it, control.applied names no one.
func TestControlForwardsClientIDDerivedFromTheConnection(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	gotID := make(chan string, 8)
	serve := func(n int) {
		for i := 0; i < n; i++ {
			req := <-ts.hub.Control()
			gotID <- req.ClientID
			req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
		}
	}
	go serve(3)

	post := func(c *http.Client) string {
		t.Helper()
		reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "x", Type: api.OpRunCancel}
		b, err := json.Marshal(reqFrame)
		if err != nil {
			t.Fatalf("marshal request frame: %v", err)
		}
		resp, err := c.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("POST /api/control: %v", err)
		}
		// Fully drain the body before Close, not merely Close it:
		// net/http's Transport only reuses a connection whose body was read
		// to EOF, and reuse is the very thing "same client, sequential
		// requests" is here to prove.
		_, _ = io.Copy(io.Discard, resp.Body)
		defer func() { _ = resp.Body.Close() }()
		select {
		case id := <-gotID:
			return id
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for the hub-side consumer to report the ClientID it received")
			return ""
		}
	}

	first := post(ts.client)
	second := post(ts.client) // same client: sequential requests reuse one idle connection
	if first == "" {
		t.Error("ClientID is empty, want a non-empty value derived from the connection")
	}
	if first != second {
		t.Errorf("ClientID changed across two sequential requests on the same connection: %q then %q", first, second)
	}

	otherTr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", ts.sockPath)
		},
	}
	defer otherTr.CloseIdleConnections()
	other := &http.Client{Transport: otherTr}
	third := post(other)
	if third == first {
		t.Errorf("ClientID = %q for a distinct connection, want it to differ from the first connection's %q", third, first)
	}
}

// TestControlForwardsStepArgsFromPayload pins the Frame.Payload ->
// ControlRequest.Args decoding step.retry needs to name which step to retry.
// Without it, the engine has no way to learn which step a step.retry request
// is even about: Frame carries no dedicated field for that; it travels as
// {"step": "..."} in the request frame's payload.
func TestControlForwardsStepArgsFromPayload(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	gotArgs := make(chan map[string]string, 1)
	go func() {
		req := <-ts.hub.Control()
		gotArgs <- req.Args
		req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
	}()

	payload, err := json.Marshal(map[string]string{"step": "build"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "r1", Type: api.OpStepRetry, Payload: payload}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	select {
	case args := <-gotArgs:
		if args["step"] != "build" {
			t.Errorf("Args[%q] = %q, want %q", "step", args["step"], "build")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the hub-side consumer to report the Args it received")
	}
}

// TestReadOnlyServerRejectsStepRetryToo extends
// TestReadOnlyServerRejectsControl's coverage: Options.ReadOnly must refuse
// EVERY control op, not merely run.cancel. A ReadOnly server must never
// forward step.retry to Hub.Control() either.
func TestReadOnlyServerRejectsStepRetryToo(t *testing.T) {
	ts := newTestServer(t, testServerOpts{ReadOnly: true})

	reqFrame := api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry}
	b, err := json.Marshal(reqFrame)
	if err != nil {
		t.Fatalf("marshal request frame: %v", err)
	}
	resp, err := ts.client.Post(ts.url("/api/control"), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	var res api.Frame
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode response frame: %v", err)
	}
	if res.OK == nil || *res.OK {
		t.Errorf("OK = %v, want a non-nil false", res.OK)
	}
}

func TestSocketModeIs0600(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	info, err := os.Stat(ts.sockPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket mode = %v, want 0600", perm)
	}
}

// A slow client must not hold a hub subscription, or its goroutine, open
// forever. Emit's own non-blocking guarantee is proven in hub_test.go;
// what is specific here is that a handler blocked inside Write to a wedged
// connection cannot be interrupted by a channel select, only by a write
// deadline on the connection.
//
// This test deliberately does NOT call Server.Close(), which force-closes
// every connection regardless of deadline and would prove nothing. It
// emits one huge event to a client that never reads (so the Write blocks
// on the OS rather than queuing in a channel), sleeps past
// streamWriteTimeout, and only then drains the response.
func TestWedgedStreamClientIsClosedByItsWriteDeadlineAlone(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})

	resp, err := ts.client.Get(ts.url("/api/stream?from=1"))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	huge := strings.Repeat("x", 32<<20) // 32MiB: far beyond the OS socket buffer either end has
	ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: huge})

	// Deliberately never read resp.Body until after the deadline (3s, per
	// server.go's streamWriteTimeout) must already have fired: reading
	// any earlier would just drain the buffered prefix and let the write
	// complete normally, proving nothing about the deadline at all.
	time.Sleep(4 * time.Second)

	drainDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		drainDone <- err
	}()

	select {
	case err := <-drainDone:
		if err == nil {
			t.Error("drain returned no error — want the connection to have been closed server-side once the write deadline fired")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("draining the response never finished — the connection is still open 9s after the huge event, so nothing closed it: the write deadline is not doing its job")
	}
}

// Closing the underlying hub must end the stream (the body reaches EOF)
// rather than leaving the connection open with nothing left to send.
// testStreamEndMarker mirrors server.go's unexported streamEndMarker JSON
// shape so an external test can decode it.
type testStreamEndMarker struct {
	StreamEnd  bool   `json:"stream_end"`
	LastSeq    uint64 `json:"last_seq"`
	Overflowed bool   `json:"overflowed"`
	Reason     string `json:"reason"`
	Hint       string `json:"hint"`
}

// decodeFirstThenRest reads exactly one api.Event (bounded by
// firstTimeout), invokes between (whatever the test needs to happen once
// the first delivery is confirmed), reads one more JSON value (the
// terminal marker, bounded by restTimeout), and confirms nothing follows.
// One json.Decoder across all three phases, each phase's goroutine handed
// off over a channel before the next touches it: sequential, not
// concurrent, use, and no arbitrary sleep.
func decodeFirstThenRest(t *testing.T, r io.Reader, firstTimeout time.Duration, between func(), restTimeout time.Duration) (api.Event, testStreamEndMarker) {
	t.Helper()
	dec := json.NewDecoder(r)

	type firstResult struct {
		e   api.Event
		err error
	}
	firstCh := make(chan firstResult, 1)
	go func() {
		var e api.Event
		err := dec.Decode(&e)
		firstCh <- firstResult{e, err}
	}()
	var first firstResult
	select {
	case first = <-firstCh:
	case <-time.After(firstTimeout):
		t.Fatalf("timed out after %v waiting for the first event", firstTimeout)
	}
	if first.err != nil {
		t.Fatalf("decode first event: %v", first.err)
	}

	between()

	// Any number of ordinary events may legitimately arrive before the
	// terminal marker: how many depends on the hub's ring/headroom sizing
	// and how fast between()'s burst outran this connection's reads, not
	// on this helper. Keep decoding raw values, identifying the marker by
	// its own "stream_end" field (which no api.Event carries), and
	// discarding ordinary events until it turns up or the timeout fires.
	markerCh := make(chan testStreamEndMarker, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				errCh <- err
				return
			}
			var probe struct {
				StreamEnd bool `json:"stream_end"`
			}
			if err := json.Unmarshal(raw, &probe); err == nil && probe.StreamEnd {
				var marker testStreamEndMarker
				if err := json.Unmarshal(raw, &marker); err != nil {
					errCh <- fmt.Errorf("terminal marker does not decode: %w (%s)", err, raw)
					return
				}
				markerCh <- marker
				return
			}
			// An ordinary event: keep going.
		}
	}()

	var marker testStreamEndMarker
	select {
	case marker = <-markerCh:
	case err := <-errCh:
		t.Fatalf("reading toward the terminal marker: %v", err)
	case <-time.After(restTimeout):
		t.Fatalf("timed out after %v waiting for the terminal marker — the stream is hanging", restTimeout)
	}

	var extra json.RawMessage
	if err := dec.Decode(&extra); err == nil {
		t.Fatalf("unexpected content after the terminal marker: %s", extra)
	}

	return first.e, marker
}

// A hub-side subscriber drop and a clean Hub.Close() are byte-identical on
// the wire: both end the response with a clean chunked terminator, leaving
// a `for e := range ch` reader with a truncated fold and no signal.
// handleStream writes one terminal streamEndMarker on that branch. This
// covers the clean-close half (Overflowed false, nothing missed), and by
// waiting for the marker it also confirms the stream ends rather than
// hanging.
func TestStreamWritesATerminalMarkerWhenTheHubClosesCleanly(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	resp, err := ts.client.Get(ts.url("/api/stream?from=1"))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	e, marker := decodeFirstThenRest(t, resp.Body, 3*time.Second, func() {
		if err := ts.hub.Close(); err != nil {
			t.Fatalf("hub.Close: %v", err)
		}
	}, 5*time.Second)

	if e.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", e.Seq)
	}
	if !marker.StreamEnd {
		t.Error("StreamEnd = false, want true")
	}
	if marker.LastSeq != 1 {
		t.Errorf("LastSeq = %d, want 1", marker.LastSeq)
	}
	if marker.Overflowed {
		t.Error("Overflowed = true, want false — the hub closed cleanly, nothing was missed")
	}
	if marker.Reason != "run_ended" {
		t.Errorf("Reason = %q, want %q", marker.Reason, "run_ended")
	}
	if marker.Hint == "" {
		t.Error("Hint is empty, want the resume remedy")
	}
}

// Pins the ordering attach.Attach.Close relies on: hub.Close() before
// srv.Close(). If Close() closed s.done immediately, that case could
// become ready alongside an ALREADY-closed hub channel, and select breaks
// ties uniformly at random, so the informative run_ended marker would lose
// roughly one trial in four. Close() instead gives such a handler a
// bounded chance to write its marker first, and handleStream rechecks ch
// as a second line of defence.
//
// hub.Close() then srv.Close() back to back is exactly Attach.Close's own
// sequence; a delay would not exercise the race. Several concurrent
// connections per trial supply the scheduling pressure a single idle one
// would not.
func TestStreamMarkerSurvivesACloseRaceAgainstAnAlreadyClosedHub(t *testing.T) {
	const trials = 15
	const connsPerTrial = 8
	for trial := 0; trial < trials; trial++ {
		ts := newTestServer(t, testServerOpts{})
		ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

		bodies := make([]io.ReadCloser, connsPerTrial)
		for i := range bodies {
			resp, err := ts.client.Get(ts.url("/api/stream?from=1"))
			if err != nil {
				t.Fatalf("trial %d conn %d: GET /api/stream: %v", trial, i, err)
			}
			bodies[i] = resp.Body
		}

		markers := make([]testStreamEndMarker, connsPerTrial)
		var wg sync.WaitGroup
		var closeOnce sync.Once
		closeHubThenServer := func() {
			closeOnce.Do(func() {
				if err := ts.hub.Close(); err != nil {
					t.Errorf("trial %d: hub.Close: %v", trial, err)
				}
				if err := ts.srv.Close(); err != nil {
					t.Errorf("trial %d: srv.Close: %v", trial, err)
				}
			})
		}
		for i, body := range bodies {
			wg.Add(1)
			go func(i int, body io.ReadCloser) {
				defer wg.Done()
				// decodeFirstThenRest Fatalf's if the marker never arrives,
				// which is the regression guarded against: a markerless
				// close reads as a bare EOF here, exactly what a real
				// client sees. Every between() races to run
				// closeHubThenServer; closeOnce makes exactly one do it,
				// matching one real Attach.Close().
				_, marker := decodeFirstThenRest(t, body, 3*time.Second, closeHubThenServer, 3*time.Second)
				markers[i] = marker
				_ = body.Close()
			}(i, body)
		}
		wg.Wait()

		for i, marker := range markers {
			if marker.Reason != "run_ended" {
				t.Errorf("trial %d conn %d: Reason = %q, want %q", trial, i, marker.Reason, "run_ended")
			}
		}
	}
}

// The other overflow case: when the hub drops THIS subscriber for falling
// behind while it keeps running for everyone else, Overflowed must be
// true. Reuses hub_test.go's shape for forcing that branch: a small ring,
// and a burst emitted faster than this connection's one-at-a-time reads,
// so Emit's non-blocking send hits the full-channel branch.
func TestStreamWritesATerminalMarkerWithOverflowedWhenTheHubDropsTheSubscriber(t *testing.T) {
	ts := newTestServer(t, testServerOpts{RingSize: 8})
	ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	resp, err := ts.client.Get(ts.url("/api/stream?from=1"))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	e, marker := decodeFirstThenRest(t, resp.Body, 3*time.Second, func() {
		for i := 2; i <= 5000; i++ {
			ts.hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
		}
	}, 10*time.Second)

	if e.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", e.Seq)
	}
	if ts.hub.Dropped() == 0 {
		t.Fatal("test setup did not actually trigger a hub-side drop — Dropped() = 0, this proves nothing about the overflowed branch")
	}
	if !marker.StreamEnd {
		t.Error("StreamEnd = false, want true")
	}
	if !marker.Overflowed {
		t.Errorf("Overflowed = false, want true — Dropped()=%d confirms the hub did disconnect this subscriber", ts.hub.Dropped())
	}
	if marker.Reason != "overflowed" {
		t.Errorf("Reason = %q, want %q", marker.Reason, "overflowed")
	}
}

// A client that merely pauses must not be told "the run ended". A write
// timeout on a REGULAR event (case 3) ends the connection with no marker,
// byte-identical to a dead client.
//
// No "write_stalled" marker is asserted: case 3 never attempts one, since
// net/http tears the connection down synchronously on any write error, so
// such a write could never reach a client.
//
// What is proven: a connection that stops draining still ends within a
// bounded time, and the hub-side ring is untouched (Dropped() stays 0,
// confirming a connection-level stall rather than the hub's overflow
// guard). The caller-visible guarantee is proven at the internal/source
// level instead (TestFallbackSurvivesAWriteStallWithoutFallingBack).
func TestStreamEndsWithoutHangingWhenTheClientStopsDraining(t *testing.T) {
	ts := newTestServer(t, testServerOpts{RingSize: 30000})
	ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	resp, err := ts.client.Get(ts.url("/api/stream?from=1"))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	dec := json.NewDecoder(resp.Body)
	var first api.Event
	if err := dec.Decode(&first); err != nil {
		t.Fatalf("decode first event: %v", err)
	}
	if first.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", first.Seq)
	}

	// Deliberately does not read resp.Body again: nothing drains the
	// connection, which is what forces a write to block past
	// streamWriteTimeout rather than merely queue. RingSize is large so
	// this lands on case 3 rather than the hub's overflow guard, which is
	// what Dropped()==0 below confirms.
	const burst = 20000
	for i := 2; i <= burst; i++ {
		ts.hub.Emit(api.Event{
			V: 1, Seq: uint64(i), Type: api.StepCreated,
			Step: fmt.Sprintf("padding-step-%020d", i),
		})
	}
	if ts.hub.Dropped() != 0 {
		t.Fatalf("hub dropped %d — the ring/subscriber channel was too small and this landed on the overflow path, not a connection-level stall; the test setup, not the fix, is what failed", ts.hub.Dropped())
	}

	// The burst above completes well under streamWriteTimeout, and
	// draining immediately after would likely keep up. This pause forces
	// the stall: nothing reads for longer than streamWriteTimeout, so the
	// write in flight once the socket buffer fills genuinely blocks.
	time.Sleep(4 * time.Second)

	done := make(chan error, 1)
	go func() {
		var raw json.RawMessage
		for {
			if err := dec.Decode(&raw); err != nil {
				done <- err
				return
			}
		}
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("stream ended with no error at all — want the connection to have actually ended, not merely stopped producing")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the stream never ended — case 3's extra write attempt appears to have hung the handler rather than returning")
	}
}

// The "attach after the run already ended" case: a client snapshots at seq
// 8, subscribes from state.Seq+1 (nothing to deliver yet, by Subscribe's
// "wait for the future" contract), and the hub closes before emitting
// more, leaving LastSeq 0. A naive hub.Seq() > lastSeq cannot tell this
// from "fell behind at seq 0", since 0 means both. This subscriber got
// exactly what it asked for, so Overflowed must be false.
func TestStreamOverflowedIsFalseWhenNothingWasEverDelivered(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	for i := 1; i <= 8; i++ {
		ts.hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	st := ts.hub.State()
	if st.Seq != 8 {
		t.Fatalf("setup: hub seq = %d, want 8", st.Seq)
	}

	resp, err := ts.client.Get(ts.url(fmt.Sprintf("/api/stream?from=%d", st.Seq+1)))
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body: %s)", resp.StatusCode, body)
	}

	if err := ts.hub.Close(); err != nil {
		t.Fatalf("hub.Close: %v", err)
	}

	type result struct {
		marker testStreamEndMarker
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		var raw json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			resultCh <- result{err: err}
			return
		}
		var marker testStreamEndMarker
		if err := json.Unmarshal(raw, &marker); err != nil {
			resultCh <- result{err: err}
			return
		}
		resultCh <- result{marker: marker}
	}()

	var r result
	select {
	case r = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the terminal marker — the stream is hanging")
	}
	if r.err != nil {
		t.Fatalf("decode terminal marker: %v", r.err)
	}

	if !r.marker.StreamEnd {
		t.Error("StreamEnd = false, want true")
	}
	if r.marker.LastSeq != 0 {
		t.Errorf("LastSeq = %d, want 0 (nothing was ever delivered to this connection)", r.marker.LastSeq)
	}
	if r.marker.Overflowed {
		t.Error("Overflowed = true, want false — this subscriber asked for the future (from=state.seq+1) and the hub simply never reached it before closing; nothing was actually missed")
	}
}

// Calling srv.Close() right after cancel() and asserting on its return
// would prove nothing: that passes whether or not ctx is wired to
// anything, because Close() alone does 100% of the shutdown work. This
// test never calls Close() anywhere in the test body (only ctx
// cancellation can end it) and polls the raw socket to prove the listener
// actually goes away.
func TestListenShutsDownWhenContextIsCancelled(t *testing.T) {
	dir := t.TempDir()
	sockPath := shortSocketPath(t)
	hub := attachsrv.NewHub(64)

	ctx, cancel := context.WithCancel(context.Background())
	srv, err := attachsrv.Listen(ctx, attachsrv.Options{Bind: sockPath, Dir: dir, Hub: hub})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	// Defensive only, registered for test cleanup, not part of the
	// assertion: Close is idempotent, so this is harmless whether or not
	// ctx cancellation already tore the server down by the time this runs.
	t.Cleanup(func() { _ = srv.Close() })

	// Confirm the server actually accepts a connection before cancelling:
	// otherwise a socket that failed to start listening in the first place
	// would make the later assertion pass for the wrong reason.
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial before cancel: %v", err)
	}
	_ = conn.Close()

	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("unix", sockPath)
		if err != nil {
			return // the listener is gone: ctx cancellation did its job, unassisted by any Close() call in this test
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("the socket is still accepting connections 5s after ctx was cancelled, and this test never calls Close() — Listen is not honouring ctx")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The listen-time half of the stale-socket fix (the registry-reaping half
// is TestDiscoverReapsTheDeadEntrysSocketFileToo). Reaping only helps a
// process that calls Discover before binding, and attach.Listen never does
// (resolveBind derives <pid>.sock directly), so a recycled pid must not
// fail Listen with a raw "address already in use".
//
// SetUnlinkOnClose(false) is what makes a UnixListener's Close leave the
// socket file behind: the behaviour a graceful shutdown gets for free and
// a hard exit never runs at all. Nothing listens on it by the time the
// test proper starts.
func TestListenRemovesAStaleSocketFromADeadProcessBeforeBinding(t *testing.T) {
	sock := shortSocketPath(t)

	orphan, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("bind throwaway listener: %v", err)
	}
	orphan.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := orphan.Close(); err != nil {
		t.Fatalf("close throwaway listener: %v", err)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("orphaned socket file missing before the real test even starts: %v", err)
	}

	hub := attachsrv.NewHub(4)
	defer func() { _ = hub.Close() }()

	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: sock, Dir: t.TempDir(), Hub: hub,
	})
	if err != nil {
		t.Fatalf("Listen over a stale socket from a dead process: %v — want it to remove the orphan and bind successfully", err)
	}
	defer func() { _ = srv.Close() }()

	if srv.Addr() != sock {
		t.Errorf("Addr() = %q, want %q", srv.Addr(), sock)
	}

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial the new listener: %v", err)
	}
	_ = conn.Close()
}

// TestListenDoesNotStealASocketFromALiveListener is the stale-socket fix's
// own safety boundary: a path with something genuinely listening on it
// (the ordinary "someone else is already using this address" case
// net.Listen's own error exists to report) must be left alone, not
// silently repossessed.
func TestListenDoesNotStealASocketFromALiveListener(t *testing.T) {
	sock := shortSocketPath(t)

	live, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("bind the live listener: %v", err)
	}
	defer func() { _ = live.Close() }()

	hub := attachsrv.NewHub(4)
	defer func() { _ = hub.Close() }()

	_, err = attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: sock, Dir: t.TempDir(), Hub: hub,
	})
	if err == nil {
		t.Fatal("Listen() over an address with a genuinely live listener succeeded, want a refusal")
	}
}

// A handler calling s.wg.Add(1) directly has no ordering against a
// concurrent Wait inside Close, which sync.WaitGroup's contract forbids.
// This launches many concurrent /api/stream attempts against a server
// Close is tearing down, over many trials so the narrow window is hit.
// Under -race, the unguarded version aborts the binary with "WaitGroup
// misuse", a stronger signal than any assertion here.
// streamHeaderBudget bounds how long a stream request below waits for
// response headers. Reaching it is NOT a failure: a client's connect() on
// a unix socket succeeds as soon as the kernel has backlog room, before
// anything calls Accept, so a listener closing afterwards strands a
// connection no code here has ever seen and none of it can answer.
//
// The defence is on the client and it ships: source.Dial sets
// ResponseHeaderTimeout (see
// TestDialDoesNotHangForeverOnAServerThatNeverAnswers). This bound only
// stops a stranded dial taking the package's whole time budget with it.
//
// The race this test exists for is unaffected: it is caught by the -race
// detector aborting on WaitGroup misuse, not by any assertion here.
const streamHeaderBudget = 20 * time.Second

func TestCloseDoesNotRaceConcurrentStreamHandlers(t *testing.T) {
	for trial := 0; trial < 300; trial++ {
		ts := newTestServer(t, testServerOpts{})

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Per-request deadline, not a Timeout on ts.client, which
				// is shared with tests that hold a stream body open on
				// purpose. It bounds a dial stranded before Accept (see
				// streamHeaderBudget), which would otherwise take the
				// package's whole time budget and report nothing useful.
				ctx, cancel := context.WithTimeout(context.Background(), streamHeaderBudget)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.url("/api/stream?from=1"), nil)
				if err != nil {
					t.Errorf("trial %d: NewRequest: %v", trial, err)
					return
				}
				resp, err := ts.client.Do(req)
				if err != nil {
					// Every error here is expected once Close wins the
					// race for a given attempt: refused by track(), the
					// connection torn down, or the dial stranded in a
					// backlog the listener closed under it (see
					// streamHeaderBudget). None of them is this test's
					// subject.
					return
				}
				_ = resp.Body.Close()
			}()
		}

		// A short pause: long enough for the goroutines above to reach the
		// OS before Close runs, short enough that most are still in
		// flight. With no pause at all, freshly spawned goroutines have
		// not reached the network stack by the time Close completes, so
		// track()'s Add is never concurrent with anything.
		//
		// 300 trials, not 30: at 30, only about half of fresh
		// `go test -race` runs catch the race against unguarded code. The
		// extra trials cost well under half a second.
		time.Sleep(200 * time.Microsecond)
		if err := ts.srv.Close(); err != nil {
			t.Errorf("trial %d: Close: %v", trial, err)
		}
		wg.Wait()
	}
}

// --- helpers ---

type testServerOpts struct {
	ReadOnly bool
	RingSize int
}

type testServer struct {
	srv      *attachsrv.Server
	hub      *attachsrv.Hub
	dir      string
	sockPath string
	client   *http.Client
}

func (ts *testServer) url(path string) string { return "http://unix" + path }

// newTestServer stands up a real Server over a real unix socket and an
// http.Client dialing it, both cleaned up automatically.
//
// The run directory uses t.TempDir(), but the SOCKET path deliberately
// does not: t.TempDir() nests the test's full name into the path, and a
// unix socket path is limited to roughly 104 bytes on darwin, ~50 of which
// os.TempDir() already consumes. shortSocketPath uses os.MkdirTemp with a
// short fixed prefix and its own t.Cleanup instead.
func newTestServer(t *testing.T, opts testServerOpts) *testServer {
	t.Helper()

	dir := t.TempDir()
	sockPath := shortSocketPath(t)

	ringSize := opts.RingSize
	if ringSize == 0 {
		ringSize = 64
	}
	hub := attachsrv.NewHub(ringSize)

	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind:     sockPath,
		Dir:      dir,
		Hub:      hub,
		ReadOnly: opts.ReadOnly,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sockPath)
		},
	}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr} // no blanket Timeout: streaming tests hold the body open deliberately

	return &testServer{srv: srv, hub: hub, dir: dir, sockPath: sockPath, client: client}
}

// shortSocketPath returns a socket path safe from the unix domain socket
// path-length limit; see newTestServer's doc for why t.TempDir() alone is
// not safe here.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "as")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func writePlanFile(t *testing.T, dir string, p *plan.Plan) {
	t.Helper()
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal plan: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "plan.json"), b, 0o644); err != nil {
		t.Fatalf("write plan.json: %v", err)
	}
}

// writeLogFile writes content directly at the path the real engine would
// have written it to (via eventlog.NewLogSet's own path computation), so
// the server is tested against exactly the on-disk layout production
// produces rather than a layout invented for the test.
func writeLogFile(t *testing.T, dir, step string, attempt int, stream, content string) {
	t.Helper()
	p := eventlog.NewLogSet(dir).Path(step, attempt, stream)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write log file: %v", err)
	}
}

// streamNDJSON reads r as newline-delimited api.Event values on a
// background goroutine, preserving order, returning a channel closed when
// the stream ends. One long-lived goroutine, not a fresh bufio.Scanner per
// read: re-creating a Scanner would discard whatever it had buffered past
// the line it returned.
func streamNDJSON(t *testing.T, r io.Reader) <-chan api.Event {
	t.Helper()
	ch := make(chan api.Event, 64)
	go func() {
		defer close(ch)
		dec := json.NewDecoder(r)
		for {
			var e api.Event
			if err := dec.Decode(&e); err != nil {
				return
			}
			ch <- e
		}
	}()
	return ch
}

func recvEvent(t *testing.T, ch <-chan api.Event, timeout time.Duration) api.Event {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("stream ended before delivering the expected event")
		}
		return e
	case <-time.After(timeout):
		t.Fatalf("timed out after %v waiting for the next stream event", timeout)
	}
	return api.Event{}
}
