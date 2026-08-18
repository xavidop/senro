package tail_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/tail"
)

// shortenReconnect makes the paced retry immaterial to a test's runtime:
// proving the loop works does not require waiting out the rate limit.
func shortenReconnect(t *testing.T) {
	t.Helper()
	prev := tail.Reconnect
	tail.Reconnect = time.Millisecond
	t.Cleanup(func() { tail.Reconnect = prev })
}

// scriptedGetter answers canned responses and records every path asked for:
// the resume contract is a statement about which URL is requested next, and
// only the recorded paths can check it.
type scriptedGetter struct {
	mu    sync.Mutex
	paths []string

	// reply is consulted for every request.
	reply func(n int, path string) (int, string, error)
	calls int
}

func (g *scriptedGetter) Get(_ context.Context, path string) (int, io.ReadCloser, error) {
	g.mu.Lock()
	n := g.calls
	g.calls++
	g.paths = append(g.paths, path)
	reply := g.reply
	g.mu.Unlock()

	status, body, err := reply(n, path)
	if err != nil {
		return 0, nil, err
	}
	return status, io.NopCloser(strings.NewReader(body)), nil
}

func (g *scriptedGetter) seen() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.paths...)
}

// snapshot renders a GET /api/state body at a given seq.
func snapshot(seq uint64) string {
	return fmt.Sprintf(`{"seq":%d,"run":{"id":"r1","done":false},"steps":{},"expansions":{},"order":[]}`, seq)
}

// runEnded is the terminal marker for a run that finished.
const runEnded = `{"stream_end":true,"last_seq":0,"overflowed":false,"reason":"run_ended","hint":"the run is over"}` + "\n"

// overflowed is the mid-stream terminal marker for a subscriber that fell
// behind the server's retained ring.
const overflowed = `{"stream_end":true,"last_seq":0,"overflowed":true,"reason":"overflowed","hint":"GET /api/state, then GET /api/stream?from=<state.seq+1>"}` + "\n"

func event(seq uint64, typ api.Type, step string) string {
	if step == "" {
		return fmt.Sprintf(`{"v":1,"seq":%d,"type":%q,"run":"r1"}`, seq, typ) + "\n"
	}
	return fmt.Sprintf(`{"v":1,"seq":%d,"type":%q,"run":"r1","step":%q}`, seq, typ, step) + "\n"
}

// The whole resume contract in one assertion: from is Seq+1, never Seq
// (which replays an event the snapshot already folded) and never 0 (which
// replays the entire run a snapshot exists to avoid).
func TestSubscribesFromSnapshotSeqPlusOne(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		switch n {
		case 0:
			return 200, snapshot(41), nil
		default:
			return 200, runEnded, nil
		}
	}}

	if err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{"/api/state", "/api/stream?from=42"}
	got := g.seen()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("requests = %v, want %v", got, want)
	}
}

// A 410 before any body exists means the resume point aged out of the ring;
// the documented remedy is a fresh snapshot.
func TestOverflowBeforeTheStreamOpensReSnapshots(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		switch n {
		case 0:
			return 200, snapshot(10), nil
		case 1:
			return 410, `{"error":"lifecycle_overflow","hint":"GET /api/state"}`, nil
		case 2:
			// The server moved on while we were away.
			return 200, snapshot(900), nil
		default:
			return 200, event(901, api.RunFinished, "") + runEnded, nil
		}
	}}

	var snaps []uint64
	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{
		OnSnapshot: func(st *api.RunState) { snaps = append(snaps, st.Seq) },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{"/api/state", "/api/stream?from=11", "/api/state", "/api/stream?from=901"}
	got := g.seen()
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("requests = %v, want %v", got, want)
	}
	if len(snaps) != 2 || snaps[0] != 10 || snaps[1] != 900 {
		t.Fatalf("snapshot seqs = %v, want [10 900]", snaps)
	}
}

// The same condition can arrive mid-stream as a terminal marker, treated
// identically: re-snapshot, not resume, which would just 410 again.
func TestOverflowMarkerMidStreamReSnapshots(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		switch n {
		case 0:
			return 200, snapshot(1), nil
		case 1:
			return 200, event(2, api.StepStarted, "build") + overflowed, nil
		case 2:
			return 200, snapshot(500), nil
		default:
			return 200, runEnded, nil
		}
	}}

	if err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := g.seen()
	want := []string{"/api/state", "/api/stream?from=2", "/api/state", "/api/stream?from=501"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("requests = %v, want %v: an overflow marker must re-snapshot, not resume", got, want)
	}
}

// A stream ending with no marker is the ambiguous case: NOT the end of the
// run, and the client resumes from where it got to rather than paying for a
// snapshot it does not need.
func TestMarkerlessCloseResumesFromWhereItGotTo(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		switch n {
		case 0:
			return 200, snapshot(1), nil
		case 1:
			// Delivers two events and then the body simply ends.
			return 200, event(2, api.StepStarted, "build") + event(3, api.StepFinished, "build"), nil
		default:
			return 200, runEnded, nil
		}
	}}

	if err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := g.seen()
	want := []string{"/api/state", "/api/stream?from=2", "/api/stream?from=4"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("requests = %v, want %v: a markerless close must reconnect from lastSeq+1, without a second snapshot", got, want)
	}
}

// A peer that answers every subscription and then closes it immediately,
// saying nothing, must not keep a browser tab looping forever. The bound is
// on futility: rounds that fold nothing.
func TestNoProgressIsBoundedAndSaidOutLoud(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		if path == tail.StatePath {
			return 200, snapshot(1), nil
		}
		return 200, "", nil // an empty stream, over and over
	}}

	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{})
	if !errors.Is(err, tail.ErrNoProgress) {
		t.Fatalf("Run error = %v, want ErrNoProgress: an endless empty stream must be reported, not looped on", err)
	}
	// Two requests per round, for at most the documented number of rounds;
	// what matters is that it is bounded at all.
	if n := len(g.seen()); n > 2*tail.MaxNoProgressRounds {
		t.Fatalf("made %d requests, want at most %d: the retry loop is not bounded", n, 2*tail.MaxNoProgressRounds)
	}
}

// A server that 410s every resume point it just handed out is the other way
// this loop could spin forever; it counts as no progress.
func TestEndlessOverflowIsAlsoBounded(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		if path == tail.StatePath {
			return 200, snapshot(1), nil
		}
		return 410, `{"error":"lifecycle_overflow"}`, nil
	}}

	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{})
	if !errors.Is(err, tail.ErrNoProgress) {
		t.Fatalf("Run error = %v, want ErrNoProgress", err)
	}
	if n := len(g.seen()); n > 2*tail.MaxNoProgressRounds {
		t.Fatalf("made %d requests, want at most %d", n, 2*tail.MaxNoProgressRounds)
	}
}

// A connection that keeps delivering before it breaks is productive and
// must not count against the futility bound: the ordinary case of a
// long-lived tab on a flaky link.
func TestProductiveReconnectsAreNotCountedAsFutile(t *testing.T) {
	shortenReconnect(t)
	// Deliver one event per connection, far more times than the futility
	// bound, then end the run.
	const rounds = tail.MaxNoProgressRounds * 4
	streamCalls := 0
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		if path == tail.StatePath {
			return 200, snapshot(0), nil
		}
		streamCalls++
		if streamCalls > rounds {
			return 200, runEnded, nil
		}
		return 200, event(uint64(streamCalls), api.StepStarted, "build"), nil
	}}

	if err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := len(g.seen()); n < rounds {
		t.Fatalf("made %d requests, want at least %d: a productive connection was cut off by the futility bound", n, rounds)
	}
	// Exactly one snapshot: every productive reconnect resumed.
	snaps := 0
	for _, p := range g.seen() {
		if p == tail.StatePath {
			snaps++
		}
	}
	if snaps != 1 {
		t.Errorf("took %d snapshots, want 1: a productive reconnect must not re-snapshot", snaps)
	}
}

// An out-of-order sequence number must stop the session outright:
// reconnecting fetches the same bytes again, folding on produces a state
// that never existed. The fold detects it; the client must not paper over
// it.
func TestOutOfOrderSeqEndsTheSessionRatherThanReconnecting(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		switch n {
		case 0:
			return 200, snapshot(50), nil
		default:
			// Seq 51 then Seq 9: a regression the fold rejects.
			return 200, event(51, api.StepStarted, "build") + event(9, api.StepStarted, "build"), nil
		}
	}}

	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{})
	if err == nil {
		t.Fatal("Run returned nil, want an out-of-order error")
	}
	if !strings.Contains(err.Error(), "out-of-order") {
		t.Fatalf("Run error = %v, want it to name the ordering violation", err)
	}
	if n := len(g.seen()); n != 2 {
		t.Fatalf("made %d requests, want 2: the session must stop, not retry, on a broken ordering guarantee", n)
	}
}

// A snapshot is the server's whole truth and replaces what the client held;
// merging would render a run that never happened.
func TestASnapshotReplacesRatherThanMerges(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		switch n {
		case 0:
			return 200, snapshot(1), nil
		case 1:
			return 200, event(2, api.StepStarted, "ghost") + overflowed, nil
		case 2:
			// The fresh snapshot knows nothing about "ghost".
			return 200, snapshot(900), nil
		default:
			return 200, runEnded, nil
		}
	}}

	var last *api.RunState
	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{
		OnSnapshot: func(st *api.RunState) { last = st },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if last == nil {
		t.Fatal("onSnapshot never fired")
	}
	if _, ok := last.Steps["ghost"]; ok {
		t.Error("the step folded before the overflow survived into the fresh snapshot: the client merged instead of replacing")
	}
	if last.Seq != 900 {
		t.Errorf("Seq = %d, want 900", last.Seq)
	}
}

// A cancelled context ends the loop promptly and reports why, rather than
// returning nil and letting a caller believe the run ended.
func TestCancellationIsReportedAsCancellation(t *testing.T) {
	shortenReconnect(t)
	ctx, cancel := context.WithCancel(context.Background())
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		if path == tail.StatePath {
			return 200, snapshot(1), nil
		}
		cancel()
		return 200, "", nil
	}}

	err := tail.Run(ctx, &tail.HTTPBackend{Getter: g}, tail.Fold{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
}

// A non-200, non-410 status is a plain failure, never an overflow, which
// would spin a silent re-snapshot loop against a broken server.
func TestAServerErrorIsNotAnOverflow(t *testing.T) {
	shortenReconnect(t)
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		if path == tail.StatePath {
			return 200, snapshot(1), nil
		}
		return 500, "boom", nil
	}}

	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{})
	if errors.Is(err, tail.ErrOverflow) {
		t.Fatal("a 500 was reported as an overflow")
	}
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("Run error = %v, want it to carry the status", err)
	}
}

// A transport failure fetching the snapshot ends the session with the
// transport's own error, rather than being retried invisibly.
func TestTransportFailureIsReported(t *testing.T) {
	shortenReconnect(t)
	sentinel := errors.New("connection refused")
	g := &scriptedGetter{reply: func(n int, path string) (int, string, error) {
		return 0, "", sentinel
	}}

	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: g}, tail.Fold{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run error = %v, want it to wrap the transport error", err)
	}
}

// LogPath's encoding is not cosmetic: an expanded child's id carries
// slashes and brackets, and a raw segment is not what the server routes on.
func TestLogPathEncodesTheStepId(t *testing.T) {
	got := tail.LogPath("deploy/apply[os=linux]", 2, "stderr", 4096)
	if strings.Contains(got, "[") || strings.Contains(got, "]") {
		t.Errorf("LogPath = %q, want the brackets percent-encoded", got)
	}
	// One slash for the route prefix, none from the step id itself.
	if n := strings.Count(got, "/"); n != 3 {
		t.Errorf("LogPath = %q has %d slashes, want 3 (/api/logs/<encoded>)", got, n)
	}
	for _, want := range []string{"attempt=2", "stream=stderr", "from=4096"} {
		if !strings.Contains(got, want) {
			t.Errorf("LogPath = %q, want it to carry %q", got, want)
		}
	}
}

// from=0 needs no parameter; sending one would differ byte for byte from
// what internal/source's own Logs sends for the same request.
func TestLogPathOmitsAZeroOffset(t *testing.T) {
	if got := tail.LogPath("build", 1, "stdout", 0); strings.Contains(got, "from=") {
		t.Errorf("LogPath = %q, want no from parameter for offset 0", got)
	}
}
