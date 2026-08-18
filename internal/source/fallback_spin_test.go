package source

// relay's reconnect logic must be an explicit loop, not two functions
// calling each other: Go has no tail-call optimization, so a stream that
// ends immediately and markerless every time would grow the stack two
// frames per cycle until a fatal overflow, and the peer decides when
// streams end, so a hostile engine could inflict this on demand.
//
// White-box (package source): these tests need SubscribeStream behavior
// impossible to arrange through the public contract, and assert on the
// internal constants.

import (
	"context"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

// spinLive accepts every SubscribeStream call and immediately ends the
// stream with no marker and no events: a server dropping the connection
// before writing anything, the shape that turned the old recursive relay
// into unbounded recursion.
type spinLive struct{ calls atomic.Int64 }

func (s *spinLive) SubscribeStream(ctx context.Context, fromSeq uint64) (<-chan api.Event, <-chan StreamEnd, error) {
	s.calls.Add(1)
	ev := make(chan api.Event)
	end := make(chan StreamEnd)
	close(ev)
	close(end)
	return ev, end, nil
}

func (s *spinLive) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	ev, _, err := s.SubscribeStream(ctx, fromSeq)
	return ev, err
}
func (s *spinLive) State(ctx context.Context) (*api.RunState, error) { return &api.RunState{}, nil }
func (s *spinLive) Logs(ctx context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error) {
	return nil, nil
}
func (s *spinLive) Control(ctx context.Context, req api.Frame) (api.Frame, error) {
	return api.Frame{}, nil
}
func (s *spinLive) Close() error { return nil }

// TestMarkerlessCloseDoesNotSpin is a permanent regression test: against a
// recursive relay this reliably produced `fatal error: stack overflow`
// (taking the whole embedding process down) in under 1.25s. Against the
// fixed relay, SubscribeStream is called a small, constant number of times
// and the stack stays flat.
func TestMarkerlessCloseDoesNotSpin(t *testing.T) {
	live := &spinLive{}
	fs := Fallback(live, t.TempDir())
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := fs.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()

	// Let it run briefly, then measure.
	time.Sleep(2 * time.Second)
	n := live.calls.Load()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("SubscribeStream calls in 2s: %d; stack in use: %d KiB", n, ms.StackInuse/1024)

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Log("relay goroutine did not exit after cancel")
	}

	if n > 50 {
		t.Fatalf("markerless close spins: %d reconnect attempts in 2s (no bound, no backoff)", n)
	}
}

// TestFallbackGivesUpOnLiveAfterBoundedNoProgressReconnectsWithoutFallingBack
// proves: the bound is exact (SubscribeStream is called exactly
// maxConsecutiveNoProgressReconnects times); the backoff is real
// wall-clock time; the channel closes once the budget is exhausted; and
// giving up does NOT fall back to disk. The directory handed to Fallback
// has no run recorded, so a wrong fallback would make the next State()
// fail outright instead of reaching the live mock.
func TestFallbackGivesUpOnLiveAfterBoundedNoProgressReconnectsWithoutFallingBack(t *testing.T) {
	live := &spinLive{}
	fs := Fallback(live, t.TempDir()) // no events.jsonl ever written here
	defer func() { _ = fs.Close() }()

	start := time.Now()
	ch, err := fs.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel produced an event from a source that never delivers one")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel never closed after exhausting the no-progress reconnect budget — the bound did not stop the retries")
	}
	elapsed := time.Since(start)

	if got := live.calls.Load(); got != maxConsecutiveNoProgressReconnects {
		t.Errorf("SubscribeStream calls = %d, want exactly %d (the bound)", got, maxConsecutiveNoProgressReconnects)
	}

	// (bound-1) backoffs must genuinely have elapsed: the backoff is real
	// wall-clock delay between attempts, not a documented-but-absent one.
	wantMinElapsed := time.Duration(maxConsecutiveNoProgressReconnects-1) * reconnectBackoff
	if elapsed < wantMinElapsed {
		t.Errorf("gave up after %v, want at least %v (the floor between attempts)", elapsed, wantMinElapsed)
	}

	// Exhausting the budget must NOT fall back to disk: proven by observing
	// State still reach the live mock's own answer rather than failing
	// against a directory with no recorded run.
	st, err := fs.State(context.Background())
	if err != nil {
		t.Fatalf("State after exhausting the reconnect budget: %v — fallback must not have triggered on ambiguous evidence", err)
	}
	if st == nil {
		t.Fatal("State returned a nil RunState with a nil error")
	}
}

// progressiveLive alternates, call by call: one delivers a new event, the
// next ends markerless with nothing. The alternation matters: an
// always-productive mock never exercises the reset, while here the
// cumulative no-progress count, if never reset, would exhaust the budget
// after only maxConsecutiveNoProgressReconnects productive events.
type progressiveLive struct {
	next  atomic.Uint64
	calls atomic.Int64
}

func (s *progressiveLive) SubscribeStream(ctx context.Context, fromSeq uint64) (<-chan api.Event, <-chan StreamEnd, error) {
	n := s.calls.Add(1)
	ev := make(chan api.Event, 1)
	end := make(chan StreamEnd)
	if n%2 == 1 {
		// Odd calls (1st, 3rd, 5th, ...) are productive.
		seq := s.next.Add(1)
		ev <- api.Event{V: 1, Seq: seq, Type: api.StepCreated, Step: "a"}
	}
	close(ev)
	close(end)
	return ev, end, nil
}

func (s *progressiveLive) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	ev, _, err := s.SubscribeStream(ctx, fromSeq)
	return ev, err
}
func (s *progressiveLive) State(ctx context.Context) (*api.RunState, error) {
	return &api.RunState{}, nil
}
func (s *progressiveLive) Logs(ctx context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error) {
	return nil, nil
}
func (s *progressiveLive) Control(ctx context.Context, req api.Frame) (api.Frame, error) {
	return api.Frame{}, nil
}
func (s *progressiveLive) Close() error { return nil }

// TestFallbackReconnectProgressResetsTheNoProgressBudget: a productive
// reconnect must reset the budget an unproductive one consumes. Draining
// more than double what an unreset budget would allow requires every
// unproductive call in between to have been forgiven.
func TestFallbackReconnectProgressResetsTheNoProgressBudget(t *testing.T) {
	live := &progressiveLive{}
	fs := Fallback(live, t.TempDir())
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := fs.Subscribe(ctx, 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const n = int64(maxConsecutiveNoProgressReconnects) * 3
	var last uint64
	for i := int64(0); i < n; i++ {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed early after %d/%d events — a productive reconnect must not be cut off by the no-progress budget", i, n)
			}
			if e.Seq != last+1 {
				t.Fatalf("event %d: seq %d after %d — gap or repeat", i, e.Seq, last)
			}
			last = e.Seq
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out after %d/%d events", i, n)
		}
	}

	// Roughly 2 SubscribeStream calls per delivered event confirms the
	// alternation actually happened. The minimum is n*2-1, not n*2: the
	// drain loop stops the instant it has n events, and the backoff
	// reliably delays the following unproductive call past that point.
	if got, want := live.calls.Load(), n*2-1; got < want {
		t.Errorf("SubscribeStream calls = %d, want at least %d (the alternating productive/unproductive pattern)", got, want)
	}
}
