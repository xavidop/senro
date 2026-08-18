package sink_test

import (
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
)

// The engine's correctness cannot depend on whether anyone is watching. A sink
// that blocks or panics must not be able to stall or kill a run.
func TestMultiIsNonBlockingAndPanicSafe(t *testing.T) {
	slow := sink.FuncSink(func(api.Event) { time.Sleep(50 * time.Millisecond) })
	boom := sink.FuncSink(func(api.Event) { panic("observer exploded") })
	rec := sink.Recording()

	m := sink.Multi(slow, boom, rec)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Emit(api.Event{Seq: 1, Type: api.RunStarted})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked — a slow observer must never stall the engine")
	}

	// The healthy sink still receives it, eventually.
	deadline := time.After(2 * time.Second)
	for {
		if len(rec.Events()) == 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("healthy sink never received the event")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestMultiDropsRatherThanBlocksWhenAnObserversQueueIsFull is a regression
// test for the drop-on-full path. TestMultiIsNonBlockingAndPanicSafe above
// emits exactly one event into a 4096-deep queue, so the drop-vs-block
// branch inside Multi's Emit (`select { case q <- e: default:
// m.dropped[i]++ }`) is never actually entered: replacing it with a plain
// blocking `q <- e` leaves that test, and every other test in this
// package, green. This test drives the queue genuinely full against an
// observer that never drains at all, which is what the drop branch exists
// for, and would hang (not merely fail an assertion) under that exact
// mutation.
func TestMultiDropsRatherThanBlocksWhenAnObserversQueueIsFull(t *testing.T) {
	blocked := make(chan struct{})
	defer close(blocked) // release the wedged worker so it does not leak past this test

	wedged := sink.FuncSink(func(api.Event) { <-blocked })
	m := sink.Multi(wedged)
	dropper, ok := m.(interface{ Dropped() map[int]int })
	if !ok {
		t.Fatal("Multi's returned Sink does not expose Dropped()")
	}

	const sends = 10000 // comfortably past Multi's own internal queue depth
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < sends; i++ {
			m.Emit(api.Event{Seq: uint64(i + 1), Type: api.RunStarted})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%d sends against a permanently wedged observer did not return — "+
			"Emit blocked instead of dropping once the queue filled", sends)
	}

	if dropper.Dropped()[0] == 0 {
		t.Error("Dropped()[0] = 0, want > 0 — a wedged observer's overflow must be visible, not silent")
	}
}

func TestNopHasNoControlChannel(t *testing.T) {
	if sink.Nop().Control() != nil {
		t.Error("a no-op sink must expose a nil control channel")
	}
}

func TestRecordingCapturesOrder(t *testing.T) {
	rec := sink.Recording()
	for i := 1; i <= 3; i++ {
		rec.Emit(api.Event{Seq: uint64(i), Type: api.StepCreated})
	}
	got := rec.Events()
	if len(got) != 3 || got[0].Seq != 1 || got[2].Seq != 3 {
		t.Errorf("Events = %v", got)
	}
}

func TestMultiPreservesOrder(t *testing.T) {
	// The fold rejects a regressing sequence number, so a sink that receives
	// events out of order cannot fold them. Order is the contract; completeness
	// is not: a gap is survivable, a regression is not.
	rec := sink.Recording()
	m := sink.Multi(rec)

	const n = 500
	for i := 1; i <= n; i++ {
		m.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	closeAndDrain(t, m)

	got := rec.Events()
	if len(got) != n {
		t.Fatalf("received %d events, want %d", len(got), n)
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("event %d has seq %d — Multi reordered the stream", i, e.Seq)
		}
	}

	// And prove it against the real consumer, not just an index check.
	s := api.NewRunState()
	for _, e := range got {
		if err := s.Apply(e); err != nil {
			t.Fatalf("the engine's own fold rejected what Multi delivered: %v", err)
		}
	}
}

func TestEmitAfterCloseDoesNotPanic(t *testing.T) {
	// Emit must never fail. A panic here happens in the engine's goroutine,
	// where no observer-side recover can reach it.
	rec := sink.Recording()
	m := sink.Multi(rec)
	closeAndDrain(t, m)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Emit after Close panicked: %v", r)
		}
	}()
	m.Emit(api.Event{V: 1, Seq: 1, Type: api.RunStarted})
}

func TestCloseIsIdempotent(t *testing.T) {
	m := sink.Multi(sink.Recording())
	c, ok := m.(interface{ Close() error })
	if !ok {
		t.Fatal("Multi must expose Close")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

// closeAndDrain type-asserts for a Close method and calls it, ensuring all
// events are drained from queues before the test inspects the sink.
func closeAndDrain(t *testing.T, m sink.Sink) {
	type closer interface {
		Close() error
	}
	c, ok := m.(closer)
	if !ok {
		t.Fatalf("Multi does not have a Close method")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
