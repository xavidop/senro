package attachsrv_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
)

// The hub is the engine's only observer. Emit must never block, whatever a
// client is doing. The engine's correctness cannot depend on who is watching.
func TestEmitNeverBlocksOnASlowSubscriber(t *testing.T) {
	h := attachsrv.NewHub(8)
	ch, cancel, _ := h.Subscribe(0)
	defer cancel()
	_ = ch // deliberately never read

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 1000; i++ {
			h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked on a subscriber that never reads")
	}
}

// A surviving subscriber must never see a gap: a silently-lost
// step.finished leaves a client permanently wrong about which steps exist.
// Overflow disconnects; it does not skip.
//
// The reader must run CONCURRENTLY and slowly: filling the buffer and only
// then reading would make the channel a contiguous prefix by construction,
// which no implementation could fail.
func TestSlowSubscriberIsDisconnectedNotSilentlyTruncated(t *testing.T) {
	h := attachsrv.NewHub(8)
	ch, cancel, err := h.Subscribe(0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		var last uint64
		for e := range ch {
			if last != 0 && e.Seq != last+1 {
				t.Errorf("a gap appeared in the lifecycle stream: %d then %d", last, e.Seq)
				return
			}
			last = e.Seq
			time.Sleep(time.Microsecond) // lag, so the buffer can overflow
		}
	}()

	for i := 1; i <= 1000; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the subscriber was neither closed nor kept up with")
	}
}

// Reading past a closed channel yields the zero api.Event, and Apply only
// checks ordering when Seq != 0, so folding zero values silently succeeds:
// an Apply-based assertion could not tell "500 ordered events arrived"
// from "the subscriber died after the first and this read 499 zero
// values". Checking ok and the exact Seq removes that blind spot.
func TestSubscribersSeeEventsInOrder(t *testing.T) {
	h := attachsrv.NewHub(4096)
	ch, cancel, _ := h.Subscribe(0)
	defer cancel()

	const n = 500
	go func() {
		for i := 1; i <= n; i++ {
			h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
		}
	}()

	var prev uint64
	for i := 0; i < n; i++ {
		e, ok := <-ch
		if !ok {
			t.Fatalf("channel closed after %d of %d events — the subscriber must not be disconnected here", i, n)
		}
		if e.Seq != prev+1 {
			t.Fatalf("event %d: got seq %d, want %d — the hub must preserve order", i, e.Seq, prev+1)
		}
		prev = e.Seq
	}
}

func TestStateIsTheFoldOfEverythingEmitted(t *testing.T) {
	h := attachsrv.NewHub(4096)
	for _, e := range twoStepEvents() {
		h.Emit(e)
	}
	st := h.State()
	if !st.Run.Done || len(st.Steps) != 2 {
		t.Errorf("hub state = %+v", st.Run)
	}
}

// Seq is a cheaper equivalent of State().Seq for a caller that only needs
// the watermark (handleStream's overflow heuristic is exactly that caller)
// without paying for a full RunState deep clone just to read one field.
func TestSeqMatchesStateSeq(t *testing.T) {
	h := attachsrv.NewHub(4096)
	for _, e := range twoStepEvents() {
		h.Emit(e)
	}
	if got, want := h.Seq(), h.State().Seq; got != want {
		t.Errorf("Seq() = %d, want %d (State().Seq)", got, want)
	}
}

// What makes version negotiation (api.CheckVersion) possible: a client's
// first contact is GET /api/state, before a single event has been folded,
// so a version that only appeared once run.started was folded would leave
// that window reporting Major/Minor as 0, indistinguishable from "no
// engine at all".
func TestStateCarriesTheEnginesProtocolVersion(t *testing.T) {
	h := attachsrv.NewHub(4096)

	st := h.State()
	if st.ProtoMajor != api.Version {
		t.Errorf("ProtoMajor = %d, want api.Version (%d)", st.ProtoMajor, api.Version)
	}
	if st.ProtoMinor != api.VersionMinor {
		t.Errorf("ProtoMinor = %d, want api.VersionMinor (%d)", st.ProtoMinor, api.VersionMinor)
	}

	// Still present after real events are folded: cloneState must carry the
	// two plain ints across just like every other RunState field, not only
	// on the freshly-constructed zero state.
	for _, e := range twoStepEvents() {
		h.Emit(e)
	}
	st2 := h.State()
	if st2.ProtoMajor != api.Version || st2.ProtoMinor != api.VersionMinor {
		t.Errorf("after Emit: ProtoMajor/ProtoMinor = %d/%d, want %d/%d",
			st2.ProtoMajor, st2.ProtoMinor, api.Version, api.VersionMinor)
	}
}

// TestDoneIsFalseForARunStillInProgress is Done()'s baseline: a hub that
// has only seen a run.started (no run.finished, not Close()d) must report
// false: server.go's own handleControl precheck depends on this NOT being
// true prematurely, or a control request against a genuinely live run
// would be refused without ever reaching the engine.
func TestDoneIsFalseForARunStillInProgress(t *testing.T) {
	h := attachsrv.NewHub(64)
	h.Emit(api.Event{V: 1, Seq: 1, Type: api.RunStarted, Run: "run1"})
	if h.Done() {
		t.Error("Done() = true for a run that has only started, want false")
	}
}

// TestDoneIsTrueAfterRunFinished covers the h.state.Run.Done half of
// Done()'s two conditions: the ordinary, expected way a hub comes to
// report Done() true.
func TestDoneIsTrueAfterRunFinished(t *testing.T) {
	h := attachsrv.NewHub(64)
	for _, e := range twoStepEvents() { // ends in run.finished; see its own doc
		h.Emit(e)
	}
	if !h.Done() {
		t.Error("Done() = false after run.finished was folded, want true")
	}
}

// Exercises the h.closed half of Done()'s condition independently of
// state.Run.Done, so a mutation dropping "h.closed ||" is caught: a hub
// that has NEVER seen run.finished, only Close()d, must still report true.
// Load-bearing for handleControl's precheck.
func TestDoneIsTrueAfterHubCloseAlone(t *testing.T) {
	h := attachsrv.NewHub(64)
	h.Emit(api.Event{V: 1, Seq: 1, Type: api.RunStarted, Run: "run1"})
	if h.Done() {
		t.Fatal("Done() = true before Close(), want false — this test would prove nothing about the h.closed branch otherwise")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !h.Done() {
		t.Error("Done() = false after Close(), with no run.finished ever folded — want true: nothing is left to act on a control request against a closed hub either way")
	}
}

// Attaching to a run already in flight must not replay from the beginning.
func TestSubscribeFromSeqSkipsWhatTheSnapshotCovered(t *testing.T) {
	h := attachsrv.NewHub(4096)
	for i := 1; i <= 10; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	st := h.State()

	ch, cancel, _ := h.Subscribe(st.Seq + 1)
	defer cancel()
	h.Emit(api.Event{V: 1, Seq: 11, Type: api.StepCreated, Step: "b"})

	e := <-ch
	if e.Seq != 11 {
		t.Errorf("first delivered seq = %d, want 11", e.Seq)
	}
}

// fromSeq is inclusive, as the snapshot-then-resume contract every Source
// shares requires. Changing hub.go's replay filter from >= to > passes
// every other test in this file; this is the one that catches it, the same
// boundary that was a real off-by-one in FileSource.Subscribe.
func TestSubscribeReplaysFromSeqInclusive(t *testing.T) {
	h := attachsrv.NewHub(8)
	for i := 1; i <= 6; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	ch, cancel, err := h.Subscribe(3)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	want := []uint64{3, 4, 5, 6}
	for i, w := range want {
		e := <-ch
		if e.Seq != w {
			t.Fatalf("replay[%d] = seq %d, want %d (replay = %v so far)", i, e.Seq, w, want[:i])
		}
	}
	select {
	case e := <-ch:
		t.Errorf("unexpected extra replayed event: seq %d — fromSeq was ignored or the replay over-delivered", e.Seq)
	default:
	}
}

// The retention boundary itself: resuming from within retained history,
// refusing what has been evicted, and never refusing the pairing State()
// promises. TestSubscribeFromSeqSkipsWhatTheSnapshotCovered asks for a seq
// beyond anything emitted, which is the "wait for the future" path
// instead.
func TestRingRetentionBoundary(t *testing.T) {
	const ringSize = 8
	h := attachsrv.NewHub(ringSize)
	for i := 1; i <= 100; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	// The ring now retains exactly the last 8 events: seq 93..100.

	if _, cancel, err := h.Subscribe(92); !errors.Is(err, attachsrv.ErrLifecycleOverflow) {
		t.Errorf("Subscribe(92) = %v, want ErrLifecycleOverflow", err)
	} else {
		cancel() // must be non-nil even on this error path; see TestSubscribeCancelIsNeverNilOnError
	}
	if _, cancel, err := h.Subscribe(0); !errors.Is(err, attachsrv.ErrLifecycleOverflow) {
		t.Errorf("Subscribe(0) = %v, want ErrLifecycleOverflow", err)
	} else {
		cancel()
	}

	ch, cancel, err := h.Subscribe(93)
	if err != nil {
		t.Fatalf("Subscribe(93): %v", err)
	}
	defer cancel()
	for want := uint64(93); want <= 100; want++ {
		e := <-ch
		if e.Seq != want {
			t.Fatalf("replay seq = %d, want %d", e.Seq, want)
		}
	}
	select {
	case e := <-ch:
		t.Errorf("unexpected extra replayed event: seq %d", e.Seq)
	default:
	}

	// The resume pairing State() itself promises (Subscribe(state.Seq+1))
	// must never overflow, however far behind the ring's retention it
	// looks: it is asking for the future, never the past.
	st := h.State()
	_, cancel2, err := h.Subscribe(st.Seq + 1)
	if err != nil {
		t.Errorf("Subscribe(state.Seq+1) = %v, want nil — this pairing must never overflow", err)
	} else {
		cancel2()
	}
}

// Callers commonly write `ch, cancel, _ := h.Subscribe(...); defer
// cancel()`. That panics on a nil cancel func the moment a caller hits the
// error path with the same one-liner, which TestRingRetentionBoundary
// above already does in passing; this pins it directly.
func TestSubscribeCancelIsNeverNilOnError(t *testing.T) {
	h := attachsrv.NewHub(8)
	for i := 1; i <= 100; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	ch, cancel, err := h.Subscribe(1) // long since evicted
	defer cancel()                    // must not panic on a nil func
	if !errors.Is(err, attachsrv.ErrLifecycleOverflow) {
		t.Fatalf("Subscribe(1) = %v, want ErrLifecycleOverflow", err)
	}
	if ch != nil {
		t.Errorf("channel on the error path = %v, want nil", ch)
	}
}

// Subscribe's initial replay must not consume the subscriber's own lag
// budget. Ring 8, emit 1..8, then Subscribe(1): the replay hands over all
// 8 before the subscriber is scheduled once, and with a capacity of only
// ringSize the next Emit would disconnect a subscriber that was never
// slow, in exactly the case the ring exists to serve. One that has just
// replayed a full ring must survive ringSize further events unread, like a
// freshly subscribed one.
func TestReplayDoesNotConsumeTheSubscribersLagBudget(t *testing.T) {
	const ringSize = 8
	h := attachsrv.NewHub(ringSize)
	for i := 1; i <= ringSize; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	ch, cancel, err := h.Subscribe(1) // replays the full ring: seq 1..ringSize
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	for i := ringSize + 1; i <= 2*ringSize; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	for want := uint64(1); want <= 2*ringSize; want++ {
		e, ok := <-ch
		if !ok {
			t.Fatalf("subscriber disconnected before seq %d — the replay consumed its own lag budget", want)
		}
		if e.Seq != want {
			t.Fatalf("event out of order: got seq %d, want %d", e.Seq, want)
		}
	}
}

// Close's whole reason to exist: a `for e := range ch` reader (exactly what
// render.Plain is, and exactly what the attach server's /api/stream handler
// is) must return once the hub it is watching is done, not hang forever
// waiting for an event that will never come.
func TestCloseUnblocksASubscriberRangingOverItsChannel(t *testing.T) {
	h := attachsrv.NewHub(8)
	ch, cancel, err := h.Subscribe(0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	h.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	// Wait for the reader to actually receive seq 1 (synchronously, via the
	// unbuffered handoff below) before closing, so this test also confirms
	// Close does not discard what was already delivered.
	received := make(chan uint64, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var n int
		for e := range ch {
			n++
			received <- e.Seq
		}
		_ = n
	}()

	select {
	case seq := <-received:
		if seq != 1 {
			t.Fatalf("first delivered seq = %d, want 1", seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received seq 1 before Close")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("range over the subscriber's channel never returned after Close — it is hanging exactly like the carried finding described")
	}
}

// A hub with no subscriber cannot exercise the double-close panic this
// test is named for: Close resets h.subs, so even a Close missing its
// idempotency guard would find nothing to close twice. Registering a real
// subscriber first is what makes the regression reachable.
func TestCloseIsIdempotent(t *testing.T) {
	h := attachsrv.NewHub(8)
	_, cancel, err := h.Subscribe(0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	if err := h.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

// Matches sink.Multi's own policy: Emit must never fail or panic, whatever
// state the sink is in, including after Close. A caller mid-Emit when the
// hub closes (a real race in production: the run's own goroutine emitting
// while something else tears the hub down) must not see a panic from
// sending on a channel Close has already closed.
func TestEmitAfterCloseIsANoOp(t *testing.T) {
	h := attachsrv.NewHub(8)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"}) // must not panic

	if st := h.State(); st.Seq != 0 || len(st.Steps) != 0 {
		t.Errorf("State after a post-Close Emit = %+v, want untouched", st)
	}
}

// Subscribe after Close must say so distinctly: ErrClosed, not
// ErrLifecycleOverflow and not a channel that is already closed, which a
// caller using the common `ch, cancel, _ := h.Subscribe(...); defer
// cancel()` convention could otherwise mistake for "nothing has happened
// yet" rather than "this hub is permanently done."
func TestSubscribeAfterCloseReturnsErrClosed(t *testing.T) {
	h := attachsrv.NewHub(8)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ch, cancel, err := h.Subscribe(0)
	defer cancel() // must not panic on a nil func
	if !errors.Is(err, attachsrv.ErrClosed) {
		t.Errorf("Subscribe after Close = %v, want ErrClosed", err)
	}
	if ch != nil {
		t.Errorf("channel on the error path = %v, want nil", ch)
	}
}

// Dropped is how a lossy observer becomes visible rather than merely
// inferable from a reconnect. Reuses the concurrent-slow-reader shape of
// TestSlowSubscriberIsDisconnectedNotSilentlyTruncated, and additionally
// checks that a subscriber's own cancel does NOT count as a drop: only
// Emit disconnecting someone for falling behind moves the counter.
func TestDroppedCountsOverflowDisconnectsNotVoluntaryCancels(t *testing.T) {
	h := attachsrv.NewHub(8)

	// A subscriber that cancels itself cleanly must not be counted as
	// dropped: cancel() Discards nothing the reader didn't already choose to
	// stop wanting.
	_, cancel, err := h.Subscribe(0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	if got := h.Dropped(); got != 0 {
		t.Fatalf("Dropped() after a voluntary cancel = %d, want 0", got)
	}

	ch, cancel2, err := h.Subscribe(0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel2()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
			time.Sleep(time.Microsecond) // lag, so the buffer can overflow
		}
	}()

	for i := 1; i <= 1000; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the slow subscriber was never disconnected")
	}

	if got := h.Dropped(); got != 1 {
		t.Errorf("Dropped() = %d, want 1 (exactly the one overflowing subscriber, not the earlier voluntary cancel)", got)
	}
}

// --- helpers ---

// twoStepEvents returns a small, finished, successful two-step run (setup
// then build, build needing setup) as raw api.Event values ready to hand
// straight to Hub.Emit. Unlike render and source's twoStepRun helpers, these
// are pre-stamped with Seq and V: Emit takes events exactly as the engine's
// ledger already stamped them, and does no stamping of its own.
func twoStepEvents() []api.Event {
	return []api.Event{
		{V: 1, Seq: 1, Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
			Pipeline:      "ci",
			EngineVersion: "test",
			PlanDigest:    "digest1",
			StartedAt:     time.Now().UTC(),
		})},
		{V: 1, Seq: 2, Type: api.StepCreated, Run: "run1", Step: "setup", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec",
		})},
		{V: 1, Seq: 3, Type: api.StepCreated, Run: "run1", Step: "build", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec", Needs: []string{"setup"},
		})},
		{V: 1, Seq: 4, Type: api.StepStarted, Run: "run1", Step: "setup", Attempt: 1},
		{V: 1, Seq: 5, Type: api.StepFinished, Run: "run1", Step: "setup", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateSucceeded,
		})},
		{V: 1, Seq: 6, Type: api.StepStarted, Run: "run1", Step: "build", Attempt: 1},
		{V: 1, Seq: 7, Type: api.StepFinished, Run: "run1", Step: "build", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateSucceeded,
		})},
		{V: 1, Seq: 8, Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
			Status: api.RunSucceeded,
		})},
	}
}

func mustBody(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
