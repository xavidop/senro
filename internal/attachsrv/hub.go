// Package attachsrv implements the engine's attach surface: the hub that
// fans a run's event stream out to attached clients, and, in a later task,
// the server that exposes it over a socket.
//
// The hub is the engine's one and only Sink for a run. See internal/sink's
// package doc for why Emit must be non-blocking and infallible: it is called
// from the engine's scheduler goroutines, and the engine's correctness
// cannot depend on whether anyone is watching.
package attachsrv

import (
	"errors"
	"sync"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
)

// ErrLifecycleOverflow reports that Subscribe's fromSeq asks for an event
// the hub's ring has already evicted to make room for newer ones. It is not
// fatal: the caller's remedy is the same resume pairing every Source
// implementation uses: fetch a fresh State() and Subscribe from
// state.Seq+1.
var ErrLifecycleOverflow = errors.New("attachsrv: fromSeq is older than the retained lifecycle ring")

// ErrClosed reports that Subscribe was called after Close. Matches the
// closed-guard shape eventlog and source already use (their own ErrClosed),
// so a caller can tell "this hub is shutting down" apart from any other
// Subscribe failure with errors.Is.
var ErrClosed = errors.New("attachsrv: hub is closed")

// controlQueueDepth bounds the hub's control channel. Nothing sends on it
// yet, since the control-plane wiring (run.cancel, step.retry) lands in a
// later task, but Control must still return a real, non-nil channel: unlike
// sink.Nop and sink.FuncSink, which have nothing to carry, the hub exists
// specifically to carry control requests eventually, and a nil channel here
// would make a future select on it silently do nothing.
const controlQueueDepth = 16

// subscriberHeadroom multiplies ringSize to size each subscriber's channel
// strictly larger than the ring itself: Subscribe's initial replay can hand
// a fresh subscriber up to ringSize events before it is scheduled once, and
// a capacity merely equal to ringSize would spend the whole lag budget on
// the replay, disconnecting a subscriber that was never slow. The headroom
// is also what keeps the replay loop's non-blocking send true: a capacity
// smaller than the ring would deadlock the replay while holding h.mu,
// wedging Emit too.
const subscriberHeadroom = 2

// Hub is the engine's one and only observer: it satisfies sink.Sink and
// fans every event out to attached clients. Behind one mutex: the
// materialized RunState (so State() is O(1) rather than a replay), a ring
// of the most recent events for resume, and the subscriber set.
//
// Emit must never block. A subscriber whose channel is full is closed and
// removed, never awaited and never skipped past: skipping would drop a
// lifecycle event silently, leaving the client's view permanently wrong
// with no signal. ErrLifecycleOverflow is the read-side half of the same
// rule: history the ring no longer has is a clear error, not a silently
// short replay.
type Hub struct {
	ringSize int

	mu      sync.Mutex
	state   *api.RunState
	ring    []api.Event // fixed-size circular buffer, capacity ringSize
	ringPos int         // index of the oldest retained entry
	ringLen int         // number of valid entries in ring, <= ringSize
	evicted bool        // true once the ring has dropped an entry to make room
	ringLo  uint64      // Seq of the oldest entry currently retained

	subs map[*subscriber]struct{}

	control chan sink.ControlRequest

	// shells carries interactive session requests to the engine. Unbuffered;
	// see Shells() for why, and for why it is not the control channel.
	shells chan sink.ShellRequest

	// closed is set once by Close. Emit becomes a no-op and Subscribe
	// returns ErrClosed once it is true: see Close's doc for why a hub
	// needs this at all.
	closed bool
	// dropped counts subscribers Emit has disconnected for falling behind:
	// the hub's counterpart to sink.Multi's Dropped(), so a lossy observer
	// is visible rather than only inferable from a client-side reconnect.
	// It does NOT count a subscriber's own cancel() or the sweep Close does
	// on shutdown: neither of those loses anything the subscriber wanted.
	dropped uint64
}

// subscriber is one lifecycle client. A struct rather than a bare channel
// so it has stable pointer identity to use as a map key.
type subscriber struct {
	ch chan api.Event
	// next is the lowest Seq this subscriber still wants. Emit filters live
	// events against it, not just the ring replay, or fromSeq would stop
	// meaning what it documents.
	next uint64
}

var (
	_ sink.Sink      = (*Hub)(nil)
	_ sink.ShellHost = (*Hub)(nil)
)

// NewHub returns a hub retaining the last ringSize events for resume, and
// bounding each subscriber's lag by the same count before it is
// disconnected. See the Hub doc comment for why a lagging subscriber is
// dropped rather than throttling Emit. ringSize is clamped to at least 1 so
// the internal ring arithmetic never divides by zero.
func NewHub(ringSize int) *Hub {
	if ringSize < 1 {
		ringSize = 1
	}
	state := api.NewRunState()
	// Set once, here, rather than left for the first folded event: GET
	// /api/state can happen before run.started ever arrives (see
	// RunState.ProtoMajor for why that window matters to version
	// negotiation).
	state.ProtoMajor = api.Version
	state.ProtoMinor = api.VersionMinor
	return &Hub{
		ringSize: ringSize,
		state:    state,
		ring:     make([]api.Event, ringSize),
		subs:     make(map[*subscriber]struct{}),
		control:  make(chan sink.ControlRequest, controlQueueDepth),
		shells:   make(chan sink.ShellRequest),
	}
}

// Emit folds e into the materialized state, appends it to the resume ring,
// and fans it out to every subscriber with a non-blocking send. It never
// blocks and never panics, whatever a subscriber is doing.
func (h *Hub) Emit(e api.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		// Matches sink.Multi's post-close policy: an observer may lose
		// events, but Emit must never fail or panic. Close has already
		// disconnected every subscriber, so there is nothing to fan out to.
		return
	}

	// The engine serializes every event through one emit(), so Apply should
	// never see a regression here; if it somehow does, Emit still must not
	// fail: the event stays on the ring and fans out below, and a client
	// folding the raw stream sees the same out-of-order signal Apply
	// defines.
	_ = h.state.Apply(e)

	h.appendToRing(e)

	for sub := range h.subs {
		if e.Seq < sub.next {
			// Not yet requested: this subscriber's fromSeq is still ahead
			// of e. Filtering live delivery, not just the ring replay, is
			// what keeps fromSeq honoured once real events start arriving.
			continue
		}
		select {
		case sub.ch <- e:
			sub.next = e.Seq + 1
		default:
			// The subscriber cannot keep up: close and remove it rather
			// than skip this event, which would leave the channel looking
			// healthy while silently missing a lifecycle event.
			h.dropped++
			h.closeSub(sub)
		}
	}
}

// appendToRing records e as the newest entry, evicting the oldest entry
// once the ring is full. Must be called with h.mu held.
func (h *Hub) appendToRing(e api.Event) {
	if h.ringLen < h.ringSize {
		idx := (h.ringPos + h.ringLen) % h.ringSize
		h.ring[idx] = e
		h.ringLen++
		if h.ringLen == 1 {
			h.ringLo = e.Seq
		}
		return
	}
	// Full: overwrite the oldest slot with the newest event, then advance
	// the head past it. What was the second-oldest entry is now the
	// oldest, so ringLo becomes its Seq.
	h.ring[h.ringPos] = e
	h.ringPos = (h.ringPos + 1) % h.ringSize
	h.evicted = true
	h.ringLo = h.ring[h.ringPos].Seq
}

// ringSnapshot returns the ring's current contents, oldest first. Must be
// called with h.mu held.
func (h *Hub) ringSnapshot() []api.Event {
	out := make([]api.Event, h.ringLen)
	for i := 0; i < h.ringLen; i++ {
		out[i] = h.ring[(h.ringPos+i)%h.ringSize]
	}
	return out
}

// closeSub removes sub and closes its channel; h.mu must be held. A no-op
// if sub is not registered, so Emit's overflow path and a user's cancel can
// both call it without coordinating: whichever runs first closes, the
// other finds nothing left to do.
func (h *Hub) closeSub(sub *subscriber) {
	if _, ok := h.subs[sub]; !ok {
		return
	}
	delete(h.subs, sub)
	close(sub.ch)
}

// Control returns the channel the engine can read control requests from.
// It is always non-nil; nothing sends on it until a later task wires the
// server's control plane through it.
func (h *Hub) Control() <-chan sink.ControlRequest { return h.control }

// Shells returns the channel the engine reads interactive session requests
// from: sink.ShellHost.
//
// A SECOND channel, never the control one: control requests are served one
// at a time from the scheduler's own loop (the ordering that makes
// run.cancel idempotent with no lock), while a session is a connection
// somebody stands in for minutes. See sink.ShellRequest.
//
// Deliberately UNBUFFERED, unlike control's queue: accepting a session
// request into a buffer nobody is reading would leave the client on an
// upgraded connection with no session behind it until handleShell's own
// timeout fired. Unbuffered makes "the engine took this" a fact rather
// than an assumption.
func (h *Hub) Shells() <-chan sink.ShellRequest { return h.shells }

// Subscribe registers a new lifecycle subscriber and returns a channel of
// events with Seq >= fromSeq, a cancel func that unregisters it, and an
// error.
//
// If fromSeq is older than what the ring retains, Subscribe returns
// ErrLifecycleOverflow rather than silently starting later than requested;
// the remedy is a fresh State() and a resubscribe from state.Seq+1. The
// cancel func is never nil, including on error paths (`defer cancel()`
// would otherwise panic), and is idempotent, safe after an overflow
// disconnect.
func (h *Hub) Subscribe(fromSeq uint64) (<-chan api.Event, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		// A closed hub will never deliver anything again, and a returned
		// channel (even a closed one) would read exactly like a normal
		// empty replay; ErrClosed says which happened. cancel is still
		// non-nil.
		return nil, func() {}, ErrClosed
	}

	if h.evicted && fromSeq < h.ringLo {
		return nil, func() {}, ErrLifecycleOverflow
	}

	sub := &subscriber{ch: make(chan api.Event, h.ringSize*subscriberHeadroom), next: fromSeq}
	for _, e := range h.ringSnapshot() {
		if e.Seq >= fromSeq {
			// Never blocks: the ring holds at most ringSize entries and
			// the channel's capacity is strictly larger (see
			// subscriberHeadroom).
			sub.ch <- e
			sub.next = e.Seq + 1
		}
	}
	h.subs[sub] = struct{}{}

	cancel := func() {
		h.mu.Lock()
		h.closeSub(sub)
		h.mu.Unlock()
	}
	return sub.ch, cancel, nil
}

// Close disconnects every subscriber and marks the hub closed: further
// Emits are silently dropped (matching sink.Multi's post-close policy) and
// further Subscribes return ErrClosed rather than a channel that is
// already closed, which would read like an immediate overflow.
//
// Without this, a `for e := range ch` reader (render.Plain, this package's
// own /api/stream handler) would never return once the run it was watching
// ended.
//
// Close is idempotent and safe to call more than once.
func (h *Hub) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true

	for sub := range h.subs {
		close(sub.ch)
	}
	h.subs = make(map[*subscriber]struct{})

	return nil
}

// Dropped reports how many subscribers Emit has disconnected for falling
// behind (see the Hub doc for why it disconnects rather than skips):
// without it, a lossy observer is only inferable from a client's
// unexpected reconnect. It does not count a subscriber's own cancel() or
// Close's sweep; neither loses anything a subscriber asked for.
func (h *Hub) Dropped() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dropped
}

// SubscriberCount reports how many lifecycle subscribers are currently
// registered: mainly a synchronization point for tests that must observe
// that a Subscribe has actually registered before acting on that ordering.
func (h *Hub) SubscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// State returns a snapshot of the fold so far, with the Seq it was folded
// at: the pairing Source.State documents. The returned RunState is an
// independent deep copy: the Hub keeps folding concurrently, and a caller
// applying further events onto the snapshot must not alias the Hub's own
// maps.
func (h *Hub) State() *api.RunState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneState(h.state)
}

// Seq reports the sequence number the hub has folded through so far:
// State().Seq without the deep-clone cost, for a caller that only needs
// the watermark (the /api/stream overflow heuristic).
func (h *Hub) Seq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state.Seq
}

// Done reports whether the run has reached a terminal state (RunInfo.Done)
// or the hub is closed: the cheap accessor for handleControl's precheck.
// Once true, NOTHING will ever read the control channel again: the
// engine's durable refusal goroutine (internal/engine/control.go) has
// already stopped by the time run.finished is folded here, so a request
// from this point on is answered immediately rather than queued for a
// reader that provably does not exist.
//
// A false answer is advisory: the run can finish between this call and the
// caller's next action, the residual race the engine's durable refusal
// exists to close.
func (h *Hub) Done() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed || h.state.Run.Done
}

// Closed reports whether Close has been called: narrower than Done, which
// is also true once the run finishes. Server.Close uses it to decide
// whether the bounded pre-drain of stream handlers is worth attempting:
// only when the hub itself is what is about to make every handler's ch
// ready, not merely when the run happens to be finished.
func (h *Hub) Closed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// cloneState deep-copies s so a caller can keep folding its own copy without
// racing Hub's own concurrent fold. A shallow copy would still alias every
// StepState's LogBytes map and its Needs/Handlers slices, which Apply
// mutates in place.
func cloneState(s *api.RunState) *api.RunState {
	out := &api.RunState{
		Seq:        s.Seq,
		ProtoMajor: s.ProtoMajor,
		ProtoMinor: s.ProtoMinor,
		Run:        s.Run,
		Steps:      make(map[string]*api.StepState, len(s.Steps)),
		Expansions: make(map[string]*api.ExpansionState, len(s.Expansions)),
		Handlers:   make(map[string]*api.HandlerState, len(s.Handlers)),
		Order:      append([]string(nil), s.Order...),
	}
	for id, st := range s.Steps {
		cp := *st
		cp.Needs = append([]string(nil), st.Needs...)
		cp.Handlers = append([]string(nil), st.Handlers...)
		if st.LogBytes != nil {
			cp.LogBytes = make(map[string]int64, len(st.LogBytes))
			for k, v := range st.LogBytes {
				cp.LogBytes[k] = v
			}
		}
		out.Steps[id] = &cp
	}
	for id, ex := range s.Expansions {
		cp := *ex
		cp.Children = append([]string(nil), ex.Children...)
		out.Expansions[id] = &cp
	}
	for id, hs := range s.Handlers {
		cp := *hs
		out.Handlers[id] = &cp
	}
	return out
}
