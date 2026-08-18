// Package sink defines the pipeline engine's one coupling to observers.
//
// Emit is non-blocking and never fails. Everything that could block (client
// fan-out, ring buffers, network writes) lives behind it. A pipeline with no
// observer pays nothing and starts no goroutines.
package sink

import (
	"io"
	"sync"

	"github.com/xavidop/senro/api"
)

// ControlRequest is an operation an observer asks the engine to perform.
type ControlRequest struct {
	ID       string
	Op       string
	ClientID string
	Args     map[string]string
	// Reply MUST be buffered with capacity at least 1: the scheduler answers
	// with a bare blocking send from its one control goroutine, so an
	// unbuffered or already-full channel stalls the whole run permanently.
	Reply chan<- ControlResponse
}

// ControlResponse is the engine's answer.
type ControlResponse struct {
	ID    string
	OK    bool
	Error string
}

// ReasonRunFinished is the ControlResponse.Error value for a request
// nothing is left to act on: the run is terminal, or no reader remains on
// the control channel.
//
// Here rather than duplicated privately because two independent producers
// must emit the exact same string: attachsrv answers with it the instant it
// can see nothing will read the request, and the engine answers with it for
// the residual race that check cannot close. A client must see one spelling
// of "you were too late".
const ReasonRunFinished = "run_finished"

// Sink observes a run. Implementations must not block in Emit.
type Sink interface {
	Emit(api.Event)
	Control() <-chan ControlRequest
}

// ShellRequest is one client asking to stand inside a step's workspaces:
// `senro shell`, or the TUI's 's' key, arriving from the attach server.
//
// It has its own channel rather than being a control operation, because of
// ordering: control requests are served one at a time from the scheduler's
// loop, which is what makes run.cancel idempotent with no lock at all. A
// shell is a connection somebody stands in for minutes, so serving one
// there would stop the run dead, and handing it off immediately would order
// the hand-off rather than the operation. The engine reads this channel
// from a goroutine of its own, and a session's I/O never runs on any
// goroutine the run's progress depends on.
type ShellRequest struct {
	// ID correlates the response, exactly as ControlRequest.ID does.
	ID string
	// ClientID is the attach connection this arrived on, server-assigned and
	// never client-supplied. It reaches the ledger via api.ShellOpenedBody.
	ClientID string
	// Step names the step whose workspaces the session stands in; the engine
	// validates it against the plan and refuses an unknown one.
	Step string
	// Cmd is the argv to run. Empty means the engine's default shell.
	Cmd []string

	// TTY asks for a session on a real pseudo-terminal rather than pipes: a
	// KIND the client asks for, not an upgrade the server applies, because a
	// terminal is one device whose output arrives on ONE stream and never
	// writes Stderr below. Silently substituting either way would cost job
	// control or merge two streams underneath the client. An executor that
	// cannot host one refuses rather than downgrades.
	TTY bool
	// Resize carries every window size after the first, for a TTY session,
	// and is nil otherwise. The producer closes it when the session ends.
	//
	// Initial is the size the terminal is created with; a zero one means the
	// client did not know, and the terminal is created without a size rather
	// than with a made-up one.
	Initial WinSize
	Resize  <-chan WinSize

	// Stdin, Stdout and Stderr are the client's byte streams, already
	// deframed and framed by the producer: the engine deals in plain streams
	// and knows nothing about the wire protocol carrying them.
	//
	// The producer owns them and MUST close them when the session ends:
	// nothing can interrupt a Read blocked on an arbitrary io.Reader, so an
	// unclosed Stdin parks a goroutine forever. A Read that FAILS is how the
	// engine learns a client is gone, so these must fail on a broken
	// connection rather than block.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// Reply MUST be buffered with capacity at least 1, as
	// ControlRequest.Reply documents. It receives exactly ONE value, when
	// the session has ENDED, or immediately on a refusal. There is no
	// separate "accepted" message: a client holding an open connection has
	// nothing else to do either way.
	Reply chan<- ShellResponse
}

// WinSize is a terminal's dimensions, in character cells. Restated rather
// than imported from internal/executor so this package keeps depending on
// nothing; the engine translates.
type WinSize struct {
	Cols uint16
	Rows uint16
}

// ShellResponse is how one session ended. OK reports whether a session ever
// ran, not whether its command succeeded: a shell exiting 1 is OK with
// ExitCode 1, while a refusal is not OK, with Error in the same short
// vocabulary control refusals use (unknown_step, run_not_active).
type ShellResponse struct {
	ID string
	OK bool
	// Session is the engine-assigned id, matching the ledger's shell.opened
	// and shell.closed. Empty for a refusal, which leaves no record at all:
	// a refusal changed nothing about the run.
	Session  string
	Error    string
	ExitCode int
}

// ShellHost is an optional interface a Sink may implement to carry
// interactive session requests to the engine, as Reporter carries an
// observer's own events back into the ledger. Optional rather than part of
// Sink, or every existing Sink would grow a method returning nil forever; a
// run observed by one that does not implement it simply has no shells.
type ShellHost interface {
	// Shells returns the channel the engine reads session requests from. It
	// may return nil, which the engine reads as "this observer hosts no
	// shells" and starts nothing for.
	Shells() <-chan ShellRequest
}

// Appender appends one event to the run's ledger and reports whether it
// landed. False means it did not and never will: the stream is sealed, or
// the event is not one a Sink may append (see the engine's runCore.report).
//
// A deliberate alias, not a defined type: a Sink implementing Reporter must
// satisfy both this and the public senro.Reporter, and Go method sets
// require identical parameter types. Two defined types would leave a
// notifier satisfying neither interface while compiling perfectly.
type Appender = func(api.Event) bool

// Reporter is an optional interface a Sink may implement to record its OWN
// events in the run's ledger, the only place an event is real. It exists
// for an observer that acts out in the world and whose success at doing so
// is itself a fact about the run (an outbound notifier). Ledger access is a
// real capability, so the engine restricts what may be written through it
// rather than trusting the caller (runCore.report).
//
// The engine calls SetAppender exactly once per run, before any Emit. An
// implementation may hold the function for the run's duration and call it
// from any goroutine, including after the run ends, where it returns false.
//
// It must NOT be called from inside Emit: Emit runs while the engine holds
// the lock making an append and its delivery atomic, and taking that lock
// from underneath it would deadlock the run.
type Reporter interface {
	SetAppender(Appender)
}

// FuncSink adapts a function to Sink. It has no control channel.
type FuncSink func(api.Event)

func (f FuncSink) Emit(e api.Event)               { f(e) }
func (f FuncSink) Control() <-chan ControlRequest { return nil }

type nop struct{}

// Nop is a sink that does nothing, for runs with no observer.
func Nop() Sink                            { return nop{} }
func (nop) Emit(api.Event)                 {}
func (nop) Control() <-chan ControlRequest { return nil }

// queueDepth bounds how far a slow observer may lag before it starts losing
// events. Deep enough to absorb a burst of log markers, shallow enough that a
// wedged sink cannot pin unbounded memory.
const queueDepth = 4096

type multi struct {
	queues []chan api.Event
	sinks  []Sink
	wg     sync.WaitGroup

	mu      sync.Mutex
	closed  bool
	dropped map[int]int
}

// Fanout is a Sink with a bounded queue in front of it, which is what Multi
// returns: the Sink contract plus the two things a queue owes its owner, a
// way to shut it down and a way to see what it lost.
//
// A named type rather than a type assertion, because an assertion that
// silently fails leaves a caller with no Close (a leaked goroutine per
// sink) or no drop accounting (a lossy observer nothing can see). Naming
// the contract makes it the compiler's problem instead.
type Fanout interface {
	Sink
	// Close closes every queue and waits for the workers to drain what is
	// already in them. Idempotent, and required: each sink's worker lives
	// until its queue closes, so an unclosed Fanout leaks one goroutine per
	// sink.
	Close() error
	// Dropped reports per-sink losses, keyed by position in the Multi call.
	Dropped() map[int]int
	// DroppedTotal is the same thing summed, for a caller checking after
	// every Emit, where a map allocation per event would not be free.
	DroppedTotal() int
}

// Multi fans an event out to several sinks.
//
// Each sink gets one worker goroutine and a bounded queue, so events arrive in
// the order they were emitted. That ordering is load-bearing: RunState.Apply
// treats a regressing sequence number as a lost-ordering error, so a sink that
// received events out of order could not fold them. A gap is survivable (the
// fold simply advances), which is why a full queue drops rather than blocks.
//
// Callers must Close the result when the run ends; see Fanout.Close.
func Multi(sinks ...Sink) Fanout {
	m := &multi{sinks: sinks, dropped: make(map[int]int)}
	for _, s := range sinks {
		q := make(chan api.Event, queueDepth)
		m.queues = append(m.queues, q)
		m.wg.Add(1)
		go func(s Sink, q <-chan api.Event) {
			defer m.wg.Done()
			for e := range q {
				func() {
					defer func() { _ = recover() }() // an observer must not kill a run
					s.Emit(e)
				}()
			}
		}(s, q)
	}
	return m
}

func (m *multi) Emit(e api.Event) {
	m.mu.Lock()
	if m.closed {
		// Dropping after close matches the drop-on-full policy: Emit must
		// never fail or panic, because the engine's correctness cannot
		// depend on whether anyone is watching.
		m.mu.Unlock()
		return
	}
	// The lock is held across the send so Close cannot close the channel
	// between check and send; the send is non-blocking, so it cannot stall.
	for i, q := range m.queues {
		select {
		case q <- e:
		default:
			m.dropped[i]++
		}
	}
	m.mu.Unlock()
}

// Close closes all sink queues and waits for workers to drain the events
// already in them. Idempotent.
func (m *multi) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	for _, q := range m.queues {
		close(q)
	}
	m.mu.Unlock()

	m.wg.Wait() // drain: workers finish what is already queued
	return nil
}

// Dropped reports how many events each sink lost to a full queue, so a slow
// observer is visible rather than silently lossy.
func (m *multi) Dropped() map[int]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[int]int, len(m.dropped))
	for k, v := range m.dropped {
		out[k] = v
	}
	return out
}

// DroppedTotal reports how many events this Multi has lost, summed over
// every sink, without allocating. Dropped answers "which observer is lossy"
// and allocates a map; this answers the hot-path question a notifier asks
// across every Emit to turn a drop into a notify.dropped event.
func (m *multi) DroppedTotal() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int
	for _, v := range m.dropped {
		n += v
	}
	return n
}

// Control returns the first non-nil control channel among the sinks.
func (m *multi) Control() <-chan ControlRequest {
	for _, s := range m.sinks {
		if c := s.Control(); c != nil {
			return c
		}
	}
	return nil
}

// Shells returns the first non-nil interactive-session channel among the
// sinks, matching Control's rule: the engine reads one, and two observers
// handing it sessions for one run is a conflict rather than a fan-out.
func (m *multi) Shells() <-chan ShellRequest {
	for _, s := range m.sinks {
		if h, ok := s.(ShellHost); ok {
			if c := h.Shells(); c != nil {
				return c
			}
		}
	}
	return nil
}

type RecordingSink struct {
	mu     sync.Mutex
	events []api.Event
}

// Recording returns a Sink that retains what it saw, for tests.
func Recording() *RecordingSink { return &RecordingSink{} }

func (r *RecordingSink) Emit(e api.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *RecordingSink) Control() <-chan ControlRequest { return nil }

func (r *RecordingSink) Events() []api.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]api.Event(nil), r.events...)
}
