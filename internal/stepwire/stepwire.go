// Package stepwire is the protocol a remote step child speaks, and the only
// place either side of it is defined.
//
// A remote func step runs as a second process on the target: the
// coordinator's own binary, staged and re-entered as `senro-<digest> __step
// --state-fd 0`.
//
// Coordinator to child is one JSON State on stdin and nothing else: it is
// sent once, before the child does anything, so a second framing direction
// would have carried exactly one message for the protocol's life. Nothing
// sensitive is on the command line, because every process on that host can
// read ps(1); secrets arrive as PATHS, as with every other executor.
//
// Child to coordinator is length-prefixed frames on stdout, in the header
// shape internal/shellwire uses (Docker's multiplexed layout). Four
// streams: StreamHello once and first, carrying the child's own binary
// digest for the skew check; StreamStdout and StreamStderr, which the
// coordinator routes through the same redactor and offset-recording writers
// a local step's output goes through, making the logs indistinguishable;
// and StreamResult last, the verdict.
//
// No stream carries api.Event: a log event is a byte RANGE into a file the
// coordinator owns, so a child cannot produce one (it knows neither the
// offsets nor the ledger's sequence numbers).
//
// The child's stderr stays raw and unframed: it is the diagnostic channel
// of last resort for a child that dies before emitting a well-formed frame
// (a binary that will not execute, a runtime that panicked before main), and
// framing it would require the protocol to work in order to explain that it
// does not.
package stepwire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Protocol is the version token both sides carry: the coordinator in
// State.Protocol, the child in Hello.Protocol, each refusing a token it
// does not recognise. Not negotiated: the two ends are the SAME BUILD by
// construction, so a mismatch is not a version to negotiate with, it is the
// skew Hello.BinaryDigest exists to make fatal.
const Protocol = "senro-step/1"

// Stream identifiers. A frame's first byte says which of the child's streams
// its payload belongs to. See the package doc for why there are four.
const (
	// StreamHello is the child's first frame and appears exactly once: a JSON
	// Hello naming the protocol it speaks and the digest of the binary it is.
	StreamHello byte = 0
	// StreamStdout and StreamStderr carry the function's own output, kept
	// apart the whole way: a failure message interleaved into a line of
	// output is what a debugging tool exists to prevent.
	StreamStdout byte = 1
	StreamStderr byte = 2
	// StreamResult is the last frame the child ever sends: a JSON Result. A
	// frame rather than simply exiting, because "the function returned an
	// error" and "the ssh connection broke" are otherwise identical from the
	// coordinator's side, and only one is the step's verdict.
	StreamResult byte = 3
)

// HeaderSize is the fixed frame header: stream, three reserved bytes, length.
const HeaderSize = 8

// MaxPayload bounds one frame's payload: an ALLOCATION made on behalf of a
// process on another machine, and a length read off a pipe and passed to
// make() is a memory-exhaustion primitive if nothing checks it. Matches
// shellwire's bound; Stream splits anything longer.
const MaxPayload = 64 * 1024

// ErrFrameTooLarge reports a frame whose declared length exceeds MaxPayload:
// a protocol violation by the child, worth telling apart from a transport
// failure.
var ErrFrameTooLarge = errors.New("stepwire: frame payload exceeds the maximum")

// ErrUnknownStream reports a frame naming a stream this build does not
// know. Refused rather than skipped: the alternative is silently discarding
// data the other side thought it was sending.
var ErrUnknownStream = errors.New("stepwire: unknown stream id")

// Hello is the JSON payload of the StreamHello frame.
//
// BinaryDigest is the point of the frame: the coordinator staged a binary
// of a known digest, and the child reports what it actually IS, from its own
// os.Executable(). A mismatch means the file at that path is not the file
// the coordinator put there, and aborts the step, because silent fleet skew
// produces failures that are unreproducible by construction.
type Hello struct {
	Protocol     string `json:"protocol"`
	BinaryDigest string `json:"binary_digest"`
	Platform     string `json:"platform"`
	PID          int    `json:"pid,omitempty"`
}

// Result is the JSON payload of the StreamResult frame: the function's
// verdict, in the two-part shape executor.Sandbox.Run reports.
//
// Panicked and Infra are the distinctions an exit code cannot carry: a
// panicked step settles as api.StatePanicked and is never retried, and a
// function that wrapped executor.ErrInfra is saying retry.OnInfra should
// match it. TimedOut says the child stopped itself on its own deadline
// rather than the coordinator giving up on it.
type Result struct {
	Exit     int    `json:"exit"`
	Error    string `json:"error,omitempty"`
	Panicked bool   `json:"panicked,omitempty"`
	Infra    bool   `json:"infra,omitempty"`
	TimedOut bool   `json:"timed_out,omitempty"`
}

// State is the document the coordinator writes to the child's stdin: one
// attempt of one func step, in full.
//
// Every path in here is a path on the TARGET. A secret VALUE never appears,
// for the reason it never appears in a command argument: the engine's rule
// is that a value reaches a step as a file and nothing else.
//
// There is no Env field: a func step is refused Env at build time (see
// plan.nodeShape), and the one variable a remote child does receive,
// TRACEPARENT, arrives in the process environment the executor launches it
// with.
type State struct {
	Protocol string `json:"protocol"`
	RunID    string `json:"run_id"`
	StepID   string `json:"step_id"`
	Attempt  int    `json:"attempt"`

	Func   string          `json:"func"`
	Params json.RawMessage `json:"params,omitempty"`

	// Workspaces maps a mount's name to the directory it was realized at on
	// the target, which is what ctx.Workspace(name) reports over there.
	Workspaces map[string]string `json:"workspaces,omitempty"`
	// Secrets maps a secret's name to the path of the file holding it on the
	// target, which is what ctx.Secret(name) reports over there.
	Secrets map[string]string `json:"secrets,omitempty"`

	// TimeoutMS is the remaining timeout as a DURATION, not a deadline: a
	// wall-clock deadline would be read against the target's own clock, and
	// two machines' clocks agreeing is not something a build tool assumes.
	// Zero means the step declared no timeout.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// Writer frames writes onto one stream, serialising them so two of the
// child's streams cannot interleave halfway through a frame. The mutex is
// load-bearing: a function may write ctx.Stdout() and ctx.Stderr() from two
// goroutines while the deadline timer writes a Result from a third, and a
// split header leaves the coordinator resynchronising onto garbage.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriter returns a Writer framing onto w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// WriteFrame writes one frame. A payload longer than MaxPayload is refused
// rather than truncated or split here: splitting is Stream's job, where the
// caller's own boundaries do not matter, and truncating would lose bytes
// silently.
func (w *Writer) WriteFrame(stream byte, payload []byte) error {
	if !knownStream(stream) {
		return fmt.Errorf("%w: %d", ErrUnknownStream, stream)
	}
	if len(payload) > MaxPayload {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(payload))
	}
	var header [HeaderSize]byte
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.w.Write(payload)
	return err
}

// WriteHello writes the child's first frame.
func (w *Writer) WriteHello(h Hello) error { return w.writeJSON(StreamHello, h) }

// WriteResult writes the child's terminal frame.
func (w *Writer) WriteResult(r Result) error { return w.writeJSON(StreamResult, r) }

func (w *Writer) writeJSON(stream byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.WriteFrame(stream, b)
}

// Stream returns an io.Writer that frames everything written to it onto
// stream id, splitting anything larger than MaxPayload across frames. It
// reports the caller's byte count, not the framed count: counting headers
// too would make io.Copy return io.ErrShortWrite on every copy.
func (w *Writer) Stream(id byte) io.Writer { return &streamWriter{w: w, id: id} }

type streamWriter struct {
	w  *Writer
	id byte
}

func (s *streamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for {
		chunk := p[written:]
		if len(chunk) > MaxPayload {
			chunk = chunk[:MaxPayload]
		}
		if err := s.w.WriteFrame(s.id, chunk); err != nil {
			return written, err
		}
		written += len(chunk)
		if written >= len(p) {
			return written, nil
		}
	}
}

// Reader reads frames off the child's stdout. Deliberately not safe for
// concurrent use: frames must be consumed in arrival order to stay framed
// at all, so there is exactly one reader.
type Reader struct {
	r      io.Reader
	header [HeaderSize]byte
	buf    []byte
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, buf: make([]byte, MaxPayload)}
}

// ReadFrame reads the next frame and returns its stream id and payload. The
// payload is valid only until the next call: it aliases an internal buffer,
// so a caller that keeps it must copy.
//
// A clean end on a frame boundary is io.EOF; a stream cut mid-frame is
// io.ErrUnexpectedEOF. A child that finished and one that died are
// different facts, and the coordinator says a different thing about each.
func (r *Reader) ReadFrame() (byte, []byte, error) {
	if _, err := io.ReadFull(r.r, r.header[:]); err != nil {
		return 0, nil, err
	}
	stream := r.header[0]
	if !knownStream(stream) {
		return 0, nil, fmt.Errorf("%w: %d", ErrUnknownStream, stream)
	}
	n := binary.BigEndian.Uint32(r.header[4:])
	if n > MaxPayload {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	if n == 0 {
		return stream, nil, nil
	}
	if _, err := io.ReadFull(r.r, r.buf[:n]); err != nil {
		return 0, nil, err
	}
	return stream, r.buf[:n], nil
}

func knownStream(id byte) bool {
	switch id {
	case StreamHello, StreamStdout, StreamStderr, StreamResult:
		return true
	default:
		return false
	}
}

// WriteState writes the coordinator's half of the protocol: one JSON
// document, and then nothing. It does not close w, because the caller owns
// whatever w is.
func WriteState(w io.Writer, s State) error {
	s.Protocol = Protocol
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("stepwire: encoding the state for step %q: %w", s.StepID, err)
	}
	_, err = w.Write(b)
	return err
}

// ReadState reads the whole of r and decodes it as one State. An unknown
// field is a refusal, not something to ignore: the two ends are the same
// build, so an unrecognised field means the file on this host is not the
// file the coordinator thinks it staged.
func ReadState(r io.Reader) (State, error) {
	var s State
	dec := json.NewDecoder(io.LimitReader(r, maxStateBytes))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&s); err != nil {
		return State{}, fmt.Errorf("stepwire: reading the step state on stdin: %w", err)
	}
	if s.Protocol != Protocol {
		return State{}, fmt.Errorf(
			"stepwire: this binary speaks %s and was handed a state document declaring %q; "+
				"the coordinator staged a binary that is not this one",
			Protocol, s.Protocol)
	}
	if strings.TrimSpace(s.Func) == "" {
		return State{}, errors.New("stepwire: the step state names no func to run")
	}
	return s, nil
}

// maxStateBytes bounds the document ReadState will read, as MaxPayload
// bounds a frame: a length somebody else controls, read into memory in one
// piece. A func step's parameters are a plan's worth of JSON, never a
// payload.
const maxStateBytes = 4 << 20
