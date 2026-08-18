// Package shellwire is the frame format an interactive session speaks over
// one hijacked connection, and the only place either side of it is defined.
//
// A session is the one thing senro's attach surface cannot express as a
// request and a response: it carries bytes both ways with no request
// boundary to end it, so it takes the connection over after an upgrade.
//
// Frames rather than a raw socket because three streams share the
// connection (stdin one way, stdout and stderr the other) plus a terminal
// result, and merging stdout and stderr is not an option for a debugging
// tool. The header is Docker's own multiplexed layout (one stream byte,
// three reserved zero bytes, a big-endian uint32 length), which
// internal/dockerd already reads.
//
// One package rather than two copies: attachsrv and internal/source both
// import this, and a wire format defined twice eventually disagrees with
// itself.
package shellwire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Protocol is the token both sides send in the HTTP Upgrade header, and the
// server requires it verbatim. Versioned in the token rather than
// negotiated: this is point-to-point between two builds of one tool, and a
// clear refusal beats a half-understood session.
const Protocol = "senro-shell/1"

// Stream identifiers. A frame's first byte says which of the session's
// streams its payload belongs to.
const (
	// StreamStdin carries what the operator typed, client to server.
	StreamStdin byte = 0
	// StreamStdout and StreamStderr carry the session's own output, server
	// to client, kept apart for the whole length of the connection.
	StreamStdout byte = 1
	StreamStderr byte = 2
	// StreamExit is the last frame the server ever sends: a JSON Exit body
	// saying how the session ended. A frame rather than simply closing,
	// because "the shell exited 0" and "your connection broke" are
	// otherwise identical from the client's side.
	StreamExit byte = 3
	// StreamStdinEOF is the operator's ^D: no more input, but the session is
	// still running. Always zero length, and a distinct stream id rather
	// than an empty StreamStdin frame, which would make "nothing right now"
	// and "nothing ever again" the same frame.
	StreamStdinEOF byte = 4

	// StreamResize carries a JSON WinSize whenever the operator's window
	// changes. Only on a TERMINAL session; a pipe-backed one has no window
	// and ignores it. On the same connection because it must be ORDERED
	// against the bytes around it: a resize overtaking the input that caused
	// it would redraw at the wrong size.
	StreamResize byte = 5
)

// A terminal session merges the two output streams, so a server hosting one
// sends everything on StreamStdout and never StreamStderr: a pty is one
// device, so there is no second stream to send. See executor.Terminal.

// HeaderSize is the fixed frame header: stream, three reserved bytes,
// length.
const HeaderSize = 8

// MaxPayload bounds one frame's payload: an ALLOCATION made on behalf of
// the peer, and a length field read off a socket and passed to make() is a
// memory-exhaustion primitive if nothing checks it. A writer with more than
// this to send splits it across frames.
const MaxPayload = 64 * 1024

// ErrFrameTooLarge reports a frame whose declared length exceeds
// MaxPayload: a protocol violation by the peer, worth telling apart from an
// ordinary transport failure.
var ErrFrameTooLarge = errors.New("shellwire: frame payload exceeds the maximum")

// ErrUnknownStream reports a frame naming a stream this build does not
// know. Refused rather than skipped: the alternative is silently discarding
// data the peer thought it was sending.
var ErrUnknownStream = errors.New("shellwire: unknown stream id")

// WinSize is a terminal's dimensions, as they travel on StreamResize. JSON
// rather than two big-endian uint16s, as api.Frame is JSON: the protocol
// stays debuggable with an ordinary tool, and a resize is far too rare for
// its encoding to matter.
type WinSize struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// Exit is the JSON payload of a StreamExit frame: how the session ended.
// The field names are the wire format. ExitCode is meaningful only when
// Error is empty, matching api.ShellClosedBody: a session killed because
// its client vanished has no exit status, and reporting 0 would say it
// succeeded.
type Exit struct {
	OK       bool   `json:"ok"`
	Session  string `json:"session,omitempty"`
	Error    string `json:"error,omitempty"`
	ExitCode int    `json:"exit_code,omitempty"`
}

// Writer frames writes onto one connection, serialising them so two streams
// writing at once cannot interleave halfway through a frame. The mutex is
// load-bearing: stdout and stderr are written by two goroutines by design,
// and a header split by another stream's header leaves the reader
// resynchronising onto garbage.
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

// WriteExit writes the terminal frame.
func (w *Writer) WriteExit(e Exit) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return w.WriteFrame(StreamExit, b)
}

// WriteResize sends the operator's new window size, refusing a zero
// dimension rather than sending it (as internal/ptyx refuses to apply one),
// so a client bug shows up in the client rather than as a terminal that
// mysteriously stops resizing.
func (w *Writer) WriteResize(ws WinSize) error {
	if ws.Cols == 0 || ws.Rows == 0 {
		return fmt.Errorf("shellwire: refusing to send a %dx%d window size", ws.Cols, ws.Rows)
	}
	b, err := json.Marshal(ws)
	if err != nil {
		return err
	}
	return w.WriteFrame(StreamResize, b)
}

// Stream returns an io.Writer that frames everything written to it onto
// stream id, splitting anything larger than MaxPayload across frames. It
// reports the caller's byte count, not the framed count: counting headers
// too would make io.Copy see more written than it supplied and return
// io.ErrShortWrite on every copy.
func (w *Writer) Stream(id byte) io.Writer { return &streamWriter{w: w, id: id} }

type streamWriter struct {
	w  *Writer
	id byte
}

func (s *streamWriter) Write(p []byte) (int, error) {
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

// Reader reads frames off one connection. Deliberately not safe for
// concurrent use: frames must be consumed in arrival order to stay framed
// at all, so a session has exactly one reader on each side.
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
// A clean end on a frame boundary is io.EOF; a connection cut mid-frame is
// io.ErrUnexpectedEOF. A peer that finished and one that died are different
// facts, and the engine's session teardown treats them differently (see
// clientStdin).
func (r *Reader) ReadFrame() (byte, []byte, error) {
	if _, err := io.ReadFull(r.r, r.header[:]); err != nil {
		return 0, nil, err
	}
	stream := r.header[0]
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

// Input adapts the client-to-server half of a session to an io.Reader, for
// an engine that knows nothing about frames.
//
// Its EOF distinction is the whole disconnect story: an explicit
// StreamStdinEOF frame becomes io.EOF, which the engine reads as ^D and
// answers by letting the shell exit by itself, while ANY other ending
// surfaces as a non-EOF error, which the engine reads as a vanished client
// and answers by killing the session. Collapsing the two gives either a ^D
// that kills rudely or an abandoned session that runs forever.
type Input struct {
	// onResize receives StreamResize frames. See OnResize.
	onResize func(WinSize)

	r    *Reader
	left []byte
	done bool
}

// NewInput returns an Input over r.
func NewInput(r *Reader) *Input { return &Input{r: r} }

// OnResize installs the handler for StreamResize frames, routed out of the
// input stream because a Read that sometimes returned a window size instead
// of bytes would be unusable as an io.Reader.
//
// Called on the frame-reading goroutine, so a handler must not block: it
// runs between one byte of the operator's typing and the next. Nil, the
// default, ignores them, which is what a pipe-backed session does.
func (in *Input) OnResize(f func(WinSize)) { in.onResize = f }

func (in *Input) Read(p []byte) (int, error) {
	if len(in.left) > 0 {
		n := copy(p, in.left)
		in.left = in.left[n:]
		return n, nil
	}
	if in.done {
		return 0, io.EOF
	}
	for {
		stream, payload, err := in.r.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Ended with no end-of-input frame: a peer that went away
				// rather than said goodbye, so NOT io.EOF. See Input's doc.
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		switch stream {
		case StreamStdin:
			if len(payload) == 0 {
				continue
			}
			n := copy(p, payload)
			if n < len(payload) {
				// A copy, not a reslice: the payload aliases the Reader's
				// buffer, which the next ReadFrame overwrites.
				in.left = append(in.left[:0], payload[n:]...)
			}
			return n, nil
		case StreamStdinEOF:
			in.done = true
			return 0, io.EOF
		case StreamResize:
			// Not input, so the loop goes round. A malformed one is dropped:
			// the size is advisory, and a terminal briefly at the wrong
			// width is not worth a disconnection.
			var ws WinSize
			if err := json.Unmarshal(payload, &ws); err == nil && in.onResize != nil {
				in.onResize(ws)
			}
			continue
		default:
			// A client sending output frames at the server is speaking the
			// protocol backwards: one side has a defect, and continuing
			// would hide it.
			return 0, fmt.Errorf("%w: %d from a client", ErrUnknownStream, stream)
		}
	}
}
