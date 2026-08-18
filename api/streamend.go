package api

// StreamEndReason names precisely why GET /api/stream ended, when the
// server has more to say than a bare closed connection would communicate on
// its own: an overflow disconnect (the subscriber fell behind) and a clean
// shutdown (the run finished, or the hub closed) are otherwise
// byte-identical on the wire, and a client needs to tell them apart to know
// whether reconnecting is even meaningful.
type StreamEndReason string

const (
	// StreamEndRunEnded means the run finished or the server's hub closed.
	// There is nothing more to ever stream for this run; a client should
	// stop, not reconnect.
	StreamEndRunEnded StreamEndReason = "run_ended"
	// StreamEndOverflowed means this subscriber fell behind the server's
	// retained ring while the server kept running for everyone else.
	// Resume from StreamEndMarker.LastSeq+1, or re-snapshot via GET
	// /api/state if that itself now returns 410.
	StreamEndOverflowed StreamEndReason = "overflowed"
	// StreamEndWriteStalled means the server gave up writing to this one
	// connection because it did not drain in time. It says nothing about
	// whether the engine is still running, and must not be treated like
	// StreamEndRunEnded. No server shipped with this module currently sends
	// it (a stalled write just closes the connection without a marker, and a
	// client must handle both the same way), but a client decodes it like
	// any other value for a server that does.
	StreamEndWriteStalled StreamEndReason = "write_stalled"
)

// StreamEndMarker is the terminal NDJSON line GET /api/stream writes
// instead of merely closing the connection, whenever the server has more to
// say than a bare closed channel would communicate on its own.
//
// Deliberately not Event-shaped: a client recognises the end-of-stream
// marker from the StreamEnd field alone, which no Event carries, before
// ever attempting to decode the line as an Event.
//
// Every field is always present on a marker this module's own server sends
// (internal/attachsrv); Reason is a plain string, not StreamEndReason, so a
// client that has never heard of a future value still decodes the marker,
// the same forward-compatibility stance Event.Type takes.
type StreamEndMarker struct {
	// StreamEnd is what distinguishes this line from an ordinary Event.
	StreamEnd bool `json:"stream_end"`
	// LastSeq is the seq of the last event this connection actually
	// delivered, 0 if none ever were. 0 is ambiguous ("never got the
	// chance" versus "fell behind before its first delivery"), so do not
	// derive a resume point as LastSeq+1 when it is 0; use Hint's pairing,
	// which is correct in every case.
	LastSeq uint64 `json:"last_seq"`
	// Overflowed is kept for wire compatibility with a client built before
	// Reason existed. A client that understands Reason should prefer it:
	// Reason can express a value (like StreamEndWriteStalled) this bool
	// structurally cannot.
	Overflowed bool `json:"overflowed"`
	// Reason is one of the StreamEndReason values above, as a plain string.
	Reason string `json:"reason"`
	// Hint is the server's own resume advice: GET /api/state for a fresh
	// snapshot, then, if the run is not yet done, GET
	// /api/stream?from=<state.seq+1>. This is the pairing to resume from,
	// not LastSeq+1; see LastSeq's own doc for why that arithmetic is not
	// always meaningful.
	Hint string `json:"hint"`
}

// OverflowBody is the JSON body a server returns alongside a 410 Gone from
// GET /api/stream when the requested fromSeq has already been evicted from
// its retained ring: the synchronous counterpart to StreamEndMarker with
// Reason == StreamEndOverflowed, sent as a real HTTP status because the seq
// range is known to be permanently unavailable before any stream begins.
type OverflowBody struct {
	// Error is always OverflowError.
	Error string `json:"error"`
	// Hint is the same resume pairing StreamEndMarker.Hint documents.
	Hint string `json:"hint"`
}

// OverflowError is OverflowBody.Error's only value, exported so a client
// can compare against it without hardcoding the wire string.
const OverflowError = "lifecycle_overflow"
