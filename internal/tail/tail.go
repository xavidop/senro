// Package tail is the client half of the attach protocol's resume
// contract: snapshot the run, tail from the snapshot's sequence number, and
// recover when the server's retained ring moves past you.
//
// Deliberately not part of internal/source: LiveSource speaks this protocol
// over net/http, which costs a wasm binary roughly 7MB that a browser
// downloads on every load. This package holds only the transport-free part
// (request sequence, status rules, stream-end handling) behind a one-method
// Getter, so the browser hands it fetch and a test hands it net/http, both
// exercising one copy of the rules.
//
// It holds no opinion about what an event MEANS: every event goes to
// api.RunState.Apply, the one fold every client shares. It lives outside
// internal/webui because nothing here is web-specific; when the TUI grows
// overflow recovery, this is the loop it should grow, not a second one.
package tail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/ndjson"
	"github.com/xavidop/senro/internal/stepid"
)

// ErrOverflow reports that a resume point is older than what the server's
// hub retains: GET /api/stream answering 410 Gone. The same condition
// internal/source.ErrOverflow names, restated so this package need not
// import net/http to recognise it.
//
// Reported for a 410 and nothing else: Run's recovery hangs off it, and a
// 500 reported as overflow would spin a silent re-snapshot loop against a
// broken server.
var ErrOverflow = errors.New("tail: the server's retained ring has moved past this resume point")

// StatePath is GET /api/state: the O(1) snapshot of the whole fold.
const StatePath = "/api/state"

// StreamPath builds GET /api/stream's URL for a resume point. fromSeq is
// snapshot.Seq+1, always: the pairing that makes attaching a snapshot plus
// a tail rather than a full replay (see source.Source).
func StreamPath(fromSeq uint64) string {
	return "/api/stream?from=" + strconv.FormatUint(fromSeq, 10)
}

// LogPath builds GET /api/logs/{step}'s URL for one attempt's stream from a
// byte offset. The step id goes through stepid.Encode (as LiveSource.Logs
// does): a raw id with slashes or brackets is not the path segment the
// server routes on.
func LogPath(step string, attempt int, stream string, from int64) string {
	q := url.Values{}
	q.Set("attempt", strconv.Itoa(attempt))
	q.Set("stream", stream)
	if from > 0 {
		q.Set("from", strconv.FormatInt(from, 10))
	}
	return "/api/logs/" + stepid.Encode(step) + "?" + q.Encode()
}

// Getter issues one GET against an attach server: the only thing in this
// package that touches a transport.
//
// The implementation owns authentication, deliberately: the browser
// client's page never receives a credential (internal/webui holds the
// bearer token and adds it to forwarded requests), so putting one in this
// interface would mean the browser had to hold it.
//
// The caller owns body and must close it, on every non-nil return, whatever
// the status; a Getter must not close it itself.
type Getter interface {
	Get(ctx context.Context, path string) (status int, body io.ReadCloser, err error)
}

// Backend is the two requests a resuming client makes. HTTPBackend is what
// every real client uses; the interface lets tests drive Run's decision
// tree with a fake that fails or overflows on cue.
type Backend interface {
	// State fetches the whole fold as the server currently holds it.
	State(ctx context.Context) (*api.RunState, error)
	// Stream opens the NDJSON event stream from fromSeq, or reports
	// ErrOverflow if that resume point has aged out of the server's ring.
	// The caller owns the returned reader.
	Stream(ctx context.Context, fromSeq uint64) (io.ReadCloser, error)
}

// HTTPBackend is Backend over any Getter, holding the attach server's
// status rules: a 410 from the stream endpoint is an overflow and nothing
// else is; any other non-200 is a plain failure with a bounded body prefix.
type HTTPBackend struct {
	Getter Getter
}

var _ Backend = (*HTTPBackend)(nil)

// errorBodyLimit bounds how much of a failing response is read into an
// error message: reading whatever was handed over would be a memory
// exhaustion primitive for anything that could answer the port.
const errorBodyLimit = 4096

// State implements Backend.
func (b *HTTPBackend) State(ctx context.Context) (*api.RunState, error) {
	status, body, err := b.Getter.Get(ctx, StatePath)
	if err != nil {
		return nil, fmt.Errorf("tail: GET %s: %w", StatePath, err)
	}
	defer func() { _ = body.Close() }()
	if status != 200 {
		return nil, fmt.Errorf("tail: GET %s: %w", StatePath, statusError(status, body))
	}
	// NewRunState, not a bare api.RunState, so the maps are non-nil even
	// when the snapshot omitted them: a renderer reading Steps before the
	// first event should not have to tolerate nil.
	st := api.NewRunState()
	if err := json.NewDecoder(body).Decode(st); err != nil {
		return nil, fmt.Errorf("tail: decode %s: %w", StatePath, err)
	}
	return st, nil
}

// Stream implements Backend.
func (b *HTTPBackend) Stream(ctx context.Context, fromSeq uint64) (io.ReadCloser, error) {
	path := StreamPath(fromSeq)
	status, body, err := b.Getter.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("tail: GET %s: %w", path, err)
	}
	switch status {
	case 200:
		return body, nil
	case 410:
		// The body's api.OverflowBody Hint is the same resume pairing Run
		// already implements; nothing here reads it.
		_ = body.Close()
		return nil, fmt.Errorf("tail: GET %s: %w", path, ErrOverflow)
	default:
		err := statusError(status, body)
		_ = body.Close()
		return nil, fmt.Errorf("tail: GET %s: %w", path, err)
	}
}

// statusError turns a non-2xx response into an error carrying its status
// and a bounded prefix of its body. It does not close body; every caller
// already does.
func statusError(status int, body io.Reader) error {
	b, _ := io.ReadAll(io.LimitReader(body, errorBodyLimit))
	return fmt.Errorf("status %d: %s", status, trimSpace(string(b)))
}

// trimSpace is strings.TrimSpace, inlined: every import here is weight in a
// wasm binary a browser downloads.
func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && isSpace(s[start]) {
		start++
	}
	for end > start && isSpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// Reconnect is how long Run waits before every attempt after its first: a
// rate limit, not a retry policy, so a server that ends every subscription
// immediately (or 410s every resume point) is not hammered as fast as this
// loop can spin. internal/source's FallbackSource carries the same constant
// for the same reason. A var only so tests can shorten it.
var Reconnect = 100 * time.Millisecond

// MaxNoProgressRounds bounds how many snapshot-and-tail rounds in a row may
// fold zero events before Run gives up. It covers a stream that opens, says
// nothing and closes over and over, and a server that 410s every resume
// point it hands out: both otherwise spin forever while looking like the
// protocol working. A round that folds anything resets the count, so a
// flaky-but-productive connection reconnects indefinitely (as
// FallbackSource does): a bound on futility, not on reconnection.
const MaxNoProgressRounds = 5

// ErrNoProgress reports that Run gave up after MaxNoProgressRounds empty
// rounds. Its own error rather than nil: a clean end here would tell an
// operator the run finished when nobody ever said so.
var ErrNoProgress = errors.New("tail: no events could be folded after repeated attempts, and the stream never said why")

// Fold is where a client's state goes and how it is guarded.
//
// The locking rule: Run folds on its own goroutine while a renderer reads
// the same api.RunState on another, a plain data race without the lock, and
// the browser's single thread does not exempt it (the Go memory model does
// not care, and the same package drives host-side tests). Lock is held by
// Run around every touch of the state; a renderer holds it to read. Every
// callback below is invoked with the lock ALREADY HELD, so a callback must
// not take it again and must not block.
type Fold struct {
	// Lock is held around every mutation and both callbacks. Nil means the
	// caller guarantees nothing else ever looks at the state: true of a
	// test that reads only after Run returns, and of nothing else.
	Lock sync.Locker

	// OnSnapshot is called with a NEW RunState on every snapshot fetch: at
	// least once, and again after every overflow. The caller must REPLACE
	// what it was rendering, not merge: merging into a state that skipped
	// events would produce a run that never happened.
	OnSnapshot func(*api.RunState)

	// OnFold is called after every api.RunState.Apply, on the RunState
	// OnSnapshot last handed over. It must be cheap: "mark yourself dirty
	// and return". Rendering synchronously would apply backpressure to the
	// engine through the subscriber, the one thing a subscriber must never
	// do.
	OnFold func(*api.RunState)
}

func (f Fold) lock() {
	if f.Lock != nil {
		f.Lock.Lock()
	}
}

func (f Fold) unlock() {
	if f.Lock != nil {
		f.Lock.Unlock()
	}
}

// publish hands a fresh snapshot over under the lock.
func (f Fold) publish(st *api.RunState) {
	f.lock()
	defer f.unlock()
	if f.OnSnapshot != nil {
		f.OnSnapshot(st)
	}
}

// apply folds one event under the lock and reports the resume point the
// next request should use. The Seq read stays inside the same critical
// section as the mutation that produced it; reading after unlocking would
// be one more unsynchronised read.
func (f Fold) apply(st *api.RunState, e api.Event) (next uint64, err error) {
	f.lock()
	defer f.unlock()
	if err := st.Apply(e); err != nil {
		return 0, err
	}
	if f.OnFold != nil {
		f.OnFold(st)
	}
	return st.Seq + 1, nil
}

// resumePoint reads the snapshot's own sequence number under the lock, so
// even the first subscription's from value is not an unguarded read.
func (f Fold) resumePoint(st *api.RunState) uint64 {
	f.lock()
	defer f.unlock()
	return st.Seq + 1
}

// Run folds a whole run into a RunState and keeps folding until the run
// ends, ctx is cancelled, or something retrying cannot fix goes wrong.
//
// The sequence every client uses: GET /api/state; GET
// /api/stream?from=snapshot.Seq+1, folding through api.RunState.Apply; on
// overflow (a 410 up front or an "overflowed" marker mid-stream), back to
// the snapshot. Re-snapshot, not resume from LastSeq+1, which would usually
// just 410 again for a client that has genuinely fallen behind.
//
// A returned nil means the server said "run_ended", the only conclusive
// end. See Fold for the locking rule a renderer must follow.
func Run(ctx context.Context, b Backend, f Fold) error {
	attempts := 0
	stalled := 0
	for {
		if err := pace(ctx, &attempts); err != nil {
			return err
		}

		st, err := b.State(ctx)
		if err != nil {
			return err
		}
		f.publish(st)

		done, folded, err := tailFrom(ctx, b, st, f, &attempts)
		if err != nil && !errors.Is(err, ErrOverflow) {
			return err
		}
		if done {
			return nil
		}
		// A round that ended without the run ending: overflow (remedied by
		// the re-snapshot above) or a stream that ended unexplained. Both go
		// round again; the only question is whether anything is happening.
		if folded > 0 {
			stalled = 0
			continue
		}
		stalled++
		if stalled >= MaxNoProgressRounds {
			return ErrNoProgress
		}
	}
}

// pace enforces Reconnect before every attempt after the first. Its own
// function so Run's outer loop and tailFrom's inner loop share one counter
// and one rate, rather than spinning twice as fast together.
func pace(ctx context.Context, attempts *int) error {
	defer func() { *attempts++ }()
	if *attempts == 0 {
		return nil
	}
	t := time.NewTimer(Reconnect)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tailFrom subscribes from st.Seq+1 and folds into st until the stream
// ends, reconnecting from wherever it got to when the end was inconclusive.
// done is true only for a "run_ended" marker; a wrapped ErrOverflow is the
// caller's cue to re-snapshot; folded counts events applied across every
// attempt, telling Run whether any progress happened.
func tailFrom(ctx context.Context, b Backend, st *api.RunState, f Fold, attempts *int) (done bool, folded int, err error) {
	from := f.resumePoint(st)
	for {
		body, err := b.Stream(ctx, from)
		if err != nil {
			return false, folded, err
		}

		// applyErr is captured out of the callback: ndjson.Read's contract
		// is "stop reading", not "report why".
		var applyErr error
		delivered := 0
		marker, gotMarker := ndjson.Read(body, func(e api.Event) bool {
			next, err := f.apply(st, e)
			if err != nil {
				applyErr = err
				return false
			}
			folded++
			delivered++
			// The fold's own Seq rather than e.Seq: where they differ (a
			// replayed event accepted idempotently), the fold's describes
			// what this client actually folded.
			from = next
			return ctx.Err() == nil
		})
		_ = body.Close()

		if applyErr != nil {
			// An out-of-order seq means client and server no longer agree on
			// the run's order; folding on would produce a state that never
			// existed, and reconnecting would deliver the same bytes again.
			return false, folded, fmt.Errorf("tail: %w", applyErr)
		}
		if ctx.Err() != nil {
			return false, folded, ctx.Err()
		}

		switch reasonOf(marker, gotMarker) {
		case string(api.StreamEndRunEnded):
			return true, folded, nil
		case string(api.StreamEndOverflowed):
			return false, folded, fmt.Errorf("tail: stream ended: %w", ErrOverflow)
		}

		// Either "write_stalled" (the server gave up on THIS connection, not
		// the run) or no marker at all (the connection died first). No
		// evidence either way; both get one more attempt from here.
		if err := pace(ctx, attempts); err != nil {
			return false, folded, err
		}
		if delivered > 0 {
			// This attempt delivered, so reconnect here from `from` and skip
			// the re-snapshot round trip. `delivered`, not the cumulative
			// `folded`: with the cumulative counter, a peer delivering one
			// event and then flapping forever would never reach a bound.
			continue
		}
		// Nothing delivered, nothing said. Hand the round back to Run, which
		// owns the one budget that decides to give up, so there is no
		// second, invisible retry policy underneath the documented one.
		return false, folded, nil
	}
}

// reasonOf normalises a terminal marker into the reason string to act on.
//
// A missing marker is NOT run_ended: reporting the ambiguous case as an
// ending would tell an operator a run finished when the connection merely
// broke. An empty Reason on a marker that DID arrive is a server predating
// api.StreamEndReason, and falls back to the Overflowed bool it did send
// (as FallbackSource does).
func reasonOf(marker api.StreamEndMarker, gotMarker bool) string {
	if !gotMarker {
		return ""
	}
	if marker.Reason != "" {
		return marker.Reason
	}
	if marker.Overflowed {
		return string(api.StreamEndOverflowed)
	}
	return string(api.StreamEndRunEnded)
}
