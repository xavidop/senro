package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/xavidop/senro/api"
)

// FallbackSource wraps a live source and the run's own directory on disk.
// While the live source answers, everything is served from it. The instant
// it learns the engine is gone (a terminal marker whose Reason is
// "run_ended", or a genuine transport failure, see isEngineGone), it falls
// back to disk, permanently, for State, Subscribe and Logs, and answers
// Control with ErrReadOnly from then on, exactly like FileSource.
//
// Two things that look like "the engine is gone" must never trigger
// fallback:
//
//   - A live, healthy server answering with its own refusal or status
//     error (a 410, a 404, a 500, its own ErrReadOnly). isEngineGone draws
//     this line for every method below.
//   - A mid-Subscribe disconnect whose Reason is "overflowed" or
//     "write_stalled", or a markerless close (no evidence either way):
//     relay retries the live source, bounded (see
//     maxConsecutiveNoProgressReconnects), before ever touching disk.
//     Treating these as run_ended would silently and permanently demote a
//     merely-paused client to a stale disk snapshot of a healthy run.
//
// This is what makes the offline debugger and the live attach client the
// same code path: a caller never has to notice, or branch on, the engine
// exiting mid-session; only Control stops working.
type FallbackSource struct {
	live Source
	dir  string

	mu       sync.Mutex
	closed   bool
	fellBack bool
	file     *FileSource
	fileErr  error

	// done, closed exactly once by Close, lets an in-flight relay goroutine
	// notice a Close whose caller's ctx is never cancelled (matching
	// FileSource.done). Without it, relay's backoff wait plus one doomed
	// reconnect could still be running after Close returned.
	done chan struct{}
}

// Fallback wraps live with disk fallback rooted at dir: the run directory
// the same engine that started live is (or was) writing to.
func Fallback(live Source, dir string) Source {
	return &FallbackSource{live: live, dir: dir, done: make(chan struct{})}
}

var _ Source = (*FallbackSource)(nil)

// isEngineGone reports whether err means the engine is no longer there to
// answer. A 410, 404, 500 or 403 all mean the engine is alive and talking,
// and must pass through unchanged, or a caller is handed a silent,
// permanent disk view of a run that is still running. Only a genuine
// transport failure (refused, reset, vanished) means departure.
//
// Classified by error type, not HTTP status: LiveSource has already turned
// any status into something else, and a transport failure never had one.
//
// The caller's own ctx cancellation or deadline is NOT engine-gone either,
// though it arrives via the same client.Do path: it is the caller giving
// up, and misclassifying it would demote a session over one slow request.
func isEngineGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EPIPE:
			return true
		}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}

// State serves live.State while reachable, and falls back to disk only on a
// genuine transport failure from it (see isEngineGone).
func (fs *FallbackSource) State(ctx context.Context) (*api.RunState, error) {
	if err := fs.checkOpen(); err != nil {
		return nil, err
	}
	if fs.isFallenBack() {
		disk, err := fs.diskSource()
		if err != nil {
			return nil, err
		}
		return disk.State(ctx)
	}

	st, err := fs.live.State(ctx)
	if err == nil || !isEngineGone(err) {
		// A live server answered (or refused); falling back here would
		// freeze the caller to a stale snapshot of a working engine.
		return st, err
	}
	fs.fallBack()
	disk, derr := fs.diskSource()
	if derr != nil {
		return nil, fmt.Errorf("source: live State failed (%v) and disk fallback failed: %w", err, derr)
	}
	return disk.State(ctx)
}

// maxConsecutiveNoProgressReconnects bounds how many reconnects in a row
// may end with zero events delivered before relay gives up on the live
// source for this Subscribe call.
//
// relay is an explicit for loop, not mutual recursion: the peer decides
// when a stream ends, so one that ends every stream immediately and
// markerless would otherwise grow the call stack until a fatal, on-demand
// stack overflow. See TestMarkerlessCloseDoesNotSpin.
//
// A reconnect that delivers at least one event resets the streak, so a
// flaky-but-productive connection keeps reconnecting indefinitely; only
// back-to-back empty reconnects exhaust the budget. A cap on futility, not
// a rate limit: reconnectBackoff bounds the rate.
const maxConsecutiveNoProgressReconnects = 5

// reconnectBackoff is the minimum wait before every SubscribeStream call
// after the first, productive or not. Pausing only after zero-event
// connections is not enough: a peer delivering exactly one event per
// connection, then closing markerless, counts as progress every cycle and
// would otherwise hammer the engine with a full HTTP request per spin. Any
// progress-based exemption resets on the same cycle and never pays the
// floor.
const reconnectBackoff = 100 * time.Millisecond

// Subscribe serves live.SubscribeStream while reachable, relaying its
// delivery onto the returned channel and, when the live side ends because
// the engine is gone, continuing seamlessly from disk wherever live left
// off: one in-flight Subscribe call survives the engine exiting mid-stream.
func (fs *FallbackSource) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	if err := fs.checkOpen(); err != nil {
		return nil, err
	}
	if fs.isFallenBack() {
		disk, err := fs.diskSource()
		if err != nil {
			return nil, err
		}
		return disk.Subscribe(ctx, fromSeq)
	}

	if lv, ok := fs.live.(subscribeStreamer); ok {
		events, end, err := lv.SubscribeStream(ctx, fromSeq)
		if err != nil {
			if !isEngineGone(err) {
				// e.g. ErrOverflow: the server is alive and says what to do
				// (re-snapshot, resubscribe from state.Seq+1). A disk
				// fallback here would freeze a busy client mid-catch-up.
				return nil, err
			}
			fs.fallBack()
			disk, derr := fs.diskSource()
			if derr != nil {
				return nil, fmt.Errorf("source: live Subscribe failed (%v) and disk fallback failed: %w", err, derr)
			}
			return disk.Subscribe(ctx, fromSeq)
		}
		out := make(chan api.Event)
		go fs.relay(ctx, lv, events, end, out, fromSeq)
		return out, nil
	}

	// A live Source without terminal-marker visibility: never happens in
	// production (Fallback is always built with a *LiveSource), honoured
	// for a hypothetical alternative implementation. The only fallback
	// trigger here is a transport failure from Subscribe itself; a clean
	// channel close is an ordinary end.
	ch, err := fs.live.Subscribe(ctx, fromSeq)
	if err != nil {
		if !isEngineGone(err) {
			return nil, err
		}
		fs.fallBack()
		disk, derr := fs.diskSource()
		if derr != nil {
			return nil, fmt.Errorf("source: live Subscribe failed (%v) and disk fallback failed: %w", err, derr)
		}
		return disk.Subscribe(ctx, fromSeq)
	}
	return ch, nil
}

// relay copies live's delivery onto out until live ends, then decides from
// the terminal marker (or its absence) whether to fall back to disk, end
// the call, or reconnect and keep serving the same out channel. An
// explicit for loop, not mutual recursion, so any number of reconnects
// runs at constant stack depth; see maxConsecutiveNoProgressReconnects and
// TestMarkerlessCloseDoesNotSpin.
func (fs *FallbackSource) relay(ctx context.Context, lv subscribeStreamer, live <-chan api.Event, end <-chan StreamEnd, out chan<- api.Event, fromSeq uint64) {
	defer close(out)

	resumeFrom := fromSeq
	noProgressStreak := 0

	for {
		delivered := 0
		for e := range live {
			select {
			case out <- e:
				resumeFrom = e.Seq + 1
				delivered++
			case <-ctx.Done():
				return
			case <-fs.done:
				return
			}
		}
		if ctx.Err() != nil {
			return // the caller gave up; nothing to fall back for
		}
		if delivered > 0 {
			// A delivering connection clears the no-progress budget: flaky
			// but productive sources keep reconnecting indefinitely.
			noProgressStreak = 0
		}

		marker, ok := <-end
		reason := ""
		if ok {
			reason = marker.Reason
			if reason == "" {
				// A server built before Reason existed: use the
				// Overflowed-only interpretation this client already shipped.
				if marker.Overflowed {
					reason = reasonOverflowed
				} else {
					reason = reasonRunEnded
				}
			}
		}

		switch {
		case ok && reason == reasonOverflowed:
			// The engine is presumably still running; this call is simply
			// done. The caller can re-snapshot and Subscribe again.
			return
		case ok && reason == reasonRunEnded:
			// Nothing more is ever coming live: the one case that justifies
			// moving to disk.
			fs.fallBackToDisk(ctx, out, resumeFrom)
			return
		}
		// reason is write_stalled (the server gave up on this connection,
		// not the engine) or no marker arrived (no evidence either way):
		// both get the benefit of the doubt and retry live, bounded, not
		// disk.

		if delivered == 0 {
			noProgressStreak++
			if noProgressStreak >= maxConsecutiveNoProgressReconnects {
				// Give up on live for THIS call without falling back:
				// ambiguous evidence must never become a permanent
				// demotion. The caller can Subscribe again, and other
				// methods still get their own shot at live.
				return
			}
		}

		// Unconditional, not nested in `delivered == 0`: a source that is
		// productive every cycle must still be rate limited. fs.done keeps
		// this wait from outliving Close; see the done field.
		select {
		case <-time.After(reconnectBackoff):
		case <-ctx.Done():
			return
		case <-fs.done:
			return
		}

		newLive, newEnd, err := lv.SubscribeStream(ctx, resumeFrom)
		if err != nil {
			if !isEngineGone(err) {
				return
			}
			fs.fallBackToDisk(ctx, out, resumeFrom)
			return
		}
		live, end = newLive, newEnd
	}
}

// fallBackToDisk marks the source fallen back and relays disk.Subscribe's
// delivery onto out until it too ends. The one path in relay's decision
// tree that actually means "the engine is gone."
func (fs *FallbackSource) fallBackToDisk(ctx context.Context, out chan<- api.Event, resumeFrom uint64) {
	fs.fallBack()
	disk, err := fs.diskSource()
	if err != nil {
		return
	}
	diskCh, err := disk.Subscribe(ctx, resumeFrom)
	if err != nil {
		return
	}
	for e := range diskCh {
		select {
		case out <- e:
		case <-ctx.Done():
			return
		case <-fs.done:
			return
		}
	}
}

// Logs serves live.Logs while reachable, and falls back to disk only on a
// genuine transport failure (see isEngineGone). A 404 (a step that never
// ran, an attempt never taken) is a live, healthy answer, not departure.
func (fs *FallbackSource) Logs(ctx context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error) {
	if err := fs.checkOpen(); err != nil {
		return nil, err
	}
	if fs.isFallenBack() {
		disk, err := fs.diskSource()
		if err != nil {
			return nil, err
		}
		return disk.Logs(ctx, step, attempt, stream, from)
	}

	rc, err := fs.live.Logs(ctx, step, attempt, stream, from)
	if err == nil || !isEngineGone(err) {
		return rc, err
	}
	fs.fallBack()
	disk, derr := fs.diskSource()
	if derr != nil {
		return nil, fmt.Errorf("source: live Logs failed (%v) and disk fallback failed: %w", err, derr)
	}
	return disk.Logs(ctx, step, attempt, stream, from)
}

// Control forwards to the live source; only a genuine transport failure
// (isEngineGone) triggers fallback, as with the other methods. Once fallen
// back, for any reason, Control answers ErrReadOnly without touching the
// live source again.
func (fs *FallbackSource) Control(ctx context.Context, req api.Frame) (api.Frame, error) {
	if err := fs.checkOpen(); err != nil {
		return api.Frame{}, err
	}
	if fs.isFallenBack() {
		return api.Frame{}, fmt.Errorf("source: control %q: %w", req.Type, ErrReadOnly)
	}

	res, err := fs.live.Control(ctx, req)
	if err == nil || !isEngineGone(err) {
		return res, err
	}
	fs.fallBack()
	return api.Frame{}, fmt.Errorf("source: control %q: %w", req.Type, ErrReadOnly)
}

// Close releases the live source and, if one was ever opened, the disk
// source, and wakes any relay goroutine still running (see the done
// field). Idempotent.
func (fs *FallbackSource) Close() error {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return nil
	}
	fs.closed = true
	close(fs.done)
	live := fs.live
	file := fs.file
	fs.mu.Unlock()

	var errs []error
	if err := live.Close(); err != nil {
		errs = append(errs, err)
	}
	if file != nil {
		if err := file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (fs *FallbackSource) checkOpen() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.closed {
		return fmt.Errorf("source: %w", ErrClosed)
	}
	return nil
}

func (fs *FallbackSource) isFallenBack() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.fellBack
}

func (fs *FallbackSource) fallBack() {
	fs.mu.Lock()
	fs.fellBack = true
	fs.mu.Unlock()
}

// diskSource lazily opens the on-disk FileSource on first fallback and
// returns the same one on every later call.
//
// The open-or-reuse check and the closed-guard share one lock acquisition
// so this can never race Close: if Close wins, diskSource refuses rather
// than opening a FileSource nothing will ever close; if diskSource wins,
// Close reads the up-to-date fs.file and closes it.
//
// follow=true: a cleanly shutting-down engine may still be flushing its
// last events, so keep watching rather than serving a snapshot one event
// short.
func (fs *FallbackSource) diskSource() (*FileSource, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.closed {
		return nil, fmt.Errorf("source: %w", ErrClosed)
	}
	if fs.file != nil {
		return fs.file, nil
	}
	if fs.fileErr != nil {
		return nil, fs.fileErr
	}
	f, err := OpenFile(fs.dir, true)
	if err != nil {
		fs.fileErr = fmt.Errorf("source: fallback: %w", err)
		return nil, fs.fileErr
	}
	fs.file = f
	return f, nil
}
