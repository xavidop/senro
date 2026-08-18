package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

// pollInterval is how often a following FileSource rechecks events.jsonl.
// Polling is imperceptible next to step runtimes and avoids a filesystem-
// notification dependency that behaves differently on darwin and linux.
const pollInterval = 50 * time.Millisecond

// ErrClosed reports that a FileSource was used after Close. Same shape as
// the eventlog package's closed guard so both match with errors.Is.
var ErrClosed = errors.New("source: use of a closed source")

// FileSource reads a run recorded to disk by the engine: events.jsonl plus
// the per-step log files under logs/. It never accepts Control (see
// ErrReadOnly).
type FileSource struct {
	dir    string
	follow bool

	mu     sync.Mutex
	closed bool
	// done, closed exactly once by Close, lets a Subscribe goroutine notice
	// Close even when its context is never cancelled; without it,
	// Subscribe(context.Background(), ...) then Close leaks the poller.
	done chan struct{}
}

// OpenFile opens the run recorded at dir. follow keeps Subscribe watching
// events.jsonl for later appends (a run still in progress); without it,
// Subscribe replays what's on disk and closes its channel.
func OpenFile(dir string, follow bool) (*FileSource, error) {
	if _, err := os.Stat(filepath.Join(dir, "events.jsonl")); err != nil {
		return nil, fmt.Errorf("source: open %s: %w", dir, err)
	}
	return &FileSource{dir: dir, follow: follow, done: make(chan struct{})}, nil
}

var _ Source = (*FileSource)(nil)

// State folds the whole ledger through api.RunState.Apply and returns it
// together with the Seq it was folded at.
func (fs *FileSource) State(ctx context.Context) (*api.RunState, error) {
	if err := fs.checkOpen(); err != nil {
		return nil, err
	}
	events, err := fs.readAll()
	if err != nil {
		return nil, err
	}
	st := api.NewRunState()
	for _, e := range events {
		if err := st.Apply(e); err != nil {
			// Apply only errors on a regressing seq: a corrupt ledger, not a
			// caller mistake.
			return nil, fmt.Errorf("source: %w", err)
		}
	}
	return st, nil
}

// readAll reads events.jsonl, tolerating a torn tail: ErrTruncated is what
// kill -9 leaves, and the events before the tear are still valid, so it is
// swallowed. Any other error is real corruption and is returned.
func (fs *FileSource) readAll() ([]api.Event, error) {
	events, err := eventlog.Read(filepath.Join(fs.dir, "events.jsonl"))
	if err != nil && !errors.Is(err, eventlog.ErrTruncated) {
		return nil, fmt.Errorf("source: %w", err)
	}
	return events, nil
}

// Subscribe streams events with Seq >= fromSeq (fromSeq inclusive, per
// Source.Subscribe). It replays what's on disk first; with follow it then
// polls for later appends until ctx is cancelled, the source is closed, or
// run.finished is delivered.
func (fs *FileSource) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	if err := fs.checkOpen(); err != nil {
		return nil, err
	}
	ch := make(chan api.Event)
	go fs.stream(ctx, fromSeq, ch)
	return ch, nil
}

func (fs *FileSource) stream(ctx context.Context, fromSeq uint64, ch chan<- api.Event) {
	defer close(ch)
	// next is the lowest Seq still owed. A "next wanted" watermark (not
	// "last delivered") keeps fromSeq itself inclusive without a fromSeq-1
	// underflow at 0.
	next := fromSeq

	for {
		events, err := fs.readAll()
		if err != nil {
			// No way to report a mid-stream read error on an event channel;
			// end delivery. State remains available to explain the stop.
			return
		}
		for _, e := range events {
			if e.Seq < next {
				// Every pass re-reads the whole file, so events below the
				// watermark are expected, not a fault.
				continue
			}
			select {
			case ch <- e:
				next = e.Seq + 1
			case <-ctx.Done():
				return
			case <-fs.done:
				return
			}
		}

		// Nothing follows run.finished: the engine writes it once, last.
		// Checked against the whole replay, not this pass's deliveries: a
		// caller resuming from beyond its seq skips it via e.Seq < next and
		// an in-loop check would miss it. Without this, a follow source on a
		// finished run polls forever and range-over-channel callers never
		// return. See TestFollowStopsAfterRunFinished.
		if len(events) > 0 && events[len(events)-1].Type == api.RunFinished {
			return
		}

		if !fs.follow {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-fs.done:
			return
		case <-time.After(pollInterval):
		}
	}
}

// Logs opens one step attempt's log stream and seeks to from. The caller
// owns the returned ReadCloser.
func (fs *FileSource) Logs(_ context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error) {
	if err := fs.checkOpen(); err != nil {
		return nil, err
	}

	path := eventlog.NewLogSet(fs.dir).Path(step, attempt, stream)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	if from > 0 {
		if _, err := f.Seek(from, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("source: %w", err)
		}
	}
	return f, nil
}

// Control always fails: a run on disk has no engine to act on it.
func (fs *FileSource) Control(_ context.Context, req api.Frame) (api.Frame, error) {
	if err := fs.checkOpen(); err != nil {
		return api.Frame{}, err
	}
	return api.Frame{}, fmt.Errorf("source: control %q: %w", req.Type, ErrReadOnly)
}

// Close releases the source. Idempotent; wakes any Subscribe goroutine
// still polling, even one whose context is never cancelled.
func (fs *FileSource) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.closed {
		return nil
	}
	fs.closed = true
	close(fs.done)
	return nil
}

func (fs *FileSource) checkOpen() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.closed {
		return fmt.Errorf("source: %w", ErrClosed)
	}
	return nil
}
