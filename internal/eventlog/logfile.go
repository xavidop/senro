package eventlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/xavidop/senro/internal/stepid"
)

// LogSet owns a run's per-step log files.
//
// Logs are files rather than event payloads so that a client can range-request
// scrollback instead of the server retaining a replay buffer, and so a 300-node
// fan-out does not push log bodies down the lifecycle channel.
type LogSet struct {
	dir string

	mu      sync.Mutex
	writers map[string]*LogWriter
	closed  bool
}

func NewLogSet(dir string) *LogSet {
	return &LogSet{dir: dir, writers: make(map[string]*LogWriter)}
}

// Path is the on-disk location of one stream of one attempt of one step.
// The step ID is percent-encoded into a single segment: it contains / and [],
// and it must stay readable for anyone debugging a run from disk.
func (ls *LogSet) Path(step string, attempt int, stream string) string {
	return filepath.Join(ls.dir, "logs", stepid.Encode(step), strconv.Itoa(attempt), stream)
}

// Writer returns the append-only writer for one stream, creating it on
// first use. Repeated calls return the same writer unless it has been
// closed, in which case it is evicted and reopened: the engine closes each
// step's writers as the step ends (bounding descriptors by MaxParallel),
// and a dead entry left in the map would hand a re-entering retry a writer
// whose Offset() leaks stale values into step.log.appended markers.
//
// A reopen appends with the offset seeded from disk: truncating would point
// every offset already recorded in the event stream at content that no
// longer exists.
func (ls *LogSet) Writer(step string, attempt int, stream string) (*LogWriter, error) {
	key := step + "\x00" + strconv.Itoa(attempt) + "\x00" + stream

	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		// Matches Ledger.Append's guard. Without it a goroutine racing run
		// teardown opens a file the LogSet has already stopped tracking, and
		// the descriptor leaks for the life of the process.
		return nil, fmt.Errorf("eventlog: %w: writer requested from a closed log set", ErrClosed)
	}

	reopen := false
	if w, ok := ls.writers[key]; ok {
		if !w.isClosed() {
			return w, nil
		}
		delete(ls.writers, key)
		reopen = true
	}

	p := ls.Path(step, attempt, stream)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if reopen {
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	f, err := os.OpenFile(p, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	var offset int64
	if reopen {
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("eventlog: %w", err)
		}
		offset = st.Size()
	}
	w := &LogWriter{f: f, offset: offset}
	ls.writers[key] = w
	return w, nil
}

func (ls *LogSet) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return nil
	}
	ls.closed = true
	var first error
	for _, w := range ls.writers {
		if err := w.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// LogWriter appends to one log file and tracks the byte offset, which is what
// a step.log.appended marker carries.
type LogWriter struct {
	mu     sync.Mutex
	f      *os.File
	offset int64
	closed bool
}

var _ io.WriteCloser = (*LogWriter)(nil)

// Write appends p. Writing to a closed writer is refused explicitly rather
// than left to os.File: that gives the error the same "eventlog:" shape as
// this package's other closed-guards and makes it matchable with
// errors.Is(err, ErrClosed), which "file already closed" is not.
func (w *LogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, fmt.Errorf("eventlog: %w: write to a closed log writer", ErrClosed)
	}
	n, err := w.f.Write(p)
	w.offset += int64(n)
	return n, err
}

// Offset is the number of bytes written so far: the position a subsequent
// write will start at.
func (w *LogWriter) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

func (w *LogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.f.Close()
}

// isClosed reports whether Close has already run. LogSet.Writer uses it to
// avoid handing a caller a writer that can no longer write.
func (w *LogWriter) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}
