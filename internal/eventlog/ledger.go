// Package eventlog writes a run's append-only ledger.
//
// The ledger is not a Sink. An event is assigned its sequence number and
// appended here synchronously before any observer sees it, and a write failure
// fails the run. Sinks are observers and may drop; the ledger may not.
package eventlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
)

// ErrTruncated reports that a log ended mid-record, normally a run killed
// mid-write. The events parsed before that point are returned and are valid.
var ErrTruncated = errors.New("eventlog: log ends in a truncated record")

// ErrClosed reports that a ledger, log set or log writer was used after it
// was closed. Every closed-guard in this package wraps it, so a caller can
// tell "we are shutting down, this event is expected to be dropped" apart
// from "the disk is full", matchable with errors.Is.
var ErrClosed = errors.New("eventlog: already closed")

// Ledger is the append-only event log for one run. Safe for concurrent use.
type Ledger struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	seq    uint64
	now    func() time.Time
	closed bool
	broken bool
}

// Open creates or truncates dir/events.jsonl.
func Open(dir string) (*Ledger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	return &Ledger{f: f, w: bufio.NewWriter(f), now: time.Now}, nil
}

// Append stamps the event with the next sequence number, the envelope version
// and a timestamp, writes it, and returns the stamped event for handing to
// sinks. The returned error is fatal to the run.
func (l *Ledger) Append(e api.Event) (api.Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return api.Event{}, fmt.Errorf("eventlog: %w: append on a closed ledger", ErrClosed)
	}
	if l.broken {
		return api.Event{}, errors.New("eventlog: append on a ledger that failed a prior write")
	}

	l.seq++
	e.Seq = l.seq
	e.V = api.Version
	if e.TS.IsZero() {
		e.TS = l.now().UTC()
	}

	b, err := json.Marshal(e)
	if err != nil {
		return api.Event{}, fmt.Errorf("eventlog: marshal seq %d: %w", e.Seq, err)
	}
	if _, err := l.w.Write(append(b, '\n')); err != nil {
		l.broken = true
		return api.Event{}, fmt.Errorf("eventlog: write seq %d: %w", e.Seq, err)
	}
	// Flush every append. A run emits thousands of events, not millions, so one
	// write syscall each is free, and without it Append reports success for
	// bytes still sitting in this process, which the engine then hands to
	// observers as though they were durable.
	if err := l.w.Flush(); err != nil {
		l.broken = true
		return api.Event{}, fmt.Errorf("eventlog: flush seq %d: %w", e.Seq, err)
	}
	return e, nil
}

// Seq reports the highest sequence number allocated.
func (l *Ledger) Seq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Close flushes and fsyncs. The ledger is the run's source of truth, so the
// data must be on disk before the process exits.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if err := l.w.Flush(); err != nil {
		// The flush error is already fatal and more actionable than
		// whatever Close itself returns; still attempt to close the fd so
		// it is not leaked, but the flush error is the one that matters.
		_ = l.f.Close()
		return fmt.Errorf("eventlog: flush: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		_ = l.f.Close()
		return fmt.Errorf("eventlog: sync: %w", err)
	}
	return l.f.Close()
}

// Read loads a whole event log. Used by offline replay and by golden tests.
// If the log ends mid-record (a torn final line, typically from kill -9), Read
// returns the events parsed before that point and an error wrapping ErrTruncated.
// A malformed line in the middle is returned as a hard error, but with events
// parsed before that point still returned.
func Read(path string) ([]api.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// Opened read-only for replay; a close error here cannot lose data this
	// reader ever wrote, so it is deliberately discarded.
	defer func() { _ = f.Close() }()

	var out []api.Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var lastUnmarshalErr error
	var lastUnmarshalLine int
	var lastUnmarshalErrorIsFinal bool

	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e api.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			lastUnmarshalErr = err
			lastUnmarshalLine = line
			lastUnmarshalErrorIsFinal = true
			continue
		}
		// We successfully parsed an event after an error, so the error wasn't final.
		lastUnmarshalErrorIsFinal = false
		out = append(out, e)
	}

	if lastUnmarshalErr != nil {
		if lastUnmarshalErrorIsFinal {
			return out, fmt.Errorf("%s:%d: %w", path, lastUnmarshalLine, ErrTruncated)
		}
		return out, fmt.Errorf("%s:%d: %w", path, lastUnmarshalLine, lastUnmarshalErr)
	}

	return out, sc.Err()
}
