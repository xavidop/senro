package eventlog_test

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

func TestLogWriterTracksOffsets(t *testing.T) {
	ls := eventlog.NewLogSet(t.TempDir())
	defer func() { _ = ls.Close() }()

	w, err := ls.Writer("build/test[unit=api]", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if got := w.Offset(); got != 0 {
		t.Errorf("initial offset = %d, want 0", got)
	}
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := w.Offset(); got != 6 {
		t.Errorf("offset after 6 bytes = %d, want 6", got)
	}
	if _, err := w.Write([]byte("world\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := w.Offset(); got != 12 {
		t.Errorf("offset = %d, want 12", got)
	}
}

// Step IDs contain / and [] and cannot be path segments. The path must stay
// readable: debugging a run from disk means finding these by eye.
func TestLogPathIsSafeAndReadable(t *testing.T) {
	ls := eventlog.NewLogSet("/runs/01JQ")
	p := ls.Path("build/test[unit=services/api]", 2, api.StreamStdout)

	if strings.Count(p, "/") != strings.Count("/runs/01JQ/logs/X/2/stdout", "/") {
		t.Errorf("path %q has unexpected depth — the step ID must be one segment", p)
	}
	if !strings.HasSuffix(p, "/2/stdout") {
		t.Errorf("path %q must end in <attempt>/<stream>", p)
	}
	if !strings.Contains(p, "build") {
		t.Errorf("path %q should stay recognisable", p)
	}
}

func TestSeparateStreamsAndAttempts(t *testing.T) {
	dir := t.TempDir()
	ls := eventlog.NewLogSet(dir)
	defer func() { _ = ls.Close() }()

	out, _ := ls.Writer("a", 1, api.StreamStdout)
	errw, _ := ls.Writer("a", 1, api.StreamStderr)
	second, _ := ls.Writer("a", 2, api.StreamStdout)

	_, _ = out.Write([]byte("out"))
	_, _ = errw.Write([]byte("err!"))
	_, _ = second.Write([]byte("retry"))

	if err := ls.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Each attempt gets its own files, per the failure taxonomy: never reuse
	// an attempt's log, or a retry inherits the output that explained the
	// original failure.
	for _, tc := range []struct {
		step, want string
		attempt    int
		stream     string
	}{
		{"a", "out", 1, api.StreamStdout},
		{"a", "err!", 1, api.StreamStderr},
		{"a", "retry", 2, api.StreamStdout},
	} {
		b, err := os.ReadFile(ls.Path(tc.step, tc.attempt, tc.stream))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(b) != tc.want {
			t.Errorf("attempt %d %s = %q, want %q", tc.attempt, tc.stream, b, tc.want)
		}
	}
}

func TestWriterTruncatesAnExistingFile(t *testing.T) {
	// A path can pre-exist: a re-run into the same run directory, or a
	// process restart. Appending to a previous attempt's bytes would corrupt
	// every offset already recorded in the event stream.
	dir := t.TempDir()

	first := eventlog.NewLogSet(dir)
	w, err := first.Writer("a", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := w.Write([]byte("stale output from an earlier run")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := eventlog.NewLogSet(dir)
	w2, err := second.Writer("a", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if got := w2.Offset(); got != 0 {
		t.Errorf("offset on reopen = %d, want 0", got)
	}
	if _, err := w2.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, err := os.ReadFile(second.Path("a", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(b) != "new" {
		t.Errorf("file = %q, want %q — the old content must be truncated, not appended to", b, "new")
	}
}

func TestWriterAfterCloseErrors(t *testing.T) {
	dir := t.TempDir()
	ls := eventlog.NewLogSet(dir)

	if err := ls.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := ls.Writer("a", 1, api.StreamStdout)
	if err == nil {
		t.Fatal("Writer after Close should error, got nil")
	}
	// "we are shutting down" must be distinguishable from "the disk is full",
	// matchable with errors.Is.
	if !errors.Is(err, eventlog.ErrClosed) {
		t.Errorf("err = %v, want it to wrap eventlog.ErrClosed", err)
	}
}

// The engine closes a step's writers as the step ends. A closed writer left
// in the map would be handed back with a nil error: writes fail and
// Offset() leaks stale values into step.log.appended markers. Retries and
// handler steps re-enter the same key.
func TestWriterReopensAfterTheCachedWriterWasClosed(t *testing.T) {
	dir := t.TempDir()
	ls := eventlog.NewLogSet(dir)
	defer func() { _ = ls.Close() }()

	first, err := ls.Writer("a", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := first.Write([]byte("one\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := ls.Writer("a", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer after the cached writer was closed: %v", err)
	}
	if second == first {
		t.Fatal("Writer returned the same, already-closed writer — it must reopen")
	}
	// The reopen appends: truncating would invalidate every offset already
	// recorded in the event stream.
	if got := second.Offset(); got != 4 {
		t.Errorf("Offset on reopen = %d, want 4 — the offset must continue from what is on disk", got)
	}
	if n, err := second.Write([]byte("two\n")); err != nil || n != 4 {
		t.Fatalf("Write after reopen = (%d, %v), want (4, nil)", n, err)
	}
	if err := ls.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, err := os.ReadFile(ls.Path("a", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(b) != "one\ntwo\n" {
		t.Errorf("file = %q, want %q — a reopen must not discard the earlier attempt's bytes",
			b, "one\ntwo\n")
	}
}

// A write to a closed writer must fail with this package's own error shape,
// not os.File's bare "file already closed", which no caller can match.
func TestWriteToAClosedWriterIsGuarded(t *testing.T) {
	ls := eventlog.NewLogSet(t.TempDir())
	defer func() { _ = ls.Close() }()

	w, err := ls.Writer("a", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	n, err := w.Write([]byte("late"))
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
	if !errors.Is(err, eventlog.ErrClosed) {
		t.Errorf("err = %v, want it to wrap eventlog.ErrClosed", err)
	}
	if !strings.HasPrefix(err.Error(), "eventlog:") {
		t.Errorf("err = %q, want the package's own error prefix", err)
	}
	// The offset must not move for bytes that were never written.
	if got := w.Offset(); got != 0 {
		t.Errorf("Offset = %d, want 0", got)
	}
}

func TestLogSetCloseIsIdempotent(t *testing.T) {
	ls := eventlog.NewLogSet(t.TempDir())
	defer func() { _ = ls.Close() }()

	w, _ := ls.Writer("a", 1, api.StreamStdout)
	_, _ = w.Write([]byte("output"))

	if err := ls.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := ls.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}
}
