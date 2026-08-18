package eventlog_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

func TestAppendStampsSeqAndVersion(t *testing.T) {
	dir := t.TempDir()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = l.Close() }()

	first, err := l.Append(api.Event{Type: api.RunStarted, Run: "01JQ"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if first.Seq != 1 {
		t.Errorf("first Seq = %d, want 1", first.Seq)
	}
	if first.V != api.Version {
		t.Errorf("V = %d, want %d", first.V, api.Version)
	}
	if first.TS.IsZero() {
		t.Error("TS must be stamped")
	}

	second, _ := l.Append(api.Event{Type: api.StepCreated, Step: "a"})
	if second.Seq != 2 {
		t.Errorf("second Seq = %d, want 2", second.Seq)
	}
}

func TestAppendIsDurableAndReadable(t *testing.T) {
	dir := t.TempDir()
	l, _ := eventlog.Open(dir)
	for i := 0; i < 3; i++ {
		if _, err := l.Append(api.Event{Type: api.StepCreated, Step: "a"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d events, want 3", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Errorf("event %d has Seq %d", i, e.Seq)
		}
	}
}

// The ledger is written from the scheduler's goroutines. A torn line or a
// duplicated seq under concurrency is a corrupted source of truth.
func TestAppendIsConcurrencySafe(t *testing.T) {
	dir := t.TempDir()
	l, _ := eventlog.Open(dir)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = l.Append(api.Event{Type: api.StepCreated, Step: "a"})
		}()
	}
	wg.Wait()
	_ = l.Close()

	got, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != n {
		t.Fatalf("read %d events, want %d", len(got), n)
	}
	seen := make(map[uint64]bool, n)
	for _, e := range got {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
	}
	for i := uint64(1); i <= n; i++ {
		if !seen[i] {
			t.Errorf("missing seq %d", i)
		}
	}
}

func TestAppendIsVisibleBeforeClose(t *testing.T) {
	// The engine hands each event to observers as soon as Append returns. If
	// the bytes are still in this process, a client can render an event that
	// does not exist on disk, and a killed run loses everything.
	dir := t.TempDir()
	l, _ := eventlog.Open(dir)
	defer func() { _ = l.Close() }()

	for i := 0; i < 3; i++ {
		if _, err := l.Append(api.Event{Type: api.StepCreated, Step: "a"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	got, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("Read before Close: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("read %d events before Close, want 3 — appends must be durable as they happen", len(got))
	}
}

func TestAppendAfterCloseErrors(t *testing.T) {
	l, _ := eventlog.Open(t.TempDir())
	_, _ = l.Append(api.Event{Type: api.RunStarted})
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := l.Append(api.Event{Type: api.StepCreated}); err == nil {
		t.Error("appending to a closed ledger must error, not silently drop the event")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	l, _ := eventlog.Open(t.TempDir())
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

func TestReadRecoversTornTail(t *testing.T) {
	// A truncated final line is exactly what kill -9 mid-write produces.
	// Reading a killed run's log is precisely when you need it most.
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")

	// Write two good events and one truncated event
	l, _ := eventlog.Open(dir)
	_, _ = l.Append(api.Event{Type: api.RunStarted})
	_, _ = l.Append(api.Event{Type: api.StepCreated, Step: "a"})
	_ = l.Close()

	// Manually truncate the file mid-record
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.Write([]byte(`{"V":1,"Seq":3,"Type":"step_created","Step":"truncated`))
	_ = f.Close()

	got, err := eventlog.Read(path)
	if err == nil {
		t.Error("Read should return an error for a torn tail")
	}
	if !errors.Is(err, eventlog.ErrTruncated) {
		t.Errorf("error should be ErrTruncated, got %v", err)
	}
	if len(got) != 2 {
		t.Errorf("read %d events, want 2 before the truncated line", len(got))
	}
}

func TestReadFailsOnMidFileMalformation(t *testing.T) {
	// A malformed line in the middle is corruption, not truncation.
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")

	// Manually create a file with: good event, corrupted line, good event
	f, _ := os.Create(path)
	_, _ = f.Write([]byte(`{"V":1,"Seq":1,"Type":"run_started"}`))
	_, _ = f.Write([]byte("\n"))
	_, _ = f.Write([]byte(`{not valid json}`))
	_, _ = f.Write([]byte("\n"))
	_, _ = f.Write([]byte(`{"V":1,"Seq":2,"Type":"step_created","Step":"after"}`))
	_, _ = f.Write([]byte("\n"))
	_ = f.Close()

	_, err := eventlog.Read(path)
	if err == nil {
		t.Error("Read should return an error for malformed JSON")
	}
	if errors.Is(err, eventlog.ErrTruncated) {
		t.Error("mid-file malformation should not be ErrTruncated")
	}
}
