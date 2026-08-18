package engine

// Redaction is a backstop, not an authorization boundary: every event
// passes through the redactor, so a payload carrying a secret's bytes
// through some bug elsewhere still never reaches the ledger or a client.
// This is a white-box test of that backstop in append() itself,
// independent of whether any current payload type puts a secret there.
// "Secret values never in cache keys, events, or logs" is unconditional
// and covers the persisted ledger, which is why redaction happens before
// rc.ledger.Append, not only before rc.sink.Emit.

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/sink"
)

// TestAppendRedactsAPayloadAndCountsIt is the mechanism test: rc.redact
// seeded with one secret, an event whose payload happens to carry that
// secret's bytes, and two assertions: the value must not survive into
// either the sink's copy or the ledger's own copy on disk, and
// redactedPayloads must report exactly one replacement (its own doc: "Read
// by internal tests").
func TestAppendRedactsAPayloadAndCountsIt(t *testing.T) {
	dir := t.TempDir()
	ledger, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = ledger.Close() }()

	rec := sink.Recording()
	red := redact.New(redact.Value{Label: "tok", Value: []byte("super-secret-token-value")})
	rc := &runCore{ledger: ledger, sink: rec, runID: "r1", redact: red}

	payload, err := json.Marshal(map[string]string{
		"note": "leaked super-secret-token-value here",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rc.emit(api.Event{Type: "test.event", Payload: payload})

	events := rec.Events()
	if len(events) != 1 {
		t.Fatalf("sink recorded %d events, want 1", len(events))
	}
	// The canary: without this, an empty or unmarked payload would make the
	// checks below pass for the wrong reason.
	if !bytes.Contains(events[0].Payload, []byte(redact.Placeholder)) {
		t.Fatalf("sink's payload has no placeholder at all: %q", events[0].Payload)
	}
	if bytes.Contains(events[0].Payload, []byte("super-secret-token-value")) {
		t.Errorf("the sink's event payload still contains the secret: %q", events[0].Payload)
	}

	onDisk, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	if len(onDisk) != 1 {
		t.Fatalf("ledger has %d events, want 1", len(onDisk))
	}
	if bytes.Contains(onDisk[0].Payload, []byte("super-secret-token-value")) {
		t.Errorf("the PERSISTED ledger still contains the secret: %q", onDisk[0].Payload)
	}

	if got := rc.redactedPayloads.Load(); got != 1 {
		t.Errorf("redactedPayloads = %d, want 1", got)
	}
}

// TestAppendWithNoSecretsNeverTouchesThePayload is the free-path guarantee:
// a nil rc.redact (a run with no secrets) must leave the payload exactly as
// given, byte for byte, and must never increment redactedPayloads.
func TestAppendWithNoSecretsNeverTouchesThePayload(t *testing.T) {
	dir := t.TempDir()
	ledger, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = ledger.Close() }()

	rec := sink.Recording()
	rc := &runCore{ledger: ledger, sink: rec, runID: "r1"} // redact left nil

	payload, err := json.Marshal(map[string]string{"note": "ordinary content"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rc.emit(api.Event{Type: "test.event", Payload: payload})

	events := rec.Events()
	if len(events) != 1 {
		t.Fatalf("sink recorded %d events, want 1", len(events))
	}
	if string(events[0].Payload) != string(payload) {
		t.Errorf("payload changed with a nil redactor: got %q, want %q", events[0].Payload, payload)
	}
	if got := rc.redactedPayloads.Load(); got != 0 {
		t.Errorf("redactedPayloads = %d, want 0", got)
	}
}

// TestAppendAfterSealNeverLeaksASecret is the negative case for an event
// emitted after the run has already finished: an orphaned goroutine (see
// emitFinal's own doc on the abandoned-step race) that emits once the stream
// is sealed must not be able to put a secret on disk or on the wire either,
// even though the drop happens for an unrelated reason (append's own
// post-seal no-op) rather than because redaction caught it here. Both must
// hold together: sealing something already-redacted before the append is
// pointless, and sealing something un-redacted would be a leak.
func TestAppendAfterSealNeverLeaksASecret(t *testing.T) {
	dir := t.TempDir()
	ledger, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = ledger.Close() }()

	rec := sink.Recording()
	red := redact.New(redact.Value{Label: "tok", Value: []byte("super-secret-token-value")})
	rc := &runCore{ledger: ledger, sink: rec, runID: "r1", redact: red}
	rc.seal()

	payload, err := json.Marshal(map[string]string{
		"note": "leaked super-secret-token-value here",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rc.emit(api.Event{Type: "test.event.after.seal", Payload: payload})

	if events := rec.Events(); len(events) != 0 {
		t.Fatalf("sink recorded %d events after seal, want 0: %+v", len(events), events)
	}
	onDisk, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	if len(onDisk) != 0 {
		t.Fatalf("ledger has %d events after seal, want 0: %+v", len(onDisk), onDisk)
	}
}
