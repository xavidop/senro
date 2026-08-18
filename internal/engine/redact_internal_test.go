package engine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestRedactPayloadReplacesAValueInAnyField is the class check: the same one
// call covers every payload type, so the test asserts against three unrelated
// shapes rather than against the one field that was reported.
func TestRedactPayloadReplacesAValueInAnyField(t *testing.T) {
	rc := &runCore{redact: redact.New(redact.Value{Label: "tok", Value: []byte("s3cr3t-value-here")})}

	for _, in := range []string{
		`{"cmd":["curl","--header","Authorization: s3cr3t-value-here"]}`,
		`{"state":"failed","error":"chdir /tmp/s3cr3t-value-here: no such file or directory"}`,
		`{"attempt":2,"reason":"exit status 1: s3cr3t-value-here rejected"}`,
	} {
		out := rc.redactPayload(json.RawMessage(in))
		if strings.Contains(string(out), "s3cr3t-value-here") {
			t.Errorf("the value survived in %s", out)
		}
		if !strings.Contains(string(out), redact.Placeholder) {
			t.Errorf("no placeholder in %s; this assertion was not looking at redacted output", out)
		}
		if !json.Valid(out) {
			t.Errorf("the redacted payload is not valid JSON: %s", out)
		}
	}
}

// TestRedactPayloadIsAnIdentityWhenThereIsNothingToDo pins the free path. The
// returned value must be the SAME slice, not a copy, because this runs on
// every event of every run and a pipeline with no secrets must pay nothing.
func TestRedactPayloadIsAnIdentityWhenThereIsNothingToDo(t *testing.T) {
	in := json.RawMessage(`{"state":"succeeded"}`)

	rc := &runCore{}
	if out := rc.redactPayload(in); &out[0] != &in[0] {
		t.Error("a nil redactor copied the payload")
	}
	rc = &runCore{redact: redact.New(redact.Value{Label: "tok", Value: []byte("absent-value-xyz")})}
	if out := rc.redactPayload(in); &out[0] != &in[0] {
		t.Error("a payload with no match was copied")
	}
	if rc.redactedPayloads.Load() != 0 {
		t.Errorf("redactedPayloads = %d with nothing redacted", rc.redactedPayloads.Load())
	}
}

// TestRedactPayloadDropsABodyItCouldNotKeepValid is the negative case for the
// one way this could produce output no reader can decode: a replacement that
// spans a JSON structural boundary. Vanishingly unlikely for a value of six
// bytes or more, and the answer must still be a body a fold can skip rather
// than a line that breaks every downstream parser of events.jsonl.
func TestRedactPayloadDropsABodyItCouldNotKeepValid(t *testing.T) {
	// The value straddles the closing quote and brace of the payload.
	rc := &runCore{redact: redact.New(redact.Value{Label: "tok", Value: []byte(`xxxxxx"}`)})}
	out := rc.redactPayload(json.RawMessage(`{"a":"yyxxxxxx"}`))
	if !json.Valid(out) {
		t.Fatalf("redactPayload produced invalid JSON: %s", out)
	}
	if strings.Contains(string(out), `xxxxxx"}`) {
		t.Errorf("the value survived: %s", out)
	}
	if string(out) != `{"redacted":true}` {
		t.Errorf("out = %s, want the documented fallback body", out)
	}
	if rc.redactedPayloads.Load() != 1 {
		t.Errorf("redactedPayloads = %d, want 1", rc.redactedPayloads.Load())
	}
}
