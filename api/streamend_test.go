package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

func TestStreamEndMarkerIsPlainJSON(t *testing.T) {
	m := api.StreamEndMarker{
		StreamEnd: true,
		LastSeq:   42,
		Reason:    string(api.StreamEndRunEnded),
		Hint:      "GET /api/state for a fresh snapshot",
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"stream_end":true,"last_seq":42,"overflowed":false,"reason":"run_ended","hint":"GET /api/state for a fresh snapshot"}`
	if string(b) != want {
		t.Errorf("StreamEndMarker encoding drifted:\n got %s\nwant %s", b, want)
	}
}

// A client must decode a stream_end line without ever attempting to fold it
// as an ordinary Event: that is what the stream_end field itself exists to
// let a client detect before decoding further.
func TestStreamEndMarkerIsNotEventShaped(t *testing.T) {
	m := api.StreamEndMarker{StreamEnd: true, Reason: string(api.StreamEndOverflowed)}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var e api.Event
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("unmarshal into Event: %v", err)
	}
	// An Event has no stream_end field, so decoding a marker into one must
	// not accidentally produce something that looks like a real event.
	if e.Type != "" || e.Seq != 0 {
		t.Errorf("decoding a StreamEndMarker into Event produced a non-empty Event: %+v", e)
	}
}

func TestOverflowBodyIsPlainJSON(t *testing.T) {
	b, err := json.Marshal(api.OverflowBody{Error: api.OverflowError, Hint: "resubscribe from state.seq+1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"error":"lifecycle_overflow","hint":"resubscribe from state.seq+1"}`
	if string(b) != want {
		t.Errorf("OverflowBody encoding drifted:\n got %s\nwant %s", b, want)
	}
}

func TestStreamEndReasonValuesAreStable(t *testing.T) {
	// Wire protocol: a client compares against these literal strings.
	cases := map[api.StreamEndReason]string{
		api.StreamEndRunEnded:     "run_ended",
		api.StreamEndOverflowed:   "overflowed",
		api.StreamEndWriteStalled: "write_stalled",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("reason = %q, want %q", got, want)
		}
	}
}
