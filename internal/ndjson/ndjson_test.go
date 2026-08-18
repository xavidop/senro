package ndjson_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/ndjson"
)

// collect runs Read over body and returns every event it delivered.
func collect(t *testing.T, body string) ([]api.Event, api.StreamEndMarker, bool) {
	t.Helper()
	var got []api.Event
	marker, ok := ndjson.Read(strings.NewReader(body), func(e api.Event) bool {
		got = append(got, e)
		return true
	})
	return got, marker, ok
}

func TestReadDeliversEventsInOrder(t *testing.T) {
	body := `{"seq":1,"type":"run.started","run":"r1"}
{"seq":2,"type":"step.started","run":"r1","step":"build"}
{"seq":3,"type":"step.finished","run":"r1","step":"build"}
`
	got, _, ok := collect(t, body)
	if ok {
		t.Error("gotMarker = true, want false: this body has no terminal marker")
	}
	if len(got) != 3 {
		t.Fatalf("delivered %d events, want 3", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Errorf("event %d has seq %d, want %d: order was not preserved", i, e.Seq, i+1)
		}
	}
}

// The terminal marker is not an Event and must never reach the fold. A
// client that folded it would advance nothing and, worse, would fold a
// zero-valued Event whose empty Type the fold ignores but whose Seq of 0
// would look like a regression to the very next real event.
func TestTerminalMarkerIsNeverDeliveredAsAnEvent(t *testing.T) {
	body := `{"seq":7,"type":"step.started","run":"r1","step":"build"}
{"stream_end":true,"last_seq":7,"overflowed":true,"reason":"overflowed","hint":"GET /api/state"}
`
	got, marker, ok := collect(t, body)
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1: the marker line was forwarded to the fold", len(got))
	}
	if !ok {
		t.Fatal("gotMarker = false, want true")
	}
	if marker.Reason != string(api.StreamEndOverflowed) {
		t.Errorf("Reason = %q, want %q", marker.Reason, api.StreamEndOverflowed)
	}
	if marker.LastSeq != 7 || !marker.Overflowed {
		t.Errorf("marker = %+v, want LastSeq 7 and Overflowed true", marker)
	}
	if marker.Hint == "" {
		t.Error("Hint is empty: the server's own resume advice was dropped")
	}
}

// A line that announces itself as a marker but is not one must end the
// stream, not be handed on as an event. Decoding it straight into
// api.StreamEndMarker and trusting the resulting zero value would report a
// run_ended-shaped marker (empty Reason) that the server never sent.
func TestMalformedMarkerEndsTheStreamWithoutAMarker(t *testing.T) {
	body := `{"seq":1,"type":"run.started","run":"r1"}
{"stream_end":true,"last_seq":"not-a-number"}
{"seq":2,"type":"step.started","run":"r1","step":"build"}
`
	got, marker, ok := collect(t, body)
	if ok {
		t.Errorf("gotMarker = true (%+v), want false: the marker line was malformed", marker)
	}
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1: reading continued past a broken marker", len(got))
	}
}

// A truncated body is not the end of a run. Read says so by reporting no
// marker, which is what lets a caller tell "the connection died" apart from
// "the run finished", the distinction api.StreamEndReason exists for.
func TestTruncatedBodyReportsNoMarker(t *testing.T) {
	body := `{"seq":1,"type":"run.started","run":"r1"}
{"seq":2,"type":"step.`
	got, _, ok := collect(t, body)
	if ok {
		t.Error("gotMarker = true, want false: the body was cut off mid-object")
	}
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1", len(got))
	}
}

// onEvent returning false is a caller giving up, which is not evidence
// about the stream. Read must stop reading and must not claim a marker.
func TestOnEventStoppingTheReadReportsNoMarker(t *testing.T) {
	body := `{"seq":1,"type":"run.started","run":"r1"}
{"seq":2,"type":"step.started","run":"r1","step":"build"}
{"stream_end":true,"reason":"run_ended"}
`
	var got []api.Event
	marker, ok := ndjson.Read(strings.NewReader(body), func(e api.Event) bool {
		got = append(got, e)
		return false
	})
	if ok {
		t.Errorf("gotMarker = true (%+v), want false: the caller stopped before the end", marker)
	}
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1: Read kept going after onEvent said stop", len(got))
	}
}

// A reader that errors partway is the transport breaking, and is reported
// exactly like a truncated body: events up to the break, no marker.
func TestReaderErrorReportsNoMarker(t *testing.T) {
	r := io.MultiReader(
		strings.NewReader("{\"seq\":1,\"type\":\"run.started\",\"run\":\"r1\"}\n"),
		errReader{errors.New("connection reset")},
	)
	var got []api.Event
	_, ok := ndjson.Read(r, func(e api.Event) bool {
		got = append(got, e)
		return true
	})
	if ok {
		t.Error("gotMarker = true, want false")
	}
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1", len(got))
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// An event type this build has never heard of is still an event: the fold
// is what decides to ignore it, and a decoder that dropped it here would
// silently swallow the Seq advance that goes with it.
func TestUnknownEventTypeIsStillDelivered(t *testing.T) {
	body := `{"seq":1,"type":"something.from.the.future","run":"r1"}
`
	got, _, _ := collect(t, body)
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1", len(got))
	}
	if got[0].Seq != 1 {
		t.Errorf("Seq = %d, want 1", got[0].Seq)
	}
}
