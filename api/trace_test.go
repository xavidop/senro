package api_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
)

// TestNewTraceIDIsThirtyTwoLowercaseHexCharacters pins the one detail that
// makes a trace ID interoperable: 16 bytes rendered as exactly 32 lowercase
// hex characters. A 31-character or uppercase value is not a trace ID that is
// slightly wrong, it is one every W3C-conformant consumer drops on the floor.
func TestNewTraceIDIsThirtyTwoLowercaseHexCharacters(t *testing.T) {
	for range 64 {
		id := api.NewTraceID()
		if len(id) != 32 {
			t.Fatalf("NewTraceID() = %q, want 32 characters, got %d", id, len(id))
		}
		if strings.Trim(id, "0123456789abcdef") != "" {
			t.Fatalf("NewTraceID() = %q, want lowercase hex only", id)
		}
		if !api.ValidTraceID(id) {
			t.Fatalf("NewTraceID() = %q, which ValidTraceID rejects", id)
		}
	}
}

// TestNewSpanIDIsSixteenLowercaseHexCharacters is TestNewTraceID's twin for
// the 8-byte identifier.
func TestNewSpanIDIsSixteenLowercaseHexCharacters(t *testing.T) {
	for range 64 {
		id := api.NewSpanID()
		if len(id) != 16 {
			t.Fatalf("NewSpanID() = %q, want 16 characters, got %d", id, len(id))
		}
		if strings.Trim(id, "0123456789abcdef") != "" {
			t.Fatalf("NewSpanID() = %q, want lowercase hex only", id)
		}
		if !api.ValidSpanID(id) {
			t.Fatalf("NewSpanID() = %q, which ValidSpanID rejects", id)
		}
	}
}

// TestNewIDsDoNotRepeat is the whole reason these are random rather than
// counted: two spans that share an ID are one span as far as any consumer is
// concerned, and a retried step producing the same ID twice is exactly the
// bug this guards.
func TestNewIDsDoNotRepeat(t *testing.T) {
	const n = 4096
	seen := make(map[string]bool, n)
	for range n {
		id := api.NewSpanID()
		if seen[id] {
			t.Fatalf("NewSpanID() returned %q twice in %d draws", id, n)
		}
		seen[id] = true
	}

	seen = make(map[string]bool, n)
	for range n {
		id := api.NewTraceID()
		if seen[id] {
			t.Fatalf("NewTraceID() returned %q twice in %d draws", id, n)
		}
		seen[id] = true
	}
}

// TestValidTraceIDRejectsEverythingThatIsNotOne enumerates the rejections the
// W3C spec requires, because "looks like hex" is not the contract. The
// all-zero value is the interesting one: it is the correct length, it is
// lowercase hex, and it is explicitly reserved as the INVALID value, which
// means an engine that emits it has told every consumer to discard the trace.
func TestValidTraceIDRejectsEverythingThatIsNotOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   string
	}{
		{"empty", ""},
		{"all zero", strings.Repeat("0", 32)},
		{"too short", strings.Repeat("a", 31)},
		{"too long", strings.Repeat("a", 33)},
		{"uppercase", strings.Repeat("A", 32)},
		{"mixed case", "A" + strings.Repeat("b", 31)},
		{"not hex", strings.Repeat("g", 32)},
		{"span ID length", strings.Repeat("a", 16)},
		{"dashes", "4bf92f35-77b3-4da6-a3ce-929d0e0e4736"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if api.ValidTraceID(tc.id) {
				t.Errorf("ValidTraceID(%q) = true, want false", tc.id)
			}
		})
	}

	if !api.ValidTraceID("4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Error("ValidTraceID rejected the W3C specification's own example trace ID")
	}
	// One non-zero nibble anywhere is enough: the reserved value is all-zero,
	// not nearly-all-zero.
	if !api.ValidTraceID(strings.Repeat("0", 31) + "1") {
		t.Error("ValidTraceID rejected a trace ID whose only non-zero nibble is the last")
	}
}

// TestValidSpanIDRejectsEverythingThatIsNotOne is the 8-byte twin.
func TestValidSpanIDRejectsEverythingThatIsNotOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   string
	}{
		{"empty", ""},
		{"all zero", strings.Repeat("0", 16)},
		{"too short", strings.Repeat("a", 15)},
		{"too long", strings.Repeat("a", 17)},
		{"uppercase", strings.Repeat("A", 16)},
		{"not hex", strings.Repeat("z", 16)},
		{"trace ID length", strings.Repeat("a", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if api.ValidSpanID(tc.id) {
				t.Errorf("ValidSpanID(%q) = true, want false", tc.id)
			}
		})
	}

	if !api.ValidSpanID("00f067aa0ba902b7") {
		t.Error("ValidSpanID rejected the W3C specification's own example parent ID")
	}
}

// TestParseTraceParentAcceptsTheSpecificationsOwnExample uses the exact
// header value printed in the W3C Trace Context recommendation, so this test
// fails if the field order is ever transposed.
func TestParseTraceParentAcceptsTheSpecificationsOwnExample(t *testing.T) {
	const header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	got, ok := api.ParseTraceParent(header)
	if !ok {
		t.Fatalf("ParseTraceParent(%q) = _, false, want true", header)
	}
	if got.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("TraceID = %q", got.TraceID)
	}
	if got.SpanID != "00f067aa0ba902b7" {
		t.Errorf("SpanID = %q", got.SpanID)
	}
	if got.Flags != 0x01 {
		t.Errorf("Flags = %#02x, want 0x01", got.Flags)
	}
	if !got.Sampled() {
		t.Error("Sampled() = false on a header whose sampled bit is set")
	}
	if got.String() != header {
		t.Errorf("String() = %q, want the header it was parsed from (%q)", got.String(), header)
	}
}

// TestParseTraceParentRejectsMalformedHeaders is the test the correctness
// note asks for by name. Every case here must produce ok == false, because
// the spec's rule is that a traceparent a vendor cannot fully understand is
// IGNORED, and senro's rule (see the engine) is that an ignored one means a
// fresh trace rather than a propagated wrong one.
func TestParseTraceParentRejectsMalformedHeaders(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
	}{
		{"empty", ""},
		{"whitespace only", "   "},
		{"no dashes at all", "004bf92f3577b34da6a3ce929d0e0e473600f067aa0ba902b701"},
		{"three fields", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7"},
		{"two fields", "00-4bf92f3577b34da6a3ce929d0e0e4736"},
		{"all-zero trace ID", "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},
		{"all-zero span ID", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"},
		{"short trace ID", "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01"},
		{"long trace ID", "00-4bf92f3577b34da6a3ce929d0e0e47366-00f067aa0ba902b7-01"},
		{"short span ID", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01"},
		{"uppercase trace ID", "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"},
		{"uppercase span ID", "00-4bf92f3577b34da6a3ce929d0e0e4736-00F067AA0BA902B7-01"},
		{"non-hex trace ID", "00-zzf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		{"version ff is forbidden", "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		{"uppercase version", "0A-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		{"one-character version", "0-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		{"flags too short", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1"},
		{"non-hex flags", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-xx"},
		{"trailing field on version 00", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra"},
		{"trailing dash on version 00", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-"},
		{"a sentence", "not a traceparent at all"},
		{"json", `{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := api.ParseTraceParent(tc.header)
			if ok {
				t.Errorf("ParseTraceParent(%q) = %+v, true, want false", tc.header, got)
			}
			if got != (api.TraceParent{}) {
				t.Errorf("ParseTraceParent(%q) returned %+v alongside false, want the zero value", tc.header, got)
			}
		})
	}
}

// TestParseTraceParentAcceptsAFutureVersionsExtraFields is the forward
// compatibility rule the spec spells out: a version above 00 may carry
// fields this build has never heard of, and the first three are still
// parsable. Refusing them would make senro stop continuing traces the day
// version 01 ships, which is the opposite of what a propagator is for.
func TestParseTraceParentAcceptsAFutureVersionsExtraFields(t *testing.T) {
	const header = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-whatever"

	got, ok := api.ParseTraceParent(header)
	if !ok {
		t.Fatalf("ParseTraceParent(%q) = _, false, want true", header)
	}
	if got.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || got.SpanID != "00f067aa0ba902b7" {
		t.Errorf("ParseTraceParent(%q) = %+v", header, got)
	}
}

// TestParseTraceParentIgnoresSurroundingWhitespace covers the transport
// rather than the value: a traceparent arrives through an HTTP header, a
// shell variable, or a CI job's export, and any of those can leave padding
// around it. Trimming padding is not accepting a malformed value; the value
// itself is still held to every rule above.
func TestParseTraceParentIgnoresSurroundingWhitespace(t *testing.T) {
	const header = "  00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\n"

	got, ok := api.ParseTraceParent(header)
	if !ok {
		t.Fatalf("ParseTraceParent(%q) = _, false, want true", header)
	}
	if got.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("TraceID = %q", got.TraceID)
	}
}

// TestParseTraceParentKeepsAnUnsampledFlagUnsampled matters because an
// exporter is entitled to drop everything in an unsampled trace, and losing
// the bit on the way in would turn a deliberate sampling decision upstream
// into a full-volume export downstream.
func TestParseTraceParentKeepsAnUnsampledFlagUnsampled(t *testing.T) {
	got, ok := api.ParseTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
	if !ok {
		t.Fatal("ParseTraceParent rejected a valid unsampled header")
	}
	if got.Sampled() {
		t.Error("Sampled() = true for trace-flags 00")
	}
	if got.Flags != 0 {
		t.Errorf("Flags = %#02x, want 0x00", got.Flags)
	}
}

// TestTraceFlagsRoundTrip pins the wire rendering of the flags byte, which
// travels in run.started's payload as two lowercase hex characters rather
// than as a boolean, so a flag this build does not know about survives a
// round trip through senro's ledger.
func TestTraceFlagsRoundTrip(t *testing.T) {
	for _, b := range []byte{0x00, 0x01, 0x02, 0x03, 0x0f, 0x10, 0xff} {
		s := api.FormatTraceFlags(b)
		if len(s) != 2 {
			t.Errorf("FormatTraceFlags(%#02x) = %q, want two characters", b, s)
		}
		if strings.ToLower(s) != s {
			t.Errorf("FormatTraceFlags(%#02x) = %q, want lowercase", b, s)
		}
		got, ok := api.ParseTraceFlags(s)
		if !ok || got != b {
			t.Errorf("ParseTraceFlags(%q) = %#02x, %v, want %#02x, true", s, got, ok, b)
		}
	}

	for _, bad := range []string{"", "0", "000", "xx", "0G", "0A"} {
		if _, ok := api.ParseTraceFlags(bad); ok {
			t.Errorf("ParseTraceFlags(%q) = _, true, want false", bad)
		}
	}
}

// TestTraceParentStringRendersAnInvalidContextAsEmpty stops the one mistake
// that would be worse than emitting nothing: rendering a zero-value
// TraceParent as "00-000...0-000...0-00", which is a syntactically perfect
// header carrying the two identifiers the spec reserves as invalid.
func TestTraceParentStringRendersAnInvalidContextAsEmpty(t *testing.T) {
	if s := (api.TraceParent{}).String(); s != "" {
		t.Errorf("TraceParent{}.String() = %q, want the empty string", s)
	}
	half := api.TraceParent{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736"}
	if s := half.String(); s != "" {
		t.Errorf("String() with no span ID = %q, want the empty string", s)
	}
}
