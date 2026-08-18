package api

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// This file is senro's whole implementation of W3C Trace Context
// (https://www.w3.org/TR/trace-context/): identifier generation, validation,
// and the traceparent header's two directions.
//
// Pure hex and string handling, so it fits the package's std-lib-only
// contract (nodeps_test.go). senro deliberately does NOT depend on
// go.opentelemetry.io/otel: an exporter that turns the event stream into
// spans is a Sink written in the consumer's own program, against their own
// otel version. See examples/otelexport and Event.TraceID.

// Identifier lengths, in hex characters rather than bytes, because hex is
// the only form these take outside this file: a trace ID is 16 bytes as 32
// lowercase hex characters, a span ID 8 bytes as 16. Exported so a consumer
// validating senro's output need not hardcode the numbers.
const (
	TraceIDLen = 32
	SpanIDLen  = 16
)

// TraceFlagSampled is bit 0 of the W3C trace-flags byte: the caller's
// recommendation that this trace be recorded. It is a recommendation and not
// an instruction, which is why senro carries it rather than acting on it. An
// exporter decides what to do about it.
const TraceFlagSampled byte = 0x01

// NewTraceID returns a fresh trace ID: 16 random bytes as TraceIDLen
// lowercase hex characters, never the all-zero value.
//
// One per run, and only when no valid inbound traceparent gave the run a
// trace to join: the ID must be stable across every event in the run, so it
// is drawn once and carried, never called per event.
func NewTraceID() string { return randomHexID(TraceIDLen / 2) }

// NewSpanID returns a fresh span ID: 8 random bytes as SpanIDLen lowercase
// hex characters, never the all-zero value.
//
// Once per span, and a span is a run or one ATTEMPT at a step: reusing an ID
// across attempts would collapse a failure and its successful retry into a
// single span that did both.
func NewSpanID() string { return randomHexID(SpanIDLen / 2) }

// randomHexID draws n random bytes and rejects the all-zero draw: the
// specification reserves all-zero to mean INVALID for both identifiers, so
// emitting it would tell every consumer to discard the trace while passing
// every length and character check.
//
// crypto/rand.Read is documented since Go 1.24 never to return an error (it
// crashes the program if OS randomness is unavailable), so there is no
// fallback path that would quietly produce guessable IDs.
func randomHexID(n int) string {
	b := make([]byte, n)
	for {
		_, _ = rand.Read(b)
		for _, c := range b {
			if c != 0 {
				return hex.EncodeToString(b)
			}
		}
	}
}

// ValidTraceID reports whether s is a trace ID a W3C-conformant consumer will
// accept: exactly TraceIDLen lowercase hex characters, not all zero.
//
// Lowercase is part of the contract: the specification's grammar admits
// lowercase hex only, and accepting uppercase here would propagate a trace
// nothing downstream will join.
func ValidTraceID(s string) bool { return validHexID(s, TraceIDLen) }

// ValidSpanID is ValidTraceID for the 8-byte identifier. See its doc.
func ValidSpanID(s string) bool { return validHexID(s, SpanIDLen) }

// validHexID checks length, alphabet and the all-zero rule in one pass.
func validHexID(s string, n int) bool {
	if len(s) != n {
		return false
	}
	nonZero := false
	for i := range n {
		switch c := s[i]; {
		case c == '0':
		case c > '0' && c <= '9', c >= 'a' && c <= 'f':
			nonZero = true
		default:
			return false
		}
	}
	return nonZero
}

// TraceParent is a parsed W3C traceparent: the trace a run belongs to, and
// the span within it that the run is a child of.
//
// SpanID is the header's "parent-id" field, named for what it is: the span
// that started us.
//
// Flags is the raw trace-flags byte rather than a bool, so a flag defined
// after this build shipped survives a round trip through senro's ledger. See
// TraceFlagSampled and Sampled.
type TraceParent struct {
	TraceID string
	SpanID  string
	Flags   byte
}

// Sampled reports whether the sampled bit is set.
func (p TraceParent) Sampled() bool { return p.Flags&TraceFlagSampled != 0 }

// Valid reports whether both identifiers are ones a consumer will accept.
func (p TraceParent) Valid() bool { return ValidTraceID(p.TraceID) && ValidSpanID(p.SpanID) }

// String renders p as a version-00 traceparent, and renders an INVALID p as
// the empty string.
//
// The empty string is the load-bearing half: formatting a zero-value
// TraceParent would produce "00-000...0-000...0-00", a syntactically perfect
// header carrying the identifiers the specification reserves to mean
// invalid. "" is correctly read as "no inbound trace"; that string is read
// as "a trace that must be discarded".
func (p TraceParent) String() string {
	if !p.Valid() {
		return ""
	}
	return "00-" + p.TraceID + "-" + p.SpanID + "-" + FormatTraceFlags(p.Flags)
}

// ParseTraceParent parses a W3C traceparent header value, reporting false for
// anything it cannot fully understand.
//
// False means IGNORE THIS, which the specification requires and which senro
// turns into "start a fresh trace" rather than "propagate it anyway": half a
// trace ID joined to a fresh span is a link to a trace that does not exist.
//
// Accepted:
//
//   - Surrounding whitespace, which is transport padding: the value arrives
//     from HTTP headers, shell exports and CI environments, any of which can
//     leave a newline on the end. The value inside is still held to every
//     rule below.
//   - A version above 00 with extra fields after the flags, per the
//     specification's forward-compatibility rule.
//
// Refused:
//
//   - Version ff, which the specification forbids outright.
//   - Version 00 with anything after the flags, which that version's fixed
//     55-character form does not allow.
//   - Either identifier being the wrong length, not lowercase hex, or the
//     reserved all-zero value.
//   - Trace-flags that are not exactly two lowercase hex characters.
func ParseTraceParent(s string) (TraceParent, bool) {
	s = strings.TrimSpace(s)

	parts := strings.Split(s, "-")
	if len(parts) < 4 {
		return TraceParent{}, false
	}

	version := parts[0]
	if len(version) != 2 || !isLowerHex(version) || version == "ff" {
		return TraceParent{}, false
	}
	// Version 00 is a fixed 55-character form: a fifth field is either a
	// newer version's payload mislabelled as 00, or a mangled value.
	if version == "00" && len(parts) != 4 {
		return TraceParent{}, false
	}

	flags, ok := ParseTraceFlags(parts[3])
	if !ok {
		return TraceParent{}, false
	}

	p := TraceParent{TraceID: parts[1], SpanID: parts[2], Flags: flags}
	if !p.Valid() {
		return TraceParent{}, false
	}
	return p, true
}

// FormatTraceFlags renders the trace-flags byte as two lowercase hex
// characters, which is the form it takes in a traceparent and in senro's own
// run.started payload.
func FormatTraceFlags(f byte) string { return hex.EncodeToString([]byte{f}) }

// ParseTraceFlags reads two lowercase hex characters back into the flags
// byte, reporting false for anything else. Uppercase is rejected for the same
// reason ValidTraceID rejects it: the grammar admits lowercase alone.
func ParseTraceFlags(s string) (byte, bool) {
	if len(s) != 2 || !isLowerHex(s) {
		return 0, false
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return 0, false
	}
	return b[0], true
}

// isLowerHex reports whether every byte of s is a lowercase hex digit. Unlike
// validHexID it says nothing about length or about the all-zero value, since
// its callers have their own rules for both.
func isLowerHex(s string) bool {
	for i := range len(s) {
		switch c := s[i]; {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}
