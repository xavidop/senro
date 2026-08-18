// Package ndjson decodes the newline-delimited JSON body that GET
// /api/stream answers with: a run's events, one JSON object per line,
// optionally terminated by the single api.StreamEndMarker line that says why
// the stream ended.
//
// One implementation, because two clients read the identical bytes off
// different transports (LiveSource off net/http, the browser off fetch,
// which cannot link net/http at a reasonable size). Two decoders
// disagreeing about whether the terminal marker is an Event would render
// the same run differently forever.
//
// The rules are api.StreamEndMarker's own, restated as code:
//
//   - The marker is recognised by its "stream_end" field, which no
//     api.Event carries, BEFORE the line is decoded as an event: a client
//     must never feed the marker to api.RunState.Apply.
//   - A line that probes as a marker but fails to decode as one ends the
//     stream without a marker rather than being forwarded as a bogus
//     zero-valued Event.
//   - Anything else is an Event. Unknown event TYPES are the fold's
//     business, not this decoder's.
package ndjson

import (
	"encoding/json"
	"io"

	"github.com/xavidop/senro/api"
)

// Read decodes r to exhaustion, handing every event to onEvent in wire
// order, and reports the terminal marker if the stream ended with one.
//
// onEvent returns false to stop reading, which Read reports as no marker:
// a caller that gave up has no business claiming it saw the end of the
// stream. Read never closes r; the caller owns it.
//
// A decode failure (a truncated body, a transport break, a cancelled
// request) also ends the read with no marker. That ambiguity is what
// api.StreamEndReason exists to resolve: "the connection ended and said
// nothing" is a different fact from "the run ended".
func Read(r io.Reader, onEvent func(api.Event) bool) (api.StreamEndMarker, bool) {
	dec := json.NewDecoder(r)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return api.StreamEndMarker{}, false
		}

		// probe mirrors only the discriminator field, separate from the full
		// decode: a line that probes true but fails to decode into the
		// published shape must still end the stream, which decoding straight
		// into api.StreamEndMarker could not tell from "never a marker".
		var probe struct {
			StreamEnd bool `json:"stream_end"`
		}
		if err := json.Unmarshal(raw, &probe); err == nil && probe.StreamEnd {
			var marker api.StreamEndMarker
			if err := json.Unmarshal(raw, &marker); err != nil {
				return api.StreamEndMarker{}, false
			}
			return marker, true
		}

		var e api.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return api.StreamEndMarker{}, false
		}
		if !onEvent(e) {
			return api.StreamEndMarker{}, false
		}
	}
}
