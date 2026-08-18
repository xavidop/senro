package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

func TestFrameIsPlainJSON(t *testing.T) {
	f := api.Frame{
		V:       api.Version,
		Kind:    api.KindReq,
		ID:      "c7",
		Type:    api.OpStepRetry,
		Payload: json.RawMessage(`{"step":"build/test"}`),
	}
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Debuggability with websocat is a stated design goal.
	want := `{"v":1,"kind":"req","id":"c7","type":"step.retry","payload":{"step":"build/test"}}`
	if string(b) != want {
		t.Errorf("frame encoding drifted:\n got %s\nwant %s", b, want)
	}
}

// log.gap belongs to the lossy per-step log channel, NOT the lifecycle event
// stream. Lifecycle events are never dropped, so a "gap" event there would be
// a contradiction.
func TestLogGapIsNotAnEventType(t *testing.T) {
	if api.Type("log.gap").Known() {
		t.Error("log.gap must not be a lifecycle event type")
	}
}

func TestControlOpNamesAreStable(t *testing.T) {
	// These strings are wire protocol. Changing one breaks every deployed CLI.
	cases := map[string]string{
		api.OpRunCancel:       "run.cancel",
		api.OpStepRetry:       "step.retry",
		api.OpStepSkip:        "step.skip",
		api.OpBreakpointSet:   "breakpoint.set",
		api.OpBreakpointClear: "breakpoint.clear",
		api.OpRunRerunFrom:    "run.rerun_from",
		api.OpRunPause:        "run.pause",
		api.OpRunResume:       "run.resume",
		api.OpAnalysisAccept:  "analysis.accept",
		api.OpAnalysisReject:  "analysis.reject",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("op name = %q, want %q", got, want)
		}
	}
}

func TestResponseFrameDistinguishesFalseFromAbsent(t *testing.T) {
	no := false
	failed, err := json.Marshal(api.Frame{V: api.Version, Kind: api.KindRes, ID: "c7", OK: &no, Error: "unknown step"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	absent, err := json.Marshal(api.Frame{V: api.Version, Kind: api.KindRes, ID: "c7"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var withOK, withoutOK map[string]any
	_ = json.Unmarshal(failed, &withOK)
	_ = json.Unmarshal(absent, &withoutOK)

	if v, ok := withOK["ok"]; !ok || v != false {
		t.Errorf("a rejected op must carry ok:false, got %s", failed)
	}
	if _, ok := withoutOK["ok"]; ok {
		t.Errorf("an unset OK must be absent, not false, got %s", absent)
	}
}
