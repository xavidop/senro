package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

// step.log.appended carries only a byte range, never content. This is what
// keeps the lifecycle channel small enough to be lossless in a 300-node
// fan-out; content is fetched on demand.
func TestStepLogAppendedCarriesOffsetsNotContent(t *testing.T) {
	in := api.StepLogAppendedBody{
		Stream: api.StreamStdout,
		Offset: 81922,
		Len:    1184,
		Lines:  9,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"content", "data", "text", "body"} {
		if _, ok := m[forbidden]; ok {
			t.Errorf("log marker must not carry content, found %q", forbidden)
		}
	}
	if m["offset"] != float64(81922) {
		t.Errorf("offset = %v, want 81922", m["offset"])
	}
}

// TestStepStartedBodyFuncRoundTrips is Func's own round trip: present and
// preserved for a func step, and omitted entirely (not merely empty) for an
// exec step, so every event an earlier build ever wrote, none of which had
// this field at all, decodes unchanged and stays byte-identical when
// re-marshalled.
func TestStepStartedBodyFuncRoundTrips(t *testing.T) {
	in := api.StepStartedBody{Func: "deploy/helm"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out api.StepStartedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Func != "deploy/helm" {
		t.Errorf("Func round-trip mismatch: %+v", out)
	}

	without, err := json.Marshal(api.StepStartedBody{Cmd: []string{"true"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(without, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["func"]; ok {
		t.Errorf("an exec step's body still serialized the func key: %s", without)
	}
}

func TestStepFinishedBodyRoundTrip(t *testing.T) {
	in := api.StepFinishedBody{State: api.StateFailed, ExitCode: 1, Error: "exit status 1"}
	b, _ := json.Marshal(in)

	var out api.StepFinishedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.State != api.StateFailed || out.ExitCode != 1 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

// The retry record must say WHY, so a run full of infra retries is
// distinguishable from one full of flaky tests.
func TestStepRetriedBodyRecordsPredicate(t *testing.T) {
	in := api.StepRetriedBody{
		Attempt:   2,
		Reason:    "ssh: connection reset by peer",
		Predicate: "OnInfra",
		BackoffMS: 2137,
	}
	b, _ := json.Marshal(in)

	var out api.StepRetriedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Predicate != "OnInfra" || out.Attempt != 2 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

// TestStepFinishedBodyReasonRoundTrips is Reason's own round trip, the same
// shape TestStepFinishedBodyRoundTrip pins for State and ExitCode. Reason is
// additive and omitempty, so a body built with no Reason (every event a
// previous build ever wrote) must not grow the key at all.
func TestStepFinishedBodyReasonRoundTrips(t *testing.T) {
	in := api.StepFinishedBody{State: api.StateSkippedCondition, Reason: "condition branch:main is false"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.StepFinishedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.State != api.StateSkippedCondition || out.Reason != "condition branch:main is false" {
		t.Errorf("round-trip mismatch: %+v", out)
	}

	without, err := json.Marshal(api.StepFinishedBody{State: api.StateSucceeded})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(without, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["reason"]; ok {
		t.Errorf("a body with no Reason still serialized the key: %s", without)
	}
}

func TestStepFinishedDurationSurvivesZero(t *testing.T) {
	b, err := json.Marshal(api.StepFinishedBody{State: api.StateCached})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["duration_ns"]; !ok {
		t.Errorf("duration_ns must be present even when zero, got %s", b)
	}
}
