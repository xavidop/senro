package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

func TestPlanExpandedBodyRoundTrip(t *testing.T) {
	in := api.PlanExpandedBody{
		Parent:   "build/per-service",
		Children: []string{"build/per-service[unit=services/api]"},
		Count:    37,
		Skipped:  263,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.PlanExpandedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Count != 37 || out.Skipped != 263 || len(out.Children) != 1 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

func TestRunFinishedBodyCarriesStateHistogram(t *testing.T) {
	in := api.RunFinishedBody{
		Status: api.RunSucceededWithRecovery,
		Steps: map[api.State]int{
			api.StateSucceeded: 12,
			api.StateRecovered: 1,
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.RunFinishedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Status != api.RunSucceededWithRecovery {
		t.Errorf("Status = %s", out.Status)
	}
	if out.Steps[api.StateRecovered] != 1 {
		t.Errorf("recovered count = %d, want 1", out.Steps[api.StateRecovered])
	}
}

// Payload bodies must decode through Event.Decode, which is how every
// consumer reads them.
func TestPayloadDecodesThroughEvent(t *testing.T) {
	body, _ := json.Marshal(api.PlanResolvedBody{Digest: "sha256:abc", Nodes: 14})
	e := api.Event{Type: api.PlanResolved, Payload: body}

	var out api.PlanResolvedBody
	if err := e.Decode(&out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.Nodes != 14 {
		t.Errorf("Nodes = %d, want 14", out.Nodes)
	}
}

func TestRunFinishedDurationSurvivesZero(t *testing.T) {
	b, err := json.Marshal(api.RunFinishedBody{Status: api.RunSucceeded})
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
