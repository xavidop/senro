package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

// A Failure is the one value in this package that leaves the machine, so
// every field on it is published to a third party. Allowlist, not denylist:
// a denylist of guessed names silently passes a future field called Env or
// Token, while enumerating what is permitted makes any new field fail until
// somebody justifies it out loud.
func TestAFailureCarriesOnlyWhatWasJustifiedFieldByField(t *testing.T) {
	in := api.Failure{
		RunID:    "01J8",
		Pipeline: "release",
		Step:     "test",
		Attempt:  2,
		State:    api.StateFailed,
		ExitCode: 1,
		Error:    "exit status 1",
		Duration: 3 * time.Second,
		Cmd:      []string{"go", "test", "./..."},
		Needs:    []string{"build"},
		LogTail:  "FAIL\tgithub.com/x/y\n",
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	allowed := map[string]bool{
		"run_id": true, "pipeline": true, "step": true, "attempt": true,
		"state": true, "exit_code": true, "error": true, "duration_ns": true,
		"cmd": true, "needs": true, "log_tail": true,
	}
	for k := range m {
		if !allowed[k] {
			t.Errorf("api.Failure carries unexpected field %q: everything on this struct "+
				"is handed to a third party's API, so a new field needs a reason, not a default", k)
		}
	}
	for k := range allowed {
		if _, ok := m[k]; !ok {
			t.Errorf("api.Failure no longer carries %q, which this test's allowlist still permits: "+
				"a removed field means the allowlist is describing a struct that does not exist", k)
		}
	}
}

// The remedy vocabulary is closed. An analyzer is a program senro did not
// write, answering over a network, and the set of things senro will actually
// DO on its say-so cannot be open to whatever string comes back.
func TestARemedyOutsideTheVocabularyIsNotApplicable(t *testing.T) {
	for _, tc := range []struct {
		remedy api.Remedy
		want   bool
	}{
		{api.RemedyRetry, true},
		{api.RemedyNone, false},
		{api.Remedy(""), false},
		{api.Remedy("retry "), false},
		{api.Remedy("RETRY"), false},
		{api.Remedy("patch"), false},
		{api.Remedy("edit_workspace"), false},
		{api.Remedy("rm -rf /"), false},
	} {
		if got := tc.remedy.Applicable(); got != tc.want {
			t.Errorf("Remedy(%q).Applicable() = %v, want %v", string(tc.remedy), got, tc.want)
		}
	}
}

// The proposal a third party returned and the payload that lands in the
// ledger are one value, not two that can drift apart. Embedding is what makes
// that true by construction rather than by a copy loop somebody has to keep
// in step.
func TestAnalysisProposedCarriesTheProposalItself(t *testing.T) {
	body := api.AnalysisProposedBody{
		ID:       "test@2",
		Analyzer: "fake",
		Duration: 250 * time.Millisecond,
		Proposal: api.Proposal{
			Summary: "the module cache was empty and the download timed out",
			Detail:  "dial tcp: i/o timeout on proxy.golang.org",
			Remedy:  api.RemedyRetry,
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Flattened, not nested under "proposal": a client reading the ledger
	// reads one object.
	if _, nested := m["Proposal"]; nested {
		t.Error("analysis.proposed nests the proposal under a field name; it should be flattened")
	}
	for _, k := range []string{"id", "summary", "remedy"} {
		if _, ok := m[k]; !ok {
			t.Errorf("analysis.proposed is missing %q; got %v", k, m)
		}
	}

	var out api.AnalysisProposedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if out.Proposal != body.Proposal {
		t.Errorf("round trip changed the proposal: %+v", out.Proposal)
	}
}

// A run where a machine applied a fix with nobody watching must be
// distinguishable, afterwards and from the ledger alone, from one where a
// person pressed a key. That is the whole point of the gate, so the fact is a
// field rather than something inferred from an absent client id.
func TestAnalysisDecisionSaysWhenNoHumanWasInvolved(t *testing.T) {
	human := api.AnalysisDecisionBody{ID: "test@1", ClientID: "tui-7", Remedy: api.RemedyRetry}
	machine := api.AnalysisDecisionBody{ID: "test@1", Remedy: api.RemedyRetry, Policy: true}

	hb, err := json.Marshal(human)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mb, err := json.Marshal(machine)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var hm, mm map[string]any
	if err := json.Unmarshal(hb, &hm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := json.Unmarshal(mb, &mm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := hm["policy"]; ok {
		t.Error("a decision a client made carries policy=true; it should be omitted")
	}
	if p, ok := mm["policy"]; !ok || p != true {
		t.Errorf("a decision no human made does not say so: %v", mm)
	}
	if _, ok := mm["client_id"]; ok {
		t.Error("a policy decision names a client; there was none")
	}
}

// The three analysis types were reserved from the beginning. Emitting them is
// meant to be additive, so they have to still be known and still spell the
// same thing on the wire.
func TestAnalysisTypesKeepTheirReservedWireStrings(t *testing.T) {
	for _, tc := range []struct {
		typ  api.Type
		want string
	}{
		{api.AnalysisProposed, "analysis.proposed"},
		{api.AnalysisApplied, "analysis.applied"},
		{api.AnalysisRejected, "analysis.rejected"},
	} {
		if string(tc.typ) != tc.want {
			t.Errorf("%v = %q, want %q", tc.typ, string(tc.typ), tc.want)
		}
		if !tc.typ.Known() {
			t.Errorf("%q is not Known()", tc.want)
		}
	}
}
