package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func TestEventRoundTrip(t *testing.T) {
	in := api.Event{
		V:       api.Version,
		Seq:     4471,
		TS:      time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC),
		Type:    api.StepStarted,
		Run:     "01JQ8ZK",
		Step:    "build/test[unit=services/api]",
		Attempt: 2,
		Group:   "build/per-service",
		Payload: json.RawMessage(`{"cmd":["go","test"]}`),
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.Event
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Seq != in.Seq || out.Type != in.Type || out.Step != in.Step {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, in)
	}
	if out.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", out.Attempt)
	}
}

// Attempt must be a routing field of its own, never folded into Step. A client
// filtering every event for "build/test" must still see attempt 2.
func TestStepIDCarriesNoAttemptSuffix(t *testing.T) {
	e := api.Event{Type: api.StepStarted, Step: "build/test", Attempt: 3}
	b, _ := json.Marshal(e)
	var m map[string]any
	_ = json.Unmarshal(b, &m)

	if got := m["step"]; got != "build/test" {
		t.Errorf("step = %v, want %q", got, "build/test")
	}
	if got := m["attempt"]; got != float64(3) {
		t.Errorf("attempt = %v, want 3", got)
	}
}

// Optional routing fields must vanish when empty, so events.jsonl stays
// readable and fixtures stay stable.
func TestEmptyRoutingFieldsOmitted(t *testing.T) {
	b, err := json.Marshal(api.Event{V: 1, Seq: 1, Type: api.RunStarted})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, absent := range []string{"step", "attempt", "group", "trace_id", "payload", "run"} {
		if bytesContainsKey(b, absent) {
			t.Errorf("expected %q to be omitted, got %s", absent, b)
		}
	}
}

func bytesContainsKey(b []byte, key string) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return false
	}
	_, ok := m[key]
	return ok
}

func TestTypeKnown(t *testing.T) {
	if !api.StepFinished.Known() {
		t.Error("step.finished should be known")
	}
	if !api.AnalysisProposed.Known() {
		t.Error("reserved types are known types")
	}
	if api.Type("step.teleported").Known() {
		t.Error("unregistered type should not be known")
	}
}

func TestEventDecode(t *testing.T) {
	type body struct {
		Cmd []string `json:"cmd"`
	}
	e := api.Event{Payload: json.RawMessage(`{"cmd":["go","test"]}`)}

	var got body
	if err := e.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Cmd) != 2 || got.Cmd[0] != "go" {
		t.Errorf("Cmd = %v, want [go test]", got.Cmd)
	}
}

func TestEventDecodeNilPayload(t *testing.T) {
	var got struct{}
	if err := (api.Event{}).Decode(&got); err != nil {
		t.Errorf("Decode with nil payload should be a no-op, got %v", err)
	}
}

// These strings are the wire format: renaming one breaks every deployed
// client and every recorded event log, and additive-only evolution means the
// rename can never be made later either. This test makes it a test failure
// rather than a silent release.
func TestWireStringsAreStable(t *testing.T) {
	// A name/got/want table rather than a map: several constants share a
	// wire string (State and RunStatus both spell "succeeded"), and a map
	// literal rejects equal constant keys at compile time.
	wireStrings := []struct{ name, got, want string }{
		// api/event.go: the types this build declares and emits.
		{"RunStarted", string(api.RunStarted), "run.started"},
		{"RunFinished", string(api.RunFinished), "run.finished"},
		{"PlanResolved", string(api.PlanResolved), "plan.resolved"},
		{"PlanExpanded", string(api.PlanExpanded), "plan.expanded"},
		{"PlanExpansionSkipped", string(api.PlanExpansionSkipped), "plan.expansion_skipped"},
		{"StepCreated", string(api.StepCreated), "step.created"},
		{"StepStarted", string(api.StepStarted), "step.started"},
		{"StepFinished", string(api.StepFinished), "step.finished"},
		{"StepRetried", string(api.StepRetried), "step.retried"},
		{"StepLogAppended", string(api.StepLogAppended), "step.log.appended"},
		{"CacheHit", string(api.CacheHit), "cache.hit"},
		{"CacheMiss", string(api.CacheMiss), "cache.miss"},
		{"CacheSaved", string(api.CacheSaved), "cache.saved"},
		{"CacheDegraded", string(api.CacheDegraded), "cache.degraded"},
		{"WSSnapshot", string(api.WSSnapshot), "ws.snapshot"},
		{"WSRestored", string(api.WSRestored), "ws.restored"},
		{"WSEvicted", string(api.WSEvicted), "ws.evicted"},
		{"SecretResolved", string(api.SecretResolved), "secret.resolved"},
		{"SecretRedacted", string(api.SecretRedacted), "secret.redacted"},
		{"ControlApplied", string(api.ControlApplied), "control.applied"},
		{"HandlerStarted", string(api.HandlerStarted), "handler.started"},
		{"HandlerSucceeded", string(api.HandlerSucceeded), "handler.succeeded"},
		{"HandlerFailed", string(api.HandlerFailed), "handler.failed"},
		{"HandlerSuperseded", string(api.HandlerSuperseded), "handler.superseded"},

		{"ShellOpened", string(api.ShellOpened), "shell.opened"},
		{"ShellClosed", string(api.ShellClosed), "shell.closed"},
		{"BinaryStaged", string(api.BinaryStaged), "binary.staged"},
		{"AnalysisProposed", string(api.AnalysisProposed), "analysis.proposed"},
		{"AnalysisApplied", string(api.AnalysisApplied), "analysis.applied"},
		{"AnalysisRejected", string(api.AnalysisRejected), "analysis.rejected"},

		// api/event.go: the rest of the vocabulary, including the reserved
		// types (plan.generated, client.attached, client.detached), pinned
		// precisely because nothing emits them yet: nothing else would
		// notice a rename before the first engine does.
		{"PlanGenerated", string(api.PlanGenerated), "plan.generated"},
		{"ClientAttached", string(api.ClientAttached), "client.attached"},
		{"ClientDetached", string(api.ClientDetached), "client.detached"},
		{"BreakpointHit", string(api.BreakpointHit), "breakpoint.hit"},
		{"NotifyDelivered", string(api.NotifyDelivered), "notify.delivered"},
		{"NotifyFailed", string(api.NotifyFailed), "notify.failed"},
		{"NotifyDropped", string(api.NotifyDropped), "notify.dropped"},

		// api/state_enum.go: the 10 terminal step states.
		{"StateSucceeded", string(api.StateSucceeded), "succeeded"},
		{"StateCached", string(api.StateCached), "cached"},
		{"StateFailed", string(api.StateFailed), "failed"},
		{"StateTimedOut", string(api.StateTimedOut), "timed_out"},
		{"StateCancelled", string(api.StateCancelled), "cancelled"},
		{"StateSkippedUpstreamFailed", string(api.StateSkippedUpstreamFailed), "skipped_upstream_failed"},
		{"StateSkippedManual", string(api.StateSkippedManual), "skipped_manual"},
		{"StateSkippedCondition", string(api.StateSkippedCondition), "skipped_condition"},
		{"StateRecovered", string(api.StateRecovered), "recovered"},
		{"StatePanicked", string(api.StatePanicked), "panicked"},

		// api/state_enum.go: the 5 rolled-up run statuses.
		{"RunSucceeded", string(api.RunSucceeded), "succeeded"},
		{"RunSucceededWithRecovery", string(api.RunSucceededWithRecovery), "succeeded_with_recovery"},
		{"RunPartial", string(api.RunPartial), "partial"},
		{"RunFailed", string(api.RunFailed), "failed"},
		{"RunCancelled", string(api.RunCancelled), "cancelled"},

		// api/frame.go: the 2 frame kinds. No evt or bye kind exists to pin;
		// see StreamEndMarker.
		{"KindReq", string(api.KindReq), "req"},
		{"KindRes", string(api.KindRes), "res"},

		// api/frame.go: the 10 control operation names. Also covered by
		// TestControlOpNamesAreStable; repeated here so the count below is the
		// module's whole wire vocabulary rather than a subset of it.
		{"OpRunCancel", api.OpRunCancel, "run.cancel"},
		{"OpStepRetry", api.OpStepRetry, "step.retry"},
		{"OpStepSkip", api.OpStepSkip, "step.skip"},
		{"OpBreakpointSet", api.OpBreakpointSet, "breakpoint.set"},
		{"OpBreakpointClear", api.OpBreakpointClear, "breakpoint.clear"},
		{"OpRunRerunFrom", api.OpRunRerunFrom, "run.rerun_from"},
		{"OpRunPause", api.OpRunPause, "run.pause"},
		{"OpRunResume", api.OpRunResume, "run.resume"},
		{"OpAnalysisAccept", api.OpAnalysisAccept, "analysis.accept"},
		{"OpAnalysisReject", api.OpAnalysisReject, "analysis.reject"},

		// api/payload_analysis.go: the remedy vocabulary, wire protocol for
		// the same reason the op names are.
		{"RemedyRetry", string(api.RemedyRetry), "retry"},

		// api/payload_step.go: the 2 log stream names.
		{"StreamStdout", api.StreamStdout, "stdout"},
		{"StreamStderr", api.StreamStderr, "stderr"},

		// api/streamend.go: the 3 stream-end reasons and the one overflow
		// error string.
		{"StreamEndRunEnded", string(api.StreamEndRunEnded), "run_ended"},
		{"StreamEndOverflowed", string(api.StreamEndOverflowed), "overflowed"},
		{"StreamEndWriteStalled", string(api.StreamEndWriteStalled), "write_stalled"},
		{"OverflowError", api.OverflowError, "lifecycle_overflow"},
	}

	for _, c := range wireStrings {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if len(wireStrings) != 71 {
		t.Errorf("pinned %d wire strings, expected 71: a new constant needs pinning here", len(wireStrings))
	}
}

// TestClientEventsAreReservedNotDeclared holds DeclaredTypes to its
// documented meaning: client.attached and client.detached are emitted by
// nothing (the attach server observes connections but cannot write the
// ledger; see their doc in event.go), and declaring them would tell a
// client to wait for something that will never arrive. Known() must still
// recognise both: a newer engine emitting one is unsurprising.
func TestClientEventsAreReservedNotDeclared(t *testing.T) {
	for _, ty := range []api.Type{api.ClientAttached, api.ClientDetached} {
		for _, declared := range api.DeclaredTypes() {
			if declared == ty {
				t.Errorf("%s is declared as a type this build emits, and nothing emits it", ty)
			}
		}
		if !ty.Known() {
			t.Errorf("%s is not known, so a client on this build would treat a newer engine's own as unrecognised", ty)
		}
	}

	// The canary: every one of these IS emitted and must stay declared, so
	// the check above cannot pass merely because DeclaredTypes went empty.
	for _, ty := range []api.Type{
		api.CacheHit, api.CacheMiss, api.CacheSaved,
		api.WSSnapshot, api.WSRestored,
		api.SecretResolved, api.SecretRedacted,
		api.ControlApplied,
	} {
		var found bool
		for _, declared := range api.DeclaredTypes() {
			if declared == ty {
				found = true
			}
		}
		if !found {
			t.Errorf("%s is emitted by this build and must stay declared", ty)
		}
	}
}
