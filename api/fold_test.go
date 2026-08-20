package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func mustEvent(t *testing.T, e api.Event, body any) api.Event {
	t.Helper()
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		e.Payload = b
	}
	if e.V == 0 {
		e.V = api.Version
	}
	return e
}

func TestApplyTracksSeq(t *testing.T) {
	s := api.NewRunState()
	for _, seq := range []uint64{1, 2, 3} {
		if err := s.Apply(api.Event{V: 1, Seq: seq, Type: api.StepCreated, Step: "a"}); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if s.Seq != 3 {
		t.Errorf("Seq = %d, want 3", s.Seq)
	}
}

// This is the guarantee that makes the schema additive-only workable: an old
// client must survive a new engine's events.
func TestApplyIgnoresUnknownTypes(t *testing.T) {
	s := api.NewRunState()
	err := s.Apply(api.Event{V: 1, Seq: 9, Type: "step.teleported", Step: "a"})
	if err != nil {
		t.Fatalf("unknown type must be ignored, got error: %v", err)
	}
	if s.Seq != 9 {
		t.Errorf("Seq must still advance past unknown events, got %d", s.Seq)
	}
	if len(s.Steps) != 0 {
		t.Errorf("unknown event must not create state, got %d steps", len(s.Steps))
	}
}

func TestApplyRunLifecycle(t *testing.T) {
	s := api.NewRunState()
	start := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.RunStarted, Run: "01JQ"},
		api.RunStartedBody{Pipeline: "./ci", EngineVersion: "0.1.0", StartedAt: start}))

	if s.Run.ID != "01JQ" || s.Run.Pipeline != "./ci" {
		t.Fatalf("run info = %+v", s.Run)
	}
	if s.Run.Done {
		t.Error("run must not be done after run.started")
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.RunFinished, Run: "01JQ"},
		api.RunFinishedBody{Status: api.RunFailed}))

	if !s.Run.Done || s.Run.Status != api.RunFailed {
		t.Errorf("run info = %+v", s.Run)
	}
	if s.Run.CleanupAbandoned {
		t.Error("cleanup_abandoned must stay false unless the engine says otherwise")
	}
}

// A run that gave up waiting for its own cleanup must say so somewhere a
// client can see without decoding payloads: the lock may still be held.
func TestApplyFoldsCleanupAbandoned(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.RunFinished, Run: "01JQ"},
		api.RunFinishedBody{Status: api.RunCancelled, CleanupAbandoned: true}))

	if !s.Run.CleanupAbandoned {
		t.Error("run.finished reported abandoned cleanup and the fold dropped it")
	}
}

func TestApplyStepLifecycle(t *testing.T) {
	s := api.NewRunState()
	// TS must be set: Running() keys off a non-zero Started, which the fold
	// takes from the event's timestamp.
	at := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, TS: at, Type: api.StepCreated, Step: "build/test"},
		api.StepCreatedBody{Kind: "exec", Needs: []string{"setup"}}))

	st := s.Steps["build/test"]
	if st == nil {
		t.Fatal("step.created must create the step")
	}
	if st.Kind != "exec" || len(st.Needs) != 1 {
		t.Errorf("step = %+v", st)
	}
	if len(s.Order) != 1 || s.Order[0] != "build/test" {
		t.Errorf("Order = %v", s.Order)
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, TS: at, Type: api.StepStarted, Step: "build/test", Attempt: 1},
		api.StepStartedBody{Cmd: []string{"go", "test"}}))

	if !st.Running() {
		t.Error("step should be running after step.started")
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, TS: at.Add(time.Second), Type: api.StepFinished, Step: "build/test", Attempt: 1},
		api.StepFinishedBody{State: api.StateSucceeded}))

	if st.State != api.StateSucceeded || st.Running() {
		t.Errorf("step = %+v", st)
	}
}

// A step.started for a step never announced must still produce state: the
// fold has to survive a truncated or mid-stream log.
func TestApplyCreatesStepImplicitly(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepStarted, Step: "orphan"}, nil))

	if s.Steps["orphan"] == nil {
		t.Fatal("step.started must create the step if it does not exist")
	}
	if len(s.Order) != 1 {
		t.Errorf("Order = %v, want the implicit step recorded once", s.Order)
	}
}

func TestApplyRetryBumpsAttempt(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepFinished, Step: "a", Attempt: 1},
		api.StepFinishedBody{State: api.StateFailed, ExitCode: 1}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, Type: api.StepRetried, Step: "a", Attempt: 2},
		api.StepRetriedBody{Attempt: 2, Predicate: "OnInfra"}))

	st := s.Steps["a"]
	if st.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", st.Attempt)
	}
	// A retried step is no longer in its previous terminal state.
	if st.State.Terminal() {
		t.Errorf("State = %q, want cleared on retry", st.State)
	}
}

// A retry starts a new attempt with its own log file at byte 0, and LogBytes
// is a max()-derived high-water mark: if StepRetried does not reset it, the
// old attempt's larger value swallows the new attempt's markers forever, and
// a client polling LogBytes (the TUI's follow mechanism) sees nothing new to
// fetch.
func TestApplyStepRetriedResetsLogBytes(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 0, Len: 500}))
	if got := s.Steps["a"].LogBytes[api.StreamStdout]; got != 500 {
		t.Fatalf("precondition: stdout bytes = %d, want 500", got)
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, Type: api.StepRetried, Step: "a", Attempt: 2},
		api.StepRetriedBody{Attempt: 2, Predicate: "OnInfra"}))

	if got := s.Steps["a"].LogBytes[api.StreamStdout]; got != 0 {
		t.Errorf("stdout bytes after retry = %d, want 0 — the new attempt's log file starts at byte 0, "+
			"and a stale watermark from the previous attempt makes its own content permanently unreachable "+
			"by max()-based accumulation", got)
	}

	// The new attempt's smaller marker must not be swallowed by a stale
	// high-water mark: max(500, 0+80) is 500, not 80.
	_ = s.Apply(mustEvent(t, api.Event{Seq: 4, Type: api.StepLogAppended, Step: "a", Attempt: 2},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 0, Len: 80}))
	if got := s.Steps["a"].LogBytes[api.StreamStdout]; got != 80 {
		t.Errorf("stdout bytes after the new attempt's first marker = %d, want 80", got)
	}
}

func TestApplyLogBytesAccumulate(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 0, Len: 100}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 100, Len: 50}))

	if got := s.Steps["a"].LogBytes[api.StreamStdout]; got != 150 {
		t.Errorf("stdout bytes = %d, want 150", got)
	}
}

func TestApplyExpansion(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.PlanExpanded, Step: "build/per-service"},
		api.PlanExpandedBody{
			Parent:   "build/per-service",
			Children: []string{"build/per-service[unit=a]", "build/per-service[unit=b]"},
			Count:    2,
			Skipped:  5,
		}))

	exp := s.Expansions["build/per-service"]
	if exp == nil || exp.Count != 2 || exp.Skipped != 5 {
		t.Fatalf("expansion = %+v", exp)
	}
	// Children must exist as steps so the renderer can show them before any
	// step.created arrives.
	if s.Steps["build/per-service[unit=a]"] == nil {
		t.Error("expansion must materialise its children")
	}
	if s.Steps["build/per-service[unit=a]"].Group != "build/per-service" {
		t.Error("children must be tagged with their group")
	}
}

func TestApplyCacheHitMarksCached(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.CacheHit, Step: "a"},
		api.CacheHitBody{Key: "4f1c", FromRun: "01JP"}))

	if !s.Steps["a"].Cached {
		t.Error("cache.hit must mark the step cached")
	}
}

func TestApplyRejectsOutOfOrderSeq(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(api.Event{V: 1, Seq: 5, Type: api.StepCreated, Step: "a"})
	err := s.Apply(api.Event{V: 1, Seq: 3, Type: api.StepCreated, Step: "b"})
	if err == nil {
		t.Error("a regressing seq means the caller lost ordering; must error")
	}
}

// TestApplyRejectsAZeroSeqAfterRealProgress guards against special-casing
// Seq == 0 out of the ordering comparison: that would let a Seq:0 event
// mutate state at any point in a stream. No real engine emits Seq:0, but a
// hand-edited or foreign events.jsonl can, and `senro attach --follow`
// folds exactly that file.
func TestApplyRejectsAZeroSeqAfterRealProgress(t *testing.T) {
	s := api.NewRunState()
	if err := s.Apply(api.Event{V: 1, Seq: 2, Type: api.StepCreated, Step: "a"}); err != nil {
		t.Fatalf("Apply(seq 2): %v", err)
	}

	err := s.Apply(api.Event{V: 1, Seq: 0, Type: api.StepFinished, Step: "a"})
	if err == nil {
		t.Fatal("Apply(seq 0) after seq 2 succeeded, want a regression error")
	}
	if s.Steps["a"].State != "" {
		t.Errorf("the rejected event still mutated step state: %+v", s.Steps["a"])
	}
	if s.Seq != 2 {
		t.Errorf("Seq = %d, want 2 — a rejected event must not move the watermark", s.Seq)
	}
}

// TestApplyLogBytesIdempotentOnReplay pins the OTHER half of Apply's Seq
// contract: re-applying the exact same, already-seen Seq is tolerated and
// idempotent, not an error. The test above is about Seq:0 bypassing the
// check for every later seq, not about honest duplicates.

// TestApplyIgnoresAStepScopedEventWithNoStepID: a known step-scoped type
// (step.finished in particular) with an empty Step field must not
// materialise a phantom "" entry in Steps and Order. Unreachable from
// senro's engine, reachable from a hand-edited or foreign events.jsonl,
// where it renders as a blank row and an off-by-one in every step count.
func TestApplyIgnoresAStepScopedEventWithNoStepID(t *testing.T) {
	s := api.NewRunState()
	err := s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepFinished, Step: ""},
		api.StepFinishedBody{State: api.StateFailed}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := s.Steps[""]; ok {
		t.Error("a step-scoped event with an empty Step field materialised a phantom \"\" step")
	}
	if len(s.Steps) != 0 {
		t.Errorf("Steps = %d, want 0", len(s.Steps))
	}
	if len(s.Order) != 0 {
		t.Errorf("Order = %v, want empty", s.Order)
	}
}

// The handler-scoped analogue of the test above: handler events key
// s.Handlers by e.Step too, so the same empty-id phantom-entry risk applies
// to RunState.handler.
func TestApplyIgnoresAHandlerEventWithNoHandlerID(t *testing.T) {
	s := api.NewRunState()
	err := s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.HandlerStarted, Step: ""},
		api.HandlerBody{Kind: "always", Parent: "a"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := s.Handlers[""]; ok {
		t.Error("a handler event with an empty handler id materialised a phantom \"\" handler")
	}
	if len(s.Handlers) != 0 {
		t.Errorf("Handlers = %d, want 0", len(s.Handlers))
	}
}

func TestApplyRestartClearsStaleFailure(t *testing.T) {
	s := api.NewRunState()
	at := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, TS: at, Type: api.StepCreated, Step: "a"},
		api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, TS: at, Type: api.StepStarted, Step: "a", Attempt: 1}, nil))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, TS: at, Type: api.StepFinished, Step: "a", Attempt: 1},
		api.StepFinishedBody{State: api.StateFailed, ExitCode: 137, Error: "OOMKilled"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 4, TS: at, Type: api.StepStarted, Step: "a", Attempt: 2}, nil))

	st := s.Steps["a"]
	if !st.Running() {
		t.Fatal("step should be running after the second start")
	}
	if st.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0 — a running step must not carry a prior attempt's exit code", st.ExitCode)
	}
	if st.Error != "" {
		t.Errorf("Error = %q, want empty — a running step must not carry a prior attempt's error", st.Error)
	}
}

func TestApplyOrderRecordsEachStepOnce(t *testing.T) {
	s := api.NewRunState()
	at := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	// plan.expanded materialises both children, then g[a] is touched three
	// more times. step() must create it once and append to Order once.
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, TS: at, Type: api.PlanExpanded, Step: "g"},
		api.PlanExpandedBody{Parent: "g", Children: []string{"g[a]", "g[b]"}, Count: 2}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, TS: at, Type: api.StepCreated, Step: "g[a]"},
		api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, TS: at, Type: api.StepStarted, Step: "g[a]", Attempt: 1}, nil))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 4, TS: at, Type: api.CacheHit, Step: "g[a]"},
		api.CacheHitBody{Key: "4f1c"}))

	if len(s.Order) != 2 {
		t.Errorf("Order = %v, want each step exactly once", s.Order)
	}
	if len(s.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(s.Steps))
	}
}

func TestApplyLogBytesIdempotentOnReplay(t *testing.T) {
	s := api.NewRunState()
	e := mustEvent(t, api.Event{Seq: 7, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 0, Len: 100})

	_ = s.Apply(e)
	_ = s.Apply(e) // a client resuming one seq too early replays exactly this

	if got := s.Steps["a"].LogBytes[api.StreamStdout]; got != 100 {
		t.Errorf("stdout bytes = %d, want 100 — replaying a marker must not inflate the count", got)
	}
}

func TestApplyLogBytesPerStream(t *testing.T) {
	// A hardcoded stream key would pass every other test in this suite:
	// StreamStderr never appears in them, so stderr bytes silently
	// attributed to stdout go unnoticed elsewhere.
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 0, Len: 100}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStderr, Offset: 0, Len: 7}))

	lb := s.Steps["a"].LogBytes
	if lb[api.StreamStdout] != 100 || lb[api.StreamStderr] != 7 {
		t.Errorf("LogBytes = %v, want stdout 100 and stderr 7 tracked separately", lb)
	}
}

func TestApplyStepCreatedDoesNotEraseExpansionGroup(t *testing.T) {
	// plan.expanded tags children with their group. A later step.created whose
	// body carries no group must not erase it, or Group() aggregation for that
	// child silently degrades.
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.PlanExpanded, Step: "g"},
		api.PlanExpandedBody{Parent: "g", Children: []string{"g[a]"}, Count: 1}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepCreated, Step: "g[a]"},
		api.StepCreatedBody{Kind: "exec"}))

	if got := s.Steps["g[a]"].Group; got != "g" {
		t.Errorf("Group = %q, want %q — step.created must not erase the expansion's tag", got, "g")
	}
}

func TestApplyPlanResolved(t *testing.T) {
	// The whole plan.resolved branch can be deleted without any test noticing.
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.PlanResolved, Run: "01JQ"},
		api.PlanResolvedBody{Digest: "sha256:abc", Nodes: 14}))

	if s.Run.PlanDigest != "sha256:abc" {
		t.Errorf("PlanDigest = %q, want sha256:abc", s.Run.PlanDigest)
	}
}

func TestApplyMidStreamSeedsRunAndAttempt(t *testing.T) {
	// A client that subscribes from a snapshot never sees run.started or the
	// first step.started. Both must still be recoverable from the envelope.
	s := api.NewRunState()
	err := s.Apply(mustEvent(t, api.Event{
		Seq: 42, Type: api.StepFinished, Run: "01JQ", Step: "build", Attempt: 3,
	}, api.StepFinishedBody{State: api.StateSucceeded}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if s.Run.ID != "01JQ" {
		t.Errorf("Run.ID = %q, want 01JQ seeded from the envelope", s.Run.ID)
	}
	if got := s.Steps["build"].Attempt; got != 3 {
		t.Errorf("Attempt = %d, want 3 from the envelope", got)
	}
	if len(s.Order) != 1 {
		t.Errorf("Order = %v, want the step recorded exactly once", s.Order)
	}
}

func TestApplyUnknownTypeCreatesNoStep(t *testing.T) {
	// The envelope-seeding that lets a mid-stream client recover Attempt must
	// not let an unhandled type conjure a step. Reserved types are
	// step-scoped and pass Type.Known(), so this is about ordering, not a
	// predicate, and a phantom step in Order renders as a real one.
	s := api.NewRunState()

	// api.ShellOpened is the strongest example here: a declared type a live
	// stream really contains that Apply deliberately does not fold, so if
	// envelope-seeding ever conjured a step from an unhandled type, opening
	// a shell on a step that had not started would materialise a phantom
	// one.
	for _, typ := range []api.Type{"quantum.entangled", api.ShellOpened, api.AnalysisProposed} {
		if err := s.Apply(api.Event{
			V: 1, Seq: uint64(len(s.Order) + 1), Type: typ,
			Run: "01JQ", Step: "ghost", Attempt: 2,
		}); err != nil {
			t.Fatalf("%s: unhandled types must be ignored, got %v", typ, err)
		}
	}

	if len(s.Steps) != 0 {
		t.Errorf("Steps = %d, want 0 — an unhandled type must not create a step", len(s.Steps))
	}
	if len(s.Order) != 0 {
		t.Errorf("Order = %v, want empty", s.Order)
	}
	// Run.ID seeding is fine: it materialises no step state.
	if s.Run.ID != "01JQ" {
		t.Errorf("Run.ID = %q, want 01JQ — run seeding should still work", s.Run.ID)
	}
}

func TestApplyToleratesRehydratedState(t *testing.T) {
	var s api.RunState
	if err := json.Unmarshal([]byte(`{"seq":3,"steps":null,"expansions":null}`), &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := s.Apply(api.Event{V: 1, Seq: 4, Type: api.StepCreated, Step: "a"}); err != nil {
		t.Fatalf("Apply on rehydrated state: %v", err)
	}
	if s.Steps["a"] == nil {
		t.Error("Apply must work on a state rehydrated from a snapshot")
	}
}

// TestApplyFoldsHandlers pins that Apply folds handler events at all:
// without that case every fold-based client could show that cleanup ran
// only by re-scanning the raw stream, which the fold exists to avoid.
func TestApplyFoldsHandlers(t *testing.T) {
	s := api.NewRunState()
	ts := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	events := []api.Event{
		mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Run: "01JQ", Step: "deploy"},
			api.StepCreatedBody{Kind: "exec"}),
		mustEvent(t, api.Event{Seq: 2, Type: api.StepFinished, Run: "01JQ", Step: "deploy", Attempt: 1},
			api.StepFinishedBody{State: api.StateFailed, ExitCode: 9}),
		mustEvent(t, api.Event{Seq: 3, Type: api.HandlerStarted, Run: "01JQ",
			Step: "deploy/on_failure/collect", Attempt: 1, TS: ts},
			api.HandlerBody{Kind: "on_failure", Parent: "deploy"}),
		mustEvent(t, api.Event{Seq: 4, Type: api.HandlerSucceeded, Run: "01JQ",
			Step: "deploy/on_failure/collect", Attempt: 1, TS: ts.Add(time.Second)},
			api.HandlerBody{Kind: "on_failure", Parent: "deploy"}),
		mustEvent(t, api.Event{Seq: 5, Type: api.HandlerStarted, Run: "01JQ",
			Step: "deploy/always/unlock", Attempt: 1, TS: ts.Add(2 * time.Second)},
			api.HandlerBody{Kind: "always", Parent: "deploy"}),
		mustEvent(t, api.Event{Seq: 6, Type: api.HandlerFailed, Run: "01JQ",
			Step: "deploy/always/unlock", Attempt: 1, TS: ts.Add(3 * time.Second)},
			api.HandlerBody{Kind: "always", Parent: "deploy", Error: "exit status 1"}),
	}
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}

	collect := s.Handlers["deploy/on_failure/collect"]
	if collect == nil {
		t.Fatalf("no on_failure handler in the fold: %v", s.Handlers)
	}
	if collect.State != api.StateSucceeded {
		t.Errorf("collect State = %q, want succeeded", collect.State)
	}
	if collect.Kind != "on_failure" || collect.Parent != "deploy" {
		t.Errorf("collect = %+v, want kind on_failure parent deploy", collect)
	}
	if collect.Started.IsZero() || collect.Finished.IsZero() {
		t.Errorf("collect timestamps = %v/%v, want both set", collect.Started, collect.Finished)
	}
	if collect.Running() {
		t.Error("a handler that reported succeeded must not fold as still running")
	}

	unlock := s.Handlers["deploy/always/unlock"]
	if unlock == nil {
		t.Fatalf("no always handler in the fold: %v", s.Handlers)
	}
	if unlock.State != api.StateFailed {
		t.Errorf("unlock State = %q, want failed", unlock.State)
	}
	if unlock.Error != "exit status 1" {
		t.Errorf("unlock Error = %q, want the handler's own error", unlock.Error)
	}

	// Both hang off their parent, in the order they ran.
	st := s.Steps["deploy"]
	want := []string{"deploy/on_failure/collect", "deploy/always/unlock"}
	if len(st.Handlers) != len(want) {
		t.Fatalf("deploy.Handlers = %v, want %v", st.Handlers, want)
	}
	for i := range want {
		if st.Handlers[i] != want[i] {
			t.Errorf("deploy.Handlers[%d] = %q, want %q — handlers list in the order they ran",
				i, st.Handlers[i], want[i])
		}
	}

	// A handler's own outcome must not touch the step it belongs to: the
	// always handler failed and deploy still exited 9 and reads failed.
	if st.State != api.StateFailed || st.ExitCode != 9 {
		t.Errorf("deploy = %+v, want failed with exit 9 — a handler's outcome must never "+
			"reach back into its parent's state", st)
	}

	// A handler is not a step: counting it in Steps or Order makes every
	// renderer's step count wrong.
	if len(s.Steps) != 1 {
		t.Errorf("Steps = %d, want 1 — a handler was folded in as a step", len(s.Steps))
	}
	if len(s.Order) != len(s.Steps) {
		t.Errorf("Order = %v for %d steps", s.Order, len(s.Steps))
	}
}

// A handler abandoned when the cleanup grace ran out leaves a handler.started
// with nothing after it. That must fold to a handler in a non-terminal state
// (distinguishable from one that succeeded), because the difference is whether
// a lock may still be held.
func TestApplyAbandonedHandlerIsNotSucceeded(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.HandlerStarted, Run: "01JQ",
		Step: "deploy/always/unlock", Attempt: 1, TS: time.Now().UTC()},
		api.HandlerBody{Kind: "always", Parent: "deploy"}))

	h := s.Handlers["deploy/always/unlock"]
	if h == nil {
		t.Fatal("no handler in the fold")
	}
	if h.State != "" {
		t.Errorf("State = %q, want empty — nothing reported this handler finishing", h.State)
	}
	if !h.Running() {
		t.Error("a handler that started and never reported back must not read as finished — " +
			"that is the difference between cleanup that ran and a lock still held")
	}
}

// TestApplyFoldsTheRunWidePause: control.applied is the only thing in the
// stream that says a run stopped on purpose, so the fold has to carry it;
// without it a run somebody paused and a run that hung are the same picture.
func TestApplyFoldsTheRunWidePause(t *testing.T) {
	s := api.NewRunState()
	apply := func(seq uint64, op string) {
		t.Helper()
		e := mustEvent(t, api.Event{Seq: seq, Type: api.ControlApplied, Run: "r1"},
			api.ControlAppliedBody{Op: op, ClientID: "tester"})
		if err := s.Apply(e); err != nil {
			t.Fatalf("Apply %s: %v", op, err)
		}
	}

	if s.Run.Paused {
		t.Fatal("a fresh RunState is Paused")
	}
	apply(1, api.OpRunPause)
	if !s.Run.Paused {
		t.Error("control.applied{run.pause} did not fold to RunInfo.Paused")
	}
	apply(2, api.OpRunResume)
	if s.Run.Paused {
		t.Error("control.applied{run.resume} did not clear RunInfo.Paused")
	}

	// A run can be cancelled while paused, and run.finished is then the only
	// event that follows. A finished run rendered as resumable is wrong.
	apply(3, api.OpRunPause)
	fin := mustEvent(t, api.Event{Seq: 4, Type: api.RunFinished, Run: "r1"},
		api.RunFinishedBody{Status: api.RunCancelled})
	if err := s.Apply(fin); err != nil {
		t.Fatalf("Apply run.finished: %v", err)
	}
	if s.Run.Paused {
		t.Error("a finished run is still folded as Paused")
	}

	// An unrelated op must not touch it, which is what keeps this a fold of
	// the two ops rather than of the control channel in general.
	s.Run.Paused = true
	apply(5, api.OpStepRetry)
	if !s.Run.Paused {
		t.Error("control.applied{step.retry} cleared RunInfo.Paused")
	}
}

// TestRunStateJSONIsUnchangedWithoutAPause: RunInfo.Paused is omitempty, so a
// snapshot of a run nobody paused must be byte-for-byte what it was before
// the field existed. GET /api/state is a published shape.
func TestRunStateJSONIsUnchangedWithoutAPause(t *testing.T) {
	s := api.NewRunState()
	b, err := json.Marshal(s.Run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(b); got != `{"id":"","done":false}` {
		t.Errorf("RunInfo of an unpaused run = %s, want no paused key at all", got)
	}
}

// A generated subgraph arrives mid-run, so a renderer learns of its nodes
// from this event rather than from the plan it was handed at the start.
// Folding it exactly like an expansion is deliberate: the TUI and the web UI
// already render groups that appear during a run, so an incrementally growing
// graph costs one fold case instead of the renderer rewrite design §2.8.3
// warns about.
func TestApplyGeneratedSubgraph(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.PlanGenerated, Step: "deploy/discover"},
		api.PlanGeneratedBody{
			Generator: "deploy/discover",
			Children:  []string{"deploy/discover/apply-a", "deploy/discover/apply-b"},
			Nodes:     2,
			Edges:     1,
			Digest:    "sha256:7c1a",
		}))

	if s.Steps["deploy/discover/apply-a"] == nil {
		t.Fatal("a generated subgraph must materialise its children, the way an expansion does")
	}
	if got := s.Steps["deploy/discover/apply-a"].Group; got != "deploy/discover" {
		t.Errorf("Group = %q, want %q: a generated node belongs to the generator that produced it",
			got, "deploy/discover")
	}
}
