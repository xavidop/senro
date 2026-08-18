package api_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
)

// replay folds a fixture file, which is exactly what FileSource and offline
// replay do.
func replay(t *testing.T, path string) *api.RunState {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	s := api.NewRunState()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e api.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("%s:%d: unmarshal: %v", path, line, err)
		}
		if err := s.Apply(e); err != nil {
			t.Fatalf("%s:%d: apply: %v", path, line, err)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return s
}

// fixturePaths is the published corpus. Every test that walks the whole set
// goes through here, so a fixture added without a dedicated test of its own is
// still held to every rule below.
func fixturePaths(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob("testdata/fixtures/*.jsonl")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no fixtures found")
	}
	return paths
}

// readFixture decodes one fixture into events without folding them, for the
// checks that are about the sequence rather than about the resulting state.
func readFixture(t *testing.T, path string) []api.Event {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var events []api.Event
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var e api.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("%s:%d: %v", path, i+1, err)
		}
		events = append(events, e)
	}
	return events
}

// Every fixture must fold without error. This is the conformance contract a
// third-party client implementation has to satisfy too.
func TestAllFixturesFold(t *testing.T) {
	for _, p := range fixturePaths(t) {
		t.Run(filepath.Base(p), func(t *testing.T) {
			s := replay(t, p)
			if s.Run.ID == "" {
				t.Error("fixture produced no run ID")
			}
			// Every fixture is a complete recorded run. Without these, a fixture
			// truncated to its first line still passes, and a future fixture
			// added without a dedicated test gets rubber-stamped.
			if !s.Run.Done {
				t.Error("fixture never reached run.finished")
			}
			if s.Run.Status == "" {
				t.Error("fixture recorded no terminal run status")
			}
			if len(s.Steps) == 0 {
				t.Error("fixture recorded no steps")
			}
			if len(s.Order) != len(s.Steps) {
				t.Errorf("Order has %d entries for %d steps — each step must be recorded once",
					len(s.Order), len(s.Steps))
			}
		})
	}
}

func TestFixtureMinimalSuccess(t *testing.T) {
	s := replay(t, "testdata/fixtures/minimal-success.jsonl")

	if s.Run.Status != api.RunSucceeded {
		t.Errorf("Status = %s, want succeeded", s.Run.Status)
	}
	if len(s.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(s.Steps))
	}
	if st := s.Steps["build"]; st == nil || st.State != api.StateSucceeded {
		t.Errorf("build = %+v", st)
	}
	if got := s.Steps["build"].LogBytes[api.StreamStdout]; got != 61 {
		t.Errorf("build stdout bytes = %d, want 61", got)
	}
	// The fixture encodes a dependency edge; nothing asserted it, so deleting
	// a step.created line passed silently.
	if got := s.Steps["build"].Kind; got != "exec" {
		t.Errorf("build kind = %q, want exec", got)
	}
	if got := s.Steps["build"].Needs; len(got) != 1 || got[0] != "setup" {
		t.Errorf("build needs = %v, want [setup]", got)
	}
	// Same hole for setup: the fold creates a step implicitly on step.started,
	// so without this, deleting setup's step.created line also passed silently.
	if got := s.Steps["setup"].Kind; got != "exec" {
		t.Errorf("setup kind = %q, want exec", got)
	}
}

// The recovery case is the reason the taxonomy exists: this run is green, but
// not the same green as a clean run.
func TestFixtureRetryRecovered(t *testing.T) {
	s := replay(t, "testdata/fixtures/retry-recovered.jsonl")

	if s.Run.Status != api.RunSucceededWithRecovery {
		t.Errorf("Status = %s, want succeeded_with_recovery", s.Run.Status)
	}
	st := s.Steps["flaky"]
	if st.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", st.Attempt)
	}
	if st.State != api.StateRecovered {
		t.Errorf("State = %s, want recovered", st.State)
	}
}

// The handler case: a failed step whose evidence handler succeeded and whose
// cleanup handler did not. It is the one shape where the handler event trio
// carries information a client cannot reconstruct: a handler with no terminal
// event is one the run abandoned, and a lock may still be held.
func TestFixtureHandlerCleanup(t *testing.T) {
	s := replay(t, "testdata/fixtures/handler-cleanup.jsonl")

	if s.Run.Status != api.RunFailed {
		t.Errorf("Status = %s, want failed", s.Run.Status)
	}
	st := s.Steps["deploy"]
	if st == nil || st.State != api.StateFailed || st.ExitCode != 9 {
		t.Fatalf("deploy = %+v, want failed with exit 9 — a handler's outcome must never "+
			"reach back into its parent's state", st)
	}
	want := []string{"deploy/on_failure/collect", "deploy/always/unlock"}
	if len(st.Handlers) != len(want) {
		t.Fatalf("deploy.Handlers = %v, want %v", st.Handlers, want)
	}
	for i := range want {
		if st.Handlers[i] != want[i] {
			t.Errorf("deploy.Handlers[%d] = %q, want %q", i, st.Handlers[i], want[i])
		}
	}
	if h := s.Handlers[want[0]]; h == nil || h.State != api.StateSucceeded || h.Kind != "on_failure" {
		t.Errorf("collect = %+v, want a succeeded on_failure handler", h)
	}
	h := s.Handlers[want[1]]
	if h == nil || h.State != api.StateFailed || h.Kind != "always" {
		t.Fatalf("unlock = %+v, want a failed always handler", h)
	}
	if h.Error != "exit status 1" {
		t.Errorf("unlock Error = %q, want the handler's own error", h.Error)
	}
	// A handler is not a step.
	if len(s.Steps) != 1 {
		t.Errorf("Steps = %d, want 1 — a handler was folded in as a step", len(s.Steps))
	}
}

// TestFixtureEventSequences checks the corpus against the sequencing rules
// the reference implementation actually follows, not just the field shapes
// TestFixturesMatchEncoder pins. The engine emits exactly ONE step.finished
// per step, at the end of the retry loop; a fixture with per-attempt
// finishes would teach a client to expect terminal events that never
// arrive, and field-shape checks cannot see that.
func TestFixtureEventSequences(t *testing.T) {
	for _, p := range fixturePaths(t) {
		t.Run(filepath.Base(p), func(t *testing.T) {
			events := readFixture(t, p)

			finished := make(map[string]int)
			handlerStarted := make(map[string]int)
			handlerTerminal := make(map[string]int)
			steps := make(map[string]bool)
			var lastSeq uint64
			for i, e := range events {
				line := i + 1
				if e.V != api.Version {
					t.Errorf("%s:%d: v = %d, want %d", p, line, e.V, api.Version)
				}
				if e.Seq <= lastSeq {
					t.Errorf("%s:%d: seq %d does not advance past %d — Apply rejects a "+
						"regressing seq, so such a fixture cannot be folded at all",
						p, line, e.Seq, lastSeq)
				}
				lastSeq = e.Seq
				if e.Run == "" {
					t.Errorf("%s:%d: no run ID; every envelope carries one", p, line)
				}

				switch e.Type {
				case api.StepCreated, api.StepStarted:
					steps[e.Step] = true
				case api.StepFinished:
					steps[e.Step] = true
					finished[e.Step]++
				case api.HandlerStarted:
					handlerStarted[e.Step]++
				case api.HandlerSucceeded, api.HandlerFailed:
					handlerTerminal[e.Step]++
				}

				if e.Type == api.RunFinished && line != len(events) {
					t.Errorf("%s:%d: run.finished is not the last event — the engine seals "+
						"the stream in the same critical section that appends it", p, line)
				}
			}

			for step, n := range finished {
				if n != 1 {
					t.Errorf("%s: %d step.finished events for %q, want exactly 1 — the engine "+
						"emits one per step, at the end of its retry loop, never one per attempt",
						p, n, step)
				}
			}
			for h, n := range handlerStarted {
				if n != 1 {
					t.Errorf("%s: %d handler.started events for %q, want 1", p, n, h)
				}
				if got := handlerTerminal[h]; got != 1 {
					t.Errorf("%s: %q has %d terminal handler events, want exactly 1 — a "+
						"handler reports started and then one of succeeded or failed; "+
						"neither of them is what an abandoned handler looks like", p, h, got)
				}
			}
			for h := range handlerTerminal {
				if handlerStarted[h] == 0 {
					t.Errorf("%s: %q reported an outcome without a handler.started", p, h)
				}
			}
		})
	}
}

func TestFixtureFanoutPartial(t *testing.T) {
	s := replay(t, "testdata/fixtures/fanout-partial.jsonl")

	counts := s.Group("test/per-unit")
	if counts.Total != 3 {
		t.Errorf("Total = %d, want 3", counts.Total)
	}
	if counts.Failed != 1 {
		t.Errorf("Failed = %d, want 1", counts.Failed)
	}
	if counts.Cached != 1 {
		t.Errorf("Cached = %d, want 1", counts.Cached)
	}
	if s.Expansions["test/per-unit"].Skipped != 12 {
		t.Errorf("Skipped = %d, want 12", s.Expansions["test/per-unit"].Skipped)
	}
}

// TestFixturesMatchEncoder pins the corpus to what the Go types actually emit.
// These files are both the fold's integration suite and the public
// conformance artifact; if the two drift, a third-party client is authored
// against a format the reference implementation never produces.
func TestFixturesMatchEncoder(t *testing.T) {
	body, err := json.Marshal(api.StepFinishedBody{State: api.StateSucceeded})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["duration_ns"]; !ok {
		t.Fatal("encoder emits no duration_ns — fixtures and this test disagree about the wire format")
	}
	if _, ok := m["exit_code"]; ok {
		t.Fatal("encoder emits exit_code for a zero value — fixtures assume it is omitted")
	}

	for _, p := range fixturePaths(t) {
		for i, e := range readFixture(t, p) {
			if e.Type != api.StepFinished && e.Type != api.RunFinished {
				continue
			}
			var pm map[string]any
			if err := json.Unmarshal(e.Payload, &pm); err != nil {
				t.Fatalf("%s:%d payload: %v", p, i+1, err)
			}
			if _, ok := pm["duration_ns"]; !ok {
				t.Errorf("%s:%d: %s payload omits duration_ns, which the encoder always emits", p, i+1, e.Type)
			}
			if v, ok := pm["exit_code"]; ok && v == float64(0) {
				t.Errorf("%s:%d: payload carries exit_code:0, which the encoder omits", p, i+1)
			}
		}
	}
}

// A build from the future: unknown event types and unknown payload fields.
// An old client must fold it without error and without losing known state.
func TestFixtureForwardCompatibility(t *testing.T) {
	s := replay(t, "testdata/fixtures/forward-compat.jsonl")

	if s.Run.Status != api.RunSucceeded {
		t.Errorf("Status = %s, want succeeded", s.Run.Status)
	}
	if st := s.Steps["build"]; st == nil || st.State != api.StateSucceeded {
		t.Errorf("known events must still fold: build = %+v", st)
	}
}
