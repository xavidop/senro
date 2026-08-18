package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/retry"
)

// scrub removes what legitimately varies between runs, so the golden file
// pins the event stream's shape without pinning a wall clock.
//
//   - Wall-clock and duration fields, the coordinator's cwd, and
//     step.retried's jittered backoff_ms vary per run by construction.
//   - step.started's executor_class and platform come from
//     runtime.GOOS/GOARCH, so a darwin golden would fail on Linux CI.
//   - The plan digest is NOT scrubbed: it is a property of the pipeline
//     alone (the $PATH default that once made it vary now lives in the
//     executor), and pinning it is the golden's mutation detection.
//   - Trace and span identifiers are ALIASED rather than nulled: the trace
//     ID becomes "TRACE" and each distinct span SPAN1, SPAN2, ... in order
//     of first appearance (see traceAliases). trace_flags and tracestate
//     are deliberately untouched, being fixed values these runs can
//     regress.
func scrub(e api.Event, a *traceAliases) api.Event {
	e.TS = api.Event{}.TS
	e.Run = "RUN"
	if e.TraceID != "" {
		e.TraceID = a.trace(e.TraceID)
	}
	if len(e.Payload) > 0 {
		var m map[string]any
		if err := json.Unmarshal(e.Payload, &m); err == nil {
			for _, k := range []string{
				"started_at", "duration_ns", "cwd", "engine_version",
				"executor_class", "platform", "backoff_ms",
				// "key" is a cache key digest, which includes the executor
				// class and platform (runtime.GOOS/GOARCH), so it is
				// host-varying. The workspace digests in ws.snapshot are
				// deliberately NOT scrubbed: a normalized tar is identical
				// on every host, and pinning it is the suite's strongest
				// mutation detection for tar normalization.
				"key",
			} {
				if _, ok := m[k]; ok {
					m[k] = nil
				}
			}
			for _, k := range []string{"span_id", "parent_span_id"} {
				if s, ok := m[k].(string); ok {
					m[k] = a.span(s)
				}
			}
			if raw, ok := m["linked_span_ids"].([]any); ok {
				links := make([]any, 0, len(raw))
				for _, v := range raw {
					if s, ok := v.(string); ok {
						links = append(links, a.span(s))
					}
				}
				m["linked_span_ids"] = links
			}
			if b, err := json.Marshal(m); err == nil {
				e.Payload = b
			}
		}
	}
	return e
}

// traceAliases replaces a run's random trace and span identifiers with
// stable symbolic names. Aliasing rather than nulling is the whole value:
// a nulled span ID pins only that a span existed, leaving the parentage
// model (the part that can actually be got wrong) untested, while aliased
// files pin the graph exactly. Assigned in order of first appearance, so
// names are a property of the event sequence, not the random draw. The
// trace ID gets ONE shared name because it is one value on every event;
// running it through the span counter would hide a regression that made it
// vary by quietly minting TRACE2.
type traceAliases struct {
	traceID string
	spans   map[string]string
}

func newTraceAliases() *traceAliases {
	return &traceAliases{spans: map[string]string{}}
}

func (a *traceAliases) trace(id string) string {
	if a.traceID == "" {
		a.traceID = id
	}
	if id != a.traceID {
		// Two trace IDs in one run is not something to alias away: it is the
		// failure this whole feature is defined against, so it goes into the
		// golden verbatim and the file stops matching.
		return "TRACE-MISMATCH-" + id
	}
	return "TRACE"
}

func (a *traceAliases) span(id string) string {
	if id == "" {
		return id
	}
	if name, ok := a.spans[id]; ok {
		return name
	}
	name := fmt.Sprintf("SPAN%d", len(a.spans)+1)
	a.spans[id] = name
	return name
}

// goldenUpdateMessage reports what an UPDATE_GOLDEN=1 write is ABOUT to do
// to goldenPath, before it does it: created (nothing was there before),
// unchanged (new content is byte-identical to the old), or changed (with
// both sides, so a developer running the update sees exactly what moved).
// Pulled out of compareOrUpdateGolden as a pure function so this decision,
// no longer silent, has something testable
// that does not require capturing *testing.T's own log output.
func goldenUpdateMessage(goldenPath, newContent string) string {
	prev, err := os.ReadFile(goldenPath)
	switch {
	case err != nil:
		return fmt.Sprintf("golden created (no previous file at %s)", goldenPath)
	case string(prev) == newContent:
		return "golden unchanged"
	default:
		return fmt.Sprintf("golden CHANGED at %s:\n--- previous ---\n%s\n--- new ---\n%s",
			goldenPath, string(prev), newContent)
	}
}

// compareOrUpdateGolden scrubs and marshals events one per line, then either
// writes goldenPath (UPDATE_GOLDEN=1) or compares against what is already
// there. Shared by every golden test in this file so the write-or-compare
// logic (and the point at which it can drift from the read side in
// readGolden) exists exactly once.
func compareOrUpdateGolden(t *testing.T, events []api.Event, goldenPath string) {
	t.Helper()
	var got strings.Builder
	aliases := newTraceAliases()
	for _, e := range events {
		b, err := json.Marshal(scrub(e, aliases))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got.WriteString(string(b) + "\n")
	}

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		// Writing and returning before comparing would let a regressed
		// golden pass and be silently overwritten, erasing the
		// plan_digest safety net. Reading the previous content first and
		// logging the diff keeps the regenerate workflow intact while
		// removing only the SILENT half: UPDATE_GOLDEN=1 now shows what
		// changed.
		t.Log(goldenUpdateMessage(goldenPath, got.String()))
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, []byte(got.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to create it): %v", err)
	}
	if got.String() != string(want) {
		t.Errorf("event log does not match golden.\n--- got ---\n%s\n--- want ---\n%s",
			got.String(), want)
	}
}

func TestGoldenTwoStepRun(t *testing.T) {
	pipe := senro.New("golden")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("echo", "setup"))
	l.Step("build", exec.Command("echo", "build")).Needs("setup")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	status, err := engine.Run(t.Context(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01TEST",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s", status)
	}

	compareOrUpdateGolden(t, readLedger(t, dir), filepath.Join("testdata", "golden", "two-step.jsonl"))
}

// The golden log must fold cleanly through the same function every client
// uses. If the engine emits something the fold cannot make sense of, the two
// halves of the system have already diverged.
func TestGoldenFoldsToASucceededRun(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "two-step.jsonl"))
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done || s.Run.Status != api.RunSucceeded {
		t.Errorf("folded run = %+v, want a finished succeeded run", s.Run)
	}
	if len(s.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(s.Steps))
	}
	if len(s.Order) != len(s.Steps) {
		t.Errorf("Order %d vs Steps %d — each step must be recorded once", len(s.Order), len(s.Steps))
	}
}

// TestGoldenRetryRecoveredRun pins the shape of a step that fails once and
// recovers: two attempts, a step.retried between them, and a final
// step.finished reporting "recovered" rather than "succeeded". The marker
// file is the relative ../marker: each attempt has its own directory, so
// one level up lands in the directory both attempts share without putting
// a host-specific absolute path into the pinned command.
func TestGoldenRetryRecoveredRun(t *testing.T) {
	pipe := senro.New("golden")
	l := pipe.Workflow("main")
	l.Step("flaky", exec.Command("sh", "-c",
		`if [ -f ../marker ]; then exit 0; else touch ../marker; exit 1; fi`)).
		RetryPolicy(retry.Policy{
			MaxAttempts: 2,
			On:          retry.OnExitCode(1),
			Backoff:     retry.Backoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, Factor: 2},
		})

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	status, err := engine.Run(t.Context(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01TEST",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceededWithRecovery {
		t.Fatalf("status = %s, want succeeded_with_recovery", status)
	}

	compareOrUpdateGolden(t, readLedger(t, dir), filepath.Join("testdata", "golden", "retry-recovered.jsonl"))
}

func TestGoldenRetryRecoveredRunFoldsToSucceededWithRecovery(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "retry-recovered.jsonl"))
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done || s.Run.Status != api.RunSucceededWithRecovery {
		t.Errorf("folded run = %+v, want a finished succeeded_with_recovery run", s.Run)
	}

	// The run status alone does not say the step was tried twice: a build
	// that recorded "recovered" after a single attempt would fold to the same
	// status. Attempt is the fold's only record of how many tries it took, and
	// it is what a renderer shows as "2/2".
	st := s.Steps["flaky"]
	if st == nil {
		t.Fatal("no flaky step in the fold")
	}
	if st.Attempt != 2 {
		t.Errorf("flaky Attempt = %d, want 2 — the golden pins a step that failed once and "+
			"then passed, and the fold has to show both tries", st.Attempt)
	}
	if st.State != api.StateRecovered {
		t.Errorf("flaky State = %s, want recovered", st.State)
	}
}

// TestGoldenHandlerEvidenceRun pins a failing step whose OnFailure handler
// succeeds: step.finished still says "failed" (a handler's outcome never
// masks the trigger), then handler.started and handler.succeeded. The test
// also reads the handler's log file back: the two events would still be
// emitted by a stubbed execHandler, so the file is the only evidence the
// command actually ran.
func TestGoldenHandlerEvidenceRun(t *testing.T) {
	pipe := senro.New("golden")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", "exit 9")).
		OnFailure(senro.Handler("collect", exec.Command("echo", "evidence")))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	status, err := engine.Run(t.Context(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01TEST",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}

	logPath := filepath.Join(dir, "logs",
		stepid.Encode("deploy/on_failure/collect"), "1", api.StreamStdout)
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("handler log: %v — the handler's command never ran", err)
	}
	if strings.TrimSpace(string(b)) != "evidence" {
		t.Errorf("handler stdout = %q, want %q — the events say the handler succeeded, so "+
			"its command has to have produced its output", b, "evidence")
	}

	compareOrUpdateGolden(t, readLedger(t, dir), filepath.Join("testdata", "golden", "handler-evidence.jsonl"))
}

func TestGoldenHandlerEvidenceRunFoldsToFailed(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "handler-evidence.jsonl"))
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done || s.Run.Status != api.RunFailed {
		t.Errorf("folded run = %+v, want a finished failed run", s.Run)
	}

	// The fold has to carry the handler. Before handler.succeeded existed and
	// before Apply had a case for any handler event, this run folded to
	// Order = [deploy] with no record whatsoever that evidence was collected,
	// so every fold-based client (the TUI, attach, FileSource, the browser UI)
	// could only show it by re-scanning the raw stream, which is the one thing
	// the fold exists to avoid.
	const id = "deploy/on_failure/collect"
	h := s.Handlers[id]
	if h == nil {
		t.Fatalf("no handler %q in the fold; got %v", id, s.Handlers)
	}
	if h.State != api.StateSucceeded {
		t.Errorf("handler State = %q, want succeeded — a handler that started and never "+
			"reported back is one the run abandoned, which is a different fact", h.State)
	}
	if h.Kind != "on_failure" || h.Parent != "deploy" {
		t.Errorf("handler = %+v, want kind on_failure with parent deploy", h)
	}
	if st := s.Steps["deploy"]; st == nil || len(st.Handlers) != 1 || st.Handlers[0] != id {
		t.Errorf("deploy.Handlers = %v, want [%s] — a renderer showing one step's story "+
			"finds its handlers there", s.Steps["deploy"].Handlers, id)
	}

	// And the handler must not have been counted as a step: it is not a node in
	// the plan, and folding it into Steps makes every step count wrong.
	if len(s.Steps) != 1 || len(s.Order) != 1 {
		t.Errorf("Steps = %d, Order = %d, want 1 each — the handler leaked into the step list",
			len(s.Steps), len(s.Order))
	}
}

// TestGoldenCancelledRun pins the shape of a run cancelled while a step is
// genuinely in flight: "quick" finishes before the cancellation, "slow"
// needs it and is still sleeping when it fires. Distinct from
// internal/source's cross-source agreement test on a live cancelled run:
// that proves three Sources agree about whatever the engine emits, this
// pins what the engine emits in the first place.
func TestGoldenCancelledRun(t *testing.T) {
	pipe := senro.New("golden")
	l := pipe.Workflow("main")
	l.Step("quick", exec.Command("true"))
	l.Step("slow", exec.Command("sleep", "5")).Needs("quick")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()

	status, err := engine.Run(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01TEST",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunCancelled {
		t.Fatalf("status = %s, want cancelled", status)
	}

	compareOrUpdateGolden(t, readLedger(t, dir), filepath.Join("testdata", "golden", "cancelled.jsonl"))
}

func TestGoldenCancelledRunFoldsToCancelled(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "cancelled.jsonl"))
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done || s.Run.Status != api.RunCancelled {
		t.Errorf("folded run = %+v, want a finished cancelled run", s.Run)
	}

	// "quick" ran to completion and exited 0 well before cancellation
	// arrived. Recording it as cancelled would tell a resume or a
	// rerun_from to run it again, exactly the bug
	// TestAStepThatSucceededBeforeCancellationIsNotRecordedAsCancelled
	// (this package's own engine-level test) exists to catch at the engine
	// end; this is the same property checked at the fold end, against the
	// pinned golden rather than a fresh run.
	quick := s.Steps["quick"]
	if quick == nil || quick.State != api.StateSucceeded {
		t.Errorf("quick.State = %v, want succeeded: it settled before the run was cancelled", quick)
	}

	// "slow" is what the cancellation actually caught: it must be recorded,
	// one way or another, as not having completed normally. It must never
	// fold as succeeded, which would silently hide that the run was cut
	// short.
	slow := s.Steps["slow"]
	if slow == nil {
		t.Fatal("no slow step in the fold")
	}
	if slow.State == api.StateSucceeded {
		t.Errorf("slow.State = %q, want anything but succeeded: it was still running when the "+
			"run was cancelled", slow.State)
	}

	if len(s.Steps) != 2 || len(s.Order) != 2 {
		t.Errorf("Steps=%d Order=%d, want 2 and 2", len(s.Steps), len(s.Order))
	}
}

// TestGoldenCachedRun pins the event stream of a run served entirely from
// cache: cache.hit, the restored workspace, the replayed log markers, and a
// step.finished carrying state cached with no step.started in front of it.
func TestGoldenCachedRun(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	build := func() *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("cached")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'hello\\n' > a.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("read", exec.Command("cat", "a.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.txt"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	var second []api.Event
	for i, runID := range []string{"r1", "r2"} {
		store, err := storage.Open(cacheDir)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run %d: %v", i+1, err)
		}
		_ = store.Close()
		second = rec.Events()
	}
	compareOrUpdateGolden(t, second, filepath.Join("testdata", "golden", "cached.jsonl"))
}

func TestGoldenCachedRunFoldsToSucceeded(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "cached.jsonl"))
	st := api.NewRunState()
	for _, e := range events {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", st.Run.Status)
	}
	if st.Steps["read"].State != api.StateCached {
		t.Errorf("read = %s, want cached", st.Steps["read"].State)
	}
}

// twoUnitGraph is a hand-rolled senro.UnitGraph, the same escape hatch
// senro_test.go's own dupUnitsGraph uses, that returns two fixed units
// without touching the filesystem, so TestGoldenExpandedRun's plan is
// deterministic regardless of what directories happen to exist next to
// wherever the test binary runs.
type twoUnitGraph struct{}

func (twoUnitGraph) Units(context.Context, string) ([]senro.Unit, error) {
	return []senro.Unit{
		{ID: "a", Name: "a", Dir: "a"},
		{ID: "b", Name: "b", Dir: "b"},
	}, nil
}

func (twoUnitGraph) Describe() string { return "two fixed units" }

// TestGoldenExpandedRun pins the shape of a run with one expansion:
// plan.expanded first, then every step.created carrying "group":"lint",
// then each child's events carrying the group in the envelope.
//
// MaxParallel(1) does NOT make the order deterministic: it bounds how many
// children run at once, not the order they start, and the two siblings
// raced for the permit (order flipped on about a quarter of -count=30
// runs). Node.Needs is set below, after Build, to make the order a real
// dependency edge; grouping and the group semaphore are unchanged
// (group_test.go proves the semaphore's behavior; this pins the wire
// shape).
func TestGoldenExpandedRun(t *testing.T) {
	pipe := senro.New("golden")
	w := pipe.Workflow("main")
	w.Expand("lint", twoUnitGraph{}).
		MaxParallel(1).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Name))
		})

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	b, ok := p.Node("lint[unit=b]")
	if !ok {
		t.Fatal("no lint[unit=b] node in the built plan")
	}
	b.Needs = []string{"lint[unit=a]"}

	dir := t.TempDir()
	status, err := engine.Run(t.Context(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01TEST",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s", status)
	}

	compareOrUpdateGolden(t, readLedger(t, dir), filepath.Join("testdata", "golden", "expanded.jsonl"))
}

func TestGoldenExpandedRunFoldsToAGroupedSucceededRun(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "expanded.jsonl"))
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done || s.Run.Status != api.RunSucceeded {
		t.Errorf("folded run = %+v, want a finished succeeded run", s.Run)
	}

	exp := s.Expansions["lint"]
	if exp == nil {
		t.Fatal("no expansion \"lint\" in the fold")
	}
	want := []string{"lint[unit=a]", "lint[unit=b]"}
	if len(exp.Children) != 2 || exp.Children[0] != want[0] || exp.Children[1] != want[1] {
		t.Errorf("expansion children = %v, want %v", exp.Children, want)
	}

	for _, id := range want {
		st := s.Steps[id]
		if st == nil {
			t.Fatalf("no step %q in the fold", id)
		}
		if st.Group != "lint" {
			t.Errorf("step %q Group = %q, want lint: plan.expanded materialised it, but "+
				"step.created's own Group field must not have overwritten it with an empty one", id, st.Group)
		}
		if st.State != api.StateSucceeded {
			t.Errorf("step %q State = %s, want succeeded", id, st.State)
		}
	}

	if len(s.Steps) != 2 || len(s.Order) != 2 {
		t.Errorf("Steps=%d Order=%d, want 2 and 2", len(s.Steps), len(s.Order))
	}
}

// readGolden reads a golden fixture file and unmarshals each line into an
// api.Event.
func readGolden(t *testing.T, path string) []api.Event {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("readGolden: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	events := make([]api.Event, 0, len(lines))
	for i, line := range lines {
		if line == "" {
			continue
		}
		var e api.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("readGolden: line %d: %v", i, err)
		}
		events = append(events, e)
	}
	return events
}

// goldenUpdateMessage is what makes an UPDATE_GOLDEN=1
// write no longer silent. Each of these pins one of its three outcomes
// directly, without needing to capture *testing.T's own log output.

func TestGoldenUpdateMessageReportsCreationForANewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent.jsonl")
	got := goldenUpdateMessage(path, "content\n")
	if !strings.Contains(got, "created") {
		t.Errorf("goldenUpdateMessage = %q, want it to say the golden was created", got)
	}
}

func TestGoldenUpdateMessageReportsNoChangeForIdenticalContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same.jsonl")
	if err := os.WriteFile(path, []byte("content\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got := goldenUpdateMessage(path, "content\n")
	if got != "golden unchanged" {
		t.Errorf("goldenUpdateMessage = %q, want %q", got, "golden unchanged")
	}
}

// This is the actual mutation proof: a golden that has drifted from what
// the current code produces (a regression, or the file having been hand-
// edited or corrupted) must be REPORTED as a change, not silently folded
// into "golden updated" with no way to tell it apart from an intentional
// regeneration.
func TestGoldenUpdateMessageReportsAChangeWithBothSidesForDriftedContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drifted.jsonl")
	if err := os.WriteFile(path, []byte("old-content\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got := goldenUpdateMessage(path, "new-content\n")
	if !strings.Contains(got, "CHANGED") {
		t.Errorf("goldenUpdateMessage = %q, want it to flag a change", got)
	}
	if !strings.Contains(got, "old-content") || !strings.Contains(got, "new-content") {
		t.Errorf("goldenUpdateMessage = %q, want both the previous and the new content", got)
	}
}
