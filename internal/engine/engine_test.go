package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
)

func run(t *testing.T, p *plan.Plan) (api.RunStatus, *sink.RecordingSink, string) {
	t.Helper()
	dir := t.TempDir()
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        rec,
		MaxParallel: 4,
		RunID:       "01TEST",
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	return status, rec, dir
}

func TestRunsAChainInOrder(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "first", Kind: "exec", Cmd: []string{"echo", "1"}},
		{ID: "second", Kind: "exec", Cmd: []string{"echo", "2"}, Needs: []string{"first"}},
	}}
	status, _, dir := run(t, p)
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}

	events := readLedger(t, dir)
	firstDone := indexOf(events, api.StepFinished, "first")
	secondStart := indexOf(events, api.StepStarted, "second")
	if firstDone < 0 || secondStart < 0 || firstDone > secondStart {
		t.Errorf("dependency order violated: first finished at %d, second started at %d",
			firstDone, secondStart)
	}
}

func TestFailurePropagatesToDependentsOnly(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
		{ID: "downstream", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"boom"}},
		{ID: "unrelated", Kind: "exec", Cmd: []string{"echo", "y"}},
	}}
	status, _, dir := run(t, p)
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}

	st := foldStates(t, dir)
	if st["boom"] != api.StateFailed {
		t.Errorf("boom = %s, want failed", st["boom"])
	}
	if st["downstream"] != api.StateSkippedUpstreamFailed {
		t.Errorf("downstream = %s, want skipped_upstream_failed", st["downstream"])
	}
	// An unrelated branch runs to completion, so one failure yields one report
	// rather than a half-explored graph.
	if st["unrelated"] != api.StateSucceeded {
		t.Errorf("unrelated = %s, want succeeded", st["unrelated"])
	}
}

func TestContinueOnErrorLetsDependentsRun(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "advisory", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}, ContinueOnError: true},
		{ID: "after", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"advisory"}},
	}}
	_, _, dir := run(t, p)
	st := foldStates(t, dir)
	if st["after"] != api.StateSucceeded {
		t.Errorf("after = %s, want succeeded — ContinueOnError must not block dependents", st["after"])
	}
}

func TestLogsAreWrittenAndMarked(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "talker", Kind: "exec", Cmd: []string{"echo", "hello"}},
	}}
	_, _, dir := run(t, p)

	events := readLedger(t, dir)
	var total int64
	for _, e := range events {
		if e.Type != api.StepLogAppended {
			continue
		}
		var b api.StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		total += b.Len
	}
	if total != 6 {
		t.Errorf("log markers total %d bytes, want 6 for \"hello\\n\"", total)
	}
}

// TestExternalCommandsGetADefaultPATH: localexec does not inherit the
// coordinator's environment, so a step declaring no Env has none, and a
// real external program needs PATH to be found. The default comes from the
// executor (a plan-level default made plan.Digest() vary with the
// operator's $PATH); this asserts it reaches a step end to end. It asserts
// the actual $PATH value the child sees, not whether some binary resolves:
// inferring via "go version" passed spuriously on Intel macOS, where
// /usr/local/bin is on sh's compiled-in fallback search path.
func TestExternalCommandsGetADefaultPATH(t *testing.T) {
	want := os.Getenv("PATH")
	if want == "" {
		t.Skip("coordinator has no PATH; nothing to prove")
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{
			ID: "checker", Kind: "exec",
			Cmd: []string{"sh", "-c", `[ "$PATH" = "$WANT" ]`},
			Env: []string{"WANT=" + want},
		},
	}}
	status, _, dir := run(t, p)
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded — child $PATH did not match the coordinator's", status)
	}
	if st := foldStates(t, dir)["checker"]; st != api.StateSucceeded {
		t.Errorf("checker = %s, want succeeded", st)
	}
}

// The ledger is the source of truth. Every event a sink saw must be in it,
// with the same sequence number.
func TestSinkSeesExactlyWhatTheLedgerRecorded(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "a"}},
	}}
	_, rec, dir := run(t, p)

	ledger := readLedger(t, dir)
	waitForSink(t, rec, len(ledger))

	seen := make(map[uint64]api.Type, len(ledger))
	for _, e := range ledger {
		seen[e.Seq] = e.Type
	}
	for _, e := range rec.Events() {
		if typ, ok := seen[e.Seq]; !ok {
			t.Errorf("sink saw seq %d which is not in the ledger", e.Seq)
		} else if typ != e.Type {
			t.Errorf("seq %d: sink saw %s, ledger has %s", e.Seq, e.Type, typ)
		}
	}
}

// TestSinkStreamFoldsInOrderUnderWideFanout guards a real trap: if emit
// released the ledger's seq allocation before Sink.Emit, two concurrent
// steps could hand the sink seq N+1 before N, and api.RunState.Apply
// rejects a regressing seq. This folds the SINK's own stream under a plan
// wide and chatty enough to make the race observable.
func TestSinkStreamFoldsInOrderUnderWideFanout(t *testing.T) {
	const nodes = 8
	const lines = 50
	p := &plan.Plan{Version: 1}
	for i := 0; i < nodes; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID:   fmt.Sprintf("n%d", i),
			Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf(
				"i=1; while [ $i -le %d ]; do echo line$i; i=$((i+1)); done", lines)},
		})
	}

	dir := t.TempDir()
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        rec,
		MaxParallel: nodes,
		RunID:       "01WIDE",
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}

	s := api.NewRunState()
	for i, e := range rec.Events() {
		if err := s.Apply(e); err != nil {
			t.Fatalf("sink stream event %d (%s, step %q) did not fold in order: %v",
				i, e.Type, e.Step, err)
		}
	}
}

// TestCancellationStopsSchedulingAndMarksUnstartedCancelled: a killed
// process must not be classified as an ordinary failure, and goroutines
// already dispatched behind the semaphore must not run anyway, emitting
// step.started for work that never executed. MaxParallel 1 with six
// sleepers queues five behind the semaphore (the "never even started"
// half); a seventh, downstream step exercises readySet's cancelled
// short-circuit.
func TestCancellationStopsSchedulingAndMarksUnstartedCancelled(t *testing.T) {
	const n = 6
	p := &plan.Plan{Version: 1}
	for i := 0; i < n; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("s%d", i), Kind: "exec", Cmd: []string{"sh", "-c", "sleep 5"},
		})
	}
	p.Nodes = append(p.Nodes, plan.Node{
		ID: "downstream", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"s0"},
	})

	dir := t.TempDir()
	rec := sink.Recording()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	status, err := engine.Run(ctx, p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        rec,
		MaxParallel: 1,
		RunID:       "01CANCEL",
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	if status != api.RunCancelled {
		t.Errorf("status = %s, want cancelled", status)
	}

	st := foldStates(t, dir)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("s%d", i)
		if st[id] != api.StateCancelled {
			t.Errorf("%s = %s, want cancelled", id, st[id])
		}
	}
	if st["downstream"] != api.StateCancelled {
		t.Errorf("downstream = %s, want cancelled — it was never even dispatched", st["downstream"])
	}

	// The event log must still fold cleanly end to end after a cancelled run.
	s := api.NewRunState()
	for _, e := range readLedger(t, dir) {
		if err := s.Apply(e); err != nil {
			t.Fatalf("ledger did not fold cleanly after cancellation: %v", err)
		}
	}
	if !s.Run.Done {
		t.Error("run.finished was never recorded")
	}
}

// TestInvalidPlanIsRejectedNotSilentlySucceeded guards against a real
// trap. engine.Run accepts any *plan.Plan, not only ones senro.Build
// already validated, including one from plan.Unmarshal, which does not
// validate. A cyclic plan left unrejected
// leaves the scheduler with no ready nodes on its very first pass, which
// looks exactly like the loop breaking because the run is simply done: a
// clean RunSucceeded over zero executed steps and zero step.finished
// events.
func TestInvalidPlanIsRejectedNotSilentlySucceeded(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "x", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"y"}},
		{ID: "y", Kind: "exec", Cmd: []string{"echo", "y"}, Needs: []string{"x"}},
	}}
	dir := t.TempDir()
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01CYCLE",
	})
	if err == nil {
		t.Fatal("Run must reject a cyclic plan, not report success over zero executed steps")
	}
}

// TestCancelledRunDoesNotStartQueuedSteps is a carried finding from the
// scheduler's review: a step still waiting on the semaphore when a run is
// cancelled must never emit step.started: it opened no sandbox and ran
// nothing. Terminal states alone cannot prove this: runStep's own ctx check
// independently recovers the state, so a regression here is invisible except
// in the event stream.
func TestCancelledRunDoesNotStartQueuedSteps(t *testing.T) {
	const nodes = 10
	p := &plan.Plan{Version: 1}
	for i := 0; i < nodes; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("q%d", i), Kind: "exec",
			Cmd: []string{"sleep", "5"},
		})
	}

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()

	status, err := engine.Run(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 2, RunID: "01CANCEL",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunCancelled {
		t.Errorf("status = %s, want cancelled", status)
	}

	var started int
	for _, e := range readLedger(t, dir) {
		if e.Type == api.StepStarted {
			started++
		}
	}
	if started > 2 {
		t.Errorf("%d steps emitted step.started with MaxParallel=2 — queued steps must not start", started)
	}
}

// A completed run directory used to contain only events.jsonl and logs/:
// plan.Marshal's single caller was a test. plan.resolved's digest therefore
// named an artifact that did not exist, nothing could re-run or reproduce the
// run, and the API a later phase serves the plan from had nothing to read.
func TestRunWritesPlanJSON(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "first", Kind: "exec", Cmd: []string{"echo", "1"}},
		{ID: "second", Kind: "exec", Cmd: []string{"echo", "2"}, Needs: []string{"first"}},
	}}
	_, _, dir := run(t, p)

	b, err := os.ReadFile(filepath.Join(dir, "plan.json"))
	if err != nil {
		t.Fatalf("plan.json: %v — a run directory must record the timetable it executed", err)
	}
	got, err := plan.Unmarshal(b)
	if err != nil {
		t.Fatalf("plan.json does not round-trip through plan.Unmarshal: %v", err)
	}
	if len(got.Nodes) != len(p.Nodes) {
		t.Fatalf("round-tripped plan has %d nodes, want %d", len(got.Nodes), len(p.Nodes))
	}
	if got.Digest() != p.Digest() {
		t.Errorf("round-tripped digest = %s, want %s", got.Digest(), p.Digest())
	}

	// The digest recorded in plan.resolved must identify this exact file, or
	// the event stream points at a timetable other than the one on disk.
	var recorded string
	for _, e := range readLedger(t, dir) {
		if e.Type != api.PlanResolved {
			continue
		}
		var body api.PlanResolvedBody
		if err := e.Decode(&body); err != nil {
			t.Fatalf("decode plan.resolved: %v", err)
		}
		recorded = body.Digest
	}
	if recorded == "" {
		t.Fatal("no plan.resolved event in the ledger")
	}
	if recorded != got.Digest() {
		t.Errorf("plan.resolved digest = %s but plan.json hashes to %s", recorded, got.Digest())
	}
}

// A run whose timetable could not be recorded is not reproducible, so the
// write is fatal exactly like a ledger failure rather than a silent omission.
func TestPlanJSONWriteFailureIsFatal(t *testing.T) {
	dir := t.TempDir()
	// A directory where plan.json must go: os.WriteFile cannot replace it.
	if err := os.MkdirAll(filepath.Join(dir, "plan.json"), 0o755); err != nil {
		t.Fatal(err)
	}

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "a"}},
	}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01PLAN",
	})
	if err == nil {
		t.Fatal("Run must fail when the plan cannot be written, not execute an unrecorded run")
	}
	if !strings.Contains(err.Error(), "plan.json") {
		t.Errorf("err = %v, want it to name plan.json", err)
	}
}

// TestSimultaneousSkipsAreEmittedInSortedOrder is a regression test for
// schedule's sort.Strings(ids). Nodes that settle in the same scheduler pass
// come out of a map, and Go randomises map iteration, so without the sort two
// runs of the same plan produce byte-different event logs, which is exactly
// what the golden test in this package pins. The reviewer removed the sort
// and the whole suite stayed green.
//
// One failing root with many dependents settles all of them in a single pass.
func TestSimultaneousSkipsAreEmittedInSortedOrder(t *testing.T) {
	const dependents = 50
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "root", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
	}}
	for i := 0; i < dependents; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			// Zero-padded so lexical order is unambiguous and differs from
			// insertion order only by chance, not by construction.
			ID: fmt.Sprintf("d%02d", i), Kind: "exec",
			Cmd: []string{"echo", "x"}, Needs: []string{"root"},
		})
	}
	_, _, dir := run(t, p)

	var skipped []string
	for _, e := range readLedger(t, dir) {
		if e.Type != api.StepFinished || e.Step == "root" {
			continue
		}
		skipped = append(skipped, e.Step)
	}
	if len(skipped) != dependents {
		t.Fatalf("%d step.finished events for dependents, want %d", len(skipped), dependents)
	}
	if !sort.StringsAreSorted(skipped) {
		t.Errorf("step.finished IDs for simultaneously skipped nodes are not sorted: %v\n"+
			"map iteration order reached the ledger, so two runs of this plan differ byte for byte",
			skipped)
	}
}

func readLedger(t *testing.T, dir string) []api.Event {
	t.Helper()
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("readLedger: %v", err)
	}
	return events
}

func indexOf(events []api.Event, typ api.Type, step string) int {
	for i, e := range events {
		if e.Type == typ && e.Step == step {
			return i
		}
	}
	return -1
}

func foldStates(t *testing.T, dir string) map[string]api.State {
	t.Helper()
	events := readLedger(t, dir)
	s := api.NewRunState()
	for _, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("foldStates: Apply: %v", err)
		}
	}
	out := make(map[string]api.State, len(s.Steps))
	for id, st := range s.Steps {
		out[id] = st.State
	}
	return out
}

// waitForSink polls rec.Events() up to two seconds for the expected count,
// since Multi fans out asynchronously.
func waitForSink(t *testing.T, rec *sink.RecordingSink, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.Events()) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if got := len(rec.Events()); got < want {
		t.Fatalf("waitForSink: got %d events after 2s, want at least %d", got, want)
	}
}

// TestAValueInARuntimeErrorIsRedactedInTheLedger pins the backstop: a
// payload field the run-start guard cannot see is still caught by
// redactPayload as it reaches the ledger. The vehicle is Retry.Predicate,
// the one field checkSecretChannels has no reason to scan (no cache-key
// component reads it): an unparseable predicate echoes its raw text into
// step.finished's error field. Reachable only through a raw *plan.Plan,
// since every builder Retry constructor round-trips through Parse.
func TestAValueInARuntimeErrorIsRedactedInTheLedger(t *testing.T) {
	const value = "s3cr3t-value-here"

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "exec", Cmd: []string{"true"},
		Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "garbage-" + value, BackoffBaseMS: 1},
	}}}

	// The run fails, which is the point: the failure is what carries the
	// value into an event. senro.RunPlan, not senro.Run: RunPlan takes the
	// raw *plan.Plan this test needs and still returns the same *RunError
	// senro.Run does, so the check below on the caller's own error is
	// unchanged from what this test always pinned.
	runErr := senro.RunPlan(context.Background(), p,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if runErr == nil {
		t.Fatal("the run succeeded; it was supposed to fail on an unparseable retry predicate")
	}

	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	// The canary, twice over: the ledger must contain the step's finished
	// event AND evidence that redaction ran, or "the value is absent" is a
	// statement about an empty file.
	if !bytes.Contains(raw, []byte(`"step.finished"`)) {
		t.Fatalf("no step.finished in the ledger; the checks below prove nothing")
	}
	if !bytes.Contains(raw, []byte(redact.Placeholder)) {
		t.Fatalf("no placeholder in the ledger; redaction did not run on this path")
	}
	if bytes.Contains(raw, []byte(value)) {
		t.Error("the ledger contains the secret value")
	}
	// And the run's OWN error, which senro.RunPlan hands the caller, must not
	// carry it either: RunError renders step names, never step error text,
	// and this pins that.
	if strings.Contains(runErr.Error(), value) {
		t.Errorf("the error senro.RunPlan returned contains the value: %q", runErr)
	}
}

// TestAValueOnAStepsStdoutNeverReachesTheLogFile exercises senro's real
// exposure: a secret shows up on a child process's stdout. go test -v
// echoing an env var, curl -v printing a URL with a token, a Helm error
// quoting the values file. mamori cannot see that stream, and
// secret.String cannot protect a byte that a subprocess wrote.
//
// The value arrives through a file the step reads, so nothing about this case
// is reachable by a plan-time check: it has to be the redactor or nothing.
func TestAValueOnAStepsStdoutNeverReachesTheLogFile(t *testing.T) {
	const value = "s3cr3t-value-here"

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "values.yaml"),
		[]byte("token: "+value+"\nother: fine\n"), 0o600); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").Step("show", exec.Command("cat", "values.yaml")).WorkDir(work)

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logPath := eventlog.NewLogSet(dir).Path("show", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading %s: %v", logPath, err)
	}
	// The canary: the step really did run and really did print the file, so
	// "the value is absent" is not a statement about an empty log.
	if !bytes.Contains(body, []byte("other: fine")) {
		t.Fatalf("the log does not contain the step's output at all: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("no placeholder in the log; redaction did not run on this path")
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the log file contains the secret value")
	}
}

// TestSecretRedactedReportsTheCountForTheAttempt covers the secret.redacted
// event: {"type":"secret.redacted","step":"...","count":3}, emitted so the
// UI can show redaction is live.
func TestSecretRedactedReportsTheCountForTheAttempt(t *testing.T) {
	const value = "s3cr3t-value-here"

	work := t.TempDir()
	// Three occurrences, two on stdout and one on stderr, so the assertion
	// also proves the two streams' counts are summed into one event rather
	// than reported as two.
	script := "cat a.txt; cat a.txt; cat b.txt 1>&2"
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte(value+"\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "b.txt"), []byte(value+"\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("leak", exec.Command("sh", "-c", script)).WorkDir(work)
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// engine.Run with a recording sink, the established pattern in this
	// package for observing an event stream (see golden_test.go). senro.Run
	// has no WithSink option in this build, and the events this test asserts
	// on are engine-emitted, so this is the real entry point for them.
	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var count int
	var seen int
	for _, e := range rec.Events() {
		if e.Type != api.SecretRedacted {
			continue
		}
		seen++
		if e.Step != "leak" {
			t.Errorf("secret.redacted has step %q, want leak", e.Step)
		}
		var b api.SecretRedactedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decoding secret.redacted: %v", err)
		}
		count += b.Count
	}
	if seen != 1 {
		t.Fatalf("got %d secret.redacted events for one attempt, want exactly 1", seen)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3 (two on stdout, one on stderr)", count)
	}
}

// TestNoSecretRedactedWhenNothingWasRedacted is the negative case. A run that
// emitted the event with a zero count would tell a UI that redaction fired
// when it did not, which is the same class of lie as not redacting.
func TestNoSecretRedactedWhenNothingWasRedacted(t *testing.T) {
	pipe := senro.New("p")
	pipe.Workflow("w").Step("clean", exec.Command("echo", "nothing to see"))
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST",
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var stepFinished bool
	for _, e := range rec.Events() {
		if e.Type == api.StepFinished {
			stepFinished = true
		}
		if e.Type == api.SecretRedacted {
			t.Errorf("secret.redacted emitted for a run with no secrets: %+v", e)
		}
	}
	if !stepFinished {
		t.Fatal("no step.finished; the assertion above proves nothing")
	}
}

// TestLogMarkersDescribeTheRedactedFile is the composition check between this
// task and the attach server's range requests. Redaction changes byte counts,
// and a step.log.appended offset that points past the end of the file is a
// scrollback fetch that returns garbage or nothing.
func TestLogMarkersDescribeTheRedactedFile(t *testing.T) {
	const value = "s3cr3t-value-here"

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "v.txt"),
		[]byte("a "+value+" b\nc "+value+" d\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("show", exec.Command("cat", "v.txt")).WorkDir(work)
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	st, err := os.Stat(eventlog.NewLogSet(dir).Path("show", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	var markers int
	var end int64
	for _, e := range rec.Events() {
		if e.Type != api.StepLogAppended || e.Step != "show" {
			continue
		}
		var b api.StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if b.Stream != api.StreamStdout {
			continue
		}
		markers++
		if b.Offset+b.Len > st.Size() {
			t.Errorf("marker [%d,%d) reaches past the %d-byte log file",
				b.Offset, b.Offset+b.Len, st.Size())
		}
		if b.Offset > end {
			t.Errorf("marker at offset %d leaves a gap after %d", b.Offset, end)
		}
		end = b.Offset + b.Len
	}
	if markers == 0 {
		t.Fatal("no step.log.appended markers; the checks above prove nothing")
	}
	if end != st.Size() {
		t.Errorf("the markers cover %d bytes but the file is %d; the redactor's "+
			"output and the marker's accounting disagree", end, st.Size())
	}
}

// TestASecretSplitAcrossTwoProcessWritesIsRedacted is redact's own
// split-write test one layer up: through the real pipe, logMarker and file
// runAttempt wires together. A real gap between two printf calls forces
// the writer chain to see two separate Writes rather than whatever
// io.Copy's buffering batched.
func TestASecretSplitAcrossTwoProcessWritesIsRedacted(t *testing.T) {
	const value = "s3cr3t-value-here"
	half := len(value) / 2

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	// printf, sleep, printf: two separate write syscalls to stdout with a
	// real gap between them, so the reader on the engine side cannot
	// coalesce them into one Write call the way two back-to-back writes
	// with no gap sometimes can.
	script := "printf '%s' 'prefix " + value[:half] + "'; sleep 0.05; printf '%s\\n' '" + value[half:] + " suffix'"

	pipe := senro.New("p")
	pipe.Workflow("w").Step("leak", exec.Command("sh", "-c", script))
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	body, err := os.ReadFile(eventlog.NewLogSet(dir).Path("leak", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	if !bytes.Contains(body, []byte("prefix "+redact.Placeholder+" suffix")) {
		t.Fatalf("the split value was not redacted as one match: %q", body)
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the log file contains the secret value")
	}
}

// TestAPartialMatchAtEndOfStreamReachesTheFileViaFlush is the case
// runAttempt's explicit, un-deferred Flush call exists for: a step's last
// write ends mid-secret, with no trailing newline and nothing after it that
// would force the automaton to give up on the match on its own. Without that
// explicit Flush call before step.finished, those held-back bytes never
// reach the file at all: not leaked, silently dropped, which is worse than
// a value leaking: the step ran and printed something, and the log would
// say it did not.
func TestAPartialMatchAtEndOfStreamReachesTheFileViaFlush(t *testing.T) {
	const value = "s3cr3t-value-here"
	prefix := value[:10] // never completes: the process exits before writing the rest

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	want := "leftover " + prefix
	pipe := senro.New("p")
	pipe.Workflow("w").Step("cutshort", exec.Command("printf", "%s", want))
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logPath := eventlog.NewLogSet(dir).Path("cutshort", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	if string(body) != want {
		t.Fatalf("the held-back partial match was dropped instead of flushed: got %q, want %q", body, want)
	}

	st, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	var end int64
	for _, e := range rec.Events() {
		if e.Type != api.StepLogAppended || e.Step != "cutshort" {
			continue
		}
		var b api.StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if b.Stream != api.StreamStdout {
			continue
		}
		end = b.Offset + b.Len
	}
	if end != st.Size() {
		t.Errorf("markers cover %d bytes but the flushed file is %d bytes", end, st.Size())
	}
}

// TestAStepWithNoOutputEmitsNoSecretRedactedAndDoesNotBreak covers a step
// that writes nothing to either stream at all: no Write call ever reaches
// the redactor, so this pins that Flush and the zero-count Redacted() check
// behave correctly with no state ever touched, not merely with a small
// amount of state touched.
func TestAStepWithNoOutputEmitsNoSecretRedactedAndDoesNotBreak(t *testing.T) {
	const value = "s3cr3t-value-here"

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("silent", exec.Command("true"))
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	status, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}

	var stepFinished bool
	for _, e := range rec.Events() {
		if e.Type == api.StepFinished {
			stepFinished = true
		}
		if e.Type == api.SecretRedacted {
			t.Errorf("secret.redacted emitted for a step with no output: %+v", e)
		}
	}
	if !stepFinished {
		t.Fatal("no step.finished; the assertion above proves nothing")
	}

	body, err := os.ReadFile(eventlog.NewLogSet(dir).Path("silent", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("expected an empty log for a step with no output, got %q", body)
	}
}

// TestAStepWhoseEntireOutputIsASecretIsFullyRedacted is the case with no
// safe prefix or suffix: the file must end up with the placeholder and
// nothing else, not empty and not the raw value. The step cats the value
// from SecretEnv's delivered file rather than argv, since
// checkSecretChannels now refuses a literal value in Cmd and this test
// needs a channel refusal does not cover.
func TestAStepWhoseEntireOutputIsASecretIsFullyRedacted(t *testing.T) {
	const value = "s3cr3t-value-here"

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("onlysecret", exec.Command("sh", "-c", `cat "$TOK"`)).
		SecretEnv("TOK", "Tok")
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	body, err := os.ReadFile(eventlog.NewLogSet(dir).Path("onlysecret", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	if string(body) != redact.Placeholder {
		t.Fatalf("got %q, want exactly the placeholder and nothing else", body)
	}

	var count int
	for _, e := range rec.Events() {
		if e.Type != api.SecretRedacted || e.Step != "onlysecret" {
			continue
		}
		var b api.SecretRedactedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		count += b.Count
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

// TestRunDeliversASecretFileAndRemovesItOnSuccess drives attempt.go's
// delivery path directly: a declared secret must reach the step as a file
// under BOTH names (plan.SecretEnvVar's uniform one, and the SecretSpec.Env
// alias when one was given), and the SecretSpec with no Env must NOT add a
// stray, emptily-named entry to Cmd.Env. The step reports the file's length
// rather than its content, so this test does not depend on the redactor at
// all: it is purely about what attempt.go put in the environment and
// whether it cleaned up after itself.
func TestRunDeliversASecretFileAndRemovesItOnSuccess(t *testing.T) {
	const value = "delivered-value-aaaaaaaa"
	set := resolvedSet(t, value)

	out := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "use", Kind: "exec",
		Cmd: []string{"sh", "-c",
			`test "$NPM_TOKEN" = "$SENRO_SECRET_NPMTOKEN" || { echo names disagree; exit 1; }
			 wc -c < "$NPM_TOKEN" | tr -d ' ' > "$OUT/len"
			 printf '%s' "$NPM_TOKEN" > "$OUT/path"
			 env | grep -c '^=' > "$OUT/blank_names" || true`},
		Env:     []string{"OUT=" + out},
		Secrets: []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
	}}}
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01DELIVER", Secrets: set,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want %s (%s)", status, api.RunSucceeded, readLogTail(t, dir, "use"))
	}

	lenBytes, err := os.ReadFile(filepath.Join(out, "len"))
	if err != nil {
		t.Fatalf("reading captured length: %v", err)
	}
	if strings.TrimSpace(string(lenBytes)) != fmt.Sprintf("%d", len(value)) {
		t.Errorf("delivered file length = %s, want %d (the value's own length)", lenBytes, len(value))
	}

	blank, err := os.ReadFile(filepath.Join(out, "blank_names"))
	if err != nil {
		t.Fatalf("reading blank-name count: %v", err)
	}
	if strings.TrimSpace(string(blank)) != "0" {
		t.Errorf("Cmd.Env contains an entry with an empty name: %s", blank)
	}

	pathBytes, err := os.ReadFile(filepath.Join(out, "path"))
	if err != nil {
		t.Fatalf("reading captured path: %v", err)
	}
	secretPath := string(pathBytes)
	if secretPath == "" {
		t.Fatal("the step never captured its delivered path; the check below proves nothing")
	}
	if _, statErr := os.Stat(secretPath); !os.IsNotExist(statErr) {
		t.Errorf("the secret file %q survived a successful run: %v", secretPath, statErr)
	}
	if _, statErr := os.Stat(filepath.Dir(secretPath)); !os.IsNotExist(statErr) {
		t.Errorf("the secret directory %q survived a successful run", filepath.Dir(secretPath))
	}
}

// TestACancelledStepStillCleansUpItsSecretFile is the negative case for
// mid-step cancellation. runAttempt's sandbox teardown is a deferred,
// unconditional call: it runs whether the step finished, timed out, or was
// killed because the run's own context was cancelled, and a credential left
// behind specifically on the cancellation path would be the one way Close's
// "removed on every path" claim could be false in practice.
func TestACancelledStepStillCleansUpItsSecretFile(t *testing.T) {
	const value = "cancel-me-aaaaaaaaaaaa"
	set := resolvedSet(t, value)

	out := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "s", Kind: "exec",
		Cmd:     []string{"sh", "-c", `printf '%s' "$SENRO_SECRET_NPMTOKEN" > "$OUT/path"; sleep 5`},
		Env:     []string{"OUT=" + out},
		Secrets: []plan.SecretSpec{{Name: "NPMToken"}},
	}}}
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	status, err := engine.Run(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01CANCELSECRET", Secrets: set,
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	if status != api.RunCancelled {
		t.Fatalf("status = %s, want %s", status, api.RunCancelled)
	}

	pathBytes, err := os.ReadFile(filepath.Join(out, "path"))
	if err != nil {
		t.Fatalf("reading the captured secret path: %v", err)
	}
	secretPath := string(pathBytes)
	if secretPath == "" {
		t.Fatal("the step never wrote its secret path; the check below proves nothing")
	}
	if _, statErr := os.Stat(secretPath); !os.IsNotExist(statErr) {
		t.Errorf("the secret file %q survived a cancelled run: %v", secretPath, statErr)
	}
	if _, statErr := os.Stat(filepath.Dir(secretPath)); !os.IsNotExist(statErr) {
		t.Errorf("the secret directory %q survived a cancelled run", filepath.Dir(secretPath))
	}
}

// TestTwoStepsDeclaringTheSameSecretEachGetTheirOwnFile guards against a
// delivery path that derives a secret's file location from the secret's name
// alone: two unrelated steps declaring the same field would then race on one
// path, and whichever step's Close ran first would delete the file out from
// under the other, still-running one.
func TestTwoStepsDeclaringTheSameSecretEachGetTheirOwnFile(t *testing.T) {
	const value = "shared-name-value-aaaaa"
	set := resolvedSet(t, value)

	out := t.TempDir()
	mk := func(id string) plan.Node {
		return plan.Node{
			ID: id, Kind: "exec",
			Cmd: []string{"sh", "-c",
				`printf '%s' "$SENRO_SECRET_NPMTOKEN" > "$OUT/` + id + `"`},
			Env:     []string{"OUT=" + out},
			Secrets: []plan.SecretSpec{{Name: "NPMToken"}},
		}
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{mk("a"), mk("b")}}
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 2, RunID: "01TWOSTEPS", Secrets: set,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want %s", status, api.RunSucceeded)
	}

	pathA, err := os.ReadFile(filepath.Join(out, "a"))
	if err != nil {
		t.Fatalf("reading step a's captured path: %v", err)
	}
	pathB, err := os.ReadFile(filepath.Join(out, "b"))
	if err != nil {
		t.Fatalf("reading step b's captured path: %v", err)
	}
	if len(pathA) == 0 || len(pathB) == 0 {
		t.Fatal("one of the steps never captured its delivered path; the check below proves nothing")
	}
	if string(pathA) == string(pathB) {
		t.Errorf("two steps declaring the same secret name were delivered the SAME path: %s", pathA)
	}
	for _, p := range []string{string(pathA), string(pathB)} {
		if _, statErr := os.Stat(p); !os.IsNotExist(statErr) {
			t.Errorf("secret file %q survived the run: %v", p, statErr)
		}
	}
}

// readLogTail is a debugging aid for a status assertion failure above: it
// reads back whatever the step actually printed, so a failure names the
// reason rather than just the wrong status.
func readLogTail(t *testing.T, dir, step string) string {
	t.Helper()
	b, err := os.ReadFile(eventlog.NewLogSet(dir).Path(step, 1, api.StreamStdout))
	if err != nil {
		return fmt.Sprintf("<no stdout log: %v>", err)
	}
	return string(b)
}
