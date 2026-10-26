package engine_test

// This file pins how the engine turns a step's declared test report into
// test.finished events: one per test, scoped to the attempt that ran it,
// always ahead of that attempt's step.finished, and never carrying a secret
// the step's log no longer holds.

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
)

// testsFinished returns every test.finished event in events, decoded, next
// to the event that carried it so a caller can check Step, Attempt and Seq.
func testsFinished(t *testing.T, events []api.Event) ([]api.Event, []api.TestFinishedBody) {
	t.Helper()
	var evs []api.Event
	var bodies []api.TestFinishedBody
	for _, e := range events {
		if e.Type != api.TestFinished {
			continue
		}
		var b api.TestFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode test.finished: %v", err)
		}
		evs = append(evs, e)
		bodies = append(bodies, b)
	}
	return evs, bodies
}

// lastStepFinished returns step's last step.finished event and its body.
func lastStepFinished(t *testing.T, events []api.Event, step string) (api.Event, api.StepFinishedBody) {
	t.Helper()
	var found *api.Event
	for i := range events {
		if events[i].Type == api.StepFinished && events[i].Step == step {
			found = &events[i]
		}
	}
	if found == nil {
		t.Fatalf("no step.finished for %q", step)
	}
	var b api.StepFinishedBody
	if err := found.Decode(&b); err != nil {
		t.Fatalf("decode step.finished: %v", err)
	}
	return *found, b
}

// assertTestsPrecedeFinish checks the ordering promise api.TestFinished
// makes: no test.finished for an attempt lands after its step.finished.
func assertTestsPrecedeFinish(t *testing.T, tests []api.Event, finished api.Event) {
	t.Helper()
	for _, e := range tests {
		if e.Seq >= finished.Seq {
			t.Errorf("test.finished seq %d is not before step.finished seq %d", e.Seq, finished.Seq)
		}
	}
}

func TestAGoTestStepEmitsOneEventPerTest(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec",
		Cmd: []string{"sh", "-c", `printf '%s\n' ` +
			`'{"Action":"run","Test":"TestA"}' ` +
			`'{"Action":"pass","Test":"TestA","Elapsed":0.01}' ` +
			`'{"Action":"run","Test":"TestB"}' ` +
			`'{"Action":"output","Test":"TestB","Output":"x_test.go:3: no\n"}' ` +
			`'{"Action":"fail","Test":"TestB","Elapsed":0.02}'; exit 1`},
		Tests: &plan.TestsSpec{Format: "gotest"},
	}}}
	_, events, _ := runPlan(t, dir, p)

	evs, bodies := testsFinished(t, events)
	if len(evs) != 2 {
		t.Fatalf("got %d test.finished events, want 2: %+v", len(evs), bodies)
	}
	for _, e := range evs {
		if e.Step != "t" || e.Attempt != 1 {
			t.Errorf("test.finished scoped to step %q attempt %d, want \"t\" attempt 1", e.Step, e.Attempt)
		}
	}
	var sawB bool
	for _, b := range bodies {
		if b.Name != "TestB" {
			continue
		}
		sawB = true
		if b.Outcome != api.TestFailed {
			t.Errorf("TestB outcome = %s, want failed", b.Outcome)
		}
		if !strings.Contains(b.Output, "no") {
			t.Errorf("TestB output = %q, want it to contain the failure line", b.Output)
		}
	}
	if !sawB {
		t.Errorf("no test.finished for TestB: %+v", bodies)
	}
	finished, _ := lastStepFinished(t, events, "t")
	assertTestsPrecedeFinish(t, evs, finished)
}

func TestATAPStepEmitsEvents(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec",
		Cmd:   []string{"sh", "-c", `printf 'ok 1 a\nnot ok 2 b\n'; exit 1`},
		Tests: &plan.TestsSpec{Format: "tap"},
	}}}
	_, events, _ := runPlan(t, dir, p)

	evs, bodies := testsFinished(t, events)
	if len(evs) != 2 {
		t.Fatalf("got %d test.finished events, want 2: %+v", len(evs), bodies)
	}
	outcomes := map[string]api.TestOutcome{}
	for _, b := range bodies {
		outcomes[b.Name] = b.Outcome
	}
	if outcomes["a"] != api.TestPassed || outcomes["b"] != api.TestFailed {
		t.Errorf("outcomes = %v, want a passed and b failed", outcomes)
	}
	finished, _ := lastStepFinished(t, events, "t")
	assertTestsPrecedeFinish(t, evs, finished)
}

// TestAJUnitStepReadsTheReportAfterTheCommand gives the step an absolute
// WorkDir: the engine resolves the report against cmdDirFor(n.WorkDir,
// mounts), which is the directory the local executor starts the command in
// only when WorkDir is an absolute host path.
func TestAJUnitStepReadsTheReportAfterTheCommand(t *testing.T) {
	dir := t.TempDir()
	report, err := filepath.Abs(filepath.Join("..", "testparse", "testdata", "junit.xml"))
	if err != nil {
		t.Fatal(err)
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec", WorkDir: dir,
		Cmd:   []string{"sh", "-c", `cp "$1" "$PWD/report.xml"`, "sh", report},
		Tests: &plan.TestsSpec{Format: "junit", Path: "report.xml"},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["t"] != api.StateSucceeded {
		t.Errorf("t = %s, want succeeded", states["t"])
	}
	evs, bodies := testsFinished(t, events)
	if len(evs) != 4 {
		t.Fatalf("got %d test.finished events, want 4: %+v", len(evs), bodies)
	}
	for _, e := range evs {
		if e.Step != "t" || e.Attempt != 1 {
			t.Errorf("test.finished scoped to step %q attempt %d, want \"t\" attempt 1", e.Step, e.Attempt)
		}
	}
	finished, _ := lastStepFinished(t, events, "t")
	assertTestsPrecedeFinish(t, evs, finished)
}

func TestAMissingJUnitReportFailsAPassingStep(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec", WorkDir: dir, Cmd: []string{"true"},
		Tests: &plan.TestsSpec{Format: "junit", Path: "report.xml"},
	}}}
	_, events, _ := runPlan(t, dir, p)

	_, b := lastStepFinished(t, events, "t")
	if b.State != api.StateFailed {
		t.Errorf("state = %s, want failed", b.State)
	}
	if !strings.Contains(b.Error, "report.xml") {
		t.Errorf("error = %q, want it to name report.xml", b.Error)
	}
}

func TestAMissingJUnitReportOnAFailedStepIsNotTheStory(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec", WorkDir: dir, Cmd: []string{"sh", "-c", "exit 3"},
		Tests: &plan.TestsSpec{Format: "junit", Path: "report.xml"},
	}}}
	_, events, _ := runPlan(t, dir, p)

	_, b := lastStepFinished(t, events, "t")
	if b.State != api.StateFailed || b.ExitCode != 3 || b.Error != "" {
		t.Errorf("state %s exit %d error %q, want failed, 3 and no error", b.State, b.ExitCode, b.Error)
	}
}

// TestTestOutputIsRedacted prints a resolved secret's value inside a
// failing test's output. The value is assembled by printf from two halves so
// the command line never holds it whole (checkSecretChannels refuses a plan
// whose argv carries a secret).
func TestTestOutputIsRedacted(t *testing.T) {
	const value = "token-aaaaaaaaaa"
	type config struct {
		Token secret.String `source:"fake://ci/tok#v"`
	}
	set, err := secrets.FromConfig(config{Token: secret.NewString(value)})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec",
		Cmd: []string{"sh", "-c",
			`printf '%s\n' '{"Action":"run","Test":"TestS"}'; ` +
				`printf '%s%s\n' '{"Action":"output","Test":"TestS","Output":"leaked token-' 'aaaaaaaaaa\n"}'; ` +
				`printf '%s\n' '{"Action":"fail","Test":"TestS"}'; exit 1`},
		Tests: &plan.TestsSpec{Format: "gotest"},
	}}}
	_, events, _ := runPlanWith(t, dir, p, func(o *engine.Options) { o.Secrets = set })

	evs, bodies := testsFinished(t, events)
	if len(bodies) != 1 {
		t.Fatalf("got %d test.finished events, want 1: %+v", len(bodies), bodies)
	}
	if !strings.Contains(bodies[0].Output, "[REDACTED]") {
		t.Errorf("output = %q, want the redaction marker", bodies[0].Output)
	}
	if strings.Contains(string(evs[0].Payload), value) {
		t.Errorf("test.finished payload carries the secret value: %s", evs[0].Payload)
	}
}

// TestAJUnitReportIsRedacted is TestTestOutputIsRedacted for a report file.
// The file never passed through the stdout redactor, so what keeps the value
// out of the ledger is the payload redaction every event gets on append.
func TestAJUnitReportIsRedacted(t *testing.T) {
	const value = "token-aaaaaaaaaa"
	type config struct {
		Token secret.String `source:"fake://ci/tok#v"`
	}
	set, err := secrets.FromConfig(config{Token: secret.NewString(value)})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "t", Kind: "exec", WorkDir: dir,
		Cmd: []string{"sh", "-c",
			`printf '%s%s%s' '<testsuite name="s"><testcase name="c"><failure>leaked token-' ` +
				`'aaaaaaaaaa' '</failure></testcase></testsuite>' > report.xml`},
		Tests: &plan.TestsSpec{Format: "junit", Path: "report.xml"},
	}}}
	_, events, _ := runPlanWith(t, dir, p, func(o *engine.Options) { o.Secrets = set })

	evs, bodies := testsFinished(t, events)
	if len(bodies) != 1 {
		t.Fatalf("got %d test.finished events, want 1: %+v", len(bodies), bodies)
	}
	if !strings.Contains(bodies[0].Output, "[REDACTED]") {
		t.Errorf("output = %q, want the redaction marker", bodies[0].Output)
	}
	if strings.Contains(string(evs[0].Payload), value) {
		t.Errorf("test.finished payload carries the secret value: %s", evs[0].Payload)
	}
}
