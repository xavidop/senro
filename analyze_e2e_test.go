package senro_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/mamori"

	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/examples/extensions/fakeanalyzer"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/source"
)

// This file drives examples/extensions/fakeanalyzer, which imports only
// github.com/xavidop/senro/api (asserted mechanically by
// TestAnExtensionImportsOnlySenrosPublicSurface), through senro.Run exactly
// as a third party would. The interface assertions below catch a
// senro.Analyzer that stopped being satisfiable from api alone, which would
// still compile everywhere else in the tree.
var _ senro.Analyzer = (*fakeanalyzer.Analyzer)(nil)
var _ senro.Analyzer = senro.AnalyzerFunc(nil)

// collect is a sink that keeps every event, so a test can assert on the
// stream a run really produced rather than on what it expects it to have
// produced.
type collect struct {
	mu sync.Mutex
	ev []api.Event
}

func (c *collect) Emit(e api.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ev = append(c.ev, e)
}

func (c *collect) all() []api.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]api.Event(nil), c.ev...)
}

func (c *collect) ofType(t api.Type) []api.Event {
	var out []api.Event
	for _, e := range c.all() {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

func decodeProposed(t *testing.T, e api.Event) api.AnalysisProposedBody {
	t.Helper()
	var b api.AnalysisProposedBody
	if err := e.Decode(&b); err != nil {
		t.Fatalf("decode analysis.proposed: %v", err)
	}
	return b
}

func decodeDecision(t *testing.T, e api.Event) api.AnalysisDecisionBody {
	t.Helper()
	var b api.AnalysisDecisionBody
	if err := e.Decode(&b); err != nil {
		t.Fatalf("decode analysis decision: %v", err)
	}
	return b
}

// TestAThirdPartyAnalyzerExplainsAFailureFromTheEventStreamAlone is the
// headline claim: an analyzer somebody else wrote, importing only api, gets
// everything it needs about a failed step and its answer lands in the ledger.
//
// The pipeline fails one step on purpose, in a way whose output the analyzer
// recognises, and lets a second step succeed so the run is not over the
// instant the failure happens.
func TestAThirdPartyAnalyzerExplainsAFailureFromTheEventStreamAlone(t *testing.T) {
	dir := t.TempDir()
	an := fakeanalyzer.New()
	var got collect

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("build", exec.Command("sh", "-c", "echo built"))
	l.Step("fetch", exec.Command("sh", "-c",
		"echo 'dial tcp 10.0.0.1:443: i/o timeout' >&2; exit 7")).Needs("build")

	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(an, senro.AnalyzerName("fake")),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	if err == nil {
		t.Fatal("Run succeeded; the pipeline was supposed to fail")
	}

	seen := an.Seen()
	if len(seen) != 1 {
		t.Fatalf("the analyzer was offered %d failures, want exactly the one that failed", len(seen))
	}
	f := seen[0]

	// Every field, checked. This struct is the whole contract between senro
	// and an analyzer, and a field that silently stopped being populated
	// would leave every analyzer in the world slightly worse with nothing
	// failing anywhere.
	if f.Step != "fetch" {
		t.Errorf("Step = %q, want fetch", f.Step)
	}
	if f.Pipeline != "release" {
		t.Errorf("Pipeline = %q, want release", f.Pipeline)
	}
	if f.RunID == "" {
		t.Error("RunID is empty")
	}
	if f.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", f.Attempt)
	}
	if f.State != api.StateFailed {
		t.Errorf("State = %q, want failed", f.State)
	}
	if f.ExitCode != 7 {
		t.Errorf("ExitCode = %d, want 7", f.ExitCode)
	}
	if f.Duration <= 0 {
		t.Errorf("Duration = %v, want a positive duration", f.Duration)
	}
	if len(f.Cmd) == 0 {
		t.Error("Cmd is empty; an analyzer with no idea what ran is reading an error message in the dark")
	}
	if len(f.Needs) != 1 || f.Needs[0] != "build" {
		t.Errorf("Needs = %v, want [build]", f.Needs)
	}
	if !strings.Contains(f.LogTail, "i/o timeout") {
		t.Errorf("LogTail = %q, want the step's own stderr in it", f.LogTail)
	}

	// And the proposal reached the ledger, on the envelope that routes it.
	proposed := got.ofType(api.AnalysisProposed)
	if len(proposed) != 1 {
		t.Fatalf("%d analysis.proposed events, want 1", len(proposed))
	}
	if proposed[0].Step != "fetch" || proposed[0].Attempt != 1 {
		t.Errorf("analysis.proposed routed to %q attempt %d, want fetch attempt 1",
			proposed[0].Step, proposed[0].Attempt)
	}
	b := decodeProposed(t, proposed[0])
	if b.ID != "fetch@1" {
		t.Errorf("proposal id = %q, want fetch@1", b.ID)
	}
	if b.Analyzer != "fake" {
		t.Errorf("Analyzer = %q, want fake", b.Analyzer)
	}
	if b.Remedy != api.RemedyRetry {
		t.Errorf("Remedy = %q, want retry", b.Remedy)
	}
	if !strings.Contains(b.Summary, "network") {
		t.Errorf("Summary = %q, want the analyzer's own words", b.Summary)
	}

	// A proposal that nobody decided about is exactly one event. Nothing was
	// applied, so nothing says it was.
	if n := len(got.ofType(api.AnalysisApplied)); n != 0 {
		t.Errorf("%d analysis.applied events in a run nobody was attached to; the default is that nothing is applied without a human", n)
	}
	if n := len(got.ofType(api.AnalysisRejected)); n != 0 {
		t.Errorf("%d analysis.rejected events; nobody rejected anything", n)
	}

	// The ledger on disk carries it too. A sink saw it, which is necessary
	// and not sufficient: an event is only real once it is in the ledger.
	if !ledgerHas(t, filepath.Join(dir, "run", "events.jsonl"), api.AnalysisProposed) {
		t.Error("analysis.proposed reached the sink but not events.jsonl")
	}
}

// ledgerHas reports whether the run's own persisted event log carries at
// least one event of this type.
func ledgerHas(t *testing.T, path string, typ api.Type) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		var e api.Event
		if json.Unmarshal(sc.Bytes(), &e) == nil && e.Type == typ {
			return true
		}
	}
	return false
}

// TestAnAnalyzerNeverSeesASecret is the test the redaction argument rests
// on: a step is given a secret, prints it to both streams, and fails. If the
// tail buffer an analyzer is fed were anywhere but downstream of the run's
// redactor, the secret would be in that struct on its way to somebody's API.
// It also checks the proposal built OUT of what the analyzer saw, since an
// analyzer that echoes its input into a Summary puts that text in the
// ledger.
func TestAnAnalyzerNeverSeesASecret(t *testing.T) {
	const value = "hunter2-not-a-real-token-9f3c"

	type Config struct {
		RegistryToken secret.String `source:"fake://ci/ghcr#token"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/ghcr#token", value)
	cfg, err := mamori.Load[Config](t.Context(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	an := fakeanalyzer.New()
	var got collect

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("leak", exec.Command("sh", "-c",
		`echo "token is $(cat "$TOKEN")"; echo "and again $(cat "$TOKEN")" >&2; exit 3`)).
		SecretEnv("TOKEN", "RegistryToken")

	err = senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(an, senro.AnalyzerName("fake")),
		senro.WithSink(&got),
		senro.WithSecrets(cfg),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	if err == nil {
		t.Fatal("Run succeeded; the pipeline was supposed to fail")
	}

	seen := an.Seen()
	if len(seen) != 1 {
		t.Fatalf("the analyzer saw %d failures, want 1: a test that analyzed nothing proves nothing", len(seen))
	}
	f := seen[0]
	if !strings.Contains(f.LogTail, "token is") {
		t.Fatalf("LogTail = %q, and it does not contain the step's output at all: "+
			"this test can only prove something if the analyzer really saw the log", f.LogTail)
	}

	// Every string field, not only the log. The whole struct crosses to a
	// third party.
	for name, field := range map[string]string{
		"LogTail":  f.LogTail,
		"Error":    f.Error,
		"Step":     f.Step,
		"Pipeline": f.Pipeline,
		"RunID":    f.RunID,
		"Cmd":      strings.Join(f.Cmd, " "),
		"Needs":    strings.Join(f.Needs, " "),
	} {
		if strings.Contains(field, value) {
			t.Errorf("api.Failure.%s carries the secret value: %q", name, field)
		}
	}
	if !strings.Contains(f.LogTail, "[REDACTED]") {
		t.Errorf("LogTail = %q, want the redactor's placeholder where the secret was", f.LogTail)
	}

	// And nothing the analyzer said about it reached the ledger carrying one
	// either.
	for _, e := range got.all() {
		if strings.Contains(string(e.Payload), value) {
			t.Errorf("event %s carries the secret in its payload", e.Type)
		}
	}
	body, err := os.ReadFile(filepath.Join(dir, "run", "events.jsonl"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if strings.Contains(string(body), value) {
		t.Error("events.jsonl carries the secret")
	}
}

// TestAnAnalyzerCannotApplyItsOwnProposal is the gate, stated as a property.
//
// The analyzer proposes a retry, the run has nobody attached and no policy,
// and the step must be exactly as failed at the end as it was when it failed.
// A design where a proposal could act on its own would show up here as a step
// with two attempts.
func TestAnAnalyzerCannotApplyItsOwnProposal(t *testing.T) {
	dir := t.TempDir()
	var got collect

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c",
		"echo 'connection refused' >&2; exit 1"))

	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(fakeanalyzer.New()),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	if err == nil {
		t.Fatal("Run succeeded; the pipeline was supposed to fail")
	}

	proposed := got.ofType(api.AnalysisProposed)
	if len(proposed) != 1 {
		t.Fatalf("%d analysis.proposed events, want 1", len(proposed))
	}
	if b := decodeProposed(t, proposed[0]); b.Remedy != api.RemedyRetry {
		t.Fatalf("Remedy = %q, want retry: this test is only meaningful if something COULD have been applied", b.Remedy)
	}

	if n := len(got.ofType(api.AnalysisApplied)); n != 0 {
		t.Errorf("%d analysis.applied events with nobody attached and no policy configured", n)
	}
	// The decisive one: no second attempt happened.
	for _, e := range got.ofType(api.StepRetried) {
		t.Errorf("step %q was retried on an analyzer's say-so alone", e.Step)
	}
	for _, e := range got.ofType(api.StepStarted) {
		if e.Attempt > 1 {
			t.Errorf("step %q reached attempt %d without anybody approving anything", e.Step, e.Attempt)
		}
	}
}

// TestAPolicyAppliesAProposalMadeWhileTheStepWasStillFinishing is the
// regression test for a lost remedy. A step keeps its scheduler claim until
// its own goroutine returns - which is AFTER its settle-time Always handler
// - while the proposal that names it is offered much earlier, immediately
// after the step.finished inside runStep. A policy answering in that window
// used to have its accepted retry refused with step_running and thrown away:
// maybeApplyPolicy spends the one application per step BEFORE the request is
// sent, so nothing ever asked again and the run failed carrying the very
// failure the analyzer had a remedy for.
//
// The slow Always handler is what makes that window wide on purpose, so this
// fails every time against the old behaviour rather than once per so many CI
// runs, which is how it actually showed up.
func TestAPolicyAppliesAProposalMadeWhileTheStepWasStillFinishing(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "attempted")
	var got collect

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c",
		"if [ -f "+marker+" ]; then echo recovered; exit 0; fi; "+
			"touch "+marker+"; echo 'connection refused' >&2; exit 1")).
		Always(senro.Handler("linger", exec.Command("sh", "-c", "sleep 0.5")))

	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(fakeanalyzer.New(),
			senro.AnalyzerName("fake"),
			senro.AcceptWithoutHumanApproval(func(_ api.Failure, p api.Proposal) bool {
				return p.Remedy == api.RemedyRetry
			})),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	if err != nil {
		t.Fatalf("Run: %v; the accepted retry was supposed to recover the step, "+
			"and a step still running its Always handler must not lose it", err)
	}

	if n := len(got.ofType(api.AnalysisApplied)); n != 1 {
		t.Errorf("%d analysis.applied events, want 1: the remedy was accepted, so it must be recorded as applied", n)
	}
	if n := len(got.ofType(api.StepRetried)); n != 1 {
		t.Errorf("%d step.retried events, want 1", n)
	}
}

// TestAPolicyIsTheOnlyWayAProposalAppliesItself checks the one escape hatch
// works, and that a run which used it says so in its own ledger.
//
// The step fails once and passes on a second attempt, so an applied retry is
// visible as a run that recovered rather than as a run that failed twice.
func TestAPolicyIsTheOnlyWayAProposalAppliesItself(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "attempted")
	var got collect

	var asked []api.Proposal
	var mu sync.Mutex

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c",
		"if [ -f "+marker+" ]; then echo recovered; exit 0; fi; "+
			"touch "+marker+"; echo 'connection refused' >&2; exit 1"))

	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(fakeanalyzer.New(),
			senro.AnalyzerName("fake"),
			senro.AcceptWithoutHumanApproval(func(_ api.Failure, p api.Proposal) bool {
				mu.Lock()
				asked = append(asked, p)
				mu.Unlock()
				return p.Remedy == api.RemedyRetry
			})),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	if err != nil {
		t.Fatalf("Run: %v; the accepted retry was supposed to recover the step", err)
	}

	mu.Lock()
	n := len(asked)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("the policy was asked %d times, want 1", n)
	}

	applied := got.ofType(api.AnalysisApplied)
	if len(applied) != 1 {
		t.Fatalf("%d analysis.applied events, want 1", len(applied))
	}
	b := decodeDecision(t, applied[0])
	if !b.Policy {
		t.Error("analysis.applied does not record that no human decided it; " +
			"a run nobody watched has to be identifiable from the ledger alone")
	}
	if b.ClientID != "" {
		t.Errorf("ClientID = %q on a decision no client made", b.ClientID)
	}
	if b.Remedy != api.RemedyRetry {
		t.Errorf("Remedy = %q, want retry", b.Remedy)
	}
	if b.ID != "fetch@1" {
		t.Errorf("ID = %q, want fetch@1", b.ID)
	}

	// The remedy actually happened, and it happened through the ordinary
	// retry path rather than through some private one.
	if len(got.ofType(api.StepRetried)) != 1 {
		t.Errorf("%d step.retried events; applying a retry remedy has to be a real retry", len(got.ofType(api.StepRetried)))
	}
	// And it is recorded as the control operation it was.
	var sawAccept bool
	for _, e := range got.ofType(api.ControlApplied) {
		var cb api.ControlAppliedBody
		if e.Decode(&cb) == nil && cb.Op == api.OpAnalysisAccept {
			sawAccept = true
			if cb.Args["step"] != "fetch" {
				t.Errorf("control.applied args = %v, want the step the proposal was about", cb.Args)
			}
		}
	}
	if !sawAccept {
		t.Error("no control.applied naming analysis.accept; the accept path has to be the recorded control path")
	}
}

// TestAPolicyIsNeverAskedAboutAProposalSenroCannotApply is the other half of
// the policy contract. An advisory proposal has nothing to decide, so asking
// would be asking a question with no answer, and a caller returning true to
// it would reasonably expect something to happen.
func TestAPolicyIsNeverAskedAboutAProposalSenroCannotApply(t *testing.T) {
	dir := t.TempDir()
	var got collect
	var asked int
	var mu sync.Mutex

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	// "no space left on device" is the case fakeanalyzer deliberately gives
	// no remedy: retrying a full disk fails identically.
	l.Step("build", exec.Command("sh", "-c",
		"echo 'write /tmp/x: no space left on device' >&2; exit 1"))

	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(fakeanalyzer.New(),
			senro.AcceptWithoutHumanApproval(func(api.Failure, api.Proposal) bool {
				mu.Lock()
				asked++
				mu.Unlock()
				return true // says yes to everything, and must never be asked
			})),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	if err == nil {
		t.Fatal("Run succeeded; the pipeline was supposed to fail")
	}

	proposed := got.ofType(api.AnalysisProposed)
	if len(proposed) != 1 {
		t.Fatalf("%d analysis.proposed events, want 1", len(proposed))
	}
	if b := decodeProposed(t, proposed[0]); b.Remedy != api.RemedyNone {
		t.Fatalf("Remedy = %q, want none: this test needs an advisory proposal to be meaningful", b.Remedy)
	}

	mu.Lock()
	n := asked
	mu.Unlock()
	if n != 0 {
		t.Errorf("the policy was asked %d times about a proposal with nothing to apply", n)
	}
	if n := len(got.ofType(api.AnalysisApplied)); n != 0 {
		t.Errorf("%d analysis.applied events for a proposal there was nothing to apply", n)
	}
	for _, e := range got.ofType(api.StepStarted) {
		if e.Attempt > 1 {
			t.Errorf("step %q reached attempt %d; an advisory proposal caused work", e.Step, e.Attempt)
		}
	}
}

// TestAnAttachedClientDecidesAProposal drives the gate the way an operator
// does: over the attach socket, with the two control operations, against a
// live run.
//
// The run is held open by a breakpoint on a later step, which is what gives a
// client time to see the proposal and answer it. That is not a contrivance
// for the test: it is exactly the shape in which the gate is useful, a run
// that is still going while somebody looks at a failure in it.
func TestAnAttachedClientDecidesAProposal(t *testing.T) {
	isolateAttachRegistry(t)

	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	var got collect

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	att, err := attach.Listen(ctx, attach.Options{
		Bind: attach.AutoUnixSocket, Dir: runDir, RunID: "analyze-e2e",
	})
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	t.Cleanup(func() { _ = att.Close() })

	// The gate is a file, not a sleep: arming a breakpoint takes a control
	// round trip, and a pipeline whose steps all finish immediately would be
	// over before that lands. Same trick startShellRun uses.
	gate := filepath.Join(dir, "go")
	marker := filepath.Join(dir, "attempted")

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("gate", exec.Command("sh", "-c",
		"while [ ! -f "+gate+" ]; do sleep 0.05; done"))
	l.Step("fetch", exec.Command("sh", "-c",
		"if [ -f "+marker+" ]; then echo recovered; exit 0; fi; "+
			"touch "+marker+"; echo 'connection refused' >&2; exit 1")).Needs("gate")
	l.Step("hold", exec.Command("sh", "-c", "echo held")).Needs("gate")

	done := make(chan error, 1)
	go func() {
		done <- senro.Run(ctx, pipe,
			senro.WithAnalyzer(fakeanalyzer.New(), senro.AnalyzerName("fake")),
			senro.WithSink(&got),
			senro.WithAttach(att),
			senro.WithCacheDir(filepath.Join(dir, "cache")))
	}()

	src, err := source.Dial(ctx, att.Addr())
	if err != nil {
		t.Fatalf("dialling the attach socket: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// hold is breakpointed before the gate opens, so the run cannot end
	// before the client has answered.
	mustControl(t, ctx, src, api.OpBreakpointSet, map[string]string{"step": "hold"})
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("opening the gate: %v", err)
	}

	id := waitForProposal(t, ctx, &got)
	if id != "fetch@1" {
		t.Fatalf("proposal id = %q, want fetch@1", id)
	}

	// An id nothing proposed is refused, and the refusal is specific.
	if reason := control(t, ctx, src, api.OpAnalysisAccept, map[string]string{"id": "nope@9"}); reason != "unknown_proposal" {
		t.Errorf("accepting a proposal that does not exist = %q, want unknown_proposal", reason)
	}
	// So is one with no id at all.
	if reason := control(t, ctx, src, api.OpAnalysisAccept, map[string]string{}); reason != "missing_proposal" {
		t.Errorf("accepting with no id = %q, want missing_proposal", reason)
	}

	mustControlSettled(t, ctx, src, api.OpAnalysisAccept, map[string]string{"id": id})

	// Deciding twice is refused, which is what stops two operators from
	// retrying one step twice.
	if reason := control(t, ctx, src, api.OpAnalysisAccept, map[string]string{"id": id}); reason != "proposal_settled" {
		t.Errorf("a second accept = %q, want proposal_settled", reason)
	}
	if reason := control(t, ctx, src, api.OpAnalysisReject, map[string]string{"id": id}); reason != "proposal_settled" {
		t.Errorf("rejecting an already-accepted proposal = %q, want proposal_settled", reason)
	}

	mustControl(t, ctx, src, api.OpBreakpointClear, map[string]string{"step": "hold"})
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	applied := got.ofType(api.AnalysisApplied)
	if len(applied) != 1 {
		t.Fatalf("%d analysis.applied events, want 1", len(applied))
	}
	b := decodeDecision(t, applied[0])
	if b.Policy {
		t.Error("a decision an attached client made is recorded as a policy decision")
	}
	if b.ClientID == "" {
		t.Error("analysis.applied names no client; a run has to be able to say who approved a change to what it was doing")
	}
	if b.ID != id {
		t.Errorf("ID = %q, want %q", b.ID, id)
	}
	if n := len(got.ofType(api.StepRetried)); n != 1 {
		t.Errorf("%d step.retried events, want the one the accepted proposal caused", n)
	}
	// The remedy really ran: the step recovered on the attempt the accept
	// dispatched.
	var recovered bool
	for _, e := range got.ofType(api.StepFinished) {
		var sb api.StepFinishedBody
		if e.Step == "fetch" && e.Decode(&sb) == nil && sb.State == api.StateRecovered {
			recovered = true
		}
	}
	if !recovered {
		t.Error("fetch never reached recovered; accepting a retry remedy has to actually retry the step")
	}
}

// control sends one control operation and returns the engine's refusal
// reason, or "" when it was accepted.
func control(t *testing.T, ctx context.Context, src *source.LiveSource, op string, args map[string]string) string {
	t.Helper()
	payload, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal control args: %v", err)
	}
	resp, err := src.Control(ctx, api.Frame{
		V: api.Version, Kind: api.KindReq, ID: op, Type: op, Payload: payload,
	})
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if resp.OK != nil && *resp.OK {
		return ""
	}
	return resp.Error
}

func mustControl(t *testing.T, ctx context.Context, src *source.LiveSource, op string, args map[string]string) {
	t.Helper()
	if reason := control(t, ctx, src, op, args); reason != "" {
		t.Fatalf("%s refused: %s", op, reason)
	}
}

// mustControlSettled is mustControl for a request aimed at a step the test
// has just seen produce a proposal. The proposal is offered from inside
// runStep, immediately after the step.finished it describes, and the step
// releases its claim only when its goroutine returns, after its handlers, so
// "step_running" is a correct, transient answer in that window: it is what an
// operator would answer by pressing the key again, which is all this does.
// Any other refusal fails the test immediately rather than being retried
// away. The engine's own policy path cannot do this - it gets one
// application per step - and holds the decision instead; see
// handleAnalysisAccept.
func mustControlSettled(t *testing.T, ctx context.Context, src *source.LiveSource, op string, args map[string]string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		reason := control(t, ctx, src, op, args)
		if reason == "" {
			return
		}
		if reason != "step_running" || time.Now().After(deadline) {
			t.Fatalf("%s refused: %s", op, reason)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForProposal(t *testing.T, ctx context.Context, got *collect) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("context ended before a proposal arrived: %v", ctx.Err())
		}
		for _, e := range got.ofType(api.AnalysisProposed) {
			return decodeProposed(t, e).ID
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no analysis.proposed arrived within 30s")
	return ""
}

// TestAWedgedAnalyzerDoesNotStallARun is the promise the whole design is
// built around, tested rather than asserted: an analyzer that never returns
// costs the run its grace and not a second more, and the run still finishes
// with a complete, sealed ledger.
func TestAWedgedAnalyzerDoesNotStallARun(t *testing.T) {
	dir := t.TempDir()
	var got collect
	var report strings.Builder

	entered := make(chan struct{}, 1)
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c", "exit 1"))

	started := time.Now()
	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(senro.AnalyzerFunc(
			func(ctx context.Context, _ api.Failure) (api.Proposal, error) {
				select {
				case entered <- struct{}{}:
				default:
				}
				// Ignores the context entirely, which is the worst case: an
				// analyzer that does honour it settles at once.
				time.Sleep(90 * time.Second)
				return api.Proposal{Summary: "far too late"}, nil
			}),
			senro.AnalyzeGrace(300*time.Millisecond),
			senro.AnalyzeReportWriter(&report)),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	took := time.Since(started)

	if err == nil {
		t.Fatal("Run succeeded; the pipeline was supposed to fail")
	}
	select {
	case <-entered:
	default:
		t.Fatal("the analyzer was never called; this test would pass vacuously")
	}
	// The grace is 300ms and the settle window after it is a second. Ten
	// seconds is enormous slack on a loaded CI machine and still nowhere near
	// the 90 the analyzer wanted.
	if took > 10*time.Second {
		t.Errorf("the run took %v with a 300ms analyze grace; a wedged analyzer stalled it", took)
	}

	// The run still ended properly: run.finished is present and is last.
	all := got.all()
	if len(all) == 0 {
		t.Fatal("no events at all")
	}
	if last := all[len(all)-1]; last.Type != api.RunFinished {
		t.Errorf("last event is %s, want run.finished: the stream did not seal cleanly", last.Type)
	}
	if n := len(got.ofType(api.AnalysisProposed)); n != 0 {
		t.Errorf("%d analysis.proposed events from an analyzer that never answered", n)
	}
	if r := report.String(); !strings.Contains(r, "senro analyze:") {
		t.Errorf("the shutdown report says nothing about a failure that went unexplained: %q", r)
	}
}

// TestAnAnalyzerThatPanicsDoesNotFailTheRun holds third-party code to the
// same rule a Renderer and a Sink are held to: an observer must not be able
// to end a build.
func TestAnAnalyzerThatPanicsDoesNotFailTheRun(t *testing.T) {
	dir := t.TempDir()
	var got collect
	var report strings.Builder

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("ok", exec.Command("sh", "-c", "echo fine"))
	l.Step("bad", exec.Command("sh", "-c", "exit 2")).Needs("ok")

	err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(senro.AnalyzerFunc(
			func(context.Context, api.Failure) (api.Proposal, error) {
				panic("the analyzer's own bug")
			}), senro.AnalyzeReportWriter(&report)),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	)
	// The run fails because the step failed, not because the analyzer did.
	if err == nil {
		t.Fatal("Run succeeded; the step was supposed to fail")
	}
	var re *senro.RunError
	if !errors.As(err, &re) {
		t.Fatalf("Run returned %v, want the ordinary step failure a run with no analyzer would return", err)
	}

	all := got.all()
	if last := all[len(all)-1]; last.Type != api.RunFinished {
		t.Errorf("last event is %s, want run.finished", last.Type)
	}
	if n := len(got.ofType(api.AnalysisProposed)); n != 0 {
		t.Errorf("%d proposals from an analyzer that panicked", n)
	}
	if r := report.String(); !strings.Contains(r, "panicked") {
		t.Errorf("the shutdown report does not name the panic: %q", r)
	}
}

// TestASucceedingRunNeverCallsTheAnalyzer: an analyzer is for failures, and a
// green run should not be paying anything for one, nor sending anybody's API
// a request about a step that worked.
func TestASucceedingRunNeverCallsTheAnalyzer(t *testing.T) {
	dir := t.TempDir()
	an := fakeanalyzer.New()
	var got collect

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("sh", "-c", "echo a"))
	l.Step("b", exec.Command("sh", "-c", "echo b")).Needs("a")

	if err := senro.Run(t.Context(), pipe,
		senro.WithAnalyzer(an),
		senro.WithSink(&got),
		senro.WithDir(filepath.Join(dir, "run")),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := len(an.Seen()); n != 0 {
		t.Errorf("the analyzer was called %d times for a run in which nothing failed", n)
	}
	for _, e := range got.all() {
		switch e.Type {
		case api.AnalysisProposed, api.AnalysisApplied, api.AnalysisRejected:
			t.Errorf("a succeeding run emitted %s", e.Type)
		}
	}
}

// TestAPolicyAppliesAtMostOneProposalPerStep pins the bound that makes the
// unsupervised path safe at all.
//
// The step always fails and the policy always says yes. Without a bound this
// is an infinite loop: apply, retry, fail, analyze, propose, apply, on a
// verdict from a program senro did not write, for as long as the process
// lives. One application per step is what stops it, and this test is what
// stops that from being removed by somebody who reads it as a limitation.
func TestAPolicyAppliesAtMostOneProposalPerStep(t *testing.T) {
	dir := t.TempDir()
	var got collect

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c", "echo 'connection refused' >&2; exit 1"))

	done := make(chan error, 1)
	go func() {
		done <- senro.Run(t.Context(), pipe,
			senro.WithAnalyzer(fakeanalyzer.New(),
				senro.AcceptWithoutHumanApproval(func(api.Failure, api.Proposal) bool {
					return true // says yes forever, and must be asked only once
				})),
			senro.WithSink(&got),
			senro.WithDir(filepath.Join(dir, "run")))
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run succeeded; the step always fails")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the run never ended: an auto-applied remedy is retrying without a bound")
	}

	// Two attempts: the original, and the one application the policy is
	// allowed. Never a third.
	if n := len(got.ofType(api.StepRetried)); n != 1 {
		t.Errorf("%d step.retried events, want exactly 1: a policy gets one application per step", n)
	}
	if n := len(got.ofType(api.AnalysisApplied)); n != 1 {
		t.Errorf("%d analysis.applied events, want exactly 1", n)
	}
	// The second failure is still explained. Bounding what may be APPLIED
	// must not bound what may be said: an operator reading this run should
	// still find out why the second attempt failed too.
	if n := len(got.ofType(api.AnalysisProposed)); n != 2 {
		t.Errorf("%d analysis.proposed events, want 2: the bound is on applying, not on explaining", n)
	}
}
