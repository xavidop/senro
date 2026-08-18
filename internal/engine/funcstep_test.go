package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

func init() {
	senro.RegisterFunc("enginetest/ok", func(ctx senro.Ctx, p struct {
		Message string `json:"message"`
	}) error {
		_, _ = io.WriteString(ctx.Stdout(), p.Message+"\n")
		return nil
	})
	senro.RegisterFunc("enginetest/fail", func(ctx senro.Ctx, p struct{}) error {
		_, _ = io.WriteString(ctx.Stderr(), "about to fail\n")
		return errors.New("the function said no")
	})
	senro.RegisterFunc("enginetest/panic", func(ctx senro.Ctx, p struct{}) error {
		panic("deliberate")
	})
	senro.RegisterFunc("enginetest/introspect", func(ctx senro.Ctx, p struct{}) error {
		ws, ok := ctx.Workspace("src")
		if !ok {
			return errors.New("no workspace")
		}
		return os.WriteFile(ws.Path("written-by-func.txt"),
			[]byte(ctx.RunID()+" "+ctx.StepID()+" "+strconv.Itoa(ctx.Attempt())), 0o644)
	})
}

// runToDirAndEvents runs p to completion against a fresh local executor and
// temp directory, returning the run directory and the decoded event stream.
// Storage is always opened, even for a plan with no Workspaces: Run refuses
// a plan needing storage that was given none, and a store costs nothing for
// a plan that declares none.
func runToDirAndEvents(t *testing.T, p *plan.Plan) (string, []api.Event) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, store.Snapshotter),
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01FUNCTEST",
		Storage:     store,
	}); err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	return dir, readLedger(t, dir)
}

// runToDirAndEventsWithSecret is runToDirAndEvents plus one resolved secret,
// delivered through engine.Options.Secrets exactly the way senro.Run
// delivers one: through a config struct mamori.Load resolved and
// secrets.FromConfig walked. name must be "Token", the one field this
// helper's config struct declares: secrets.FromConfig has no way to invent
// a struct field at runtime, so this helper cannot be generic over the name
// without a second config type per caller.
func runToDirAndEventsWithSecret(t *testing.T, p *plan.Plan, name, value string) (string, []api.Event) {
	t.Helper()
	if name != "Token" {
		t.Fatalf("runToDirAndEventsWithSecret only supports the secret name %q, got %q", "Token", name)
	}

	type config struct {
		Token secret.String `source:"fake://enginetest/token#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("enginetest/token#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("secrets.FromConfig: %v", err)
	}

	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, store.Snapshotter),
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01FUNCTEST",
		Secrets:     set,
		Storage:     store,
	}); err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	return dir, readLedger(t, dir)
}

// hasEventFor reports whether events contains one of type typ scoped to
// step. step is "" for a run-level event such as run.finished.
func hasEventFor(events []api.Event, typ api.Type, step string) bool {
	return indexOf(events, typ, step) >= 0
}

// readLog reads one attempt's log file for one stream straight off disk, the
// same path attach's own range requests read from.
func readLog(t *testing.T, dir, step string, attempt int, stream string) string {
	t.Helper()
	body, err := os.ReadFile(eventlog.NewLogSet(dir).Path(step, attempt, stream))
	if err != nil {
		t.Fatalf("reading log %s/%d/%s: %v", step, attempt, stream, err)
	}
	return string(body)
}

func TestAFuncStepRunsAndItsOutputLandsInItsLogFile(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "hello", Kind: "func",
		Func: &plan.FuncSpec{Name: "enginetest/ok", Params: []byte(`{"message":"from a function"}`)},
	}}}
	dir, events := runToDirAndEvents(t, p)

	if st, _ := stepFinished(t, events, "hello"); st != api.StateSucceeded {
		t.Fatalf("state = %s", st)
	}
	body := readLog(t, dir, "hello", 1, api.StreamStdout)
	if body != "from a function\n" {
		t.Errorf("log = %q, want the function's own output", body)
	}
	// step.started says what ran, which for a func step is a NAME rather than
	// a command line.
	for _, e := range events {
		if e.Type != api.StepStarted {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if b.Func != "enginetest/ok" {
			t.Errorf("step.started func = %q", b.Func)
		}
	}
	// And step.log.appended markers describe that file, exactly as they do for
	// a command: a func step is not exempt from the log protocol.
	if !hasEventFor(events, api.StepLogAppended, "hello") {
		t.Error("a func step produced no step.log.appended marker")
	}
}

func TestAFailingFuncIsAnOrdinaryStepFailure(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/fail"},
	}}}
	_, events := runToDirAndEvents(t, p)
	st, body := stepFinished(t, events, "boom")
	if st != api.StateFailed {
		t.Fatalf("state = %s, want failed", st)
	}
	if !strings.Contains(body.Error, "the function said no") {
		t.Errorf("error = %q, want the function's own message", body.Error)
	}
}

// TestAPanickingFuncSettlesAsPanickedAndTheRunSurvives is the state nothing
// in this engine has ever produced, and the reason it exists: a local func
// runs in the coordinator's process, so this is the one step kind that can
// take the run down with it.
func TestAPanickingFuncSettlesAsPanickedAndTheRunSurvives(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "boom", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/panic"}},
		{ID: "after", Kind: "exec", Cmd: []string{"true"}},
	}}
	dir, events := runToDirAndEvents(t, p)
	if st, _ := stepFinished(t, events, "boom"); st != api.StatePanicked {
		t.Fatalf("state = %s, want panicked", st)
	}
	if st, _ := stepFinished(t, events, "after"); st != api.StateSucceeded {
		t.Errorf("an unrelated step settled as %s; the panic escaped its own step", st)
	}
	if !hasEventFor(events, api.RunFinished, "") {
		t.Fatal("the run never emitted run.finished, so the panic took the ledger with it")
	}
	// The stack is evidence and belongs somewhere a person can find it.
	if !strings.Contains(readLog(t, dir, "boom", 1, api.StreamStderr), "deliberate") {
		t.Error("the panic's own value is in neither log stream")
	}
}

func TestAFuncStepSeesItsWorkspaceAndItsIdentity(t *testing.T) {
	p := &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "write", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/introspect"},
			Mounts: []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
		}},
	}
	dir, events := runToDirAndEvents(t, p)
	if st, _ := stepFinished(t, events, "write"); st != api.StateSucceeded {
		t.Fatalf("state = %s", st)
	}
	body, err := os.ReadFile(filepath.Join(dir, "ws", "src", "written-by-func.txt"))
	if err != nil {
		t.Fatalf("the function did not write into the workspace: %v", err)
	}
	if !strings.HasSuffix(string(body), " write 1") {
		t.Errorf("ctx identity = %q", body)
	}
	// The workspace was snapshotted like any other step's, which is what makes
	// a func step's output content-addressed and reusable downstream.
	if !hasEventFor(events, api.WSSnapshot, "write") {
		t.Error("a func step's workspace was not snapshotted")
	}
}

// TestAFuncHandlerRunsToo is the both-legs assertion. execHandler and
// runAttempt have diverged three times in this project; a fork between two
// step kinds is exactly the kind of change that does it a fourth.
func TestAFuncHandlerRunsToo(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func",
			Func: &plan.FuncSpec{Name: "enginetest/ok", Params: []byte(`{"message":"handled"}`)},
		}},
	}}}
	dir, events := runToDirAndEvents(t, p)
	if !hasEventFor(events, api.HandlerSucceeded, "boom/on_failure/notify") {
		t.Fatal("a func handler did not run")
	}
	if got := readLog(t, dir, "boom/on_failure/notify", 1, api.StreamStdout); got != "handled\n" {
		t.Errorf("the handler's output = %q", got)
	}
}

// TestTimeoutAppliesToAFuncHandlerToo checks the handler side of the same
// timeout-classification trap TestTimeoutAppliesToAFuncStepToo checks for a
// step: execHandler's classification switch must check handlerCtx.Err(),
// not only runErr, or a func handler that ignores its own TimeoutMS and
// returns nil is reported as handler.succeeded no matter how badly it
// overran its declared deadline. execHandler and runAttempt have diverged
// before (see TestAFuncHandlerRunsToo's own doc); this is that same seam,
// on timeout classification specifically.
func TestTimeoutAppliesToAFuncHandlerToo(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func", TimeoutMS: 50,
			Func: &plan.FuncSpec{Name: "enginetest/ignorescontext", Params: []byte(`{"sleep_ms":300}`)},
		}},
	}}}
	_, events := runToDirAndEvents(t, p)

	if hasEventFor(events, api.HandlerSucceeded, "boom/on_failure/notify") {
		t.Fatal("a func handler that ignored and outran its own Timeout was reported as " +
			"handler.succeeded")
	}
	if !hasEventFor(events, api.HandlerFailed, "boom/on_failure/notify") {
		t.Fatal("a func handler that outran its own Timeout must settle as handler.failed, " +
			"exactly as a timed-out exec handler already does")
	}
}

func init() {
	senro.RegisterFunc("enginetest/panic-secret", func(ctx senro.Ctx, p struct{}) error {
		token, err := os.ReadFile(ctx.Secret("Token"))
		if err != nil {
			panic(err)
		}
		panic("leaked: " + string(token))
	})
	senro.RegisterFunc("enginetest/waitcancel", func(ctx senro.Ctx, p struct{}) error {
		<-ctx.Done()
		return ctx.Err()
	})
}

// TestAPanicValueContainingASecretIsRedactedEverywhere: a panic's value
// reaches the step's stderr and step.finished's Error, both ordinary sinks
// the redactor already sits in front of. Nothing about a func step is
// exempt merely because the bytes arrived through recover() rather than a
// Write.
func TestAPanicValueContainingASecretIsRedactedEverywhere(t *testing.T) {
	const value = "func-panic-secret-long-enough"
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/panic-secret"},
		Secrets: []plan.SecretSpec{{Name: "Token"}},
	}}}
	dir, events := runToDirAndEventsWithSecret(t, p, "Token", value)

	if st, _ := stepFinished(t, events, "boom"); st != api.StatePanicked {
		t.Fatalf("state = %s, want panicked", st)
	}

	stderr := readLog(t, dir, "boom", 1, api.StreamStderr)
	if !strings.Contains(stderr, "leaked:") {
		t.Fatalf("stderr = %q, the panic's own text is missing; this test proves nothing", stderr)
	}
	if strings.Contains(stderr, value) {
		t.Error("the panic's value leaked the secret into the step's own stderr log")
	}

	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), value) {
		t.Error("the panic's value leaked the secret into events.jsonl")
	}
}

// TestAFuncStepWhoseContextIsCancelledMidRunSettlesAsCancelled is a
// negative case. funcCtx embeds the attempt's own
// context.Context (invoke builds it from the same ctx runAttempt was given),
// so a function that respects cancellation the way any other context-aware
// Go code does sees the run's own teardown exactly as an exec step's process
// does when the run's context is cancelled out from under it: settled as
// StateCancelled, never retried, with the run still reaching run.finished.
func TestAFuncStepWhoseContextIsCancelledMidRunSettlesAsCancelled(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "wait", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/waitcancel"},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()

	if _, err := engine.Run(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, store.Snapshotter), Sink: sink.Nop(),
		MaxParallel: 4, RunID: "01FUNCTEST", Storage: store,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := readLedger(t, dir)
	if st, _ := stepFinished(t, events, "wait"); st != api.StateCancelled {
		t.Fatalf("state = %s, want cancelled", st)
	}
	if !hasEventFor(events, api.RunFinished, "") {
		t.Fatal("no run.finished after a cancelled func step; the ledger is incomplete")
	}
}

func init() {
	senro.RegisterFunc("enginetest/ignorescontext", func(ctx senro.Ctx, p struct {
		SleepMS int `json:"sleep_ms"`
	}) error {
		// Deliberately does NOT select on ctx.Done(): funcs.Invoke has no
		// way to preempt a function that ignores its context (Go cannot
		// forcibly stop a running goroutine), so a func step's Timeout can
		// never bound how
		// long the function actually runs, unlike an exec step's process,
		// which localexec kills outright. What Timeout MUST still do is
		// classify the outcome correctly once the function returns, rather
		// than reporting a deadline-violating step as succeeded.
		time.Sleep(time.Duration(p.SleepMS) * time.Millisecond)
		return nil
	})
}

// TestTimeoutAppliesToAFuncStepToo is TestTimeoutProducesTimedOut's
// func-kind sibling: runAttempt's classification must not gate the timeout
// and cancellation branches on runErr != nil alone, since a func that
// ignores its context and returns nil would then fall through to
// StateSucceeded however badly it overran. A timeout inert for half the
// step kinds is worse than no timeout.
func TestTimeoutAppliesToAFuncStepToo(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "func",
		Func:      &plan.FuncSpec{Name: "enginetest/ignorescontext", Params: []byte(`{"sleep_ms":300}`)},
		TimeoutMS: 50,
	}}}
	_, events := runToDirAndEvents(t, p)

	if st, body := stepFinished(t, events, "slow"); st != api.StateTimedOut {
		t.Fatalf("state = %s (error %q), want timed_out: Timeout must not be inert for a func step",
			st, body.Error)
	}
	if !hasEventFor(events, api.RunFinished, "") {
		t.Fatal("no run.finished after a timed-out func step")
	}
}

// TestACancelledFuncStepDoesNotWriteTheCache is the second half of finding
// 2: because the same runErr != nil gate also hid the
// cancellation branch, a Pure() func step whose function ignored the run's
// own cancellation and returned nil settled as StateSucceeded, which made
// runStep's cacheSave call, storing a cache entry from a run the operator
// had already cancelled. A cancelled run must never look, to a later run
// reading the same cache, like a completed one.
func TestACancelledFuncStepDoesNotWriteTheCache(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "cancelme", Kind: "func", Pure: true, Inputs: []string{"glob:**/*.go"},
		Func: &plan.FuncSpec{Name: "enginetest/ignorescontext", Params: []byte(`{"sleep_ms":300}`)},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	if _, err := engine.Run(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, store.Snapshotter), Sink: sink.Nop(),
		MaxParallel: 4, RunID: "01FUNCTEST", Storage: store,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := readLedger(t, dir)
	if st, _ := stepFinished(t, events, "cancelme"); st != api.StateCancelled {
		t.Fatalf("state = %s, want cancelled", st)
	}
	if hasEventFor(events, api.CacheSaved, "cancelme") {
		t.Error("a cancelled func step wrote a cache entry: a later run would " +
			"read this cancelled run's output as if it had completed")
	}
}

func init() {
	senro.RegisterFunc("enginetest/flaky", func(ctx senro.Ctx, p struct {
		Marker string `json:"marker"`
	}) error {
		if ctx.Attempt() == 1 {
			return fmt.Errorf("attempt one: %w", executor.ErrInfra)
		}
		token, err := os.ReadFile(ctx.Secret("Token"))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "read %d secret bytes on attempt %d\n", len(token), ctx.Attempt())
		return os.WriteFile(p.Marker, token, 0o600)
	})
}

// TestAFuncStepComposesWithRetryAndSecretsAndAlways is a composition
// check: each of retry, secret delivery and Always handlers being correct
// for exec steps does not prove they are correct for STEPS in general
// rather than only for commands; a second step kind is exactly the change
// that finds out.
func TestAFuncStepComposesWithRetryAndSecretsAndAlways(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "written")
	params, err := json.Marshal(map[string]string{"marker": marker})
	if err != nil {
		t.Fatal(err)
	}
	const value = "func-secret-value-long-enough"

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "func",
		Func:    &plan.FuncSpec{Name: "enginetest/flaky", Params: params},
		Secrets: []plan.SecretSpec{{Name: "Token"}},
		Retry:   &plan.RetrySpec{MaxAttempts: 2, Predicate: "infra"},
		Always:  []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"true"}}},
	}}}

	dir, events := runToDirAndEventsWithSecret(t, p, "Token", value)

	if st, _ := stepFinished(t, events, "flaky"); st != api.StateRecovered {
		t.Fatalf("state = %s, want recovered: attempt one failed with an infra error", st)
	}
	if !hasEventFor(events, api.StepRetried, "flaky") {
		t.Error("no step.retried; the retry predicate did not see the function's error")
	}
	body, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("the function never read its secret file: %v", err)
	}
	if string(body) != value {
		t.Errorf("the function read %q, want the delivered value", body)
	}
	if !hasEventFor(events, api.HandlerSucceeded, "flaky/always/cleanup") {
		t.Error("the Always handler did not run for a func step")
	}

	// The canary, then the search: the run's own record mentions the secret's
	// NAME, so a search that finds neither name nor value is reading the wrong
	// bytes.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Token") {
		t.Fatal("events.jsonl does not mention the secret's name; this search proves nothing")
	}
	if strings.Contains(string(raw), value) {
		t.Error("the secret's value is in events.jsonl")
	}
}

// TestAFuncStepsStartedEventNeverNamesAWorkingDirectory pins the ledger
// consequence of plan.Validate refusing WorkDir on a func node:
// emitStepStarted copies n.WorkDir for both kinds, but funcs.Ctx has no
// working directory (a func step runs in the coordinator's process, where
// cwd is process-global), so the event would assert a directory the
// function never ran in. Both halves are asserted, since either alone would
// pass for the wrong reason.
func TestAFuncStepsStartedEventNeverNamesAWorkingDirectory(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "fn", Kind: "func", WorkDir: "/tmp",
		Func: &plan.FuncSpec{Name: "enginetest/ok", Params: []byte(`{"message":"hello"}`)},
	}}}

	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	_, err = engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, store.Snapshotter),
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01FUNCTEST",
		Storage:     store,
	})
	if err == nil {
		t.Fatal("Run started a plan whose func step declares a WorkDir the function can never run in")
	}
	if !strings.Contains(err.Error(), "Workspace") {
		t.Errorf("the refusal does not point at ctx.Workspace: %v", err)
	}

	p.Nodes[0].WorkDir = ""
	_, events := runToDirAndEvents(t, p)
	i := indexOf(events, api.StepStarted, "fn")
	if i < 0 {
		t.Fatal("no step.started for the func step, so the assertion below would prove nothing")
	}
	var body map[string]any
	if err := json.Unmarshal(events[i].Payload, &body); err != nil {
		t.Fatalf("decoding step.started: %v", err)
	}
	// The canary: this payload is the one that would have carried a workdir,
	// and it names the function, so an empty map cannot pass for an absence.
	if body["func"] != "enginetest/ok" {
		t.Fatalf("step.started does not describe the func step: %v", body)
	}
	if v, ok := body["workdir"]; ok {
		t.Errorf("a func step's step.started names a working directory %q", v)
	}
}
