//go:build unix

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// A service whose command is a wrapper that backgrounds the real server and
// exits fails, and the server it left in its process group is killed with
// it rather than left holding the port after the run.
func TestAWrapperThatExitsTakesItsServerWithIt(t *testing.T) {
	port := freePort(t)
	db := serveNode(t, "db", port, false)
	self := db.Cmd[0]
	// The wrapper backgrounds the server, waits for the marker the server
	// writes once it is listening (so the child provably holds the port),
	// then exits on its own. Whether the probe passed first is a race the
	// test does not care about: either way the service fails.
	bound := filepath.Join(t.TempDir(), "bound")
	db.Env = append(db.Env, "SENRO_TEST_SERVE_BOUND_FILE="+bound)
	db.Cmd = []string{"sh", "-c", fmt.Sprintf(
		`%s '-test.run=^TestHelperServe$' & while [ ! -f %s ]; do sleep 0.05; done; exit 0`, self, bound)}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		db,
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sleep", "5"}},
	}}
	dir := t.TempDir()
	rec, _, done := startRun(t, dir, p, nil)
	awaitEvent(t, rec, api.StepFinished, "db", 5*time.Second)
	if _, err := os.Stat(bound); err != nil {
		t.Fatalf("the server never listened: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for listening(port) {
		if time.Now().After(deadline) {
			t.Fatal("the wrapper's server still holds the port 2s after the service settled")
		}
		time.Sleep(20 * time.Millisecond)
	}
	awaitRun(t, done, 10*time.Second)
	if st := foldStates(t, dir)["db"]; st != api.StateFailed {
		t.Errorf("db = %s, want failed", st)
	}
}

// A consumer retried while its service is still inside its StopGrace (a
// server that ignores SIGTERM, a long grace) waits for the service to stop
// and restart: a second service.ready precedes the consumer's second
// step.started, and the consumer runs against it.
func TestRetryingAConsumerDuringItsServicesStopGraceWaitsForTheRestart(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "tried")
	db := serveNode(t, "db", freePort(t), false)
	db.Env = append(db.Env, "SENRO_TEST_SERVE_STUBBORN=1")
	db.Service.StopGraceMS = 2000
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		db,
		{ID: "keep", Kind: "exec", Cmd: []string{"sleep", "6"}},
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: retriedConsumer(marker)},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	useFinished := make(chan api.Event, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "use" {
			useFinished <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GRACESVC",
	})

	// use fails, so db is told to stop and sits in its grace, ignoring the
	// SIGTERM. The pause lets the scheduler's pass that stops it run first.
	waitForEvent(t, useFinished, func(api.Event) bool { return true })
	time.Sleep(300 * time.Millisecond)
	if indexOf(csink.Events(), api.StepFinished, "db") >= 0 {
		t.Fatalf("db finished before the retry: the grace did not hold it (db: %s)", dbVerdict(csink.Events()))
	}
	resp := sendSettled(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "use"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK (db: %s)", resp, dbVerdict(csink.Events()))
	}
	awaitResult(t, out, 15*time.Second)

	events := readLedger(t, dir)
	ready, started := attemptIndex(events, api.ServiceReady, "db", 2), attemptIndex(events, api.StepStarted, "use", 2)
	if ready < 0 {
		t.Fatal("no second service.ready for db: the service in its grace was not restarted")
	}
	if started < ready {
		t.Errorf("use's second step.started at %d, before db's second service.ready at %d", started, ready)
	}
	if st := foldStates(t, dir)["use"]; st != api.StateRecovered {
		t.Errorf("use = %s, want recovered", st)
	}
}

// Two consumers retried back to back both wait for the one service the
// first retry revived: the second retry finds it unsettled and not yet
// dispatched, or dispatched and still being probed, neither of which is
// ready. b's retry may also land in the moment the scheduler has marked the
// revived service running but not yet dispatched it, which must read as
// live rather than stopping. a lingers on its second run so the service
// stays up until b's retry has landed whatever the scheduling: one restart,
// never two.
func TestTwoConsumersRetriedBackToBackBothWaitForTheRevivedService(t *testing.T) {
	marks := t.TempDir()
	lingering := retriedConsumer(filepath.Join(marks, "a"))
	lingering[2] = strings.Replace(lingering[2], "then exit 0", "then sleep 2; exit 0", 1)
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", freePort(t), false),
		{ID: "keep", Kind: "exec", Cmd: []string{"sleep", "6"}},
		{ID: "a", Kind: "exec", Needs: []string{"db"}, Cmd: lingering},
		{ID: "b", Kind: "exec", Needs: []string{"db"}, Cmd: retriedConsumer(filepath.Join(marks, "b"))},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	dbFinished := make(chan api.Event, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "db" {
			dbFinished <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01TWORETRY",
	})

	waitForEvent(t, dbFinished, func(e api.Event) bool { return e.Attempt == 1 })
	for _, step := range []string{"a", "b"} {
		resp := sendSettled(t, csink, sink.ControlRequest{
			ID: "r-" + step, Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": step},
		})
		if !resp.OK {
			t.Fatalf("step.retry %s response = %+v, want OK (db: %s)", step, resp, dbVerdict(csink.Events()))
		}
	}
	awaitResult(t, out, 15*time.Second)

	events := readLedger(t, dir)
	ready := attemptIndex(events, api.ServiceReady, "db", 2)
	if ready < 0 {
		t.Fatal("no second service.ready for db")
	}
	if attemptIndex(events, api.ServiceReady, "db", 3) >= 0 {
		t.Error("db was restarted twice: the second retry should wait for the first one's restart")
		for _, e := range events {
			t.Logf("%d %s %s #%d %s", e.Seq, e.Type, e.Step, e.Attempt, e.Payload)
		}
	}
	states := foldStates(t, dir)
	for _, step := range []string{"a", "b"} {
		if started := attemptIndex(events, api.StepStarted, step, 2); started < ready {
			t.Errorf("%s's second step.started at %d, before db's second service.ready at %d", step, started, ready)
		}
		if states[step] != api.StateRecovered {
			t.Errorf("%s = %s, want recovered", step, states[step])
		}
	}
}

// A retried SERVICE whose own service need does not come back is skipped
// without ever running, and a consumer retried right behind it, waiting for
// that retried service's execution, is told so rather than left waiting.
//
// a serves once and fails on its restart (after a second, which is the
// window the two retries land in); b needs a, is ready once and dies a
// second later; c needs b and fails on its first run because b died under
// it. keep holds the run open.
func TestAConsumerOfARetriedServiceThatNeverRunsIsNotLeftWaiting(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "a-ran")
	a := serveNode(t, "a", freePort(t), false)
	self := a.Cmd[0]
	a.Cmd = []string{"sh", "-c", fmt.Sprintf(
		`if [ -f %s ]; then sleep 1; exit 1; fi; touch %s; exec %s '-test.run=^TestHelperServe$'`,
		marker, marker, self)}
	b := serveNode(t, "b", freePort(t), false)
	b.Needs = []string{"a"}
	b.Env = append(b.Env, "SENRO_TEST_SERVE_EXIT_AFTER_MS=1000")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		a, b,
		{ID: "keep", Kind: "exec", Cmd: []string{"sleep", "8"}},
		{ID: "c", Kind: "exec", Needs: []string{"b"}, Cmd: []string{"sh", "-c", "sleep 2; exit 1"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	cFinished := make(chan api.Event, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "c" {
			cFinished <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01NEVERRUNS",
	})

	waitForEvent(t, cFinished, func(api.Event) bool { return true })
	for _, step := range []string{"b", "c"} {
		resp := sendSettled(t, csink, sink.ControlRequest{
			ID: "r-" + step, Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": step},
		})
		if !resp.OK {
			t.Fatalf("step.retry %s response = %+v, want OK (db: %s)", step, resp, dbVerdict(csink.Events()))
		}
	}
	// keep ends at 8s; a consumer left waiting would hold the run until the
	// test's own deadline.
	awaitResult(t, out, 15*time.Second)
	states := foldStates(t, dir)
	if states["b"] != api.StateSkippedUpstreamFailed {
		t.Errorf("b = %s, want skipped_upstream_failed: its need a did not come back", states["b"])
	}
	if states["c"] != api.StateSkippedUpstreamFailed {
		t.Errorf("c = %s, want skipped_upstream_failed: the b it waited for never ran", states["c"])
	}
}

// retriedConsumer fails on its first run and passes on a later one only
// when it is handed db's address.
func retriedConsumer(marker string) []string {
	return []string{"sh", "-c", fmt.Sprintf(
		`if [ -f %s ] && [ -n "$SENRO_SERVICE_DB_HOST" ]; then exit 0; fi; touch %s; exit 1`, marker, marker)}
}

// attemptIndex is the index of the event of typ for step's attempt, or -1.
func attemptIndex(events []api.Event, typ api.Type, step string, attempt int) int {
	for i, e := range events {
		if e.Type == typ && e.Step == step && e.Attempt == attempt {
			return i
		}
	}
	return -1
}

// awaitResult waits for a runAsync run and fails on an engine error.
func awaitResult(t *testing.T, out <-chan runResult, within time.Duration) {
	t.Helper()
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
	case <-time.After(within):
		t.Fatalf("the run did not end within %v", within)
	}
}

// envSpyExecutor is the local executor recording every environment it was
// asked to report the effective form of: the input to a cache key's env
// component.
type envSpyExecutor struct {
	executor.Executor
	mu   sync.Mutex
	seen [][]string
}

func (e *envSpyExecutor) EffectiveEnv(ctx context.Context, declared []string) ([]string, error) {
	e.mu.Lock()
	e.seen = append(e.seen, append([]string(nil), declared...))
	e.mu.Unlock()
	return e.Executor.EffectiveEnv(ctx, declared)
}

// A Pure consumer that is handed its service's address, and even names the
// variable in CacheEnv, still builds its key from an environment without it:
// SENRO_SERVICE_* is added to the attempt's command environment only, after
// the key's env input has been read. The allow list is not what keeps it out
// (TestServiceEnvNeverEntersTheCacheKey pins that half); the input is.
func TestAServiceAddressIsNeverInTheCacheKeysEnvInput(t *testing.T) {
	dir := t.TempDir()
	// A Pure step needs an input, resolved against the working directory.
	if err := os.WriteFile(filepath.Join(dir, "in.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	store := openStore(t, dir)
	spy := &envSpyExecutor{Executor: localexec.New(dir, store.Snapshotter)}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", freePort(t), false),
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Pure: true, Inputs: []string{"file:in.txt"},
			CacheEnv: []string{"SENRO_SERVICE_DB_HOST"},
			Cmd:      []string{"sh", "-c", `test -n "$SENRO_SERVICE_DB_HOST"`}},
	}}
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "01ENVSPY", Sink: sink.Nop(), MaxParallel: 4, Storage: store, Executor: spy,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		_, fin := lastStepFinished(t, readLedger(t, dir), "use")
		t.Fatalf("Run = %s; want succeeded: the consumer must see the address (use: %q)", status, fin.Error)
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.seen) == 0 {
		t.Fatal("EffectiveEnv was never asked: the consumer's key was not built")
	}
	for _, env := range spy.seen {
		for _, kv := range env {
			if strings.HasPrefix(kv, "SENRO_SERVICE_") {
				t.Errorf("the cache key's env input carried %q", kv)
			}
		}
	}
}

// noBridgeExecutor runs everything on the local executor, but its
// sandboxes cannot say where another container reaches them: a daemon
// failing the one question only a container consumer needs answered.
type noBridgeExecutor struct{ executor.Executor }

func (e noBridgeExecutor) Sandbox(ctx context.Context, spec executor.SandboxSpec) (executor.Sandbox, error) {
	sb, err := e.Executor.Sandbox(ctx, spec)
	if err != nil {
		return nil, err
	}
	return noBridgeSandbox{sb}, nil
}

type noBridgeSandbox struct{ executor.Sandbox }

func (s noBridgeSandbox) StartService(
	ctx context.Context, c executor.Cmd, stdout, stderr io.Writer,
) (executor.RunningService, error) {
	return s.Sandbox.(executor.ServiceRunner).StartService(ctx, c, stdout, stderr)
}

func (s noBridgeSandbox) HostAddress(ctx context.Context, port int) (string, error) {
	return s.Sandbox.(executor.Addressable).HostAddress(ctx, port)
}

func (noBridgeSandbox) ContainerAddress(context.Context, int) (string, error) {
	return "", errors.New("no bridge address")
}

// The bridge address of a container service is asked for only when a
// consumer runs in a container: with local consumers only, a daemon that
// cannot answer it does not fail the service. With a container consumer it
// does, since that consumer could not be told where to dial.
func TestTheBridgeAddressIsAskedForOnlyForAContainerConsumer(t *testing.T) {
	spec := &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"}
	for _, tc := range []struct {
		name     string
		consumer *plan.ExecutorSpec
		want     api.RunStatus
	}{
		{"local consumer", nil, api.RunSucceeded},
		{"container consumer", spec, api.RunFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := serveNode(t, "db", freePort(t), false)
			db.Executor = spec
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{
				db,
				{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"true"}, Executor: tc.consumer},
			}}
			dir := t.TempDir()
			status, err := engine.Run(context.Background(), p, engine.Options{
				Dir: dir, RunID: "01NOBRIDGE", Sink: sink.Nop(), MaxParallel: 4,
				Executor:  localexec.New(dir, nil),
				Executors: map[string]executor.Executor{spec.Key(): noBridgeExecutor{localexec.New(dir, nil)}},
			})
			if err != nil || status != tc.want {
				_, fin := lastStepFinished(t, readLedger(t, dir), "db")
				t.Fatalf("Run = %s, %v; want %s (db: %s %q)", status, err, tc.want, fin.State, fin.Error)
			}
			if tc.want == api.RunFailed {
				if _, fin := lastStepFinished(t, readLedger(t, dir), "db"); !strings.Contains(fin.Error, "no bridge address") {
					t.Errorf("db error = %q, want the bridge address failure", fin.Error)
				}
			}
		})
	}
}
