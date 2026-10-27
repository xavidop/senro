//go:build unix

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// freePort hands out a port for a service to bind, from a range BELOW every
// platform's ephemeral range (Linux starts at 32768, macOS at 49152), so no
// other test package running beside this one, and no outbound connection,
// is ever handed the same number by the kernel. Asking the kernel for :0 and
// giving the port back was racy under CI load: the test packages `go test
// ./...` runs in parallel took the port in the gap, and the service was
// refused with "already in use".
//
// Each test binary walks its own hundred-port window chosen by pid, and a
// port something else happens to hold is skipped.
func freePort(t *testing.T) int {
	t.Helper()
	for range 100 {
		port := int(nextTestPort.Add(1))
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			continue
		}
		_ = ln.Close()
		return port
	}
	t.Fatal("no free port in this test binary's window")
	return 0
}

// nextTestPort is the last port freePort handed out. Windows of a hundred
// ports from 10000 upward, one per test binary, keep two binaries on one
// machine apart.
var nextTestPort = func() *atomic.Int64 {
	var p atomic.Int64
	p.Store(int64(10000 + (os.Getpid()%200)*100))
	return &p
}()

// serveNode is a service step that runs this test binary as TestHelperServe
// on port, probed over TCP, or over HTTP on /healthz when overHTTP.
func serveNode(t *testing.T, id string, port int, overHTTP bool) plan.Node {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	env := []string{
		"SENRO_TEST_SERVE_PORT=" + strconv.Itoa(port),
		// Under -race the helper otherwise sleeps a second on exit to let
		// late race reports land, which reads as a stop that waited.
		"GORACE=atexit_sleep_ms=0",
	}
	probe := plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: port}
	if overHTTP {
		env = append(env, "SENRO_TEST_SERVE_HTTP=1")
		probe = plan.ProbeSpec{Kind: plan.ServiceProbeHTTP, Port: port, Path: "/healthz"}
	}
	return plan.Node{
		ID: id, Kind: "exec", Cmd: []string{self, "-test.run=^TestHelperServe$"}, Env: env,
		Service: &plan.ServiceSpec{
			Ports: []int{port}, Probe: probe, ReadyTimeoutMS: 5000, StopGraceMS: 2000,
		},
	}
}

// startRun runs p in the background with a recording sink, so a test can
// watch the run while it is live, and cancel it.
func startRun(t *testing.T, dir string, p *plan.Plan, tweak func(*engine.Options)) (
	*sink.RecordingSink, context.CancelFunc, <-chan api.RunStatus,
) {
	t.Helper()
	rec := sink.Recording()
	ctx, cancel := context.WithCancel(context.Background())
	opts := engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 4, RunID: "01SERVICE",
	}
	if tweak != nil {
		tweak(&opts)
	}
	done := make(chan api.RunStatus, 1)
	go func() {
		status, err := engine.Run(ctx, p, opts)
		if err != nil {
			t.Errorf("Run returned an engine error: %v", err)
		}
		done <- status
	}()
	t.Cleanup(cancel)
	return rec, cancel, done
}

// awaitEvent polls rec until an event of typ for step arrives.
func awaitEvent(t *testing.T, rec *sink.RecordingSink, typ api.Type, step string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, e := range rec.Events() {
			if e.Type == typ && e.Step == step {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %s for %q within %v", typ, step, within)
}

// awaitRun waits for a background run to return.
func awaitRun(t *testing.T, done <-chan api.RunStatus, within time.Duration) api.RunStatus {
	t.Helper()
	select {
	case s := <-done:
		return s
	case <-time.After(within):
		t.Fatalf("the run did not end within %v", within)
		return ""
	}
}

func TestAServiceBecomesReadyAndItsConsumerSeesItsAddress(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	out := filepath.Join(t.TempDir(), "out")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", port, false),
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sh", "-c",
			fmt.Sprintf("echo $SENRO_SERVICE_DB_HOST $SENRO_SERVICE_DB_PORT_%d > %s", port, out)}},
	}}
	status, events, states := runPlan(t, dir, p)

	ready := indexOf(events, api.ServiceReady, "db")
	started := indexOf(events, api.StepStarted, "use")
	if ready < 0 || started < 0 || ready > started {
		t.Fatalf("service.ready at %d, consumer step.started at %d: ready must come first", ready, started)
	}
	var body api.ServiceReadyBody
	if err := json.Unmarshal(events[ready].Payload, &body); err != nil {
		t.Fatalf("service.ready payload: %v", err)
	}
	want := api.Endpoint{Port: port, Address: "127.0.0.1:" + strconv.Itoa(port)}
	if len(body.Endpoints) != 1 || body.Endpoints[0] != want {
		t.Errorf("endpoints = %+v, want [%+v]", body.Endpoints, want)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading what the consumer saw: %v", err)
	}
	if s := strings.TrimSpace(string(got)); s != "127.0.0.1 "+strconv.Itoa(port) {
		t.Errorf("consumer saw %q, want %q", s, "127.0.0.1 "+strconv.Itoa(port))
	}
	if states["db"] != api.StateSucceeded || states["use"] != api.StateSucceeded {
		t.Errorf("db = %s, use = %s, want both succeeded", states["db"], states["use"])
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
	if indexOf(events, api.StepFinished, "db") < indexOf(events, api.StepFinished, "use") {
		t.Error("db finished before its consumer: a service must outlive every consumer")
	}

	created := indexOf(events, api.StepCreated, "db")
	var cb api.StepCreatedBody
	if err := json.Unmarshal(events[created].Payload, &cb); err != nil || !cb.Service {
		t.Errorf("step.created for db = %+v (%v), want Service: true", cb, err)
	}
}

func TestAServiceProbedOverHTTPBecomesReady(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "web", freePort(t), true),
		{ID: "use", Kind: "exec", Needs: []string{"web"}, Cmd: []string{"true"}},
	}}
	status, events, _ := runPlan(t, dir, p)
	if indexOf(events, api.ServiceReady, "web") < 0 || status != api.RunSucceeded {
		t.Errorf("status = %s, service.ready seen = %v: an HTTP probe answering 200 is ready",
			status, indexOf(events, api.ServiceReady, "web") >= 0)
	}
}

func TestAServiceThatExitsBeforeReadyFails(t *testing.T) {
	dir := t.TempDir()
	db := serveNode(t, "db", freePort(t), false)
	db.Cmd = []string{"sh", "-c", "exit 3"}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		db,
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"true"}},
	}}
	seen := &failureRecorder{}
	_, events, states := runPlanWith(t, dir, p, func(o *engine.Options) {
		o.Analyze = &engine.AnalyzeOptions{Analyzer: seen, Name: "test", Grace: 2 * time.Second}
	})

	if states["db"] != api.StateFailed {
		t.Errorf("db = %s, want failed", states["db"])
	}
	if states["use"] != api.StateSkippedUpstreamFailed {
		t.Errorf("use = %s, want skipped_upstream_failed", states["use"])
	}
	_, fin := lastStepFinished(t, events, "db")
	if !strings.Contains(fin.Error, "status 3") || !strings.Contains(fin.Error, "before it was ready") {
		t.Errorf("error = %q, want it to name exit status 3 before ready", fin.Error)
	}
	// A failed service is offered to the analyzer like any failed step.
	if got := seen.steps(); len(got) != 1 || got[0] != "db" {
		t.Errorf("the analyzer saw %v, want exactly [db]", got)
	}
}

// failureRecorder is an analyzer that only records which steps it was asked
// about, and proposes nothing.
type failureRecorder struct {
	mu   sync.Mutex
	seen []string
}

func (f *failureRecorder) Analyze(_ context.Context, fail api.Failure) (api.Proposal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, fail.Step)
	return api.Proposal{}, errors.New("no proposal")
}

func (f *failureRecorder) steps() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

func TestAServiceThatNeverListensTimesOut(t *testing.T) {
	dir := t.TempDir()
	db := serveNode(t, "db", freePort(t), false)
	db.Cmd = []string{"sleep", "60"}
	db.Service.ReadyTimeoutMS = 500
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{db}}

	start := time.Now()
	_, events, states := runPlan(t, dir, p)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the run took %v: the readiness deadline must bound it", elapsed)
	}
	if states["db"] != api.StateFailed {
		t.Errorf("db = %s, want failed", states["db"])
	}
	if _, fin := lastStepFinished(t, events, "db"); !strings.Contains(fin.Error, "not ready") {
		t.Errorf("error = %q, want it to say not ready", fin.Error)
	}
}

func TestAServiceStopsOnlyWhenItsLastConsumerSettles(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", freePort(t), false),
		{ID: "quick", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"true"}},
		{ID: "slow", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sleep", "1"}},
	}}
	status, events, states := runPlan(t, dir, p)

	db := indexOf(events, api.StepFinished, "db")
	if db < indexOf(events, api.StepFinished, "quick") || db < indexOf(events, api.StepFinished, "slow") {
		t.Error("db finished before one of its consumers: it must wait for the last")
	}
	if states["db"] != api.StateSucceeded || status != api.RunSucceeded {
		t.Errorf("db = %s, status = %s, want succeeded", states["db"], status)
	}
}

// A ready service gives its MaxParallel slot back: waiting for its consumers
// is not work, and holding the slot under MaxParallel 1 would leave the
// consumer it is waiting for unable to start.
func TestAReadyServiceDoesNotHoldItsParallelismSlot(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", freePort(t), false),
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"true"}},
	}}
	_, _, done := startRun(t, dir, p, func(o *engine.Options) { o.MaxParallel = 1 })
	if status := awaitRun(t, done, 5*time.Second); status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

func TestCancelStopsARunningService(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", freePort(t), false),
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sleep", "30"}},
	}}
	rec, cancel, done := startRun(t, dir, p, nil)
	awaitEvent(t, rec, api.ServiceReady, "db", 5*time.Second)

	start := time.Now()
	cancel()
	status := awaitRun(t, done, 5*time.Second)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("cancelling took %v", elapsed)
	}
	if status != api.RunCancelled {
		t.Errorf("status = %s, want cancelled", status)
	}
	if st := foldStates(t, dir)["db"]; st != api.StateSucceeded {
		t.Errorf("db = %s, want succeeded: a service stopped by the run ended as it should", st)
	}
}

func TestHoldServicesKeepsAServiceUntilCancel(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{serveNode(t, "db", freePort(t), false)}}
	rec, cancel, done := startRun(t, dir, p, func(o *engine.Options) { o.HoldServices = true })
	awaitEvent(t, rec, api.ServiceReady, "db", 5*time.Second)

	select {
	case s := <-done:
		t.Fatalf("the run ended (%s) with a held service and no cancellation", s)
	case <-time.After(time.Second):
	}
	cancel()
	awaitRun(t, done, 5*time.Second)
	if st := foldStates(t, dir)["db"]; st != api.StateSucceeded {
		t.Errorf("db = %s, want succeeded: ending a held service is a clean end", st)
	}
}

// The consumer's SENRO_SERVICE_* variables carry a port that differs every
// run, so they must never reach a cache key, the same promise the trace
// context keeps. The key's env component reads only CacheEnv-declared names,
// and this pins that a service variable in the effective environment is not
// one of them by accident.
func TestServiceEnvNeverEntersTheCacheKey(t *testing.T) {
	declared := []string{"PATH=/usr/bin", "LANG=C"}
	allow := []string{"PATH", "LANG"}
	withService := append(append([]string(nil), declared...),
		"SENRO_SERVICE_DB_HOST=127.0.0.1", "SENRO_SERVICE_DB_PORT_5432=54321")
	if a, b := cache.EnvComponent(declared, allow), cache.EnvComponent(withService, allow); a != b {
		t.Errorf("env component moved with a service variable present:\n%q\n%q", a, b)
	}
}

// A service on the container executor passes plan validation and reaches
// runService. countingExecutor's sandbox implements neither ServiceRunner nor
// Addressable, so it lands on runService's refusal, and that refusal is the
// evidence: Validate used to refuse every container service before the run
// started (it compared ExecutorKey, which carries the image, to "container"),
// which surfaced as an engine error from Run instead.
func TestAContainerServiceReachesRunService(t *testing.T) {
	db := serveNode(t, "db", freePort(t), false)
	db.Executor = &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{db}}
	dir := t.TempDir()
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "01CONTAINERSVC", Sink: sink.Nop(), MaxParallel: 1,
		Executor:  localexec.New(dir, nil),
		Executors: map[string]executor.Executor{db.Executor.Key(): &countingExecutor{class: "c"}},
	})
	if err != nil {
		t.Fatalf("Run refused a container service: %v", err)
	}
	_, fin := lastStepFinished(t, readLedger(t, dir), "db")
	if fin.State != api.StateFailed || !strings.Contains(fin.Error, "cannot run one") {
		t.Errorf("db = %s %q, want failed by runService's capability refusal", fin.State, fin.Error)
	}
}

// The real thing, against a Docker daemon: a container service is published
// on the coordinator's loopback and a local consumer is given that address.
// Skips with no daemon, as every container test does.
func TestAContainerServiceIsReachableFromALocalConsumer(t *testing.T) {
	cli := dockertest.Require(t)
	dockertest.Pull(t, cli)
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	spec := plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image}
	ex, err := containerexec.New(spec, store.Snapshotter,
		containerexec.WithClient(cli), containerexec.WithRunID(testRunID(t)))
	if err != nil {
		t.Fatalf("containerexec.New: %v", err)
	}

	out := filepath.Join(t.TempDir(), "out")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "web", Kind: "exec", Cmd: []string{"httpd", "-f", "-p", "8080"}, Executor: &spec,
			Service: &plan.ServiceSpec{
				Ports: []int{8080}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 8080},
				ReadyTimeoutMS: 30000, StopGraceMS: 1000,
			}},
		{ID: "use", Kind: "exec", Needs: []string{"web"}, Cmd: []string{"sh", "-c",
			"echo $SENRO_SERVICE_WEB_HOST $SENRO_SERVICE_WEB_PORT_8080 > " + out}},
	}}
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "01CONTAINERSVC", Sink: sink.Nop(), MaxParallel: 4, Storage: store,
		Executor:  localexec.New(dir, store.Snapshotter),
		Executors: map[string]executor.Executor{spec.Key(): ex},
	})
	if err != nil || status != api.RunSucceeded {
		t.Fatalf("Run = %s, %v; want succeeded", status, err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading what the consumer saw: %v", err)
	}
	// A local consumer gets the address as published: loopback and the port
	// the daemon picked, which is not the declared one.
	host, port, _ := strings.Cut(strings.TrimSpace(string(got)), " ")
	if host != "127.0.0.1" || port == "" {
		t.Errorf("consumer saw %q, want 127.0.0.1 and the published port", got)
	}
}

// containerService is the daemon-backed setup the container service tests
// share: a store, a container executor on busybox, and the spec it serves.
func containerService(t *testing.T) (*storage.Storage, plan.ExecutorSpec, executor.Executor) {
	t.Helper()
	cli := dockertest.Require(t)
	dockertest.Pull(t, cli)
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	spec := plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image}
	ex, err := containerexec.New(spec, store.Snapshotter,
		containerexec.WithClient(cli), containerexec.WithRunID(testRunID(t)))
	if err != nil {
		t.Fatalf("containerexec.New: %v", err)
	}
	return store, spec, ex
}

// A container service that takes three seconds to listen is not ready
// before it does. Docker's port forwarder accepts on the published port from
// the moment the container starts, so a probe that only connected passed at
// once; the probe must see the forwarder drop the connection for what it is.
// Skips with no daemon.
func TestAContainerServiceIsNotReadyBeforeItListens(t *testing.T) {
	store, spec, ex := containerService(t)
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "web", Kind: "exec", Executor: &spec,
			Cmd: []string{"sh", "-c", "sleep 3; exec httpd -f -p 8080"},
			Service: &plan.ServiceSpec{
				Ports: []int{8080}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 8080},
				ReadyTimeoutMS: 30000, StopGraceMS: 1000,
			}},
	}}
	dir := t.TempDir()
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "01SLOWLISTEN", Sink: rec, MaxParallel: 4, Storage: store,
		Executor:  localexec.New(dir, store.Snapshotter),
		Executors: map[string]executor.Executor{spec.Key(): ex},
	})
	if err != nil || status != api.RunSucceeded {
		t.Fatalf("Run = %s, %v; want succeeded", status, err)
	}
	for _, e := range rec.Events() {
		if e.Type != api.ServiceReady {
			continue
		}
		var body api.ServiceReadyBody
		if err := json.Unmarshal(e.Payload, &body); err != nil {
			t.Fatalf("decoding service.ready: %v", err)
		}
		if body.Duration < 3*time.Second {
			t.Errorf("service.ready after %v, want at least 3s: the probe passed before httpd listened", body.Duration)
		}
		return
	}
	t.Fatal("no service.ready")
}

// A container consumer of a container service reaches it container to
// container: the service's bridge IP and the declared port, which works on
// native Linux where the coordinator's loopback is out of a container's
// reach. Skips with no daemon.
func TestAContainerServiceIsReachableFromAContainerConsumer(t *testing.T) {
	store, spec, ex := containerService(t)
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "web", Kind: "exec", Executor: &spec, Cmd: []string{"sh", "-c",
			// Under /tmp: the container runs as the coordinator's uid, which
			// cannot create /www in the image's root on a CI runner.
			"mkdir -p /tmp/www && echo hello > /tmp/www/index.html && exec httpd -f -p 8080 -h /tmp/www"},
			Service: &plan.ServiceSpec{
				Ports: []int{8080}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 8080},
				ReadyTimeoutMS: 30000, StopGraceMS: 1000,
			}},
		{ID: "use", Kind: "exec", Needs: []string{"web"}, Executor: &spec, Cmd: []string{"sh", "-c",
			"wget -qO- http://$SENRO_SERVICE_WEB_HOST:$SENRO_SERVICE_WEB_PORT_8080/ | grep -q hello"}},
	}}
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "01BOXTOBOX", Sink: sink.Nop(), MaxParallel: 4, Storage: store,
		Executor:  localexec.New(dir, store.Snapshotter),
		Executors: map[string]executor.Executor{spec.Key(): ex},
	})
	if err != nil || status != api.RunSucceeded {
		events := readLedger(t, dir)
		_, web := lastStepFinished(t, events, "web")
		_, use := lastStepFinished(t, events, "use")
		t.Fatalf("Run = %s, %v; want succeeded (web: %s exit %d %q; use: %s %q)",
			status, err, web.State, web.ExitCode, web.Error, use.State, use.Error)
	}
}

// A service retried through step.retry runs as a service again: it becomes
// ready on its new attempt, publishes, and a consumer still waiting runs
// against it. The consumer waits behind gate (listed first in Needs) so the
// first attempt's failure does not skip it before the retry lands.
func TestARetriedServiceRunsAsAService(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "tried")
	db := serveNode(t, "db", freePort(t), false)
	self := db.Cmd[0]
	db.Cmd = []string{"sh", "-c", fmt.Sprintf(
		`if [ -f %s ]; then exec %s '-test.run=^TestHelperServe$'; fi; touch %s; exit 1`, marker, self, marker)}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		db,
		{ID: "gate", Kind: "exec", Cmd: []string{"sleep", "2"}},
		{ID: "use", Kind: "exec", Needs: []string{"gate", "db"}, Cmd: []string{"true"}},
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
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RETRYSVC",
	})

	waitForEvent(t, dbFinished, func(e api.Event) bool { return e.Attempt == 1 })
	resp := sendSettled(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "db"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not end within 5s of the retry")
	}

	events := readLedger(t, dir)
	ready := -1
	for i, e := range events {
		if e.Type == api.ServiceReady && e.Step == "db" && e.Attempt == 2 {
			ready = i
		}
	}
	if ready < 0 {
		t.Fatal("no service.ready for db attempt 2: the retry did not run it as a service")
	}
	if started := indexOf(events, api.StepStarted, "use"); started < ready {
		t.Errorf("use started at %d, before db's service.ready at %d", started, ready)
	}
	states := foldStates(t, dir)
	if states["use"] != api.StateSucceeded {
		t.Errorf("use = %s, want succeeded", states["use"])
	}
	// Recovered, not succeeded: it failed once before it passed, the rule a
	// manually retried step follows too.
	if states["db"] != api.StateRecovered {
		t.Errorf("db = %s, want recovered once its consumer settled", states["db"])
	}
	if indexOf(events, api.StepFinished, "use") > lastIndex(events, api.StepFinished, "db") {
		t.Error("db's last step.finished precedes its consumer's: it must outlive the consumer")
	}
}

// A consumer retried after its service stopped (it was the last consumer,
// and it failed) restarts the service: a second service.ready lands before
// the consumer's second step.started, and the consumer runs against it.
// keep holds the run open long enough to send the retry.
func TestRetryingAConsumerRestartsItsStoppedService(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "tried")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", freePort(t), false),
		{ID: "keep", Kind: "exec", Cmd: []string{"sleep", "4"}},
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sh", "-c", fmt.Sprintf(
			`if [ -f %s ] && [ -n "$SENRO_SERVICE_DB_HOST" ]; then exit 0; fi; touch %s; exit 1`,
			marker, marker)}},
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
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01REVIVESVC",
	})

	// db stops once use, its only consumer, has failed.
	waitForEvent(t, dbFinished, func(e api.Event) bool { return e.Attempt == 1 })
	resp := sendSettled(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "use"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run did not end within 10s of the retry")
	}

	events := readLedger(t, dir)
	ready, started := -1, -1
	for i, e := range events {
		if e.Type == api.ServiceReady && e.Step == "db" && e.Attempt == 2 {
			ready = i
		}
		if e.Type == api.StepStarted && e.Step == "use" && e.Attempt == 2 {
			started = i
		}
	}
	if ready < 0 {
		t.Fatal("no second service.ready for db: the retry did not restart it")
	}
	if started < ready {
		t.Errorf("use's second step.started at %d, before db's second service.ready at %d", started, ready)
	}
	states := foldStates(t, dir)
	// Recovered: a manual retry of a failed step that then passes.
	if states["use"] != api.StateRecovered {
		t.Errorf("use = %s, want recovered", states["use"])
	}
	if states["db"] != api.StateSucceeded {
		t.Errorf("db = %s, want succeeded once its consumer settled again", states["db"])
	}
}

// A shell on a consumer held after failing is told where its services are,
// as the attempt was: the service is still up (its consumer has not
// settled), and poking it by hand is the point of standing there.
func TestAShellOnAHeldConsumerSeesItsServices(t *testing.T) {
	port := freePort(t)
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", port, false),
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sh", "-c", "exit 3"}},
	}}
	dir := t.TempDir()
	out, s, events := breakOnFailureRunIn(t, context.Background(), dir, openStore(t, dir), p, true)
	waitForEvent(t, events, func(e api.Event) bool { return e.Type == api.BreakpointHit && e.Step == "use" })

	sn := open(t, s, sink.ShellRequest{ID: "sh1", ClientID: "c1", Step: "use",
		Cmd: []string{"sh", "-c", "echo $SENRO_SERVICE_DB_HOST:$SENRO_SERVICE_DB_PORT_" + strconv.Itoa(port)}})
	if resp := sn.wait(t, 5*time.Second); !resp.OK || resp.ExitCode != 0 {
		t.Fatalf("shell response = %+v, stderr = %q", resp, sn.stderr.String())
	}
	if got, want := strings.TrimSpace(sn.stdout.String()), "127.0.0.1:"+strconv.Itoa(port); got != want {
		t.Errorf("the shell saw %q, want %q", got, want)
	}
	if resp := send(t, s.controlSink, sink.ControlRequest{ID: "c", Op: api.OpBreakpointClear,
		ClientID: "tester", Args: map[string]string{"step": "use"}}); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	waitRun(t, out)
}

// lastIndex is indexOf from the end: a retried step has one step.finished
// per execution.
func lastIndex(events []api.Event, typ api.Type, step string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == typ && events[i].Step == step {
			return i
		}
	}
	return -1
}

// listening reports whether anything accepts a connection on port.
func listening(port int) bool {
	c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// A service that ignores SIGTERM and declares a grace far longer than
// teardown's is still killed inside teardown's window once the run is
// cancelled, so Run never returns with the process left running.
func TestACancelledRunCutsALongStopGraceShort(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	db := serveNode(t, "db", port, false)
	db.Env = append(db.Env, "SENRO_TEST_SERVE_STUBBORN=1")
	db.Service.StopGraceMS = 60000
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		db,
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sleep", "30"}},
	}}
	// CleanupGrace 2s: teardown waits 1s for in-flight work, and the stop is
	// cut to a kill after 500ms.
	rec, cancel, done := startRun(t, dir, p, func(o *engine.Options) { o.CleanupGrace = 2 * time.Second })
	awaitEvent(t, rec, api.ServiceReady, "db", 5*time.Second)

	start := time.Now()
	cancel()
	awaitRun(t, done, 5*time.Second)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Run took %v after cancel with a 60s grace: the stop must be clamped to teardown's", elapsed)
	}
	if listening(port) {
		t.Error("the service still accepts connections after Run returned: an orphan")
	}
}

// A run cancelled while a service is still being probed settles it
// cancelled, never ready, and the process is gone. It listens on one
// declared port while the probe watches the other, so it is never ready.
func TestCancelDuringTheProbeSettlesCancelled(t *testing.T) {
	dir := t.TempDir()
	served, probed := freePort(t), freePort(t)
	db := serveNode(t, "db", served, false)
	db.Service.Ports = []int{served, probed}
	db.Service.Probe.Port = probed
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{db}}
	rec, cancel, done := startRun(t, dir, p, nil)
	awaitEvent(t, rec, api.StepStarted, "db", 5*time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for !listening(served) {
		if time.Now().After(deadline) {
			t.Fatal("the service never started listening")
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	awaitRun(t, done, 5*time.Second)
	if st := foldStates(t, dir)["db"]; st != api.StateCancelled {
		t.Errorf("db = %s, want cancelled", st)
	}
	if indexOf(readLedger(t, dir), api.ServiceReady, "db") >= 0 {
		t.Error("service.ready for a service that never passed its probe")
	}
	if listening(served) {
		t.Error("the service still accepts connections after Run returned")
	}
}

// recordingExecutor is countingExecutor that keeps every SandboxSpec it was
// given and every command its sandboxes ran.
type recordingExecutor struct {
	countingExecutor
	mu    sync.Mutex
	specs []executor.SandboxSpec
	cmds  []executor.Cmd
}

func (r *recordingExecutor) Sandbox(_ context.Context, spec executor.SandboxSpec) (executor.Sandbox, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.specs = append(r.specs, spec)
	return &recordingSandbox{countingSandbox: countingSandbox{owner: &r.countingExecutor}, owner: r}, nil
}

type recordingSandbox struct {
	countingSandbox
	owner *recordingExecutor
}

func (s *recordingSandbox) Run(_ context.Context, c executor.Cmd, _, _ io.Writer) (int, error) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	s.owner.cmds = append(s.owner.cmds, c)
	return 0, nil
}

// A consumer in a container reaches a local service through the gateway:
// its sandbox resolves host.docker.internal, and its environment names that
// host with the published port.
func TestAContainerConsumerGetsTheGatewayHost(t *testing.T) {
	port := freePort(t)
	spec := &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"}
	boxed := &recordingExecutor{countingExecutor: countingExecutor{class: "c"}}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		serveNode(t, "db", port, false),
		{ID: "use", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"true"}, Executor: spec},
	}}
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "01GATEWAY", Sink: sink.Nop(), MaxParallel: 4,
		Executor:  localexec.New(dir, nil),
		Executors: map[string]executor.Executor{spec.Key(): boxed},
	})
	if err != nil || status != api.RunSucceeded {
		t.Fatalf("Run = %s, %v; want succeeded", status, err)
	}
	if len(boxed.specs) != 1 || !slices.Equal(boxed.specs[0].ExtraHosts, []string{"host.docker.internal:host-gateway"}) {
		t.Fatalf("container consumer specs = %+v, want ExtraHosts [host.docker.internal:host-gateway]", boxed.specs)
	}
	env := boxed.cmds[0].Env
	for _, want := range []string{
		"SENRO_SERVICE_DB_HOST=host.docker.internal",
		fmt.Sprintf("SENRO_SERVICE_DB_PORT_%d=%d", port, port),
	} {
		if !slices.Contains(env, want) {
			t.Errorf("container consumer env %v lacks %q", env, want)
		}
	}
}

// A service that dies after it was ready fails, and a consumer that had not
// started yet is skipped rather than handed an address nothing answers on.
// quick keeps the service needed while it dies; late waits behind gate.
func TestAServiceThatDiesAfterReadySkipsAPendingConsumer(t *testing.T) {
	dir := t.TempDir()
	db := serveNode(t, "db", freePort(t), false)
	db.Env = append(db.Env, "SENRO_TEST_SERVE_EXIT_AFTER_MS=500")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		db,
		{ID: "quick", Kind: "exec", Needs: []string{"db"}, Cmd: []string{"sleep", "2"}},
		{ID: "gate", Kind: "exec", Cmd: []string{"sleep", "1.5"}},
		{ID: "late", Kind: "exec", Needs: []string{"gate", "db"}, Cmd: []string{"true"}},
	}}
	_, events, states := runPlan(t, dir, p)

	if states["db"] != api.StateFailed {
		t.Errorf("db = %s, want failed", states["db"])
	}
	if _, fin := lastStepFinished(t, events, "db"); !strings.Contains(fin.Error, "before its consumers finished") {
		t.Errorf("error = %q, want it to say it exited before its consumers finished", fin.Error)
	}
	if states["late"] != api.StateSkippedUpstreamFailed {
		t.Errorf("late = %s, want skipped_upstream_failed", states["late"])
	}
}
