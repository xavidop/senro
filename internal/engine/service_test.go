//go:build unix

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// freePort asks the kernel for a port nothing is listening on, then gives it
// back for the service to bind. Racy in principle, and the race is the
// kernel's to lose: it does not hand the same ephemeral port out twice in
// quick succession.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("picking a free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

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
	_, events, states := runPlan(t, dir, p)

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
