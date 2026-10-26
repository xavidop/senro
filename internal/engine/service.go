package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
)

// probeInterval is how often a service's readiness probe runs. A var so a
// test can shorten it; nothing else writes it.
var probeInterval = 250 * time.Millisecond

// defaultReadyTimeout bounds readiness for a plan that declares no
// ReadyTimeoutMS. senro.Service always fills it in (service.DefaultReadyTimeout),
// so only a hand-assembled *plan.Plan reaches this; waiting forever on a
// probe that will never pass is the one outcome a deadline exists to prevent.
const defaultReadyTimeout = 60 * time.Second

// dockerHostName is the host a container consumer reaches a service by, and
// dockerHostGateway the ExtraHosts entry that makes it resolve on Linux.
// One scheme for every container consumer: the service is published on the
// coordinator's loopback, whichever executor runs it, and from inside a
// container the coordinator is the gateway.
const (
	dockerHostName    = "host.docker.internal"
	dockerHostGateway = dockerHostName + ":host-gateway"
)

// serviceTable is where every ready service in this run answers, keyed by
// step id: written by runService once the probe passes, read by
// prepareAttempt for each consumer's environment. Its own lock rather than
// oc.mu: a consumer's attempt setup reads it from a step goroutine, and
// nothing here needs to be consistent with the scheduler's maps, because a
// consumer is dispatched only after publish has returned (markReady follows
// it).
type serviceTable struct {
	mu        sync.Mutex
	endpoints map[string][]api.Endpoint
}

// publish records step's endpoints. A rerun's new execution replaces the
// previous one's, which is the address its consumers must now be given.
func (t *serviceTable) publish(step string, eps []api.Endpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.endpoints == nil {
		t.endpoints = make(map[string][]api.Endpoint)
	}
	t.endpoints[step] = eps
}

// envFor is the SENRO_SERVICE_* environment for consumer: for each service
// it needs DIRECTLY, the host and one variable per declared port, carrying
// the PUBLISHED host port. A transitive service is not included: a step
// that wants to reach one names it in Needs, which is also what keeps the
// service up until that step is done.
//
// The host depends on the CONSUMER's executor, not the service's: every
// service is published on the coordinator, so a process on the coordinator
// reaches it as published, and one in a container reaches the coordinator
// as host.docker.internal on the same port.
//
// In Needs order, so the environment is deterministic.
func (t *serviceTable) envFor(consumer *plan.Node) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var env []string
	for _, need := range consumer.Needs {
		eps, ok := t.endpoints[need]
		if !ok || len(eps) == 0 {
			continue
		}
		name := serviceEnvName(need)
		host, _, err := net.SplitHostPort(eps[0].Address)
		if err != nil {
			continue
		}
		if consumesFromContainer(consumer) {
			host = dockerHostName
		}
		env = append(env, name+"_HOST="+host)
		for _, ep := range eps {
			_, port, err := net.SplitHostPort(ep.Address)
			if err != nil {
				continue
			}
			env = append(env, name+"_PORT_"+strconv.Itoa(ep.Port)+"="+port)
		}
	}
	return env
}

// serviceEnv is envFor over this run's table.
func (rc *runCore) serviceEnv(consumer *plan.Node) []string {
	return rc.services.envFor(consumer)
}

// executorKind is n's executor kind as plan.Validate reads it: no spec, or
// an empty Kind, is local. Not ExecutorKey, which carries the image too.
func executorKind(n *plan.Node) string {
	if n.Executor == nil || n.Executor.Kind == "" {
		return plan.ExecutorLocal
	}
	return n.Executor.Kind
}

// consumesFromContainer reports whether n runs in a container, and so
// reaches a service through the gateway rather than on loopback.
func consumesFromContainer(n *plan.Node) bool {
	return executorKind(n) == plan.ExecutorContainer
}

// serviceEnvName is the variable prefix for a service step: SENRO_SERVICE_
// and the id upper-cased with every rune that is not an ASCII letter or
// digit replaced by "_" ("infra/db" is SENRO_SERVICE_INFRA_DB). Two ids that
// collide under this mapping get the same prefix, and the later need wins;
// an author who names two services "db-x" and "db_x" has the fix in hand.
func serviceEnvName(stepID string) string {
	var b strings.Builder
	b.WriteString("SENRO_SERVICE_")
	for _, r := range stepID {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// probeOnce runs p against address once. nil means ready.
//
// TCP is a connection accepted and closed: the cheapest evidence a server is
// listening. HTTP is a GET answered 200..399, so a redirect to a login page
// counts as up, and a 5xx from a server still starting does not.
func probeOnce(ctx context.Context, p plan.ProbeSpec, address string) error {
	if p.Kind == plan.ServiceProbeHTTP {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+p.Path, nil)
		if err != nil {
			return err
		}
		// Redirects are not followed: the status of the first answer is the
		// verdict, and following one could probe a different server.
		client := &http.Client{
			Timeout:       2 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 399 {
			return fmt.Errorf("GET %s answered %d", p.Path, resp.StatusCode)
		}
		return nil
	}
	d := net.Dialer{Timeout: time.Second}
	c, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	return c.Close()
}

// readyTimeout is how long n may take to pass its probe: ReadyTimeoutMS, or
// the node's TimeoutMS when that is shorter. On a service TimeoutMS bounds
// readiness, never the lifetime, which the run decides (see plan.ServiceSpec).
func readyTimeout(n *plan.Node) time.Duration {
	d := time.Duration(n.Service.ReadyTimeoutMS) * time.Millisecond
	if d <= 0 {
		d = defaultReadyTimeout
	}
	if t := time.Duration(n.TimeoutMS) * time.Millisecond; t > 0 && t < d {
		d = t
	}
	return d
}

// runService runs a service step to a terminal state: start it, probe it
// until ready, publish where it answers, then keep it up until stop closes
// or the run is cancelled, and stop it with its grace.
//
// It is the service's whole lifetime, so it is not runStep: no cache (a
// service is never Pure), no retry loop (never Retry), no handlers and no
// generator (Validate refuses all of them), one attempt. It shares
// runAttempt's setup through prepareAttempt and settles through finishStep,
// so the sandbox, the environment, the log plumbing and "exactly one
// step.finished" are the same code a step uses.
//
// stop is closed by the scheduler once every consumer has settled (never
// under Options.HoldServices). release gives back the MaxParallel slot the
// caller acquired, and runService owns calling it, once, however it returns:
// a ready service is waiting, not working, and holding the slot for its
// whole life would starve its own consumers under a small MaxParallel (with
// MaxParallel 1, deadlock them). wake asks the scheduler for a pass, which
// is how a ready service's dependents get dispatched.
//
// retried says this execution is a step.retry of a failed one. A clean stop
// then settles recovered, not succeeded, by the rule runStep and
// handleStepRetry apply to a step that failed before it passed: collapsing
// the two is how a flaky dependency stays invisible in a run's rolled-up
// status. A rerun_from execution is not a retry, as it is not for a step.
func (rc *runCore) runService(
	ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet,
	stop <-chan struct{}, release, wake func(), retried bool,
) api.State {
	start := time.Now()
	attempt := rc.oc.attemptOf(n.ID) + 1

	var once sync.Once
	giveBack := func() { once.Do(release) }
	defer giveBack()

	res := rc.serviceAttempt(ctx, n, opts, logs, attempt, start, stop, giveBack, wake)
	// After serviceAttempt has returned: its writer closes have run, so the
	// log files are final (see archiveAttempt).
	rc.archiveAttempt(logs, n.ID, attempt)
	finalState := res.state
	if finalState == api.StateSucceeded && retried {
		finalState = api.StateRecovered
	}
	state := rc.finishStep(n, start, attempt, finalState, res.exitCode, errText(res), false)
	// After finishStep, as runStep does: an analyzer's proposal refers to a
	// failure that must already be in the ledger. A service that died before
	// it was ready, or before its consumers finished, is a failure somebody
	// will want explained exactly like a step's.
	if finalState.Failed() {
		rc.offerAnalysis(n, attempt, finalState, res, time.Since(start), opts.Pipeline)
	}
	return state
}

// serviceAttempt is runService's one attempt, owning its sandbox and writers
// the way runAttempt owns a step's: everything it opened is closed by the
// time it returns.
func (rc *runCore) serviceAttempt(
	ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, attempt int,
	start time.Time, stop <-chan struct{}, release, wake func(),
) attemptResult {
	s, err := rc.prepareAttempt(ctx, ctx, n, opts, logs, attempt)
	if err != nil {
		return serviceFailure(ctx, err)
	}
	defer s.close()

	// Both capabilities, asserted up front: a sandbox that could start the
	// process but not say where it listens would leave its consumers with no
	// address. plan.Validate already refuses a service on any other
	// executor; this is the belt for a hand-assembled plan.
	runner, okRun := s.sb.(executor.ServiceRunner)
	addr, okAddr := s.sb.(executor.Addressable)
	if !okRun || !okAddr {
		return attemptResult{state: api.StateFailed, err: fmt.Errorf(
			"engine: step %q is a service on the %s executor, which cannot run one: "+
				"services run on the local and container executors", n.ID, executorKind(n))}
	}

	svc, err := runner.StartService(ctx, executor.Cmd{
		Args: n.Cmd, Env: s.cmdEnv, Dir: cmdDirFor(n.WorkDir, s.mounts),
	}, s.stdout, s.stderr)
	if err != nil {
		return serviceFailure(ctx, err)
	}
	grace := time.Duration(n.Service.StopGraceMS) * time.Millisecond

	// Wait is called once, here, and its answer raced against everything
	// else below; buffered so the goroutine never outlives a Stop that
	// already reaped the process.
	type exited struct {
		code int
		err  error
	}
	waited := make(chan exited, 1)
	go func() {
		code, err := svc.Wait()
		waited <- exited{code, err}
	}()

	// stopNow ends the process and drains its output, WithoutCancel because
	// a cancelled run is exactly when a service must still be given its
	// grace.
	//
	// Given its grace, but not past teardown's: once the run is cancelled,
	// teardown waits only CleanupGrace/2 for in-flight work before abandoning
	// it, and a service still inside a long StopGrace at that moment would be
	// left running after Run returned, an orphan holding its port. So the stop
	// is cut to a kill stopBudget after the cancellation, whether the stop
	// began before it (a scheduler stop with a long grace) or because of it.
	// Half of teardown's wait, so the kill and the reap that follows land
	// inside it rather than racing the abandonment.
	stopBudget := opts.cleanupGrace() / 4
	stopNow := func() error {
		sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		go func() {
			select {
			case <-ctx.Done():
				t := time.NewTimer(stopBudget)
				defer t.Stop()
				select {
				case <-t.C:
					cancel()
				case <-sctx.Done():
				}
			case <-sctx.Done():
			}
		}()
		err := svc.Stop(sctx, grace)
		if ferr := rc.flushAttempt(n, attempt, s); ferr != nil && err == nil {
			err = ferr
		}
		return err
	}
	// exitedEarly is the verdict for a process that ended on its own.
	exitedEarly := func(e exited, before string) attemptResult {
		_ = rc.flushAttempt(n, attempt, s)
		msg := fmt.Sprintf("exited with status %d before %s", e.code, before)
		if e.err != nil {
			msg += ": " + e.err.Error()
		}
		return attemptResult{state: api.StateFailed, exitCode: e.code, err: errors.New(msg), logTail: s.tail.String()}
	}

	// The address the probe dials, asked for after StartService because a
	// container has no published port before it starts.
	probeAddr, err := addr.HostAddress(ctx, n.Service.Probe.Port)
	if err != nil {
		_ = stopNow()
		return serviceFailure(ctx, err)
	}

	// Readiness: probe now, then every probeInterval, until it passes, the
	// process ends, the deadline passes or the run is cancelled.
	limit := readyTimeout(n)
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	tick := time.NewTicker(probeInterval)
	defer tick.Stop()
	for probeOnce(ctx, n.Service.Probe, probeAddr) != nil {
		select {
		case e := <-waited:
			return exitedEarly(e, "it was ready")
		case <-deadline.C:
			_ = stopNow()
			return attemptResult{state: api.StateFailed, err: fmt.Errorf("not ready after %v", limit),
				logTail: s.tail.String()}
		case <-ctx.Done():
			// Never ready, so this is an ordinary cancelled attempt: it is the
			// stop AFTER readiness that is a service's clean end.
			_ = stopNow()
			return attemptResult{state: api.StateCancelled, err: ctx.Err(), logTail: s.tail.String()}
		case <-tick.C:
		}
	}

	// Every declared port, in declared order, as published.
	eps := make([]api.Endpoint, 0, len(n.Service.Ports))
	for _, port := range n.Service.Ports {
		a, err := addr.HostAddress(ctx, port)
		if err != nil {
			_ = stopNow()
			return serviceFailure(ctx, err)
		}
		eps = append(eps, api.Endpoint{Port: port, Address: a})
	}
	rc.emit(api.Event{
		Type: api.ServiceReady, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(api.ServiceReadyBody{Endpoints: eps, Duration: time.Since(start)}),
	})
	// In this order: the endpoints are in the table before readySet can see
	// the service as ready, so no consumer is dispatched to an environment
	// missing its address; and the event precedes both, so service.ready is
	// in the ledger before any consumer's step.started.
	rc.services.publish(n.ID, eps)
	rc.oc.markReady(n.ID)
	release()
	wake()

	select {
	case <-stop:
	case <-ctx.Done():
	case e := <-waited:
		return exitedEarly(e, "its consumers finished")
	}
	// Stopped by the scheduler or by the run: the end a service is meant to
	// have, so it settles succeeded with exit 0 whatever status SIGTERM or
	// the kill produced. Only a stop that could not be carried out fails it.
	if err := stopNow(); err != nil {
		return attemptResult{state: api.StateFailed, err: fmt.Errorf("stopping: %w", err), logTail: s.tail.String()}
	}
	return attemptResult{state: api.StateSucceeded, logTail: s.tail.String()}
}

// serviceFailure classifies an error that kept a service from starting or
// being addressed: cancelled when the run was, failed otherwise, matching
// classifyAttempt's reading of an error under a done context.
func serviceFailure(ctx context.Context, err error) attemptResult {
	if ctx.Err() != nil {
		return attemptResult{state: api.StateCancelled, err: err}
	}
	return attemptResult{state: api.StateFailed, err: err}
}
