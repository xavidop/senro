package containerexec

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
)

// StartService creates the container with the sandbox's declared ports
// published, starts it and follows its output: senroexec.ServiceRunner. See
// the interface doc for the contract.
//
// The create, start and log-following sequence is Run's, and the start
// failure is classified exactly as Run classifies it, but it is written out
// again here rather than shared: Run's wait is interleaved with its
// cancellation (ctx cancelled means kill now), and a service's end is the
// opposite, a graceful Stop that ctx does not trigger. A helper covering both
// would have to move Run's select, which is the ordinary step's timeout path.
func (s *sandbox) StartService(
	ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer,
) (senroexec.RunningService, error) {
	if len(c.Args) == 0 {
		return nil, fmt.Errorf("containerexec: %w: empty command", senroexec.ErrInfra)
	}
	spec, err := s.containerSpecFor(c)
	if err != nil {
		return nil, err
	}
	for _, p := range s.spec.Ports {
		spec.Ports = append(spec.Ports, dockerd.Port{Container: p})
	}
	id, err := s.ex.cli.ContainerCreate(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	s.mu.Lock()
	s.id = id
	s.mu.Unlock()

	svc := &containerService{
		cli: s.ex.cli, id: id,
		exited: make(chan struct{}), done: make(chan struct{}),
	}
	if err := s.ex.cli.ContainerStart(ctx, id); err != nil {
		// Run's rule: a program the image lacks, or cannot execute, is the
		// pipeline's 127 or 126, reported as a service that has already
		// exited so the engine sees a step's verdict for the same typo.
		if code := dockerd.ClassifyStartFailure(err); code != dockerd.StartFailureNone {
			_, _ = fmt.Fprintf(stderr, "senro: %s\n", err)
			svc.code = int(code)
			close(svc.exited)
			close(svc.done)
			return svc, nil
		}
		return nil, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}

	// Run's background context, for Run's reason: the stream and the wait
	// must outlive the start's ctx, since the service outlives it.
	bg := context.WithoutCancel(ctx)
	logCtx, stopLogs := context.WithCancel(bg)
	logsDone := make(chan struct{})
	go func() {
		_ = s.ex.cli.ContainerLogs(logCtx, id, stdout, stderr)
		close(logsDone)
	}()
	go func() {
		defer close(svc.done)
		defer stopLogs()
		svc.code, svc.err = s.ex.cli.ContainerWait(bg, id)
		close(svc.exited)
		// Drained exactly as Run drains: bounded, and cancelled rather than
		// abandoned on expiry, so nothing writes to stdout or stderr after
		// Wait has returned.
		select {
		case <-logsDone:
		case <-time.After(logDrainGrace):
			stopLogs()
			<-logsDone
		}
	}()
	return svc, nil
}

// containerService is a started service container. code and err are written
// once, before exited is closed. done closes after the log stream has
// drained, which is what Wait waits for.
type containerService struct {
	cli    *dockerd.Client
	id     string
	exited chan struct{}
	done   chan struct{}
	code   int
	err    error
}

// Wait implements senroexec.RunningService. The status code is the daemon's,
// which already reports a signal death the shell's way (137 for SIGKILL).
func (c *containerService) Wait() (int, error) {
	<-c.done
	if c.err != nil {
		return c.code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, c.err)
	}
	return c.code, nil
}

// Stop implements senroexec.RunningService through `docker stop`: the daemon
// sends the image's stop signal, waits out grace and kills. A cancelled ctx
// abandons that request and kills at once, on a context that survives the
// cancellation, so a container is never left running because its caller gave
// up.
//
// It returns once ContainerWait has seen the container exit, so a Wait in
// flight is already on its way out.
func (c *containerService) Stop(ctx context.Context, grace time.Duration) error {
	select {
	case <-c.exited:
		return nil
	default:
	}
	err := c.cli.ContainerStop(ctx, c.id, grace)
	if ctx.Err() != nil {
		err = c.cli.ContainerKill(context.WithoutCancel(ctx), c.id)
	}
	if err != nil {
		return fmt.Errorf("containerexec: %w: stopping service container %s: %w",
			senroexec.ErrInfra, c.id, err)
	}
	<-c.exited
	return nil
}

// HostAddress implements senroexec.Addressable: where on the coordinator's
// loopback the daemon published a declared port.
//
// Not published is an error naming the port rather than an empty address: a
// caller asking for a port the step never declared has made a mistake, and
// one asking before StartService has nothing to ask about yet.
func (s *sandbox) HostAddress(ctx context.Context, port int) (string, error) {
	id := s.ContainerID()
	if id == "" {
		return "", fmt.Errorf(
			"containerexec: step %q has no container yet, so port %d is not published",
			s.spec.StepID, port)
	}
	addr, ok, err := s.ex.cli.ContainerHostAddress(ctx, id, port)
	if err != nil {
		return "", fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	if !ok {
		return "", fmt.Errorf(
			"containerexec: port %d of step %q is not published; a service publishes only the ports it declares",
			port, s.spec.StepID)
	}
	return addr, nil
}

// The engine reaches both through interface assertions, so losing either
// silently would turn every container service into a refusal with nothing
// failing at build time.
var (
	_ senroexec.ServiceRunner = (*sandbox)(nil)
	_ senroexec.Addressable   = (*sandbox)(nil)
)
