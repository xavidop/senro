//go:build unix

package localexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// StartService starts a long-running command: senroexec.ServiceRunner. See
// the interface doc for the contract.
//
// The command is built exactly as Run builds one (commandDir, the explicit
// environment with the host PATH, waitDelay) so a service starts in the same
// place and sees the same environment a step would. Two things differ, both
// on purpose:
//
//   - exec.Command, not exec.CommandContext. ctx bounds the start only; a
//     service's end is Stop, with a grace, and tying the process to ctx would
//     turn every run cancellation into the immediate kill Stop exists to
//     avoid.
//   - Its own process group (Setpgid). A service is often a wrapper that forks
//     the real server (a shell script, `npm start`, a database's launcher),
//     and a signal to the wrapper's pid alone would leave the server running
//     with the port still bound and the pipes still held. Signalling the group
//     stops the whole tree. It also keeps an operator's ^C on the coordinator's
//     terminal from reaching the service directly: the engine stops it, with
//     the grace, rather than the tty killing it out from under its consumers.
func (s *sandbox) StartService(
	ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer,
) (senroexec.RunningService, error) {
	if len(c.Args) == 0 {
		return nil, fmt.Errorf("localexec: %w: empty command", senroexec.ErrInfra)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	if err := portsFree(s.spec.Ports); err != nil {
		return nil, err
	}

	cmd := exec.Command(c.Args[0], c.Args[1:]...)
	cmd.Dir = s.commandDir(c)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = envWithDefaultPATH(c.Env)
	cmd.WaitDelay = waitDelay
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	svc := &localService{done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		// classifyRunError draws Run's line for a start failure too: a
		// missing or unexecutable program is the pipeline's 127 or 126 with a
		// nil error, anything else is infrastructure. The former is reported
		// as a service that has already exited, so the engine sees the same
		// verdict a step with the same typo would get.
		exit, err := s.classifyRunError(ctx, cmd, err)
		if err != nil {
			return nil, err
		}
		svc.exit = exit
		close(svc.done)
		return svc, nil
	}
	svc.pgid = cmd.Process.Pid
	go func() {
		svc.exit, svc.err = serviceVerdict(cmd, cmd.Wait())
		close(svc.done)
	}()
	return svc, nil
}

// portsFree refuses a service whose declared port something on this host
// already holds.
//
// A local service binds the declared port itself, and the probe dials it on
// the loopback, so whatever already owns the port (a developer's own
// database, a previous run's orphan) would pass the probe and be handed to
// the consumers as this service, while the real one fails to bind and dies
// in a log nobody reads.
//
// Three checks, because no one of them sees every squatter. A bind on
// 127.0.0.1 catches a loopback listener. On macOS that bind succeeds while
// another socket holds the wildcard (0.0.0.0 or [::]) on the same port, so a
// wildcard bind is tried too. And a dial on 127.0.0.1 catches whatever
// answers there that neither bind noticed (a listener bound with
// SO_REUSEPORT, a port forwarder), bounded by portProbeTimeout so a port a
// firewall drops costs a fraction of a second rather than a connect timeout.
// The window between these checks and the service's own bind is a race only
// another process starting at that instant can lose.
//
// Not ErrInfra: the host's state is the pipeline's to fix, so the service
// settles failed with this message, like a typo in its command.
func portsFree(ports []int) error {
	for _, p := range ports {
		port := strconv.Itoa(p)
		for _, host := range []string{"127.0.0.1", ""} {
			ln, err := net.Listen("tcp", net.JoinHostPort(host, port))
			if err != nil {
				return fmt.Errorf("localexec: port %d is already in use", p)
			}
			_ = ln.Close()
		}
		if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), portProbeTimeout); err == nil {
			_ = c.Close()
			return fmt.Errorf("localexec: port %d is already in use", p)
		}
	}
	return nil
}

// portProbeTimeout bounds portsFree's dial. A listener on the loopback
// answers in microseconds; this only caps a port nothing answers on.
const portProbeTimeout = 100 * time.Millisecond

// serviceVerdict is the (exit, err) for a finished service process.
//
// Not classifyRunError: that one reads a cancelled context as infrastructure,
// and a service has no context to be cancelled by; its ordinary end is a
// signal Stop sent. And it reports a signal death as ExitCode's -1, where a
// service killed after its grace must read as the shell's 137, which is what
// the engine and an operator compare it against.
func serviceVerdict(cmd *exec.Cmd, err error) (int, error) {
	var ee *exec.ExitError
	if err != nil && !errors.As(err, &ee) && !errors.Is(err, exec.ErrWaitDelay) {
		// An output copy that failed: the process may have ended fine, but
		// the log did not get its bytes, which is the substrate's failure.
		return exitCode(cmd), fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	// A nil error, a non-zero exit, and ErrWaitDelay (the process ended but
	// something it left behind held the pipes past waitDelay) all mean the
	// process is gone and ProcessState holds its verdict.
	return exitCode(cmd), nil
}

// exitCode reads a finished process's status the way a shell reports it.
func exitCode(cmd *exec.Cmd) int {
	ps := cmd.ProcessState
	if ps == nil {
		return 0
	}
	if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return ps.ExitCode()
}

// localService is a started service process. exit and err are written once,
// before done is closed, and read only after it is.
type localService struct {
	pgid int
	done chan struct{}
	exit int
	err  error
}

// Wait implements senroexec.RunningService.
func (l *localService) Wait() (int, error) {
	<-l.done
	return l.exit, l.err
}

// Stop implements senroexec.RunningService: SIGTERM to the process group,
// up to grace for it to end, then SIGKILL, then Wait's own completion.
//
// It returns only once the reaping goroutine has finished, so the process is
// gone and the pipes drained (bounded by waitDelay) by the time the caller
// moves on to Close.
//
// A wrapper that already exited on its own may have left children in its
// group (a backgrounded server that did not detach): they would keep the
// port and outlive the run. So a Stop that finds the wrapper gone still
// sends the group one SIGKILL; an empty group is ESRCH, which is success.
// pgid 0 is a program that never started, and kill(0) would signal senro's
// own group.
//
// That SIGKILL goes to a group whose leader has been reaped, which is safe
// while the group still has a member: a process group id is not handed out
// again while any process in the group lives, so a survivor makes the id
// still ours. The hazard is an EMPTY group whose id the kernel has since
// given to a new group leader. kill(-pgid, 0) first rules out the empty
// group that was never reused (ESRCH, nothing to do); what is left is the
// reuse arriving between the reap and this call, which the engine keeps to
// the moment it takes to notice the exit (it stops a service that exited
// early at once), and which needs the pid space to wrap in that moment.
//
// A ctx already done skips the SIGTERM: the caller has run out of time for
// a grace, so the group is killed at once, as the kill after an expired
// grace would.
func (l *localService) Stop(ctx context.Context, grace time.Duration) error {
	select {
	case <-l.done:
		if l.pgid == 0 {
			return nil
		}
		if err := syscall.Kill(-l.pgid, 0); errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return signalGroup(l.pgid, syscall.SIGKILL)
	default:
	}
	if ctx.Err() == nil {
		if err := signalGroup(l.pgid, syscall.SIGTERM); err != nil {
			return err
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-l.done:
			return nil
		case <-timer.C:
		case <-ctx.Done():
		}
	}
	if err := signalGroup(l.pgid, syscall.SIGKILL); err != nil {
		return err
	}
	<-l.done
	return nil
}

// signalGroup signals every process in the group. ESRCH is success: the group
// emptied between the check and the signal, and the outcome Stop wants is
// already true.
func signalGroup(pgid int, sig syscall.Signal) error {
	if err := syscall.Kill(-pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("localexec: %w: signalling service process group %d: %w",
			senroexec.ErrInfra, pgid, err)
	}
	return nil
}

// The engine reaches a service through an interface assertion, so losing this
// silently would turn every local service into a refusal with nothing failing
// at build time.
var _ senroexec.ServiceRunner = (*sandbox)(nil)
