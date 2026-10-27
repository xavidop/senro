//go:build unix

package localexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
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

	// The output pipes are made here, not by exec, so cmd.Wait returns the
	// moment the process is reaped instead of after the copies finish: a
	// wrapper that backgrounded its server leaves the server holding the
	// write ends, and with exec's own pipes its exit would go unseen for a
	// whole waitDelay while the server kept the port.
	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_, _ = outR.Close(), outW.Close()
		return nil, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}

	cmd := exec.Command(c.Args[0], c.Args[1:]...)
	cmd.Dir = s.commandDir(c)
	cmd.Stdout = outW
	cmd.Stderr = errW
	cmd.Env = envWithDefaultPATH(c.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	svc := &localService{done: make(chan struct{})}
	startErr := cmd.Start()
	// The child holds its own copies; ours would keep the pipes open forever.
	_, _ = outW.Close(), errW.Close()
	if startErr != nil {
		_, _ = outR.Close(), errR.Close()
		// classifyRunError draws Run's line for a start failure too: a
		// missing or unexecutable program is the pipeline's 127 or 126 with a
		// nil error, anything else is infrastructure. The former is reported
		// as a service that has already exited, so the engine sees the same
		// verdict a step with the same typo would get.
		exit, err := s.classifyRunError(ctx, cmd, startErr)
		if err != nil {
			return nil, err
		}
		svc.exit = exit
		close(svc.done)
		return svc, nil
	}
	svc.pgid = cmd.Process.Pid

	copied := make(chan error, 2)
	for _, p := range []struct {
		w io.Writer
		r *os.File
	}{{stdout, outR}, {stderr, errR}} {
		go func() {
			_, err := io.Copy(p.w, p.r)
			copied <- err
		}()
	}
	go func() {
		waitErr := cmd.Wait()
		svc.sweepUnlessStopping()
		svc.exit, svc.err = serviceVerdict(cmd, firstErr(waitErr, drainCopies(copied, outR, errR)))
		close(svc.done)
	}()
	return svc, nil
}

// drainCopies waits for both output copies, the way exec.Cmd.Wait does with
// a WaitDelay: up to waitDelay for whatever the process left behind to let
// go of the pipes, then the read ends are closed under the copies and the
// answer is exec.ErrWaitDelay. A copy that failed otherwise is its error.
func drainCopies(copied <-chan error, readers ...*os.File) error {
	var copyErr error
	drained := make(chan struct{})
	go func() {
		for range readers {
			if err := <-copied; err != nil && copyErr == nil {
				copyErr = err
			}
		}
		close(drained)
	}()
	timer := time.NewTimer(waitDelay)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		for _, r := range readers {
			_ = r.Close()
		}
		<-drained
		copyErr = exec.ErrWaitDelay
	}
	for _, r := range readers {
		_ = r.Close()
	}
	return copyErr
}

// firstErr is the first non-nil error.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
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
// before done is closed, and read only after it is. stopping and swept are
// under mu.
type localService struct {
	pgid int
	done chan struct{}
	exit int
	err  error

	mu       sync.Mutex
	stopping bool // Stop has begun: the group's end is Stop's to decide
	swept    bool // the group was killed when the process exited on its own
}

// Wait implements senroexec.RunningService.
func (l *localService) Wait() (int, error) {
	<-l.done
	return l.exit, l.err
}

// sweepUnlessStopping kills what is left of the process group once the
// process has exited on its own, unless Stop is already ending it.
//
// A wrapper that exited on its own (a `sh -c "server &"`, a launcher that
// forks and returns) may have left children in its group: they would keep
// the port and the pipes and outlive the run, and the service has failed
// regardless. Killed here, straight after the reap, rather than whenever the
// caller next calls Stop: the group's id is not handed out again while any
// member lives, so a group with survivors is still ours, and the one hazard,
// an EMPTY group whose id the kernel has since given to a new group leader,
// needs the pid space to wrap in the moment between the reap and this call.
// An empty group is ESRCH, which is success.
//
// Not while stopping: a wrapper that exits on Stop's SIGTERM may leave a
// server still shutting down inside the grace, and Stop gives it that.
func (l *localService) sweepUnlessStopping() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopping {
		return
	}
	l.swept = true
	_ = signalGroup(l.pgid, syscall.SIGKILL)
}

// Stop implements senroexec.RunningService: SIGTERM to the process group,
// up to grace for it to end, then SIGKILL, then Wait's own completion.
//
// It returns only once the reaping goroutine has finished, so the process is
// gone and the pipes drained (bounded by waitDelay) by the time the caller
// moves on to Close.
//
// The grace is the group's, not only the process's: a wrapper that exits on
// the SIGTERM may leave its server still shutting down, and the server gets
// what is left of the grace before the kill. A process that had already
// exited on its own had its group swept at that moment (see
// sweepUnlessStopping), so Stop has nothing to do. pgid 0 is a program
// that never started, and kill(0) would signal senro's own group.
//
// A ctx already done skips the SIGTERM: the caller has run out of time for
// a grace, so the group is killed at once, as the kill after an expired
// grace would.
func (l *localService) Stop(ctx context.Context, grace time.Duration) error {
	l.mu.Lock()
	l.stopping = true
	swept := l.swept
	l.mu.Unlock()
	// Swept means the process was reaped and its group killed: the id is no
	// longer ours to signal, even while done is still open behind the
	// output drain. Waiting for done keeps "returns once the process is gone
	// and the pipes drained".
	if swept {
		<-l.done
		return nil
	}
	select {
	case <-l.done:
		if l.pgid == 0 {
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
			if l.groupEmptyWithin(ctx, timer.C) {
				return nil
			}
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

// groupEmptyWithin polls the process group until it is empty (true) or
// expired or ctx ends first (false, and the caller kills it).
func (l *localService) groupEmptyWithin(ctx context.Context, expired <-chan time.Time) bool {
	tick := time.NewTicker(groupPoll)
	defer tick.Stop()
	for {
		if err := killGroup(l.pgid, 0); errors.Is(err, syscall.ESRCH) {
			return true
		}
		select {
		case <-expired:
			return false
		case <-ctx.Done():
			return false
		case <-tick.C:
		}
	}
}

// groupPoll is how often Stop looks for what a stopped wrapper left behind.
const groupPoll = 50 * time.Millisecond

// killGroup is kill(-pgid, sig): every signal a service's process group is
// sent, the probe for an empty group included, goes through it. A var so an
// internal test can count them; nothing else writes it.
var killGroup = func(pgid int, sig syscall.Signal) error { return syscall.Kill(-pgid, sig) }

// signalGroup signals every process in the group. ESRCH is success: the group
// emptied between the check and the signal, and the outcome Stop wants is
// already true.
func signalGroup(pgid int, sig syscall.Signal) error {
	if err := killGroup(pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("localexec: %w: signalling service process group %d: %w",
			senroexec.ErrInfra, pgid, err)
	}
	return nil
}

// The engine reaches a service through an interface assertion, so losing this
// silently would turn every local service into a refusal with nothing failing
// at build time.
var _ senroexec.ServiceRunner = (*sandbox)(nil)
