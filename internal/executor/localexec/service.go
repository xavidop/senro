//go:build unix

package localexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
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
func (l *localService) Stop(ctx context.Context, grace time.Duration) error {
	select {
	case <-l.done:
		return nil
	default:
	}
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
