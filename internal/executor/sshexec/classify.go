package sshexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// sshFailureCode is the exit status the ssh client uses for its own
// failures: "ssh exits with the exit status of the remote command or with
// 255 if an error occurred" (ssh(1)). Every other code is unambiguously the
// remote command's; 255 means either, nothing at the process level can tell
// which, and retry.OnInfra keys off exactly this distinction.
const sshFailureCode = 255

// statusReadTimeout bounds the extra connection that resolves an ambiguous
// exit. It is short: the ordinary reason for reaching this path at all is that
// the host became unreachable, and hanging there would turn a clear
// infrastructure failure into the step's own timeout.
const statusReadTimeout = 30 * time.Second

// classify turns one ssh invocation's outcome into the (exit, err) verdict
// the Sandbox interface requires:
//
//   - The ssh binary could not be run, or the run was cancelled:
//     infrastructure, as everywhere else in this build (see
//     localexec.classifyRunError).
//   - ssh exited with anything other than 255: that IS the remote command's
//     exit status, reported with no error. An OOM-killed command arrives as
//     137, as it does on the container and k8s executors.
//   - ssh exited 255, or was killed locally so its status says nothing: the
//     ambiguous case, resolved by asking the host. The wrapper wrote the
//     real status to a file BEFORE exiting with it: present means the
//     command ran and its contents are the verdict; absent means it never
//     ran, which is infrastructure.
//
// The extra connection is paid only in the ambiguous case. The alternatives
// were all worse: a sentinel exit code of senro's own collides with a real
// command's, and a marker on stdout or stderr corrupts streams the step
// owns.
func (s *sandbox) classify(ctx context.Context, res result) (int, error) {
	if res.err != nil {
		return 0, fmt.Errorf("sshexec: %w: step %q: running ssh to %s: %w",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Host, res.err)
	}
	if ctx.Err() != nil {
		// Checked before the exit code is trusted: a cancelled run kills the
		// local ssh, which arrives as an ordinary non-zero exit, and
		// trusting it would record a cancellation as a step failure. The
		// same check localexec makes, in the same place.
		return 0, fmt.Errorf("sshexec: %w: step %q: %w",
			senroexec.ErrInfra, s.spec.StepID, ctx.Err())
	}
	if !res.killed && res.exit != sshFailureCode {
		return res.exit, nil
	}

	status, found, err := s.readStatus(ctx)
	switch {
	case err != nil:
		return 0, fmt.Errorf(
			"sshexec: %w: step %q: ssh to %s %s, and the status of the step's own command could not "+
				"be read back from %s, so senro does not know whether it ran: %w",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Host, ambiguity(res), s.paths.dir, err)
	case found:
		// The command ran and recorded its own verdict, whatever ssh then
		// did; a command legitimately exiting 255 is reported as 255 with no
		// error, exactly as exit 7 would be.
		return status, nil
	default:
		return 0, fmt.Errorf(
			"sshexec: %w: step %q: ssh to %s %s and the step's command left no exit status behind, "+
				"so it did not run",
			senroexec.ErrInfra, s.spec.StepID, s.ex.spec.Host, ambiguity(res))
	}
}

// ambiguity describes why an outcome needed resolving, for the message.
func ambiguity(res result) string {
	if res.killed {
		return "was killed locally before it reported a status"
	}
	return "exited " + strconv.Itoa(sshFailureCode) + ", which is the code it uses for its own failures"
}

// readStatus opens a second connection and reads what the wrapper recorded.
// A missing file and an unreadable one are deliberately the same answer (no
// verdict recorded); a connection that failed outright is an error, because
// "the command did not run" and "senro could not find out" are different
// messages for the person reading them.
func (s *sandbox) readStatus(ctx context.Context) (int, bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusReadTimeout)
	defer cancel()

	var out bytes.Buffer
	res := s.ex.run(ctx, statusScript(s.paths), nil, &out, io.Discard)
	if res.err != nil {
		return 0, false, res.err
	}
	if res.exit != 0 {
		return 0, false, fmt.Errorf("reading the recorded exit status exited %d", res.exit)
	}
	raw, ok := parseFacts(out.String())["status"]
	if !ok {
		return 0, false, nil
	}
	code, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		// A recorded status that is not a number is not a verdict; reported
		// as "nothing was recorded", the same outcome.
		return 0, false, nil
	}
	return code, true, nil
}
