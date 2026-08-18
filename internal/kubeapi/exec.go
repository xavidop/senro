package kubeapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// The exec subresource multiplexes several streams over one connection by
// prefixing every message with a channel byte. These are the channel numbers
// the v4 and v5 sub-protocols assign.
const (
	execStdin  = 0
	execStdout = 1
	execStderr = 2
	// execError carries a single metav1.Status document at the end of the
	// session, and it is the ONLY place the command's exit code appears. No
	// status means no verdict, which is why an interrupted exec is an error
	// here rather than an exit code of zero.
	execError = 3
	// execResize is a channel the far side READS: a JSON TermSize per
	// message, applied to the command's pseudo-terminal. It is opened only
	// for a request that asked for a tty, so writing to it otherwise reaches
	// nothing.
	execResize = 4
	// execClose is v5's addition: a message of {execClose, stream} closes one
	// stream. Closing stdin is how a command reading its input learns the
	// input has ended, and without it `tar -x` on the far side waits forever
	// for a block that is never coming.
	execClose = 255
)

// The two sub-protocols this client will speak, in the order it prefers them.
//
// v5 is v4 plus the close signal above. It has been in the apiserver and the
// kubelet since Kubernetes 1.29 and is what kubectl itself negotiates. v4 is
// accepted as a fallback because a session with no stdin needs nothing v5
// added, and reading a workspace back out is exactly such a session; a
// session WITH stdin on a v4 server is refused by Exec rather than started
// and hung.
const (
	execProtocolV5 = "v5.channel.k8s.io"
	execProtocolV4 = "v4.channel.k8s.io"
)

// execChunk is how much stdin travels in one message. 32 KiB is io.Copy's own
// buffer size and comfortably under any frame limit either end applies.
const execChunk = 32 << 10

// stdinReportGrace is how long Exec waits for the stdin pump's own error once
// the far side has finished. Long enough for a pump that already failed to be
// scheduled, short enough that an interactive session ends when its command
// does rather than when its operator next types. See Exec.
const stdinReportGrace = 250 * time.Millisecond

// ExecSpec is one command to run inside a container that is already running.
//
// Container is required rather than defaulted: a pod here always has more
// than one container once a workspace is involved, and the apiserver's own
// default ("the only container, if there is exactly one") would pick
// differently depending on the step's mounts.
type ExecSpec struct {
	Namespace string
	Pod       string
	Container string
	Command   []string

	// Stdin is the command's input, or nil for a command with none. It is
	// read to EOF and then CLOSED on the far side, so a command that consumes
	// its input can finish.
	Stdin io.Reader
	// Stdout and Stderr receive the command's two streams, kept apart. This
	// is where exec differs from the pod log endpoint, which merges them.
	// A nil writer discards.
	Stdout io.Writer
	Stderr io.Writer

	// TTY runs the command on a pseudo-terminal the container runtime
	// allocates, which is one device: everything the command writes arrives
	// on Stdout, Stderr is never written, and the far side echoes what Stdin
	// carries. It also changes what ends a command, since a terminal has no
	// EOF: closing Stdin does not end a shell, and the VEOF byte is what
	// does (k8sexec's RunTerminal sends it).
	//
	// The container does not have to have been created with a tty of its
	// own: an exec allocates its own, which is why `kubectl exec -it` works
	// against any pod.
	TTY bool
	// Resize carries the terminal's size, the FIRST value included: the
	// subresource has no parameter for an initial size, so a caller that
	// wants one sends it as the first size on this channel. Read until it
	// closes; a nil channel means the size never changes. Ignored without
	// TTY, because the far side opens the resize channel only for a
	// terminal.
	Resize <-chan TermSize
}

// TermSize is a terminal's dimensions as the resize channel carries them: a
// JSON object with capitalised keys, which is the shape client-go sends and
// the only shape the far side decodes.
type TermSize struct {
	Width  uint16 `json:"Width"`
	Height uint16 `json:"Height"`
}

// Exec runs a command in a container of a running pod and reports the
// command's exit code.
//
// The split is executor.Sandbox.Run's and load-bearing for the same reason:
// exit is the command's verdict, err is a failure of the channel, and a
// caller must never mistake a dropped connection for `tar` exiting 2.
//
// An interrupted session is always an error: the exit code arrives in a
// status document on the error channel after the command finishes, and a
// stream that ends without one returns an error rather than exit 0. The
// workspace transfer stands on that: a `tar -c` whose connection died
// halfway is a parseable prefix of a tarball, distinguished from a complete
// one only by the missing status.
//
// The pod does not have to be Running: an exec routes by assigned node, not
// phase, so a Pending pod's init container is reachable, which is what lets
// a workspace land before the step's own process starts.
//
// With TTY the command runs on a pseudo-terminal instead of on pipes, and
// the session gains a fifth channel the client writes window sizes to. That
// is what lets `senro shell --tty` stand in a pod; see k8sexec's terminal.go.
func (c *Client) Exec(ctx context.Context, s ExecSpec) (int, error) {
	if s.Container == "" {
		return 0, errors.New("kubeapi: exec with no container name")
	}
	if len(s.Command) == 0 {
		return 0, errors.New("kubeapi: exec with no command")
	}
	q := url.Values{}
	q.Set("container", s.Container)
	for _, arg := range s.Command {
		q.Add("command", arg)
	}
	if s.Stdin != nil {
		q.Set("stdin", "true")
	}
	q.Set("stdout", "true")
	if s.TTY {
		// stderr is not requested at all, rather than requested and ignored:
		// a pty merges the two streams into the one device, and the
		// apiserver drops a stderr request for a tty session anyway.
		q.Set("tty", "true")
	} else {
		q.Set("stderr", "true")
	}
	path := podPath(s.Namespace, s.Pod) + "/exec?" + q.Encode()

	conn, err := c.dialWS(ctx, path, []string{execProtocolV5, execProtocolV4})
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	if s.Stdin != nil && conn.sub != execProtocolV5 {
		return 0, fmt.Errorf(
			"kubeapi: %s negotiated the %q exec sub-protocol, and senro needs %q to send a "+
				"command its input: only v5 can signal the end of stdin, and a command reading "+
				"input that never ends would hang instead of failing. v5 has been in the "+
				"apiserver and the kubelet since Kubernetes 1.29",
			c.server, conn.sub, execProtocolV5)
	}

	// Both pumps run alongside the read loop rather than before it, because
	// the far side may write to stdout while it is still reading stdin, and a
	// client that sent everything first would deadlock against a command
	// whose output nobody was draining. The resize pump has the same shape
	// for the same reason: a window changes while the command is writing.
	//
	// Deferred AFTER the connection's own close, so it runs BEFORE it: the
	// pump stops writing frames and only then does the connection go.
	sessionOver := make(chan struct{})
	defer close(sessionOver)
	if s.TTY && s.Resize != nil {
		go pumpResize(conn, s.Resize, sessionOver)
	}

	stdinDone := make(chan error, 1)
	if s.Stdin != nil {
		go func() { stdinDone <- pumpStdin(conn, s.Stdin) }()
	}

	raw, readErr := readExecStreams(conn, s)

	// Closed before collecting the pump's verdict, so a pump still writing
	// into a connection whose reader has given up fails immediately instead
	// of blocking this call for as long as the far side is willing to buffer.
	//
	// The pump is then given a grace rather than waited for: its source may
	// be a person who is not typing, and this call must end when the far
	// side does rather than when somebody presses a key. Nothing is lost,
	// because a source that FAILED has already reported here: its failure is
	// what closed the far side's stdin and ended the command a round trip
	// ago, and the grace covers only the scheduling of that send. See
	// TestAStdinSourceThatFailsEndsTheCommandRatherThanHangingIt, which pins
	// the error still arriving, and executor.Interactive, which puts closing
	// a session's stdin on the caller.
	_ = conn.Close()
	var writeErr error
	if s.Stdin != nil {
		select {
		case writeErr = <-stdinDone:
		case <-time.After(stdinReportGrace):
		}
	}

	if ctx.Err() != nil {
		return 0, fmt.Errorf("kubeapi: exec in %s/%s (%s): %w",
			s.Namespace, s.Pod, s.Container, ctx.Err())
	}
	// The write half first when both failed. A command that went away takes
	// the pump's writes down with it, so the read side's account of WHY (a
	// status document, or the fact that none arrived) is the one that
	// explains the pair, and it is reported second so it is the last thing
	// read.
	if err := errors.Join(writeErr, readErr); err != nil {
		return 0, fmt.Errorf("kubeapi: exec in %s/%s (%s): %w",
			s.Namespace, s.Pod, s.Container, err)
	}
	exit, err := execExit(raw)
	if err != nil {
		return 0, fmt.Errorf("kubeapi: exec in %s/%s (%s): %w",
			s.Namespace, s.Pod, s.Container, err)
	}
	return exit, nil
}

// pumpStdin sends r on the stdin channel and then closes that stream.
//
// Closed on EVERY way out, including a read that failed halfway: a far-side
// command reads until its input ends, and without the close it waits for a
// block that never comes, failing as a timeout instead of the real cause
// (TestAStdinSourceThatFailsEndsTheCommandRatherThanHangingIt). The close
// SIGNAL, not a closed connection: dropping the connection would also end
// stdout and the status document, the only proof the command finished.
func pumpStdin(conn *wsConn, r io.Reader) error {
	// One buffer, with the channel byte already in place, so each chunk is
	// one frame and one copy rather than two.
	frame := make([]byte, execChunk+1)
	frame[0] = execStdin
	var readErr error
	for {
		n, err := r.Read(frame[1:])
		if n > 0 {
			if werr := conn.writeFrame(opBinary, frame[:n+1]); werr != nil {
				// No close signal on this path, and deliberately none: the
				// write that just failed WAS the channel, so there is nothing
				// left to send it on. The read loop ends with the connection.
				return werr
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = fmt.Errorf("reading the data to send: %w", err)
			}
			break
		}
	}
	closeErr := conn.writeFrame(opBinary, []byte{execClose, execStdin})
	// The reader's failure first: it is the cause, and the truncated input the
	// far side is about to complain about is its consequence.
	return errors.Join(readErr, closeErr)
}

// pumpResize sends every window size on the resize channel until the session
// ends.
//
// Failures are dropped rather than reported: a size is advisory, and ending
// a session over a cosmetic problem would be the worse trade (containerexec's
// pump of the same name makes it the same way). The done channel is what
// stops a pump whose caller has already returned, since the size channel
// belongs to that caller and may never close.
func pumpResize(conn *wsConn, in <-chan TermSize, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case size, ok := <-in:
			if !ok {
				return
			}
			b, err := json.Marshal(size)
			if err != nil {
				return
			}
			if err := conn.writeFrame(opBinary, append([]byte{execResize}, b...)); err != nil {
				return
			}
		}
	}
}

// readExecStreams drains the connection until the far side is done, routing
// each message by its channel byte, and returns whatever arrived on the error
// channel.
func readExecStreams(conn *wsConn, s ExecSpec) ([]byte, error) {
	stdout, stderr := s.Stdout, s.Stderr
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	var status []byte
	for {
		msg, err := conn.readMessage()
		if errors.Is(err, io.EOF) {
			return status, nil
		}
		if err != nil {
			return status, err
		}
		if len(msg) == 0 {
			// A message with no channel byte at all. The apiserver does not
			// send one; ignoring it is cheaper than a rule about it.
			continue
		}
		channel, data := msg[0], msg[1:]
		if len(data) == 0 {
			continue
		}
		var werr error
		switch channel {
		case execStdout:
			_, werr = stdout.Write(data)
		case execStderr:
			_, werr = stderr.Write(data)
		case execError:
			// Copied rather than retained: readMessage reuses its buffer.
			status = append(status, data...)
		}
		if werr != nil {
			return status, fmt.Errorf("writing the command's output: %w", werr)
		}
	}
}

// execStatus is the metav1.Status document the error channel carries. Only
// the fields that decide an exit code are here, in the spelling the wire
// uses: a StatusCause's type is serialized as "reason".
type execStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Details struct {
		Causes []struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"causes"`
	} `json:"details"`
}

// execExit turns that document into an exit code, or into an error when it
// describes something other than a command that ran.
func execExit(raw []byte) (int, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return 0, errors.New(
			"the stream ended before the command reported its status, so senro does not know " +
				"whether it finished; whatever it wrote must be treated as incomplete")
	}
	var st execStatus
	if err := json.Unmarshal(raw, &st); err != nil {
		return 0, fmt.Errorf("the command's status was not a status document (%q): %w",
			strings.TrimSpace(string(raw)), err)
	}
	if st.Status == "Success" {
		return 0, nil
	}
	if st.Reason == "NonZeroExitCode" {
		for _, cause := range st.Details.Causes {
			if cause.Reason != "ExitCode" {
				continue
			}
			code, err := strconv.Atoi(cause.Message)
			if err != nil {
				return 0, fmt.Errorf("the command reported exit code %q, which is not a number",
					cause.Message)
			}
			return code, nil
		}
		return 0, fmt.Errorf("the command failed without an exit code: %s", st.Message)
	}
	// Everything else is the substrate rather than the command: the container
	// is gone, the runtime refused, the executable was not found.
	msg := st.Message
	if msg == "" {
		msg = st.Status
	}
	return 0, errors.New(msg)
}
