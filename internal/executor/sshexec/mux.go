package sshexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/internal/attachsrv"
	senroexec "github.com/xavidop/senro/internal/executor"
)

// DefaultMasterIdleTTL bounds how long an idle control master survives when
// this coordinator never runs Close. Close covers every ordinary path, so
// this fires only when Close never ran at all; it is ssh's own ControlPersist,
// held by the master process itself, so nothing about the coordinator's
// health can stop it. DefaultSecretTTL's shape and a far shorter value: what
// is left behind here is an authenticated session nobody is using, not a file
// a running step still reads.
const DefaultMasterIdleTTL = 5 * time.Minute

// maxMuxSessions bounds how many of this executor's invocations ride the
// master at once. sshd's MaxSessions defaults to 10 and refuses the eleventh
// session on a connection; ssh then opens its own connection and the command
// still runs, but it PRINTS "Session open refused by peer" first, on the
// stderr this package hands straight to the step. A step's output is the one
// stream senro cannot clean up after itself (see the login banner note in the
// package doc), so the cap keeps senro under the limit rather than discovering
// it per step. Under sshd's default, with room for it having been lowered a
// little.
const maxMuxSessions = 8

// controlPathBudget is the longest control socket path senro will use. A unix
// socket address holds 104 bytes on darwin and 108 on linux, and the master
// binds "<path>.<pid>" before linking it into place, so a path has to fit the
// smaller limit with room for that suffix. Past it OpenSSH prints an error and
// carries on unmultiplexed once per invocation, which is the per-step
// complaint this package announces once instead.
const controlPathBudget = 104 - len(".4294967296")

// configReadTimeout bounds `ssh -G`. It connects to nothing, so this only has
// to survive a Match exec block in somebody's configuration.
const configReadTimeout = 15 * time.Second

// masterListenWait bounds the wait for the master to bind its socket. `ssh -f`
// returns as soon as the connection is authenticated, and the listen follows.
const masterListenWait = 5 * time.Second

// muxDialTimeout bounds the local liveness connect to the control socket. It
// crosses no network, so anything a running master does not answer inside is
// a master that is not answering.
const muxDialTimeout = time.Second

// errOperatorConfigured says the operator's own configuration already names a
// ControlPath for this destination. senro then manages nothing and says
// nothing: their configuration is in force, which is deference rather than a
// degradation.
var errOperatorConfigured = errors.New(
	"your ssh configuration already names a ControlPath for this destination")

// muxer owns one destination's OpenSSH control master for the life of one
// run: opened by the first invocation that needs it, reused by every later
// one, so a run pays connection setup once per host instead of once per
// command.
//
// The socket is credential-adjacent: anyone who can open it gets the
// authenticated session, with no further authentication. It therefore lives
// in the same 0700 directory under the platform runtime dir that the attach
// socket does (attachsrv.Dir), under a name carrying a random nonce rather
// than the run id, so no other run and no other coordinator can name it and
// no run id is long enough to break it.
type muxer struct {
	ex      *Executor
	idleTTL time.Duration
	notice  io.Writer
	// sessions caps concurrent multiplexed invocations; see maxMuxSessions.
	sessions chan struct{}

	mu sync.Mutex
	// path is the control socket, resolved on first use and empty until then.
	path string
	// up records that a master answered at path. The socket file is the
	// master's own liveness: ssh unlinks it when the master exits.
	up bool
	// off ends multiplexing for the rest of the run. Set once, after the one
	// announcement. Deliberately unlike resolve, which caches only success:
	// the facts are something the run NEEDS and a second attempt is worth a
	// connection, while the master is an optimisation and re-attempting it
	// per invocation would pay a whole handshake to learn what the last
	// attempt already reported.
	off bool
}

func newMuxer(e *Executor, idleTTL time.Duration, notice io.Writer) *muxer {
	return &muxer{
		ex: e, idleTTL: idleTTL, notice: notice,
		sessions: make(chan struct{}, maxMuxSessions),
	}
}

// acquire reports the ssh options one invocation adds to ride the master, and
// the function that releases its slot. It returns no options at all, and the
// invocation opens its own connection exactly as it did before this file
// existed, when this executor multiplexes nothing, when the master could not
// be opened, or when the session cap is full.
func (m *muxer) acquire(ctx context.Context) ([]string, func()) {
	if m == nil {
		return nil, func() {}
	}
	select {
	case m.sessions <- struct{}{}:
	default:
		return nil, func() {}
	}
	path, ok := m.ensure(ctx)
	if !ok {
		<-m.sessions
		return nil, func() {}
	}
	// ControlMaster=no, always: only the dedicated master invocation above may
	// become a master. A command allowed to become one would background a
	// daemon holding the pipes this executor's caller is reading a step's
	// output from.
	return []string{"-o", "ControlMaster=no", "-o", "ControlPath=" + path},
		func() { <-m.sessions }
}

// ensure opens the master if one is not already listening. One at a time: the
// mutex makes concurrent steps wait for a single attempt rather than opening a
// master each, the same shape resolveMu has.
func (m *muxer) ensure(ctx context.Context) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.off {
		return "", false
	}
	if m.path == "" {
		path, err := m.resolvePath(ctx)
		if err != nil {
			m.disable(err)
			return "", false
		}
		m.path = path
	}
	if m.up {
		if m.alive() {
			return m.path, true
		}
		// The master is gone: an exhausted ControlPersist, a dropped link, an
		// operator's own `ssh -O exit`. Opened again rather than given up on,
		// because a master that died is not evidence that one cannot be had.
		m.up = false
	}
	if err := m.open(ctx); err != nil {
		m.disable(err)
		return "", false
	}
	m.up = true
	return m.path, true
}

// alive reports whether a master is listening on the control socket.
//
// A connect and not a stat: ssh unlinks the socket when the master exits, so
// a stat would be enough for the ordinary case, but anything else left at the
// path would pass one and then make EVERY invocation print a connect failure
// of its own into the step's output before falling back. Local, so it costs
// no round trip.
func (m *muxer) alive() bool {
	c, err := net.DialTimeout("unix", m.path, muxDialTimeout)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// resolvePath decides where this run's control socket goes, or reports why
// senro will not manage one.
func (m *muxer) resolvePath(ctx context.Context) (string, error) {
	own, err := m.operatorControlPath(ctx)
	if err != nil {
		return "", err
	}
	if own != "" {
		return "", fmt.Errorf("%w (%s)", errOperatorConfigured, own)
	}
	dir, err := attachsrv.Dir()
	if err != nil {
		return "", err
	}
	nonce, err := newNonce()
	if err != nil {
		return "", err
	}
	return controlPath(dir, nonce)
}

// controlPath is the socket path and the two rules it has to satisfy.
//
// The nonce rather than the run id: a run id is a label for a person reading
// `ls` on a remote host, there is no such reader for a local socket, and a
// long one would eat a budget measured in single bytes.
func controlPath(dir, nonce string) (string, error) {
	path := filepath.Join(dir, "sshmux-"+nonce)
	if len(path) > controlPathBudget {
		return "", fmt.Errorf(
			"a control socket at %s would be %d bytes, past the %d a unix socket address holds. "+
				"Shorten the runtime directory senro puts it in ($XDG_RUNTIME_DIR on linux, the "+
				"user cache directory elsewhere), or declare ssh.NoMultiplexing()",
			path, len(path), controlPathBudget)
	}
	if strings.ContainsAny(path, "%\n\r") {
		// ssh expands %-tokens inside ControlPath, so a directory carrying one
		// would name a socket senro never chose.
		return "", fmt.Errorf(
			"a control socket at %s would hold a character ssh reads as a token or a line break. "+
				"Move the runtime directory senro puts it in, or declare ssh.NoMultiplexing()",
			path)
	}
	return path, nil
}

// operatorControlPath is the ControlPath the operator's own configuration
// resolves for this destination, or "" when it names none.
//
// `ssh -G` prints the configuration ssh WOULD use and connects to nothing, so
// asking costs a local process and no handshake, and the answer comes from
// ssh's own parser rather than a second one written here. Unreadable is not
// "none": senro would rather multiplex nothing than override a ControlPath it
// failed to see.
func (m *muxer) operatorControlPath(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, configReadTimeout)
	defer cancel()

	args := append(m.ex.baseArgs(), "-G", "--", m.ex.spec.Host)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf(
			"`ssh -G %s` could not be read, so senro cannot tell whether your own configuration "+
				"already multiplexes this destination: %w%s",
			m.ex.spec.Host, err, detail(errb.String()))
	}
	for _, line := range strings.Split(out.String(), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || !strings.EqualFold(key, "controlpath") {
			continue
		}
		if strings.EqualFold(value, "none") {
			return "", nil
		}
		return value, nil
	}
	return "", nil
}

// open starts the master and waits for it to listen.
func (m *muxer) open(ctx context.Context) error {
	// Anything at this path can only be something this run left behind: the
	// name carries a fresh nonce, so no other run and no other coordinator
	// ever names it. `ssh -M` refuses to bind over an existing file, and a
	// master that died without unlinking its socket would otherwise end
	// multiplexing for the rest of the run.
	_ = os.Remove(m.path)

	diag, err := os.CreateTemp(filepath.Dir(m.path), ".sshmux-log-")
	if err != nil {
		return err
	}
	name := diag.Name()
	defer func() { _ = diag.Close(); _ = os.Remove(name) }()

	args := append(m.ex.baseArgs(),
		"-N", "-f",
		"-o", "ControlMaster=yes",
		"-o", "ControlPath="+m.path,
		"-o", "ControlPersist="+strconv.Itoa(int(m.idleTTL.Seconds())),
		"--", m.ex.spec.Host)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	// A FILE for both streams, never a pipe: -f with ControlPersist leaves a
	// daemonized master holding whichever descriptors it was handed, and a
	// pipe this process waits on would hold this invocation open for
	// WaitDelay and every later one for as long as the master lives.
	cmd.Stdout, cmd.Stderr = diag, diag
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("opening a control master on %s: %w%s",
			m.ex.spec.Host, err, detail(readDiag(name)))
	}
	if err := m.waitForSocket(ctx); err != nil {
		return fmt.Errorf("%w%s", err, detail(readDiag(name)))
	}
	return nil
}

// waitForSocket waits for the master to bind. `ssh -f` returns once the
// connection is authenticated and the listen follows it, and a master that
// could not listen at all (a ControlPath OpenSSH judged too long, a directory
// it could not write) disables its own multiplexing and STILL exits 0, so the
// socket is the only honest answer to "is there a master".
func (m *muxer) waitForSocket(ctx context.Context) error {
	deadline := time.Now().Add(masterListenWait)
	for {
		if _, err := os.Stat(m.path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ssh reported no error and bound no control socket at %s within %s",
				m.path, masterListenWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// disable ends multiplexing for the rest of the run and announces it once.
// Called with m.mu held; m.off is what makes the line appear once rather than
// once per step.
//
// Announced rather than swallowed, and on stderr rather than nowhere, for
// remotecache.Config.ReportWriter's reason: "this run opened a connection per
// command" is the one fact that explains a build nobody can account for the
// slowness of, and a run with no attached client and no configured sink must
// still be able to say it.
func (m *muxer) disable(reason error) {
	m.off = true
	if errors.Is(reason, errOperatorConfigured) {
		return
	}
	_, _ = fmt.Fprintf(m.notice,
		"senro: ssh %s: connection multiplexing is off for this run, so every command opens its "+
			"own connection: the run is slower and correct regardless. %v\n",
		m.ex.spec.Host, reason)
}

// stop tears the master down. `ssh -O exit` asks the master itself to go,
// which is what unlinks the socket; the removal afterwards covers a master
// that had already gone.
func (m *muxer) stop() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	path, up := m.path, m.up
	// Closed for good: an executor whose master has been torn down must not
	// open another from a straggling cleanup connection.
	m.path, m.up, m.off = "", false, true
	m.mu.Unlock()

	if path == "" {
		return nil
	}
	defer func() { _ = os.Remove(path) }()
	if !up {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	args := append(m.ex.baseArgs(), "-O", "exit", "-o", "ControlPath="+path,
		"--", m.ex.spec.Host)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	var errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = io.Discard, &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"sshexec: %w: could not close the control master to %s, which its own ControlPersist "+
				"removes within %s: %w%s",
			senroexec.ErrInfra, m.ex.spec.Host, m.idleTTL, err, detail(errb.String()))
	}
	return nil
}

// path reports the control socket, for a test that has to prove one exists.
func (m *muxer) socket() string {
	if m == nil {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.path
}

// readDiag is the master's own account of why it did not start, for the one
// message that needs it. Unreadable is empty: this is already an error path.
func readDiag(name string) string {
	b, err := os.ReadFile(name)
	if err != nil {
		return ""
	}
	return string(b)
}
