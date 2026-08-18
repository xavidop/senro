package sshexec_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/attachsrv"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/sshexec"
	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
)

// ─────────────────────────────────────────────────────────────────────────────
// One connection per host
// ─────────────────────────────────────────────────────────────────────────────

// sshConnection asks the host which TCP connection this step's session arrived
// on. `env -i` keeps SSH_CONNECTION out of the step's OWN environment, so the
// step reads it from the wrapper that launched it. The value carries the
// client's source port, which is one value for every session multiplexed over
// one connection and a different one for every separate connection: that is
// what makes this a test of reuse rather than of "the command still ran".
func sshConnection(t *testing.T, sb senroexec.Sandbox) string {
	t.Helper()
	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", `tr '\0' '\n' < /proc/$PPID/environ | grep '^SSH_CONNECTION='`},
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("reading SSH_CONNECTION back from the host: exit %d, err %v", exit, err)
	}
	got := strings.TrimSpace(out.String())
	if got == "" {
		t.Fatal("the host reported no SSH_CONNECTION, so this test cannot tell two connections apart")
	}
	return got
}

// TestEveryCommandOnAHostRidesOneConnection is the claim this feature makes:
// a run pays connection setup once per host, not once per command. Both halves
// are asserted, because either alone passes for the wrong reason: the master
// is listening on senro's own socket, AND two commands really arrived on the
// same TCP connection.
func TestEveryCommandOnAHostRidesOneConnection(t *testing.T) {
	ex, srv := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})

	first := sshConnection(t, sb)
	second := sshConnection(t, sb)
	if first != second {
		t.Errorf("two commands arrived on different connections (%q and %q), so nothing is being "+
			"multiplexed and every step pays a handshake", first, second)
	}

	sock := ex.ControlPath()
	if sock == "" {
		t.Fatal("the executor is managing no control socket")
	}
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("no control socket at %s: %v", sock, err)
	}
	if out, err := controlCommand(srv, sock, "check"); err != nil {
		t.Errorf("`ssh -O check` on %s: %v: %s", sock, err, out)
	}
}

// The switch has to work, because multiplexing changes failure modes: one
// master carries many steps. NoMultiplexing is also the exact code path a
// run takes when a master could not be opened, so this covers the fallback's
// correctness too.
func TestNoMultiplexingGivesEveryCommandItsOwnConnection(t *testing.T) {
	srv := sshdtest.Require(t)
	ex := newExecutorFor(t, srv, plan.ExecutorSpec{
		Kind: plan.ExecutorSSH, Host: srv.Alias, NoMultiplex: true,
	})
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})

	if sock := ex.ControlPath(); sock != "" {
		t.Errorf("a target that declared ssh.NoMultiplexing() opened a control socket at %s", sock)
	}
	if first, second := sshConnection(t, sb), sshConnection(t, sb); first == second {
		t.Errorf("two commands arrived on the same connection (%q) with multiplexing declared off",
			first)
	}
}

// The control socket is credential-adjacent: opening it is the authenticated
// session, with nothing further to prove. It therefore lives where the attach
// socket lives, in a directory of this user's own.
func TestTheControlSocketLivesInAPrivateDirectory(t *testing.T) {
	ex, _ := newExecutor(t)
	if _, err := ex.DeclaredPlatform(context.Background()); err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	sock := ex.ControlPath()
	if sock == "" {
		t.Fatal("the executor is managing no control socket")
	}

	want, err := attachsrv.Dir()
	if err != nil {
		t.Fatalf("attachsrv.Dir: %v", err)
	}
	if dir := filepath.Dir(sock); dir != want {
		t.Errorf("the control socket is at %s, outside the private runtime directory %s", dir, want)
	}
	fi, err := os.Stat(filepath.Dir(sock))
	if err != nil {
		t.Fatalf("stat %s: %v", filepath.Dir(sock), err)
	}
	if mode := fi.Mode().Perm(); mode != 0o700 {
		t.Errorf("the control socket's directory is mode %o, want 0700: anyone who can open the "+
			"socket has the authenticated session", mode)
	}
}

// The master must not outlive the run on any path. Close is what senro.Run
// defers for every executor it builds.
func TestClosingTheExecutorTakesTheControlMasterAway(t *testing.T) {
	ex, srv := newExecutor(t)
	if _, err := ex.DeclaredPlatform(context.Background()); err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	sock := ex.ControlPath()
	if sock == "" {
		t.Fatal("the executor is managing no control socket")
	}

	if err := ex.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("the control socket %s survived Close (stat error %v), so an authenticated "+
			"session to the host is still open", sock, err)
	}
	if out, err := controlCommand(srv, sock, "check"); err == nil {
		t.Errorf("`ssh -O check` still finds a master after Close: %s", out)
	}
	if err := ex.Close(); err != nil {
		t.Errorf("a second Close reported %v, want a no-op", err)
	}
}

// A killed coordinator leaves whatever it left; the next thing to use the
// path must not inherit it. Here the master is gone and something is sitting
// at its socket path, which is what `ssh -M` refuses to bind over.
func TestAStaleControlSocketDoesNotStopTheRun(t *testing.T) {
	ex, srv := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	before := sshConnection(t, sb)

	sock := ex.ControlPath()
	if sock == "" {
		t.Fatal("the executor is managing no control socket")
	}
	if _, err := controlCommand(srv, sock, "exit"); err != nil {
		t.Fatalf("stopping the master out from under the executor: %v", err)
	}
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatalf("leaving a stale file at %s: %v", sock, err)
	}

	after := sshConnection(t, sb)
	if after == before {
		t.Errorf("the command reported the connection of a master that was stopped (%q)", before)
	}
	if _, err := controlCommand(srv, sock, "check"); err != nil {
		t.Errorf("no master was opened again over the stale socket at %s: %v", sock, err)
	}
}

// senro adds `-o BatchMode=yes` and overrides nothing else; multiplexing is
// held to the same rule. An operator whose own configuration names a
// ControlPath for the destination keeps it, master and all, and senro adds
// no option of its own.
func TestSenroDoesNotOverrideYourOwnControlPath(t *testing.T) {
	srv := sshdtest.Require(t)
	own, err := os.MkdirTemp("", "senro-mux-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(own) })

	generated, err := os.ReadFile(srv.ConfigPath)
	if err != nil {
		t.Fatalf("reading the generated ssh_config: %v", err)
	}
	mine := strings.Replace(string(generated),
		"ControlPath none", "ControlPath "+filepath.Join(own, "s"), 1)
	if mine == string(generated) {
		t.Fatal("the generated ssh_config no longer says ControlPath none; this test edits nothing")
	}
	path := filepath.Join(t.TempDir(), "ssh_config")
	if err := os.WriteFile(path, []byte(mine), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	var notice bytes.Buffer
	ex := newExecutorFor(t, srv, plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: srv.Alias},
		sshexec.WithConfig(path), sshexec.WithNoticeWriter(&notice))
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	sshConnection(t, sb) // the run works through the operator's configuration

	if sock := ex.ControlPath(); sock != "" {
		t.Errorf("senro opened its own control master at %s while the configuration already names "+
			"a ControlPath for this destination", sock)
	}
	if notice.Len() != 0 {
		t.Errorf("senro complained about honouring the operator's own configuration: %q",
			notice.String())
	}
}

// A run that could not multiplex is slower and correct, so it carries on. It
// says so once: the alternative is the same line in front of every step, or
// nothing at all in front of a build nobody can account for the slowness of.
func TestMultiplexingSaysItIsOffOnceRatherThanPerCommand(t *testing.T) {
	srv := sshdtest.Require(t)
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// The generated config's `Host *` block sends this destination through
	// ProxyCommand /bin/false, so the master cannot be opened and no packet
	// leaves this machine.
	var notice bytes.Buffer
	ex, err := sshexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "senro-no-such-host-under-test"},
		store.Snapshotter,
		sshexec.WithConfig(srv.ConfigPath), sshexec.WithNoticeWriter(&notice))
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	t.Cleanup(func() { _ = ex.Close() })

	// Twice: resolve caches no failure, so this is two more invocations after
	// the one that found the master could not be opened.
	for range 2 {
		if _, err := ex.DeclaredPlatform(context.Background()); err == nil {
			t.Fatal("DeclaredPlatform succeeded through a destination that reaches nothing")
		}
	}

	lines := strings.Count(strings.TrimSpace(notice.String()), "\n") + 1
	if strings.TrimSpace(notice.String()) == "" {
		t.Fatal("a run that could not multiplex said nothing at all, so a build that opens a " +
			"connection per command looks exactly like one that does not")
	}
	if lines != 1 {
		t.Errorf("the fallback was announced %d times, want once:\n%s", lines, notice.String())
	}
	if !strings.Contains(notice.String(), "multiplexing") {
		t.Errorf("the announcement does not say what was lost: %q", notice.String())
	}
}

// The cost multiplexing introduces: every session of a run rides one
// connection, and sshd refuses the eleventh session on one (MaxSessions,
// default 10). ssh then opens a connection of its own and the command runs
// anyway, but it says "Session open refused by peer" on the way, on the
// stream this package hands straight to the step. Senro's own noise in a
// step's output is not recoverable from afterwards, so it must not happen:
// more parallel commands than one connection carries must run cleanly.
func TestMoreParallelCommandsThanOneConnectionCarriesRunCleanly(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})

	const parallel = 12
	type outcome struct {
		err            error
		stdout, stderr string
	}
	got := make([]outcome, parallel)
	var wg sync.WaitGroup
	for i := range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var stdout, stderr bytes.Buffer
			exit, err := sb.Run(context.Background(),
				senroexec.Cmd{Args: []string{"sh", "-c", "sleep 2; echo ok"}}, &stdout, &stderr)
			if err == nil && exit != 0 {
				err = fmt.Errorf("exit %d", exit)
			}
			got[i] = outcome{err, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String())}
		}()
	}
	wg.Wait()

	for i, o := range got {
		switch {
		case o.err != nil:
			t.Errorf("command %d of %d run at once failed: %v", i, parallel, o.err)
		case o.stdout != "ok":
			t.Errorf("command %d of %d printed %q, want %q", i, parallel, o.stdout, "ok")
		case o.stderr != "":
			t.Errorf("command %d of %d has ssh's own diagnostics in its stderr, which is the "+
				"step's own output stream: %q", i, parallel, o.stderr)
		}
	}
}

// controlCommand runs `ssh -O <op>` against a control socket, outside the
// executor, so a test can ask the master itself whether it is there.
func controlCommand(srv sshdtest.Server, sock, op string) (string, error) {
	cmd := exec.Command("ssh", "-T", "-o", "BatchMode=yes", "-F", srv.ConfigPath,
		"-O", op, "-o", "ControlPath="+sock, "--", srv.Alias)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
