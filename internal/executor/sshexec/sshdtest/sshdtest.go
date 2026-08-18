package sshdtest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
)

// Image is the base every ssh test in this repository runs against: Alpine
// plus openssh from apk, so this package owns the host key, the authorized
// key and the whole of sshd_config (a prebuilt sshd image's opinions are
// each a place a real host key could come from). Pinned to a dated tag, as
// dockertest.Image is. The shell, coreutils and tar are BusyBox's, so
// GNU-only behaviour is not exercised here.
const Image = "alpine:3.21"

// Alias is the one host the generated ssh_config defines; the `Host *`
// block (see writeConfig) makes it the only reachable one.
const Alias = "senro-sshd-under-test"

// Server is one test's handle on the shared sshd: the ssh_config that reaches
// it and the alias to name.
type Server struct {
	// ConfigPath is passed to sshexec.WithConfig (`ssh -F`), replacing the
	// invoking account's ~/.ssh/config entirely.
	ConfigPath string
	// Alias is the destination to give ssh.Host. It resolves only through
	// ConfigPath.
	Alias string
	// Addr is the verified loopback address, for a test that needs to say what
	// it connected to in a failure message.
	Addr string
}

// Require returns a live SSH server, or skips the test with a reason.
//
// The server is started once per test binary and shared; each sandbox gets
// its own directory on the host, so sharing costs nothing. Skipping follows
// dockertest.Require's rule: set SENRO_REQUIRE_DOCKER=1 and every skip
// becomes a failure. A test binary that calls this MUST route its TestMain
// through RunMain, which is what stops the container outliving the run.
func Require(t *testing.T) Server {
	t.Helper()
	// The daemon first, so a machine with no Docker gets dockertest.Require's
	// own carefully worded skip rather than a failure from further in.
	_ = dockertest.Require(t)
	requireOpenSSH(t)

	srv, err := shared()
	if err != nil {
		t.Fatalf("starting an sshd in %s: %v. This test needs a live SSH server and did not run.",
			Image, err)
	}
	return Server{ConfigPath: srv.configPath, Alias: Alias, Addr: srv.addr}
}

// requireOpenSSH gates on the two binaries this executor is built on: it
// shells out to ssh, and this package generates keys with ssh-keygen, so a
// machine without them cannot run the executor either.
func requireOpenSSH(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"ssh", "ssh-keygen"} {
		if _, err := exec.LookPath(bin); err != nil {
			if os.Getenv("SENRO_REQUIRE_DOCKER") == "1" {
				t.Fatalf("SENRO_REQUIRE_DOCKER=1 is set, but %s is not on PATH: %v. This test was "+
					"not run, and senro's ssh executor cannot work on this machine either.", bin, err)
			}
			t.Skipf("%s is not on PATH: %v. SSH executor tests were not run. Set "+
				"SENRO_REQUIRE_DOCKER=1 to make that a failure instead of a skip.", bin, err)
		}
	}
}

// RunMain runs a package's tests and then stops whatever container they
// started.
//
//	func TestMain(m *testing.M) { os.Exit(sshdtest.RunMain(m)) }
//
// The shared server outlives every individual test by construction, so no
// t.Cleanup can be the one to stop it: the first test to finish would take the
// server away from the rest.
func RunMain(m *testing.M) int {
	code := m.Run()
	stopShared()
	return code
}

var (
	once     sync.Once
	srv      *server
	startErr error
)

type server struct {
	client     *dockerd.Client
	id         string
	addr       string
	dir        string
	configPath string
}

func shared() (*server, error) {
	once.Do(func() { srv, startErr = start() })
	return srv, startErr
}

func stopShared() {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.client.ContainerRemove(ctx, srv.id)
	_ = srv.client.Close()
	// The keys and the config are a fixture for one test binary and go with
	// it.
	_ = os.RemoveAll(srv.dir)
	srv = nil
}

// start creates the container, waits for sshd, guards the address it landed
// on, and only then writes the ssh_config that can reach it.
//
// It opens its own daemon client rather than borrowing a test's: the client a
// test received from dockertest.Require is closed when that test finishes, and
// this container has to outlive it.
func start() (*server, error) {
	dir, err := os.MkdirTemp("", "senro-sshdtest-")
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(dir)
		}
	}()

	clientKey, clientPub, err := keypair(dir, "id_ed25519")
	if err != nil {
		return nil, err
	}
	hostKey, hostPub, err := keypair(dir, "host_ed25519")
	if err != nil {
		return nil, err
	}
	hostKeyBytes, err := os.ReadFile(hostKey)
	if err != nil {
		return nil, err
	}

	client, err := dockerd.Open()
	if err != nil {
		return nil, err
	}

	// Clear away any sshd left by a killed test process: TestMain covers
	// every ordinary exit but not a SIGKILL. Shared with the MinIO harness
	// rather than reimplemented.
	reapCtx, reapCancel := context.WithTimeout(context.Background(), 15*time.Second)
	dockertest.ReapAbandoned(reapCtx, client, "sshd")
	reapCancel()
	defer func() {
		if !ok {
			_ = client.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if _, found, err := client.ImageInspect(ctx, Image); err != nil || !found {
		if err := client.ImagePull(ctx, Image, nil); err != nil {
			return nil, err
		}
	}

	id, err := client.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image:      Image,
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{containerScript},
		Env: []string{
			"SENRO_HOST_KEY=" + base64.StdEncoding.EncodeToString(hostKeyBytes),
			"SENRO_AUTHORIZED_KEY=" + strings.TrimSpace(readOrEmpty(clientPub)),
		},
		Ports:  []dockerd.Port{{Container: 22}},
		Labels: dockertest.OwnerLabels("sshd"),
	})
	if err != nil {
		return nil, err
	}
	s := &server{client: client, id: id, dir: dir}
	defer func() {
		if !ok {
			stop, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelStop()
			_ = client.ContainerRemove(stop, id)
		}
	}()

	if err := client.ContainerStart(ctx, id); err != nil {
		return nil, err
	}
	addr, err := waitForPort(ctx, client, id, 22)
	if err != nil {
		return nil, err
	}

	// Guarded BEFORE anything is written that could connect to it, and the
	// Conn is the only input to the configuration below.
	conn, err := guard(addr)
	if err != nil {
		return nil, err
	}
	s.addr = net.JoinHostPort(conn.Host, strconv.Itoa(conn.Port))

	knownHosts := filepath.Join(dir, "known_hosts")
	if err := writeKnownHosts(knownHosts, conn, hostPub); err != nil {
		return nil, err
	}
	s.configPath = filepath.Join(dir, "ssh_config")
	if err := writeConfig(s.configPath, conn, clientKey, knownHosts); err != nil {
		return nil, err
	}
	if err := s.waitForSSH(ctx); err != nil {
		return nil, fmt.Errorf("%w (container logs: %s)", err, containerLogs(ctx, client, id))
	}
	ok = true
	return s, nil
}

// containerScript installs sshd, installs the key material this package
// generated, and writes the whole of sshd_config. The host key comes from
// OUTSIDE the container, so the known_hosts entry is built from a key this
// process created and the very first connection is verified strictly;
// reading a key back off a started container would be trust on first use,
// which this executor's documentation promises senro never does.
const containerScript = `set -e
apk add --no-cache openssh-server >/dev/null 2>&1
printf '%s' "$SENRO_HOST_KEY" | base64 -d > /etc/ssh/ssh_host_ed25519_key
chmod 600 /etc/ssh/ssh_host_ed25519_key
mkdir -p /root/.ssh
printf '%s\n' "$SENRO_AUTHORIZED_KEY" > /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/authorized_keys
cat > /etc/ssh/sshd_config <<'EOF'
Port 22
HostKey /etc/ssh/ssh_host_ed25519_key
PermitRootLogin prohibit-password
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
AuthorizedKeysFile .ssh/authorized_keys
PrintMotd no
PermitUserEnvironment no
EOF
exec /usr/sbin/sshd -D -e
`

// keypair writes one ed25519 key with ssh-keygen and returns both paths:
// the executor already requires OpenSSH, and the file ssh-keygen produces
// is by construction one OpenSSH itself reads.
func keypair(dir, name string) (private, public string, err error) {
	private = filepath.Join(dir, name)
	cmd := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "senro-sshdtest",
		"-f", private)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", "", fmt.Errorf("ssh-keygen: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return private, private + ".pub", nil
}

func readOrEmpty(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(b)
}

// writeKnownHosts records the generated host key against the exact address
// the daemon published. The bracketed [host]:port form is what ssh looks a
// non-default port up under; getting it wrong at least fails closed.
func writeKnownHosts(path string, conn Conn, hostPub string) error {
	pub, err := os.ReadFile(hostPub)
	if err != nil {
		return err
	}
	fields := strings.Fields(string(pub))
	if len(fields) < 2 {
		return fmt.Errorf("sshdtest: %s is not an openssh public key", hostPub)
	}
	line := fmt.Sprintf("[%s]:%d %s %s\n", conn.Host, conn.Port, fields[0], fields[1])
	return os.WriteFile(path, []byte(line), 0o600)
}

// writeConfig writes the only ssh_config these tests ever use. Everything
// is stated, so nothing is inherited or prompted for. The fail-closed half:
//
//   - StrictHostKeyChecking yes, with a UserKnownHostsFile this package
//     wrote and GlobalKnownHostsFile /dev/null: the host key is verified on
//     the very first connection against a key generated here, which also
//     proves the executor works under strict checking.
//   - A `Host *` block whose ProxyCommand is /bin/false: first-match-wins
//     keeps the one alias working, and nothing else can connect at all.
func writeConfig(path string, conn Conn, identity, knownHosts string) error {
	cfg := fmt.Sprintf(`# Written by internal/executor/sshexec/sshdtest. One host, and nothing else.
Host %s
    HostName %s
    Port %d
    User root
    IdentityFile %s
    IdentitiesOnly yes
    UserKnownHostsFile %s
    GlobalKnownHostsFile /dev/null
    StrictHostKeyChecking yes
    BatchMode yes
    ProxyCommand none
    ControlMaster no
    ControlPath none
    ForwardAgent no
    ForwardX11 no
    LogLevel ERROR

# Fail closed: no destination other than the alias above can connect.
Host *
    ProxyCommand /bin/false
    StrictHostKeyChecking yes
    BatchMode yes
`, Alias, conn.Host, conn.Port, identity, knownHosts)
	return os.WriteFile(path, []byte(cfg), 0o600)
}

// waitForPort polls until the daemon reports the published host port. There is
// a window after start in which the container exists and the binding does not
// yet, and it is not an error.
func waitForPort(ctx context.Context, client *dockerd.Client, id string, port int) (string, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		addr, ok, err := client.ContainerHostAddress(ctx, id, port)
		if err != nil {
			return "", err
		}
		if ok {
			return addr, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("container %s published no host port for %d within 60s", id[:12], port)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// waitForSSH polls until an actual authenticated session succeeds: a
// published port answers before sshd is behind it, and sshd answers before
// apk has finished installing it. Running the real client also verifies the
// key material, the known_hosts entry and the strict checking once, here. A
// container that has EXITED ends the wait immediately: sshd refusing its
// own configuration should fail in seconds, not three minutes.
func (s *server) waitForSSH(ctx context.Context) error {
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for {
		attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
		cmd := exec.CommandContext(attempt, "ssh", "-T", "-o", "BatchMode=yes",
			"-F", s.configPath, "--", Alias, "true")
		out, err := cmd.CombinedOutput()
		cancel()
		if err == nil {
			return nil
		}
		last = strings.TrimSpace(string(out))
		if last == "" {
			last = err.Error()
		}
		if !s.running(ctx) {
			return fmt.Errorf("the sshd container exited before answering: %s", last)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no ssh session to %s succeeded within 3m: %s", s.addr, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// running reports whether the container is still up. A read that fails is
// treated as "still running", so a transient daemon hiccup does not end the
// wait early: the deadline above is what bounds it in that case.
func (s *server) running(ctx context.Context) bool {
	raw, err := s.client.ContainerInspectRaw(ctx, s.id)
	if err != nil {
		return true
	}
	var doc struct {
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return true
	}
	return doc.State.Running
}

// containerLogs is the server's own account of why it never came up, for the
// one failure message that needs it.
func containerLogs(ctx context.Context, client *dockerd.Client, id string) string {
	var out strings.Builder
	if err := client.ContainerLogs(ctx, id, &out, &out); err != nil {
		return "unavailable: " + err.Error()
	}
	logs := strings.TrimSpace(out.String())
	if len(logs) > 2000 {
		logs = logs[len(logs)-2000:]
	}
	if logs == "" {
		return "empty"
	}
	return logs
}
