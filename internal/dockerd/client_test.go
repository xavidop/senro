package dockerd_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
)

func TestSocketPathRefusesATCPDockerHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://build-07.internal:2375")
	_, err := dockerd.SocketPath()
	if err == nil {
		t.Fatal("SocketPath accepted a tcp:// DOCKER_HOST")
	}
	if !strings.Contains(err.Error(), "bind-mount") {
		t.Fatalf("the refusal does not say why a remote daemon cannot work: %v", err)
	}
}

// TestSocketPathUsesTheDefaultWhenDockerHostIsUnset proves SocketPath's own
// wiring end to end (discovery itself is unit-tested in
// discover_internal_test.go). /var/run/docker.sock is the one candidate a
// black-box test can check against real ground truth, so it asks the
// filesystem directly rather than assuming either answer.
func TestSocketPathUsesTheDefaultWhenDockerHostIsUnset(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	fi, statErr := os.Stat("/var/run/docker.sock")
	if statErr != nil || fi.Mode()&os.ModeSocket == 0 {
		t.Skip("no real /var/run/docker.sock on this machine to prove discovery picks it first; " +
			"see discover_internal_test.go for the environment-independent discovery-order tests")
	}

	got, err := dockerd.SocketPath()
	if err != nil {
		t.Fatalf("SocketPath: %v, but /var/run/docker.sock is a real socket", err)
	}
	if got != "/var/run/docker.sock" {
		t.Fatalf("SocketPath = %q, want /var/run/docker.sock (first in the discovery order)", got)
	}
}

func TestSplitRef(t *testing.T) {
	for _, tc := range []struct{ in, repo, tag string }{
		{"node:22-bookworm-slim", "node", "22-bookworm-slim"},
		{"node", "node", "latest"},
		{"ghcr.io/acme/builder:v3", "ghcr.io/acme/builder", "v3"},
		{"localhost:5000/acme/x:v1", "localhost:5000/acme/x", "v1"},
		{"localhost:5000/acme/x", "localhost:5000/acme/x", "latest"},
		{"node@sha256:" + strings.Repeat("a", 64), "node", "sha256:" + strings.Repeat("a", 64)},
	} {
		repo, tag := dockerd.SplitRef(tc.in)
		if repo != tc.repo || tag != tc.tag {
			t.Errorf("SplitRef(%q) = %q, %q; want %q, %q", tc.in, repo, tag, tc.repo, tc.tag)
		}
	}
}

func TestAnImageResolvesToADigestAndAPlatform(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, ok, err := c.ImageInspect(ctx, dockertest.Image)
	if err != nil || !ok {
		t.Fatalf("ImageInspect(%s) = ok %v, err %v", dockertest.Image, ok, err)
	}
	repo, _, _ := strings.Cut(dockertest.Image, ":")
	d := info.Digest(repo)
	if !strings.HasPrefix(d, "sha256:") {
		t.Errorf("Digest = %q, want a sha256 digest", d)
	}
	if info.OS == "" || info.Arch == "" {
		t.Errorf("platform = %q/%q, want both", info.OS, info.Arch)
	}
	if len(info.Env) == 0 {
		t.Error("the image reported no environment; EffectiveEnv depends on this")
	}
}

func TestInspectingAnAbsentImageIsNotAnError(t *testing.T) {
	c := dockertest.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, ok, err := c.ImageInspect(ctx, "senro-does-not-exist:0")
	if err != nil {
		t.Fatalf("ImageInspect of an absent image errored: %v", err)
	}
	if ok {
		t.Fatal("the daemon claims to have senro-does-not-exist:0")
	}
}

func TestAContainerRunsAndReportsBothStreamsAndItsExitCode(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	host := t.TempDir()
	if err := os.WriteFile(host+"/hello.txt", []byte("from the host\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image:  dockertest.Image,
		Cmd:    []string{"sh", "-c", "cat /work/hello.txt; echo oops >&2; exit 3"},
		Binds:  []dockerd.Bind{{Source: host, Target: "/work", ReadOnly: true}},
		Labels: map[string]string{"senro.test": "1"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()

	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}
	var out, errOut bytes.Buffer
	if err := c.ContainerLogs(ctx, id, &out, &errOut); err != nil {
		t.Fatalf("ContainerLogs: %v", err)
	}
	code, err := c.ContainerWait(ctx, id)
	if err != nil {
		t.Fatalf("ContainerWait: %v", err)
	}
	if code != 3 {
		t.Errorf("exit code = %d, want 3", code)
	}
	if got := out.String(); got != "from the host\n" {
		t.Errorf("stdout = %q; the bind mount or the demuxer is wrong", got)
	}
	if got := errOut.String(); got != "oops\n" {
		t.Errorf("stderr = %q", got)
	}
}

// TestContainerWaitAfterLogsHaveAlreadyClosedReturnsImmediately pins the
// ordering senro always uses (start, logs, wait) with a short deadline on
// the wait, so a regression to condition=next-exit (which would wait for an
// exit that never comes) fails in seconds rather than hanging.
func TestContainerWaitAfterLogsHaveAlreadyClosedReturnsImmediately(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	setupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := c.ContainerCreate(setupCtx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "exit 0"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()
	if err := c.ContainerStart(setupCtx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}
	if err := c.ContainerLogs(setupCtx, id, io.Discard, io.Discard); err != nil {
		t.Fatalf("ContainerLogs: %v", err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if _, err := c.ContainerWait(waitCtx, id); err != nil {
		t.Fatalf("ContainerWait after the container had already exited: %v", err)
	}
}

func TestAReadOnlyBindIsEnforcedByTheDaemon(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	host := t.TempDir()
	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "touch /work/written"},
		Binds: []dockerd.Bind{{Source: host, Target: "/work", ReadOnly: true}},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()
	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}
	code, err := c.ContainerWait(ctx, id)
	if err != nil {
		t.Fatalf("ContainerWait: %v", err)
	}
	if code == 0 {
		t.Fatal("a write through a read-only bind succeeded; senro.RO's container guarantee is false")
	}
	if _, err := os.Stat(host + "/written"); err == nil {
		t.Fatal("the file reached the host through a read-only bind")
	}
}

// TestContainerKillStopsARunningContainer covers ContainerKill's actual
// active path (as opposed to client_internal_test.go's already-stopped
// carve-out): a container genuinely running is genuinely killed, and Wait
// unblocks with a signal-killed status rather than hanging for its natural
// exit.
func TestContainerKillStopsARunningContainer(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "sleep 300"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()
	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}
	if err := c.ContainerKill(ctx, id); err != nil {
		t.Fatalf("ContainerKill: %v", err)
	}
	code, err := c.ContainerWait(ctx, id)
	if err != nil {
		t.Fatalf("ContainerWait after kill: %v", err)
	}
	if code == 0 {
		t.Errorf("exit code = 0 after a kill, want a nonzero signal-killed status")
	}

	// Killing it again, now that it has already exited, must stay a no-op:
	// this is the exact race a step's teardown path hits when the container
	// finishes on its own between the caller deciding to kill it and the
	// kill request landing.
	if err := c.ContainerKill(context.Background(), id); err != nil {
		t.Errorf("ContainerKill on an already-exited container: %v", err)
	}
}

// TestContainerInspectRawContainsWhatWasDeclared is the canary for
// containerexec's secret-leakage test: a broken inspect response "contains"
// nothing, so a value planted in Env must come back before the same call
// can be trusted to prove another value never went in.
func TestContainerInspectRawContainsWhatWasDeclared(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "true"},
		Env:   []string{"SENRO_CANARY=findme-12345"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()

	raw, err := c.ContainerInspectRaw(ctx, id)
	if err != nil {
		t.Fatalf("ContainerInspectRaw: %v", err)
	}
	if !strings.Contains(string(raw), "findme-12345") {
		t.Fatalf("ContainerInspectRaw did not contain a value known to be in the container's own Env; "+
			"raw: %s", raw)
	}
}
