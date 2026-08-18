package dockerd_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
)

// TestContainerAttachCarriesBytesInBothDirections is the daemon-side half
// of an interactive session. Attach BEFORE start is not incidental: /attach
// replays nothing, so attaching after start loses whatever the container
// said in between (see ContainerAttach).
func TestContainerAttachCarriesBytesInBothDirections(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image, Cmd: []string{"sh"}, Stdin: true,
		Labels: map[string]string{"senro.run": "attach-test"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()

	stream, err := c.ContainerAttach(ctx, id)
	if err != nil {
		t.Fatalf("ContainerAttach: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}

	var out, errb lockedBuffer
	demuxDone := make(chan error, 1)
	go func() { demuxDone <- stream.Demux(&out, &errb) }()

	if _, err := io.WriteString(stream, "echo first\n"); err != nil {
		t.Fatalf("write to container stdin: %v", err)
	}
	waitFor(t, &out, "first")

	// A second command proves this is a session rather than one round trip
	// that happened to work.
	if _, err := io.WriteString(stream, "echo second\n"); err != nil {
		t.Fatalf("write to container stdin: %v", err)
	}
	waitFor(t, &out, "second")

	// CloseWrite is the container's ^D. StdinOnce means the daemon closes the
	// container's own stdin when this attach's stdin half closes, so the
	// shell reaches EOF and exits, and the stream then ends by itself.
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	select {
	case err := <-demuxDone:
		if err != nil {
			t.Errorf("Demux returned %v, want a clean end after the container exited", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the attach stream did not end after stdin was closed")
	}

	code, err := c.ContainerWait(ctx, id)
	if err != nil {
		t.Fatalf("ContainerWait: %v", err)
	}
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
}

// TestContainerAttachSeparatesStdoutFromStderr holds the attach stream to the
// same rule the log stream is held to. The daemon frames both the same way,
// and a session that merged them would show an operator a shell whose errors
// were indistinguishable from its output.
func TestContainerAttachSeparatesStdoutFromStderr(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "echo to-stdout; echo to-stderr >&2"},
		Stdin: true,
		Labels: map[string]string{
			"senro.run": "attach-test",
		},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()

	stream, err := c.ContainerAttach(ctx, id)
	if err != nil {
		t.Fatalf("ContainerAttach: %v", err)
	}
	defer func() { _ = stream.Close() }()
	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}

	var out, errb bytes.Buffer
	if err := stream.Demux(&out, &errb); err != nil {
		t.Fatalf("Demux: %v", err)
	}
	if !strings.Contains(out.String(), "to-stdout") || strings.Contains(out.String(), "to-stderr") {
		t.Errorf("stdout = %q, want only to-stdout", out.String())
	}
	if !strings.Contains(errb.String(), "to-stderr") || strings.Contains(errb.String(), "to-stdout") {
		t.Errorf("stderr = %q, want only to-stderr", errb.String())
	}
}

// TestContainerAttachStreamClosesUnblocksADemux is the disconnect path in its
// smallest form: something has to make a Demux blocked on a container that
// is producing nothing return, and Close is it. Without this the engine
// could not end a session whose client vanished while the shell sat idle,
// which is the state essentially every abandoned session is in.
func TestContainerAttachStreamClosesUnblocksADemux(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image, Cmd: []string{"sh", "-c", "sleep 300"}, Stdin: true,
		Labels: map[string]string{"senro.run": "attach-test"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()

	stream, err := c.ContainerAttach(ctx, id)
	if err != nil {
		t.Fatalf("ContainerAttach: %v", err)
	}
	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var out, errb bytes.Buffer
		_ = stream.Demux(&out, &errb)
	}()

	// Nothing is coming: the container sleeps for five minutes. Close has to
	// be what ends the read.
	time.Sleep(200 * time.Millisecond)
	_ = stream.Close()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Demux did not return after the stream was closed")
	}
	_ = c.ContainerKill(context.Background(), id)
}

// TestContainerCreateWithoutStdinLeavesItClosed pins the default. Every step
// senro has ever run is created without a stdin, and a container that
// silently gained one would change what `cat` does in a pipeline: it would
// wait forever instead of reading nothing.
func TestContainerCreateWithoutStdinLeavesItClosed(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image, Cmd: []string{"sh", "-c", "cat; echo done"},
		Labels: map[string]string{"senro.run": "attach-test"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()
	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}

	waitDone := make(chan int, 1)
	go func() {
		code, _ := c.ContainerWait(context.Background(), id)
		waitDone <- code
	}()
	select {
	case code := <-waitDone:
		if code != 0 {
			t.Errorf("exit = %d, want 0", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a container created with no stdin blocked on cat: OpenStdin leaked into the default spec")
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func waitFor(t *testing.T, buf *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q on the attach stream; got %q", want, buf.String())
}
