package attachsrv_test

import (
	"net"
	"os"
	"testing"

	"github.com/xavidop/senro/internal/attachsrv"
)

// acceptedSelfConn dials a fresh unix socket and returns the SERVER's own
// accepted *net.UnixConn: the side CheckPeer is called on in production.
// Either side would report our own uid here, since dialer and listener are
// one process, but the accepted side exercises the real shape.
//
// Uses shortSocketPath, not t.TempDir(), which nests this test's long name
// into a path limited to ~104 bytes on darwin.
func acceptedSelfConn(t *testing.T) net.Conn {
	t.Helper()
	sock := shortSocketPath(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()

	client, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case c := <-accepted:
		t.Cleanup(func() { _ = c.Close() })
		return c
	case err := <-acceptErr:
		t.Fatalf("accept: %v", err)
	}
	panic("unreachable")
}

// The positive half of the peer-credential guard, exercising the REAL
// platform syscall end to end over an actual connected unix socket. On its
// own it would pass even if CheckPeer unconditionally returned nil, which
// is why TestCheckUIDRejectsAMismatchedUID exists alongside it.
func TestCheckPeerAcceptsAConnectionFromTheSameUID(t *testing.T) {
	conn := acceptedSelfConn(t)
	if err := attachsrv.CheckPeer(conn); err != nil {
		t.Fatalf("CheckPeer on our own connection = %v, want nil (uid %d connecting to itself)", err, os.Getuid())
	}
}

// SO_PEERCRED and LOCAL_PEERCRED are unix domain socket options with no
// equivalent on any other net.Conn, so a caller handing CheckPeer
// something else must be refused rather than told a success it cannot back
// up. net.Pipe supplies a non-*net.UnixConn without opening a real socket.
func TestCheckPeerRejectsANonUnixConnection(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	if err := attachsrv.CheckPeer(server); err == nil {
		t.Fatal("CheckPeer on a non-unix net.Conn = nil, want an error — it must fail closed")
	}
}
