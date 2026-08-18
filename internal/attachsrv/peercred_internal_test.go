package attachsrv

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// checkUID is CheckPeer's access decision as a pure function, so it can be
// tested without fabricating a connection from a different uid: something
// a non-root test cannot do at all. The platform half is proven separately
// over a real connection by TestPeerUIDReturnsOurOwnUIDOverARealConnection
// and TestCheckPeerAcceptsAConnectionFromTheSameUID, both of which can
// only prove the same-uid case, which is why this proves rejection.
func TestCheckUIDAcceptsMatchingUIDs(t *testing.T) {
	if err := checkUID(501, 501); err != nil {
		t.Errorf("checkUID(501, 501) = %v, want nil", err)
	}
}

func TestCheckUIDRejectsAMismatchedUID(t *testing.T) {
	err := checkUID(501, 502)
	if err == nil {
		t.Fatal("checkUID(501, 502) = nil, want an error — a different uid must be refused")
	}
	if !errors.Is(err, ErrPeerRejected) {
		t.Errorf("checkUID(501, 502) = %v, want it to wrap ErrPeerRejected", err)
	}
}

// selfConnectedUnixSocket returns the server-side *net.UnixConn of a real,
// connected unix socket pair in this process, for peerUID's tests to call
// the platform implementation directly against.
//
// os.MkdirTemp with a short prefix rather than t.TempDir(), matching
// server_test.go's shortSocketPath (unreachable from this package):
// t.TempDir() nests this test's long name into a path limited to ~104
// bytes on darwin.
func selfConnectedUnixSocket(t *testing.T) *net.UnixConn {
	t.Helper()
	dir, err := os.MkdirTemp("", "as")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan *net.UnixConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c.(*net.UnixConn)
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

// Exercises whichever platform peerUID this binary was built with, with no
// build tag of its own, by calling the unqualified symbol exactly one file
// defines. A peerUID mutated to return a wrong-but-successful uid (always
// 0) would pass TestCheckPeerAcceptsAConnectionFromTheSameUID whenever
// os.Getuid() was also 0; pinning the returned value against os.Getuid()
// fails on that mutation whatever uid the test runs as.
func TestPeerUIDReturnsOurOwnUIDOverARealConnection(t *testing.T) {
	conn := selfConnectedUnixSocket(t)

	uid, err := peerUID(conn)
	switch runtime.GOOS {
	case "linux", "darwin":
		if err != nil {
			t.Fatalf("peerUID: %v", err)
		}
		if uid != uint32(os.Getuid()) {
			t.Errorf("peerUID = %d, want %d (os.Getuid())", uid, os.Getuid())
		}
	default:
		// peercred_other.go: no known mechanism on this platform. Failing
		// closed (an error, not a fabricated uid) is the only correct
		// answer; see peercred_other.go's own doc.
		if err == nil {
			t.Fatalf("peerUID on unsupported platform %s returned no error and uid %d — must fail closed", runtime.GOOS, uid)
		}
	}
}
