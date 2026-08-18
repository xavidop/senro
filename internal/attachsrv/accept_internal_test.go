package attachsrv

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// alwaysRejectPeerCheck simulates a peer-credential failure
// deterministically, which no unprivileged test can produce by connecting
// from a genuinely different uid. It proves the ACCEPT-PATH WIRING: the
// check is consulted for every accepted connection, a failure stops that
// connection before net/http, it is counted, and Accept keeps serving
// others. Whether peerUID itself reports a mismatch correctly is
// TestCheckUIDRejectsAMismatchedUID's claim.
func alwaysRejectPeerCheck(net.Conn) error {
	return errors.New("simulated peer rejection")
}

// shortUnixSocketPath mirrors server_test.go's own shortSocketPath
// (unreachable from here: this is the internal, not the _test, package):
// t.TempDir() nests this test's own (long) name into the path, which blows
// past darwin's ~104-byte unix socket path limit; os.MkdirTemp with a
// short, fixed prefix does not.
func shortUnixSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "as")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// The negative half of the accept-time peer check, and the one that
// matters: a same-uid-only positive test would pass even if the check were
// never wired in. Reached through the listen seam with
// alwaysRejectPeerCheck standing in for "the peer failed", whatever the
// reason.
//
// Two things are proven: the rejected connection never reaches a handler
// (the round trip fails at the transport level, no status and no body,
// rather than completing with a response net/http could only produce by
// serving the request), and the listener keeps accepting afterward.
//
// This proves the MECHANISM, not that Listen hands it the real CheckPeer;
// that is TestListenWiresTheRealPeerCheck's claim below.
func TestARejectedPeerNeverReachesAHandlerAndIsCounted(t *testing.T) {
	dir := t.TempDir()
	sock := shortUnixSocketPath(t)

	hub := NewHub(64)
	srv, err := listen(context.Background(), Options{Bind: sock, Dir: dir, Hub: hub}, alwaysRejectPeerCheck)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = srv.Close() }()

	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()

	resp, err := client.Get("http://unix/api/state")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("GET /api/state over a rejected connection succeeded, want a transport-level failure — it must never reach a handler")
	}
	if got := srv.PeerRejected(); got != 1 {
		t.Errorf("PeerRejected() = %d, want 1 after one rejected connection", got)
	}

	// The first rejection must not have torn the accept loop down:
	// http.Server.Serve stops its whole loop the instant Accept returns a
	// real error, which is why peerCheckedListener.Accept never does on a
	// check failure. A second request reaching the SAME rejection path
	// proves the loop is still live.
	resp2, err := client.Get("http://unix/api/state")
	if err == nil {
		_ = resp2.Body.Close()
		t.Fatal("second GET over a second rejected connection succeeded, want a transport-level failure")
	}
	if got := srv.PeerRejected(); got != 2 {
		t.Errorf("PeerRejected() = %d, want 2 after two rejected connections — the accept loop must keep evaluating and rejecting, not wedge or die on the first one", got)
	}
}

// Every other test here either drives the listen seam with its own check
// (proving the mechanism) or drives Listen with a legitimate same-uid
// connection, which a permissive check would admit just as readily.
// Neither would notice Listen's body mutated to
// `listen(ctx, opts, func(net.Conn) error { return nil })`.
//
// So this calls the real, public Listen and inspects the check the
// resulting Server retained, via srv.ln's concrete *peerCheckedListener:
// the field every production call populates, not a test-only accessor.
//
// reflect.ValueOf(fn).Pointer() is NOT generally a reliable func
// comparison: the compiler may collapse two distinct closures with
// identical code into one address. It is safe HERE because CheckPeer is a
// single top-level named function, never a closure, whose body nothing
// else shares, so that address can only mean listen selected CheckPeer.
func TestListenWiresTheRealPeerCheck(t *testing.T) {
	dir := t.TempDir()
	sock := shortUnixSocketPath(t)
	hub := NewHub(64)

	srv, err := Listen(context.Background(), Options{Bind: sock, Dir: dir, Hub: hub})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()

	pcl, ok := srv.ln.(*peerCheckedListener)
	if !ok {
		t.Fatalf("srv.ln is %T, want *peerCheckedListener — Listen must wrap its listener with the peer check", srv.ln)
	}

	got := reflect.ValueOf(pcl.check).Pointer()
	want := reflect.ValueOf(CheckPeer).Pointer()
	if got != want {
		t.Fatalf("Listen wired a check function at %#x, want CheckPeer itself (%#x) — the production entry point must use the real credential check, not a substitute", got, want)
	}
}
