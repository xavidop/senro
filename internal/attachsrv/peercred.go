package attachsrv

import (
	"errors"
	"fmt"
	"net"
	"os"
)

// ErrPeerRejected reports that a connection failed CheckPeer: its peer's
// uid did not match ours, or its credentials could not be determined at
// all. Wrapped, not returned bare, so a caller can tell this specific
// refusal apart from a transport-level error with errors.Is.
var ErrPeerRejected = errors.New("attachsrv: connection rejected: peer credential check failed")

// CheckPeer verifies that conn's peer is running as the same uid as this
// process, and errors if it is not or if that cannot be determined at all.
//
// This is the actual access guard for the attach socket. Socket mode 0600
// is defence in depth, not a substitute: a 0600 socket still admits root,
// and mode bits say nothing about who is on the other end of an accepted
// connection; only a peer-credential syscall, captured by the kernel at
// connect time and not writable by the peer, can answer that. See the
// per-platform peerUID implementations, and peercred_other.go for why an
// unsupported platform refuses outright.
//
// conn must be a *net.UnixConn, the only kind either SO_PEERCRED or
// LOCAL_PEERCRED can be read from; anything else is refused rather than
// reported as passing a check that was never performed.
func CheckPeer(conn net.Conn) error {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("attachsrv: %w: not a unix socket connection (%T)", ErrPeerRejected, conn)
	}

	uid, err := peerUID(uc)
	if err != nil {
		return fmt.Errorf("attachsrv: %w: could not determine peer credentials: %v", ErrPeerRejected, err)
	}

	return checkUID(uid, uint32(os.Getuid()))
}

// checkUID is CheckPeer's actual access decision, isolated from the
// syscall so it can be tested with a fabricated mismatch, which no test
// running without root can otherwise produce. See
// TestCheckUIDRejectsAMismatchedUID.
func checkUID(peerUID, ourUID uint32) error {
	if peerUID != ourUID {
		return fmt.Errorf("attachsrv: %w: peer uid %d does not match our uid %d", ErrPeerRejected, peerUID, ourUID)
	}
	return nil
}
