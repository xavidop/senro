//go:build linux

package attachsrv

import (
	"fmt"
	"net"
	"syscall"
)

// peerUID reads the effective uid of the process on the other end of conn
// via SO_PEERCRED, the linux mechanism for reading a unix-domain socket
// peer's credentials. The kernel captures these at connect/accept time from
// the actual connecting process, not from anything the peer writes over the
// connection afterward, which is what makes this trustworthy as an access
// check rather than merely advisory.
//
// The standard library's syscall package already exports everything this
// needs on linux: GetsockoptUcred, SOL_SOCKET, SO_PEERCRED, unlike
// darwin, where the equivalent constants and struct are not exposed at all
// (see peercred_darwin.go).
func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("attachsrv: peer credentials: %w", err)
	}

	var ucred *syscall.Ucred
	var sockErr error
	if ctrlErr := raw.Control(func(fd uintptr) {
		ucred, sockErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); ctrlErr != nil {
		return 0, fmt.Errorf("attachsrv: peer credentials: %w", ctrlErr)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("attachsrv: SO_PEERCRED: %w", sockErr)
	}
	return ucred.Uid, nil
}
