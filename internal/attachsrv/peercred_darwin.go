//go:build darwin

package attachsrv

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

// solLocal and localPeercred are SOL_LOCAL and LOCAL_PEERCRED from
// <sys/un.h> in the macOS SDK. syscall does not export them on darwin, and
// golang.org/x/sys/unix would be a third-party dependency this module
// carries none of, so they are reproduced directly from the platform
// header: stable, longstanding kernel ABI (SOL_LOCAL 0, LOCAL_PEERCRED
// 0x001), not something this package invents.
const (
	solLocal      = 0x0
	localPeercred = 0x001
)

// xucred mirrors `struct xucred` from <sys/ucred.h>, field for field. Go
// inserts the same 2-byte gap a C compiler does between cr_ngroups (a
// short) and cr_groups (uint32-aligned), so no explicit padding is needed;
// confirmed empirically, unsafe.Sizeof(xucred{}) == 76 == sizeof(struct
// xucred) on this SDK.
type xucred struct {
	Version uint32
	Uid     uint32
	Ngroups int16
	Groups  [16]uint32
}

// peerUID reads the effective uid of the process on the other end of conn
// via LOCAL_PEERCRED: darwin's equivalent of SO_PEERCRED, captured by the
// kernel at connect time and just as untamperable by the peer.
//
// syscall on darwin exports no generic getsockopt wrapper (its own is
// unexported), but SYS_GETSOCKOPT and Syscall6 are exported, so the raw
// syscall is issued directly over the connection's descriptor rather than
// adding a dependency to reach a wrapper the standard library has but does
// not export.
func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("attachsrv: peer credentials: %w", err)
	}

	var cred xucred
	// vallen is passed in AS the buffer size and overwritten with how many
	// bytes getsockopt actually wrote: declared outside the closure so it
	// can be checked below. The &vallen conversion stays inline in the
	// Syscall6 call, which is what the unsafe.Pointer rule requires.
	vallen := uint32(unsafe.Sizeof(cred))
	var sockErr syscall.Errno
	ctrlErr := raw.Control(func(fd uintptr) {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_GETSOCKOPT,
			fd,
			uintptr(solLocal),
			uintptr(localPeercred),
			uintptr(unsafe.Pointer(&cred)),
			uintptr(unsafe.Pointer(&vallen)),
			0,
		)
		sockErr = errno
	})
	if ctrlErr != nil {
		return 0, fmt.Errorf("attachsrv: peer credentials: %w", ctrlErr)
	}
	if sockErr != 0 {
		return 0, fmt.Errorf("attachsrv: LOCAL_PEERCRED: %w", sockErr)
	}
	if err := validateXucred(cred, vallen); err != nil {
		return 0, err
	}
	return cred.Uid, nil
}

// xucredVersion is XUCRED_VERSION from <sys/ucred.h>: the only value
// cred.Version is ever valid at.
const xucredVersion = 0

// validateXucred checks that getsockopt actually wrote a complete, genuine
// xucred into cred. Split out as a pure function so both checks can be
// pinned by a test without coaxing a real short write out of the kernel.
//
// The length check is the one that matters: cred starts zero-valued, so a
// short write would leave cred.Uid at its untouched 0, which checkUID
// reads as "the peer is uid 0": an ACCEPTANCE, and on an engine itself
// running as root (ordinary in a CI container) it would accept a peer
// whose credentials were never read. Exactly ==, not <=: an unexpectedly
// LONG write is just as much a wrong-struct sign as a short one.
//
// The version check is weak on its own (XUCRED_VERSION is 0, also the zero
// value of an untouched field); it only adds signal paired with the length
// check. Do not read it, alone, as the guard.
func validateXucred(cred xucred, vallen uint32) error {
	if want := uint32(unsafe.Sizeof(cred)); vallen != want {
		return fmt.Errorf("attachsrv: LOCAL_PEERCRED: kernel returned %d bytes, want %d", vallen, want)
	}
	if cred.Version != xucredVersion {
		return fmt.Errorf("attachsrv: LOCAL_PEERCRED: unexpected xucred version %d, want %d", cred.Version, xucredVersion)
	}
	return nil
}
