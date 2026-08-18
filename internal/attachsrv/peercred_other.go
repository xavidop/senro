//go:build !linux && !darwin

package attachsrv

import (
	"fmt"
	"net"
	"runtime"
)

// peerUID has no implementation on this platform: this package only reads
// peer credentials via SO_PEERCRED (linux) or LOCAL_PEERCRED (darwin).
//
// Deliberately fails closed rather than silently skipping the check: an
// attach endpoint that degrades to "allow everyone" the moment its
// credential check cannot run claims a protection it does not deliver.
func peerUID(_ *net.UnixConn) (uint32, error) {
	return 0, fmt.Errorf("attachsrv: peer credential check is not implemented on %s", runtime.GOOS)
}
