//go:build darwin

package ptyx

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// grant unlocks the slave side and returns its path.
//
// Darwin's sequence is three ioctls, not interchangeable with Linux's:
// TIOCPTYGRANT is grantpt(3), TIOCPTYUNLK is unlockpt(3), and TIOCPTYGNAME
// fills a buffer with the slave's path. The buffer is 128 bytes because
// darwin's own ptsname_r uses that.
func grant(master *os.File) (string, error) {
	fd := int(master.Fd())
	if err := unix.IoctlSetInt(fd, unix.TIOCPTYGRANT, 0); err != nil {
		return "", fmt.Errorf("ptyx: TIOCPTYGRANT: %w", err)
	}
	if err := unix.IoctlSetInt(fd, unix.TIOCPTYUNLK, 0); err != nil {
		return "", fmt.Errorf("ptyx: TIOCPTYUNLK: %w", err)
	}
	// x/sys/unix has no helper for a string-returning ioctl on darwin, and
	// its preferred libSystem wrappers mean cgo; senro builds with
	// CGO_ENABLED=0 (see internal/funcbin), so raw syscall.Syscall it is.
	var buf [128]byte
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		uintptr(unix.TIOCPTYGNAME), uintptr(unsafe.Pointer(&buf[0]))); errno != 0 {
		return "", fmt.Errorf("ptyx: TIOCPTYGNAME: %w", errno)
	}
	n := 0
	for n < len(buf) && buf[n] != 0 {
		n++
	}
	return string(buf[:n]), nil
}
