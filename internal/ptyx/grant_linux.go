//go:build linux

package ptyx

import (
	"fmt"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// grant unlocks the slave side and returns its path.
//
// TIOCSPTLCK with zero is unlockpt(3); TIOCGPTN returns the pty number,
// whose device is /dev/pts/<n>. There is no grantpt equivalent on Linux:
// devpts assigns ownership and mode itself at open.
func grant(master *os.File) (string, error) {
	fd := int(master.Fd())
	var unlock int32
	if err := unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, int(unlock)); err != nil {
		return "", fmt.Errorf("ptyx: TIOCSPTLCK: %w", err)
	}
	n, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		return "", fmt.Errorf("ptyx: TIOCGPTN: %w", err)
	}
	return "/dev/pts/" + strconv.Itoa(n), nil
}
