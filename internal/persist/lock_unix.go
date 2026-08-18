//go:build unix

package persist

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// tryLock takes an exclusive advisory lock on f without waiting, and reports
// whether it got one.
//
// flock, not an O_CREATE|O_EXCL lock file (as internal/scratch claims a
// key): a lock file survives its process and needs a staleness heuristic,
// while the kernel releases an advisory lock however the process dies. It
// is per open file description, not per process, so two engine.Run calls in
// one embedder process exclude each other exactly as two processes do,
// which also makes the exclusion testable without spawning anything.
func tryLock(f *os.File) (bool, error) {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, unix.EWOULDBLOCK):
		return false, nil
	default:
		return false, err
	}
}

func unlock(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
