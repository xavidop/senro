//go:build unix

// Package ptyx opens a pseudo-terminal on the two platforms senro targets.
//
// The standard library has no openpty; this is a few ioctls over
// golang.org/x/sys/unix (already a direct dependency), cheaper than a new
// one. Unix only, via build tag, so an unsupported platform fails to compile
// rather than at session time; senro does not support Windows at all.
//
// A pty is one device: the child's stdout and stderr are the same open file
// description, so they can never be told apart again. That is why a terminal
// session is a different kind from the pipe-backed one rather than a flag on
// it; see internal/executor.Terminal. The line discipline is real: the child
// gets job control, line editing, a window size, and ^C as a signal to its
// foreground process group, and output carries the discipline's own CRLF.
package ptyx

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// WinSize is a terminal's dimensions, in character cells.
type WinSize struct {
	Cols uint16
	Rows uint16
}

// Open returns a connected pty master and slave.
//
// The caller owns both files and must close both. Closing the master delivers
// EOF (and SIGHUP) to the child; the slave must be closed in the parent after
// handing it to a child, or the master never reports EOF, since a pty master
// reports EOF only when no process holds the slave open.
func Open() (master, slave *os.File, err error) {
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("ptyx: opening /dev/ptmx: %w", err)
	}
	name, err := grant(m)
	if err != nil {
		_ = m.Close()
		return nil, nil, err
	}
	s, err := os.OpenFile(name, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		_ = m.Close()
		return nil, nil, fmt.Errorf("ptyx: opening %s: %w", name, err)
	}
	return m, s, nil
}

// SetSize tells the pty how large the operator's window is.
//
// Called on the master; the kernel delivers SIGWINCH to the child's
// foreground process group. A pty whose creator never sets a size reports
// "0 0", and a full-screen program that reads that draws nothing.
func SetSize(master *os.File, ws WinSize) error {
	if ws.Cols == 0 || ws.Rows == 0 {
		// Refused rather than passed through: zero is indistinguishable
		// from an unset size, so "I do not know" must not be settable.
		return fmt.Errorf("ptyx: refusing a %dx%d window size", ws.Cols, ws.Rows)
	}
	err := unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{
		Col: ws.Cols, Row: ws.Rows,
	})
	if err != nil {
		return fmt.Errorf("ptyx: setting the window size: %w", err)
	}
	return nil
}
