//go:build darwin

package main

import "golang.org/x/sys/unix"

// Darwin spells the termios ioctls differently from Linux, and the constants
// are not interchangeable: TIOCGETA/TIOCSETA here, TCGETS/TCSETS there.
const (
	ioctlReadTermios  = unix.TIOCGETA
	ioctlWriteTermios = unix.TIOCSETA
)
