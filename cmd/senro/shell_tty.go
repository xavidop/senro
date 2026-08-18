//go:build unix

package main

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/xavidop/senro/internal/source"
)

// The client half of a terminal session: put this terminal into raw mode,
// tell the server how big it is, and keep telling it.
//
// Raw mode is not optional. The REMOTE end has the line discipline; if this
// end keeps its own, keystrokes buffer until Enter, ^C kills `senro shell`
// instead of the remote command, and the remote echo lands on top of the
// local one. Restoring matters as much: a client that exits without it
// leaves the operator's shell with no echo and no line editing, which looks
// exactly like a hung terminal, so the restore runs on every path out,
// including a panic.

// terminalSize reports the size of the terminal on fd, and whether it is a
// terminal at all. A non-terminal is not an error: `senro shell --tty` with
// stdin from a file is legitimate, and the answer is a session whose
// terminal has no size rather than a refusal.
func terminalSize(fd uintptr) (source.WinSize, bool) {
	ws, err := unix.IoctlGetWinsize(int(fd), unix.TIOCGWINSZ)
	if err != nil || ws.Col == 0 || ws.Row == 0 {
		return source.WinSize{}, false
	}
	return source.WinSize{Cols: ws.Col, Rows: ws.Row}, true
}

// makeRaw puts fd into raw mode and returns a function that puts it back.
// Hand-written against x/sys/unix rather than pulled from a terminal
// library (the reason internal/ptyx gives for its openpty): these are
// cfmakeraw's own flags, exactly.
func makeRaw(fd uintptr) (restore func(), err error) {
	prev, err := unix.IoctlGetTermios(int(fd), ioctlReadTermios)
	if err != nil {
		return nil, err
	}
	raw := *prev
	raw.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP |
		unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON
	raw.Oflag &^= unix.OPOST
	raw.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
	raw.Cflag &^= unix.CSIZE | unix.PARENB
	raw.Cflag |= unix.CS8
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(int(fd), ioctlWriteTermios, &raw); err != nil {
		return nil, err
	}
	return func() {
		// Discarded: this runs on the way out, including during a panic,
		// with nothing left to report through. A failure leaves the
		// terminal raw, which only `reset` fixes.
		_ = unix.IoctlSetTermios(int(fd), ioctlWriteTermios, prev)
	}, nil
}

// watchWinch forwards this terminal's size on every SIGWINCH until stop is
// closed. The returned channel closes when the watcher ends, which is what
// tells the session's pump to stop.
func watchWinch(stop <-chan struct{}, fd uintptr) <-chan source.WinSize {
	out := make(chan source.WinSize, 1)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, unix.SIGWINCH)
	go func() {
		defer close(out)
		defer signal.Stop(sig)
		for {
			select {
			case <-stop:
				return
			case <-sig:
				ws, ok := terminalSize(fd)
				if !ok {
					continue
				}
				// Non-blocking: an unread resize is superseded by this one,
				// and blocking would park this goroutine on a session that
				// is already ending.
				select {
				case out <- ws:
				default:
				}
			}
		}
	}()
	return out
}
