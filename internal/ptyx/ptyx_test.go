//go:build unix

package ptyx_test

import (
	"bufio"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/ptyx"
)

// The child must get a real terminal, not a pipe; `test -t 0` is the
// cheapest statement of the difference this package exists for.
func TestTheChildGetsARealTerminal(t *testing.T) {
	m, s, err := ptyx.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = m.Close() }()

	cmd := exec.Command("sh", "-c", "test -t 0 && echo TTY || echo PIPE")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = s, s, s
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Closed as soon as the child holds it: the master reports EOF only
	// when nobody holds the slave open, so keeping it would hang the read.
	_ = s.Close()

	line := readLine(t, m)
	_ = cmd.Wait()
	if !strings.Contains(line, "TTY") {
		t.Errorf("the child saw %q, want a tty", line)
	}
}

// A window size the caller set is the window size the child reads.
func TestTheChildReadsTheWindowSizeItWasGiven(t *testing.T) {
	m, s, err := ptyx.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = m.Close() }()

	if err := ptyx.SetSize(m, ptyx.WinSize{Cols: 132, Rows: 43}); err != nil {
		t.Fatalf("SetSize: %v", err)
	}

	cmd := exec.Command("sh", "-c", "stty size </dev/tty")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = s, s, s
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = s.Close()

	line := readLine(t, m)
	_ = cmd.Wait()
	// stty prints "rows cols".
	if !strings.Contains(line, "43") || !strings.Contains(line, "132") {
		t.Errorf("the child read %q, want 43 rows and 132 columns", line)
	}
}

// A zero size is refused rather than set: zero is indistinguishable from
// never having set one.
func TestAZeroWindowSizeIsRefused(t *testing.T) {
	m, s, err := ptyx.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = m.Close() }()
	defer func() { _ = s.Close() }()

	for _, ws := range []ptyx.WinSize{{Cols: 0, Rows: 24}, {Cols: 80, Rows: 0}, {}} {
		if err := ptyx.SetSize(m, ws); err == nil {
			t.Errorf("SetSize(%+v) was accepted", ws)
		}
	}
}

// Both ends close cleanly and the slave path is a real device, which is the
// part a platform-specific ioctl sequence gets wrong first.
func TestOpenReturnsTwoUsableEnds(t *testing.T) {
	m, s, err := ptyx.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if fi, err := s.Stat(); err != nil {
		t.Errorf("stat on the slave: %v", err)
	} else if fi.Mode()&os.ModeCharDevice == 0 {
		t.Errorf("the slave is not a character device: %v", fi.Mode())
	}
	if err := s.Close(); err != nil {
		t.Errorf("closing the slave: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("closing the master: %v", err)
	}
}

// readLine reads one line, bounded, so a broken pty fails the test rather
// than hanging it.
func readLine(t *testing.T, f *os.File) string {
	t.Helper()
	if err := f.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil && line == "" {
		t.Fatalf("reading from the pty: %v", err)
	}
	return line
}
