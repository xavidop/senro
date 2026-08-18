package attachsrv

import (
	"os"
	"testing"
)

// linuxRuntimeDir is the pure decision inside runtimeBaseDir's linux
// branch: $XDG_RUNTIME_DIR, then /dev/shm, then give up. A function of
// plain values rather than os.Getenv/os.Stat calls, so it is exercised on
// every platform this suite runs on rather than only the linux CI leg.
func TestLinuxRuntimeDirPrefersXDGRuntimeDir(t *testing.T) {
	dir, err := linuxRuntimeDir("/run/user/1000", true)
	if err != nil {
		t.Fatalf("linuxRuntimeDir: %v", err)
	}
	if dir != "/run/user/1000" {
		t.Errorf("dir = %q, want /run/user/1000 (XDG_RUNTIME_DIR wins even when /dev/shm is also available)", dir)
	}
}

func TestLinuxRuntimeDirFallsBackToDevShm(t *testing.T) {
	dir, err := linuxRuntimeDir("", true)
	if err != nil {
		t.Fatalf("linuxRuntimeDir: %v", err)
	}
	if dir != "/dev/shm" {
		t.Errorf("dir = %q, want /dev/shm", dir)
	}
}

func TestLinuxRuntimeDirErrorsWhenNeitherIsAvailable(t *testing.T) {
	_, err := linuxRuntimeDir("", false)
	if err == nil {
		t.Fatal("linuxRuntimeDir(\"\", false) = nil error, want one — there is nowhere left to resolve to")
	}
}

// pidAlive's default branch is the EPERM case (a process that exists but
// is not ours to signal), which its doc argues must count as alive.
// Without this test, mutating `default: return true` to `false` leaves the
// suite green.
//
// pid 1 is always running and, unless this runs as root, never ours, so
// signalling it with sig 0 returns EPERM rather than ESRCH: a stronger
// guarantee than synthesizing EPERM another way, and no process to spawn.
func TestPidAliveTreatsEPERMAsAlive(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root: Kill(1, 0) succeeds for root, so EPERM is not reachable this way")
	}
	if !pidAlive(1) {
		t.Fatal("pidAlive(1) = false, want true — EPERM (process exists, just isn't ours) must not be treated as dead")
	}
}
