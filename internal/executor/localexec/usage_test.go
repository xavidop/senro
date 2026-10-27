//go:build unix

package localexec_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/executor"
)

func usageOf(t *testing.T, sb executor.Sandbox) (executor.ResourceUsage, bool) {
	t.Helper()
	ur, ok := sb.(executor.UsageReporter)
	if !ok {
		t.Fatal("localexec sandbox does not implement executor.UsageReporter")
	}
	return ur.Usage()
}

// A command that ran reports its peak memory and its CPU, read after Run has
// returned. Maxrss is kilobytes on Linux and bytes on Darwin; either way the
// figure must come out in bytes, which a 20 MB pipe through head/tail puts
// comfortably above a kilobyte-sized misreading.
func TestRunReportsResourceUsage(t *testing.T) {
	sb := newSandbox(t)
	if _, ok := usageOf(t, sb); ok {
		t.Error("Usage() ok before any Run")
	}
	exit, err := sb.Run(context.Background(), executor.Cmd{Args: []string{
		"sh", "-c", "head -c 20000000 /dev/zero | tail -c 1 >/dev/null",
	}}, io.Discard, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	u, ok := usageOf(t, sb)
	if !ok {
		t.Fatal("Usage() not ok after Run")
	}
	if u.MaxRSS < 64*1024 {
		t.Errorf("MaxRSS = %d bytes, want a plausible process size (kilobytes not normalised?)", u.MaxRSS)
	}
	if u.CPUUser < 0 || u.CPUSystem < 0 {
		t.Errorf("negative CPU: %+v", u)
	}
}

// A workload killed by cancellation still consumed something, and says so.
func TestAKilledRunStillReportsUsage(t *testing.T) {
	sb := newSandbox(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	t.Cleanup(cancel)
	if _, err := sb.Run(ctx, executor.Cmd{Args: []string{"sleep", "30"}}, io.Discard, io.Discard); err == nil {
		t.Fatal("cancelled Run returned no error")
	}
	u, ok := usageOf(t, sb)
	if !ok {
		t.Fatal("Usage() not ok after a killed Run")
	}
	if u.MaxRSS <= 0 {
		t.Errorf("MaxRSS = %d, want > 0", u.MaxRSS)
	}
}

// A program that never started measured nothing, and must not report the
// previous Run's figures as its own.
func TestARunThatNeverStartedReportsNoUsage(t *testing.T) {
	sb := newSandbox(t)
	if _, err := sb.Run(context.Background(), executor.Cmd{Args: []string{"true"}}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	exit, _ := sb.Run(context.Background(), executor.Cmd{Args: []string{"/definitely/not/here"}}, io.Discard, io.Discard)
	if exit != 127 {
		t.Fatalf("exit = %d, want 127", exit)
	}
	if u, ok := usageOf(t, sb); ok {
		t.Errorf("Usage() = %+v, ok after a Run that never started", u)
	}
}
