package containerexec

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// foldSample keeps the LAST cpu figures and the PEAK memory, net of the
// inactive page cache, and falls back to total_usage as user time when the
// daemon does not split it (cgroup v2). The zeroed sample a daemon sends for
// a stopped container erases nothing.
func TestFoldSampleKeepsLastCPUAndPeakMemory(t *testing.T) {
	var u senroexec.ResourceUsage
	var ok bool
	for _, s := range []dockerd.StatsSample{
		{CPUTotal: 500, CPUUser: 300, CPUKernel: 200, MemUsage: 10_000, MemInactiveFile: 4_000},
		{CPUTotal: 900, CPUUser: 600, CPUKernel: 300, MemUsage: 7_000},
		{CPUTotal: 950, CPUUser: 640, CPUKernel: 310, MemUsage: 5_000, MemInactiveFile: 1_000},
		{}, // the stopped container's empty sample
	} {
		u, ok = foldSample(u, ok, s)
	}
	if !ok {
		t.Fatal("ok = false after real samples")
	}
	want := senroexec.ResourceUsage{CPUUser: 640, CPUSystem: 310, MaxRSS: 7_000}
	if u != want {
		t.Errorf("usage = %+v, want %+v", u, want)
	}

	u, ok = foldSample(senroexec.ResourceUsage{}, false, dockerd.StatsSample{CPUTotal: 1234, MemUsage: 99})
	if !ok || u.CPUUser != 1234 || u.CPUSystem != 0 || u.MaxRSS != 99 {
		t.Errorf("cgroup v2 sample folded to %+v ok=%v, want total as user", u, ok)
	}

	if _, ok := foldSample(senroexec.ResourceUsage{}, false, dockerd.StatsSample{}); ok {
		t.Error("an empty sample alone reported ok")
	}
}

// Once the container is known to have exited, the empty sample the daemon
// sends for a stopped container ends the stream; before that, nothing does.
func TestStatsFollowingStopsOnTheEmptySampleAfterExit(t *testing.T) {
	if !keepFollowing(false, dockerd.StatsSample{}) {
		t.Error("an empty sample before exit stopped the stream")
	}
	if !keepFollowing(true, dockerd.StatsSample{MemUsage: 1}) {
		t.Error("a real sample after exit stopped the stream")
	}
	if keepFollowing(true, dockerd.StatsSample{}) {
		t.Error("the empty sample after exit did not stop the stream")
	}
}

// The stats stream of a stopped container is not ended by the daemon (only
// removal ends it, and Close removes after Run returns), so Run must cut it
// itself instead of waiting out logDrainGrace. Skips without a daemon.
func TestRunDoesNotWaitOutTheDrainGraceForTheStatsStream(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := New(plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image},
		workspace.NewSnapshotter(store), WithClient(c), WithRunID("stats-drain"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{StepID: "sleepy", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	// Timed from the container's exit, not from Run's start: create and
	// start on a loaded daemon can take seconds by themselves, and that is
	// not the wait this test is about.
	start := time.Now()
	at := map[string]time.Duration{}
	var phases []string
	runPhase = func(phase string) {
		at[phase] = time.Since(start)
		phases = append(phases, fmt.Sprintf("%s at %s", phase, at[phase].Round(time.Millisecond)))
	}
	defer func() { runPhase = nil }()
	code, err := sb.Run(ctx, senroexec.Cmd{Args: []string{"sh", "-c", "sleep 1"}}, io.Discard, io.Discard)
	if err != nil || code != 0 {
		t.Fatalf("Run = %d, %v", code, err)
	}
	waited, stats := at["waited"], at["stats"]
	if waited == 0 || stats == 0 {
		t.Fatalf("Run did not pass both the waited and stats phases: %s", strings.Join(phases, ", "))
	}
	if drain, limit := stats-waited, logDrainGrace-time.Second; drain >= limit {
		t.Errorf("the drain after exit took %s (limit %s): the stats stream was waited out, not cut; phases: %s",
			drain.Round(time.Millisecond), limit, strings.Join(phases, ", "))
	}
}
