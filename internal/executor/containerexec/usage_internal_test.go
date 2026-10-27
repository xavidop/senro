package containerexec

import (
	"testing"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
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
