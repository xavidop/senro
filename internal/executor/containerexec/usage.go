package containerexec

import (
	"context"
	"math"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
)

// Usage implements senroexec.UsageReporter: the CPU and peak memory of the
// most recent Run's container, as sampled from the daemon's stats stream.
// ok is false when the stream produced no non-empty sample, which is what a
// container that exits before the daemon's first sample (about a second)
// looks like.
func (s *sandbox) Usage() (senroexec.ResourceUsage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage, s.usageOK
}

func (s *sandbox) resetUsage() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage, s.usageOK = senroexec.ResourceUsage{}, false
}

// followStats reads the container's stats stream until it ends, which the
// daemon does when the container stops, or until ctx is cancelled. Each
// sample is folded into the sandbox's usage under its mutex. The returned
// channel closes when the goroutine is done.
func (s *sandbox) followStats(ctx context.Context, id string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The stream's error is not the step's verdict: a stats stream cut
		// short only leaves the figures less complete.
		_ = s.ex.cli.ContainerStats(ctx, id, func(smp dockerd.StatsSample) bool {
			s.mu.Lock()
			s.usage, s.usageOK = foldSample(s.usage, s.usageOK, smp)
			s.mu.Unlock()
			return true
		})
	}()
	return done
}

// foldSample folds one stats sample into u: CPU from the last sample that
// has any (the daemon's counters are cumulative, and the empty sample it
// sends for a stopped container must not erase them), and memory as the
// peak of usage less the inactive page cache. CPU falls back to total_usage
// as user time when the daemon does not split it, as on cgroup v2.
func foldSample(u senroexec.ResourceUsage, ok bool, smp dockerd.StatsSample) (senroexec.ResourceUsage, bool) {
	user, sys := smp.CPUUser, smp.CPUKernel
	if user == 0 && sys == 0 {
		user = smp.CPUTotal
	}
	if user != 0 || sys != 0 {
		u.CPUUser = time.Duration(clampInt64(user))
		u.CPUSystem = time.Duration(clampInt64(sys))
		ok = true
	}
	mem := smp.MemUsage
	if smp.MemInactiveFile < mem {
		mem -= smp.MemInactiveFile
	}
	if m := clampInt64(mem); m > u.MaxRSS {
		u.MaxRSS = m
		ok = true
	}
	return u, ok
}

func clampInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}
