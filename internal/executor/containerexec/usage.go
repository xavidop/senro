package containerexec

import (
	"context"
	"math"
	"sync/atomic"
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

// statsTailGrace is how long Run lets the stats stream deliver one more
// sample once the container has exited, before cutting it.
const statsTailGrace = 300 * time.Millisecond

// followStats reads the container's stats stream, folding each sample into
// the sandbox's usage under its mutex, until ctx is cancelled or, once
// exited reports true, the daemon sends the empty sample of a stopped
// container (see keepFollowing).
//
// The daemon does NOT end a stream=true subscription when the container
// stops, only when it is removed, and Close removes it after Run returns;
// so ending the stream is Run's job, by cancelling ctx (see Run). The
// returned channel closes when the goroutine is done.
func (s *sandbox) followStats(ctx context.Context, id string, exited *atomic.Bool) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The stream's error is not the step's verdict: a stats stream cut
		// short only leaves the figures less complete.
		_ = s.ex.cli.ContainerStats(ctx, id, func(smp dockerd.StatsSample) bool {
			s.mu.Lock()
			s.usage, s.usageOK = foldSample(s.usage, s.usageOK, smp)
			s.mu.Unlock()
			return keepFollowing(exited.Load(), smp)
		})
	}()
	return done
}

// keepFollowing is false only for an all-zero sample (the daemon fills in
// just the name and id for a stopped container) after the container is
// known to have exited: nothing after it can carry new figures. Before the
// exit is known an empty sample is not trusted to mean that, since a
// container can be sampled before its cgroup reports anything.
func keepFollowing(exited bool, smp dockerd.StatsSample) bool {
	return !exited || smp != (dockerd.StatsSample{})
}

// foldSample folds one stats sample into u: CPU from the last sample that
// has any (the daemon's counters are cumulative, so this is the latest
// figure, and an all-zero sample, which a stopped container produces, must
// not erase it), and memory as the
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
