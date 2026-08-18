//go:build unix

package engine_test

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

// TestWidePlanDoesNotExhaustFileDescriptors is a regression test for
// runStep's per-attempt log-writer Close calls: without them descriptor use
// scales with PLAN SIZE rather than MaxParallel, and nothing else in the
// tree runs a plan wide enough to notice. RLIMIT_NOFILE is lowered for the
// duration so the property holds on any host: a correct run needs a couple
// of dozen descriptors regardless of node count, a leaking one two per node.
// Tests in this package never call t.Parallel, so nothing else runs under
// the reduced limit.
func TestWidePlanDoesNotExhaustFileDescriptors(t *testing.T) {
	const (
		nodes = 300
		limit = 160
	)

	var orig syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
		t.Skipf("Getrlimit: %v", err)
	}
	if orig.Cur <= limit {
		t.Skipf("ambient RLIMIT_NOFILE is already %d; lowering it would prove nothing", orig.Cur)
	}
	lowered := orig
	lowered.Cur = limit
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lowered); err != nil {
		t.Skipf("Setrlimit: %v", err)
	}
	t.Cleanup(func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
			t.Errorf("restoring RLIMIT_NOFILE: %v", err)
		}
	})

	p := &plan.Plan{Version: 1}
	for i := 0; i < nodes; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("n%03d", i), Kind: "exec", Cmd: []string{"true"},
		})
	}

	status, _, dir := run(t, p)
	if status != api.RunSucceeded {
		// Name the first non-succeeded step: with the leak it is an
		// "open ...: too many open files" from LogSet.Writer, which runStep
		// reports as a failed step rather than as an engine error.
		st := foldStates(t, dir)
		for i := 0; i < nodes; i++ {
			id := fmt.Sprintf("n%03d", i)
			if st[id] != api.StateSucceeded {
				t.Fatalf("status = %s: %s = %s — descriptors are leaking per node, "+
					"so a wide plan runs the process out of them", status, id, st[id])
			}
		}
		t.Fatalf("status = %s, want succeeded", status)
	}
}

// TestHandlersDoNotExhaustFileDescriptors is the handler-shaped half of the
// test above and not redundant with it: execHandler opens its own sandbox
// and log writers on a path no wide-plan test reached, and handlers
// multiply the exposure (plan size times handler count). The failure shows
// as handler.failed rather than a failed run, since a handler's outcome
// never reaches its parent's state, which is why counting those events is
// the only way to see this.
func TestHandlersDoNotExhaustFileDescriptors(t *testing.T) {
	const (
		nodes = 60
		limit = 160
	)

	var orig syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
		t.Skipf("Getrlimit: %v", err)
	}
	if orig.Cur <= limit {
		t.Skipf("ambient RLIMIT_NOFILE is already %d; lowering it would prove nothing", orig.Cur)
	}
	lowered := orig
	lowered.Cur = limit
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lowered); err != nil {
		t.Skipf("Setrlimit: %v", err)
	}
	t.Cleanup(func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
			t.Errorf("restoring RLIMIT_NOFILE: %v", err)
		}
	})

	p := &plan.Plan{Version: 1}
	for i := 0; i < nodes; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("n%02d", i), Kind: "exec", Cmd: []string{"false"},
			OnFailure: []plan.Node{
				{ID: "f1", Kind: "exec", Cmd: []string{"true"}},
				{ID: "f2", Kind: "exec", Cmd: []string{"true"}},
			},
			Always: []plan.Node{
				{ID: "a1", Kind: "exec", Cmd: []string{"true"}},
				{ID: "a2", Kind: "exec", Cmd: []string{"true"}},
			},
		})
	}

	dir := t.TempDir()
	_, events, _ := runPlan(t, dir, p)

	var started int
	var firstFailure string
	for _, e := range events {
		switch e.Type {
		case api.HandlerStarted:
			started++
		case api.HandlerFailed:
			if firstFailure == "" {
				var b api.HandlerBody
				_ = e.Decode(&b)
				firstFailure = fmt.Sprintf("%s: %s", e.Step, b.Error)
			}
		}
	}
	if want := 4 * nodes; started != want {
		t.Errorf("%d handler.started events, want %d", started, want)
	}
	if firstFailure != "" {
		t.Fatalf("a handler failed under a lowered descriptor limit: %s\n"+
			"every handler here runs `true`, so this is the engine running out of "+
			"descriptors: they are leaking per handler run", firstFailure)
	}
}

// TestIdleSchedulerDoesNotSpin is a regression test for schedule's `<-wake`
// block: without it the idle loop re-ran immediately at ~0.97 of a core,
// and the whole suite stayed green because a spin changes only CPU. A slow
// root with a queued dependent gives a ~1.5s window with nothing to do, and
// CPU is measured directly across it, with ~30x headroom on the healthy
// side and 3x on the broken one.
func TestIdleSchedulerDoesNotSpin(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "slow", Kind: "exec", Cmd: []string{"sleep", "1.5"}},
		{ID: "after", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"slow"}},
	}}

	before := cpuTime(t)
	start := time.Now()
	status, _, _ := run(t, p)
	wall := time.Since(start)
	used := cpuTime(t) - before

	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}
	if wall < time.Second {
		t.Fatalf("the run finished in %s — the idle window this measures never happened", wall)
	}
	if used > 500*time.Millisecond {
		t.Errorf("the scheduler burned %s of CPU across a %s run that was idle almost "+
			"throughout — the idle pass is spinning instead of waiting for a step to "+
			"report back", used, wall)
	}
}

// cpuTime is this process's user+system CPU time. Tests in this package never
// call t.Parallel, so a delta across one test is that test's own consumption.
func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Skipf("Getrusage: %v", err)
	}
	tv := func(v syscall.Timeval) time.Duration {
		return time.Duration(v.Sec)*time.Second + time.Duration(v.Usec)*time.Microsecond
	}
	return tv(ru.Utime) + tv(ru.Stime)
}
