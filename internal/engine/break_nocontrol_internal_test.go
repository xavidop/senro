package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// With nothing attached no breakpoint.clear can ever arrive, so
// BreakOnFailure warns once and does not hold: the failing step settles and
// the run ends instead of hanging until it is killed.
func TestBreakOnFailureWithNoControlChannelWarnsAndDoesNotHold(t *testing.T) {
	var warned bytes.Buffer
	prev := warnOut
	warnOut = &warned
	t.Cleanup(func() { warnOut = prev })

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"sh", "-c", "exit 3"}},
		{ID: "b", Kind: "exec", Cmd: []string{"sh", "-c", "exit 4"}},
	}}
	dir := t.TempDir()
	rec := sink.Recording()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := Run(ctx, p, Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec, RunID: "01NOCONTROL",
		MaxParallel: 2, BreakOnFailure: true,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the run hung: a step was held with nothing able to release it")
	}
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}
	for _, e := range rec.Events() {
		if e.Type == api.BreakpointHit {
			t.Errorf("a step was held: %+v", e)
		}
	}
	if got := strings.Count(warned.String(), noControlForBreakOnFailure); got != 1 {
		t.Errorf("warning printed %d times, want once:\n%s", got, warned.String())
	}
}
