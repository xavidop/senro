package containerexec_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
)

// A short container run reports usage once Run returns. The command lasts a
// little over a second because the daemon samples about once a second, and a
// container gone before the first sample has nothing to report.
func TestRunReportsTheContainersResourceUsage(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "usage", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	ur, ok := sb.(executor.UsageReporter)
	if !ok {
		t.Fatal("containerexec sandbox does not implement executor.UsageReporter")
	}
	code, err := sb.Run(ctx, executor.Cmd{Args: []string{
		"sh", "-c", "head -c 20000000 /dev/zero | tail -c 1 >/dev/null; sleep 2",
	}}, io.Discard, io.Discard)
	if err != nil || code != 0 {
		t.Fatalf("Run = %d, %v", code, err)
	}
	u, ok := ur.Usage()
	if !ok {
		t.Fatal("Usage() not ok after a two-second container run")
	}
	if u.MaxRSS <= 0 || u.CPUUser < 0 || u.CPUSystem < 0 {
		t.Errorf("usage = %+v", u)
	}
}
