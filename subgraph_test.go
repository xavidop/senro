package senro_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
)

func init() {
	// Registered once, in init, because funcs.Register panics on a duplicate
	// name and a test body can run more than once under -count.
	senro.RegisterFunc("test/rolling", func(ctx senro.Ctx, p struct {
		Marker string `json:"marker"`
	}) error {
		f := senro.NewFragment()
		f.Step("apply", exec.Command("touch", p.Marker))
		return senro.RunSubgraph(ctx, f)
	})

	senro.RegisterFunc("test/rolling-fails", func(ctx senro.Ctx, _ struct{}) error {
		f := senro.NewFragment()
		f.Step("boom", exec.Command("false"))
		return senro.RunSubgraph(ctx, f)
	})
}

// The imperative escape hatch: some control flow genuinely is not a DAG
// ("roll out one at a time until quorum, then stop"), and for that a func
// step runs a graph itself rather than describing one.
func TestRunSubgraphRunsANestedGraphFromInsideAFuncStep(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "nested-ran")

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("rolling", senro.Func("test/rolling", struct {
		Marker string `json:"marker"`
	}{Marker: marker}))

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(dir, "run"))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the nested subgraph never ran: %v", err)
	}
}

// A subgraph that fails is the step's failure. The function asked for that
// work; if it could not be done, the function did not do its job, and
// swallowing it would let a rollout report success having deployed nothing.
func TestAFailingSubgraphFailsTheStepThatRanIt(t *testing.T) {
	dir := t.TempDir()

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("rolling", senro.Func("test/rolling-fails", struct{}{}))

	err := senro.Run(context.Background(), pipe, senro.WithDir(filepath.Join(dir, "run")))
	if err == nil {
		t.Fatal("a subgraph whose step failed must fail the func step that ran it")
	}
	if !strings.Contains(err.Error(), "rolling") {
		t.Errorf("error %q must name the step that ran the subgraph", err)
	}
}
