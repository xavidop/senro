package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// runToEvents runs p to completion with the local executor and a no-op sink,
// then reads back the ledger it wrote: the durable record, not whatever a
// sink happened to observe. Modeled on golden_test.go's own Run calls and on
// this package's readLedger.
func runToEvents(t *testing.T, p *plan.Plan) []api.Event {
	t.Helper()
	return runToEventsWithParallel(t, p, 4)
}

// runToEventsWithParallel is runToEvents with the run's global MaxParallel
// under the caller's control, for TestAGroupsMaxParallelBoundsItsChildren:
// the global limit is set well above the group's own so only the group's
// semaphore can produce the observed ceiling.
func runToEventsWithParallel(t *testing.T, p *plan.Plan, maxParallel int) []api.Event {
	t.Helper()
	dir := t.TempDir()
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        sink.Nop(),
		MaxParallel: maxParallel,
		RunID:       "01GROUPTEST",
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	return readLedger(t, dir)
}

// maxRecordedPeak reads every peak.* file TestAGroupsMaxParallelBoundsItsChildren's
// script leaves behind and returns the largest recorded value: the peak
// concurrency actually observed across every child.
func maxRecordedPeak(t *testing.T, dir string) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "peak.*"))
	if err != nil {
		t.Fatalf("glob peak files: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("no peak files were recorded; nothing ran")
	}
	peak := 0
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		n, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil {
			t.Fatalf("parse %s (%q): %v", m, b, err)
		}
		if n > peak {
			peak = n
		}
	}
	return peak
}

// TestAnExpansionIsAnnouncedBeforeItsChildrenAreCreated pins the order a
// client depends on: api.RunState.Apply materialises a group's children on
// plan.expanded so a renderer can show "37 units" before a single
// step.created arrives (see its own doc). An engine that emitted step.created
// first would make that materialisation pointless and would flash the
// children as ungrouped.
func TestAnExpansionIsAnnouncedBeforeItsChildrenAreCreated(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint", MaxParallel: 2}},
		Nodes: []plan.Node{
			{ID: "lint[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "lint"},
			{ID: "lint[unit=b]", Kind: "exec", Cmd: []string{"true"}, Group: "lint"},
			{ID: "publish", Kind: "exec", Cmd: []string{"true"},
				Needs: []string{"lint[unit=a]", "lint[unit=b]"}},
		},
	}
	events := runToEvents(t, p)

	expandedAt, firstCreatedAt := -1, -1
	for i, e := range events {
		switch e.Type {
		case api.PlanExpanded:
			if expandedAt < 0 {
				expandedAt = i
			}
			var b api.PlanExpandedBody
			if err := e.Decode(&b); err != nil {
				t.Fatal(err)
			}
			if b.Parent != "lint" || len(b.Children) != 2 || b.Count != 2 {
				t.Errorf("plan.expanded body = %+v", b)
			}
			if b.Children[0] != "lint[unit=a]" || b.Children[1] != "lint[unit=b]" {
				t.Errorf("children are not in plan order: %v", b.Children)
			}
			if e.Step != "lint" {
				t.Errorf("plan.expanded routed to step %q, want the group", e.Step)
			}
		case api.StepCreated:
			if firstCreatedAt < 0 {
				firstCreatedAt = i
			}
		}
	}
	if expandedAt < 0 {
		t.Fatal("no plan.expanded event")
	}
	if firstCreatedAt < expandedAt {
		t.Fatalf("step.created (%d) came before plan.expanded (%d)", firstCreatedAt, expandedAt)
	}
}

// TestEveryEventForAChildCarriesItsGroup proves the event stream carries a
// group field so a client can aggregate without knowing the plan structure.
// Tagging at each emit site would mean tagging a dozen of
// them and missing the thirteenth, so it happens in runCore.append, and this
// test checks a HANDLER's events too, because those are routed under a
// composite id no node-keyed lookup would match.
func TestEveryEventForAChildCarriesItsGroup(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint"}},
		Nodes: []plan.Node{{
			ID: "lint[unit=a]", Kind: "exec", Cmd: []string{"sh", "-c", "echo out; exit 1"},
			Group:     "lint",
			OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"true"}}},
		}},
	}
	events := runToEvents(t, p)

	var childEvents, handlerEvents int
	for _, e := range events {
		switch {
		case e.Step == "lint[unit=a]":
			childEvents++
			if e.Group != "lint" {
				t.Errorf("%s for the child carries group %q", e.Type, e.Group)
			}
		case strings.HasPrefix(e.Step, "lint[unit=a]/"):
			handlerEvents++
			if e.Group != "lint" {
				t.Errorf("%s for the child's handler carries group %q", e.Type, e.Group)
			}
		}
	}
	if childEvents == 0 || handlerEvents == 0 {
		t.Fatalf("saw %d child and %d handler events; this test proves nothing", childEvents, handlerEvents)
	}
}

func TestAnEmptyExpansionIsReportedRatherThanIgnored(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint"}},
		Nodes:   []plan.Node{{ID: "other", Kind: "exec", Cmd: []string{"true"}}},
	}
	events := runToEvents(t, p)
	for _, e := range events {
		if e.Type != api.PlanExpansionSkipped {
			continue
		}
		var b api.PlanExpansionSkippedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if b.Parent != "lint" || b.Reason == "" {
			t.Fatalf("body = %+v", b)
		}
		return
	}
	t.Fatal("an expansion that produced no children was silent; a mistyped glob has no other symptom")
}

// TestARunWithNoGroupsEmitsNoExpansionEvents is the negative case: a plan
// that never calls Expand must not gain a phantom plan.expanded or a stray
// Group on an ordinary step's events. It is also the only test here that
// exercises append's `rc.groups != nil` guard, since buildGroupIndex
// returns nil for an ungrouped plan and every other test declares a group.
func TestARunWithNoGroupsEmitsNoExpansionEvents(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"true"}},
	}}
	events := runToEvents(t, p)
	for _, e := range events {
		if e.Type == api.PlanExpanded || e.Type == api.PlanExpansionSkipped {
			t.Errorf("a plan with no Groups produced %s", e.Type)
		}
		if e.Step == "build" && e.Group != "" {
			t.Errorf("ungrouped step %q carries group %q", e.Step, e.Group)
		}
	}
}

// TestAGroupWhoseNodesAllSkipStillReportsItsExpansion: the members exist in
// the plan (unlike the empty-expansion case above) but all skip behind a
// failed shared upstream. plan.expanded must still announce them, or a
// renderer shows orphaned step.finished(skipped) events with no group
// heading. Each child's step.finished must carry the group too: for a
// skipped step those are the only events a client ever sees.
func TestAGroupWhoseNodesAllSkipStillReportsItsExpansion(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint"}},
		Nodes: []plan.Node{
			{ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
			{ID: "lint[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "lint", Needs: []string{"boom"}},
			{ID: "lint[unit=b]", Kind: "exec", Cmd: []string{"true"}, Group: "lint", Needs: []string{"boom"}},
		},
	}
	events := runToEvents(t, p)

	var sawExpanded bool
	finished := map[string]api.Event{}
	for _, e := range events {
		if e.Type == api.PlanExpanded {
			sawExpanded = true
		}
		if e.Type == api.StepFinished && (e.Step == "lint[unit=a]" || e.Step == "lint[unit=b]") {
			finished[e.Step] = e
		}
	}
	if !sawExpanded {
		t.Fatal("no plan.expanded event for a group whose children all skipped")
	}
	if len(finished) != 2 {
		t.Fatalf("saw %d step.finished for the group's children, want 2", len(finished))
	}
	for id, e := range finished {
		var b api.StepFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if b.State != api.StateSkippedUpstreamFailed {
			t.Errorf("%s state = %s, want skipped_upstream_failed", id, b.State)
		}
		if e.Group != "lint" {
			t.Errorf("%s step.finished carries group %q, want lint", id, e.Group)
		}
	}
}

// TestTwoGroupsDoNotBlockEachOther is the concurrency half of the semaphore
// contract: two groups each capped at MaxParallel(1) must still run one
// child from EACH at once, since their permits are independent channels. A
// shared "any group" semaphore, or acquiring the global permit before the
// group's own, shows up here as children never overlapping. Proven with
// real concurrent execution, not by inspecting how groupSem is built.
func TestTwoGroupsDoNotBlockEachOther(t *testing.T) {
	dir := t.TempDir()
	running := filepath.Join(dir, "running.*")
	script := "id=$$; : > " + filepath.Join(dir, "running.$id") + "; sleep 0.3; " +
		"c=$(ls " + running + " 2>/dev/null | wc -l); echo $c > " + filepath.Join(dir, "peak.$id") + "; " +
		"rm -f " + filepath.Join(dir, "running.$id")

	p := &plan.Plan{
		Version: 1,
		Groups: []plan.GroupSpec{
			{Name: "a", MaxParallel: 1},
			{Name: "b", MaxParallel: 1},
		},
		Nodes: []plan.Node{
			{ID: "a[unit=1]", Kind: "exec", Group: "a", Cmd: []string{"sh", "-c", script}},
			{ID: "a[unit=2]", Kind: "exec", Group: "a", Cmd: []string{"sh", "-c", script}},
			{ID: "b[unit=1]", Kind: "exec", Group: "b", Cmd: []string{"sh", "-c", script}},
			{ID: "b[unit=2]", Kind: "exec", Group: "b", Cmd: []string{"sh", "-c", script}},
		},
	}

	runToEventsWithParallel(t, p, 8)

	peak := maxRecordedPeak(t, dir)
	if peak < 2 {
		t.Errorf("peak concurrency across the two MaxParallel(1) groups was %d; they never overlapped, "+
			"which means their semaphores are not independent of one another", peak)
	}
}

// TestAGroupsMaxParallelBoundsItsChildren proves the per-group semaphore
// does something, with the global limit set higher so only the group's own
// limit can produce the observed ceiling. Each child creates a file named
// after its PID, sleeps, counts how many such files exist, and removes its
// own: the count is exactly how many children are in the critical section
// at that moment.
//
// A shared append-only counter file cannot work here: it only grows, so its
// "peak" is a running total across every child that ever ran, and it
// reported 2, 4, 6 against a correctly enforced MaxParallel(2). This
// per-PID version was verified against a serialized run (reports 1) and an
// unbounded one (reports 6) before being used here.
func TestAGroupsMaxParallelBoundsItsChildren(t *testing.T) {
	dir := t.TempDir()
	running := filepath.Join(dir, "running.*")
	script := "id=$$; : > " + filepath.Join(dir, "running.$id") + "; sleep 0.3; " +
		"c=$(ls " + running + " 2>/dev/null | wc -l); echo $c > " + filepath.Join(dir, "peak.$id") + "; " +
		"rm -f " + filepath.Join(dir, "running.$id")

	var nodes []plan.Node
	for _, u := range []string{"a", "b", "c", "d", "e", "f"} {
		nodes = append(nodes, plan.Node{
			ID: "lint[unit=" + u + "]", Kind: "exec", Group: "lint",
			Cmd: []string{"sh", "-c", script},
		})
	}
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "lint", MaxParallel: 2}}, Nodes: nodes}

	runToEventsWithParallel(t, p, 8)

	peak := maxRecordedPeak(t, dir)
	if peak > 2 {
		t.Errorf("as many as %d children ran at once under MaxParallel(2)", peak)
	}
	if peak < 2 {
		t.Errorf("peak concurrency was %d, so the run was serial and this test proves nothing about the limit", peak)
	}
}
