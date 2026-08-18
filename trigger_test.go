package senro_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/trigger"
)

// triggerPipeline is a one-step pipeline that succeeds, and records the
// params it was run with by writing them where the test can read them back.
func triggerPipeline(t *testing.T, out string) *senro.Pipeline {
	t.Helper()
	pipe := senro.New("triggered")
	pipe.Workflow("w").Step("touch", exec.Command("sh", "-c", "echo ran > "+out))
	return pipe
}

// runRoot is a directory that stands in for the run root, so a test can list
// it before and after and see exactly what a run left behind.
func runRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "runs")
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func pushEvent(branch string) *trigger.Event {
	return &trigger.Event{
		Kind: trigger.Push, Provider: "github", Repo: "acme/app",
		Ref: "refs/heads/" + branch, Branch: branch, DefaultBranch: "main",
		Files: []string{"services/api/main.go"},
		Base:  trigger.Base{From: "aaaa", To: "bbbb"},
	}
}

// TestRunDoesNothingAtAllWhenNoTriggerMatched is the load-bearing claim: a
// no-match is inert. The run root is listed before and after, and it must be
// identical, because a dispatcher that fires this binary for every push to
// every branch would otherwise fill a disk with the skeletons of runs that
// never happened.
func TestRunDoesNothingAtAllWhenNoTriggerMatched(t *testing.T) {
	root := runRoot(t)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	marker := filepath.Join(t.TempDir(), "ran")
	before := listDir(t, root)

	err := senro.Run(context.Background(), triggerPipeline(t, marker),
		senro.WithDir(filepath.Join(root, "should-not-exist")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pushEvent("somebodys-branch"),
			trigger.OnPush(trigger.Branches("main"))))

	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Run = %v, want trigger.ErrNoMatch", err)
	}
	if after := listDir(t, root); len(after) != len(before) {
		t.Errorf("the run root changed: before %v, after %v", before, after)
	}
	if _, statErr := os.Stat(filepath.Join(root, "should-not-exist")); !os.IsNotExist(statErr) {
		t.Error("a no-match created a run directory")
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Error("a no-match ran a step")
	}
}

// TestRunReturnsNoMatchAsAnErrorRatherThanExiting. The whole reason the
// contract is a sentinel and not an os.Exit: this test process is still
// alive to make the assertion.
func TestRunReturnsNoMatchAsAnErrorRatherThanExiting(t *testing.T) {
	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(filepath.Join(t.TempDir(), "run")),
		senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pushEvent("wip"), trigger.OnPush(trigger.Branches("main"))))

	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Run = %v, want trigger.ErrNoMatch", err)
	}
	// It is not a RunError: no run reached a terminal state, because no run
	// started.
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Errorf("Run reported a no-match as a *RunError with status %q", runErr.Status)
	}
	if !strings.HasPrefix(err.Error(), "senro: ") {
		t.Errorf("the error must read like this package's others; got %q", err)
	}
}

// TestRunRunsWhenATriggerMatched, and the run is an ordinary one: the step
// ran and the ledger is there.
func TestRunRunsWhenATriggerMatched(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	marker := filepath.Join(t.TempDir(), "ran")

	err := senro.Run(context.Background(), triggerPipeline(t, marker),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pushEvent("main"), trigger.OnPush(trigger.Branches("main"))))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Errorf("the step did not run: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "events.jsonl")); statErr != nil {
		t.Errorf("no ledger: %v", statErr)
	}
}

// TestRunWithNoTriggerOptionGatesNothing: every pipeline written before this
// existed still runs, unchanged.
func TestRunWithNoTriggerOptionGatesNothing(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	err := senro.Run(context.Background(), triggerPipeline(t, marker),
		senro.WithDir(filepath.Join(t.TempDir(), "run")), senro.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Errorf("the step did not run: %v", statErr)
	}
}

// TestRunWithATriggerButNoEventRuns is the local loop: ./pipeline with no
// --trigger-event builds everything. A dispatcher that forgets the flag
// over-runs, which somebody notices, rather than never running, which
// nobody does.
func TestRunWithATriggerButNoEventRuns(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	err := senro.Run(context.Background(), triggerPipeline(t, marker),
		senro.WithDir(filepath.Join(t.TempDir(), "run")), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(nil, trigger.OnPush(trigger.Branches("main"))))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Errorf("the step did not run: %v", statErr)
	}
}

// TestRunReportsAWiringErrorDifferentlyFromANoMatch is the distinction the
// whole design turns on: "not my business" and "somebody wired this wrong"
// must not be the same exit code.
func TestRunReportsAWiringErrorDifferentlyFromANoMatch(t *testing.T) {
	root := runRoot(t)
	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(filepath.Join(root, "r")), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pushEvent("main"), trigger.OnTag(trigger.Semver("not-a-constraint"))))

	if err == nil {
		t.Fatal("Run accepted a trigger whose constraint does not parse")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Run reported a wiring error as a no-match: %v", err)
	}
	// A refused declaration is as inert as a no-match: nothing started.
	if names := listDir(t, root); len(names) != 0 {
		t.Errorf("a refused declaration left %v in the run root", names)
	}
}

// TestAMatchedTriggersParamsReachTheRun, which is what lets a scheduled run
// say "this one is the full suite" and a condition read it.
func TestAMatchedTriggersParamsReachTheRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	out := filepath.Join(t.TempDir(), "params")

	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("gate", exec.Command("sh", "-c", "echo gated > "+out)).
		When(senro.ParamIs("suite", "full"))

	ev := &trigger.Event{Kind: trigger.Schedule, Schedule: "0 3 * * *"}
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trigger.OnSchedule("0 3 * * *", trigger.Params{"suite": "full"})))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, statErr := os.Stat(out); statErr != nil {
		t.Errorf("the step guarded by ParamIs(suite, full) did not run: %v", statErr)
	}
}

// TestTheEventsBranchBecomesTheBranchParam connects a trigger to the
// condition senro already had. Without it, every trigger-driven pipeline
// would have to pass --param branch=... alongside the event that already
// said so.
func TestTheEventsBranchBecomesTheBranchParam(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	out := filepath.Join(t.TempDir(), "onmain")

	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("deploy", exec.Command("sh", "-c", "echo deployed > "+out)).
		When(senro.Branch("main"))

	err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pushEvent("main"), trigger.OnPush()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, statErr := os.Stat(out); statErr != nil {
		t.Errorf("senro.Branch(main) did not see the event's branch: %v", statErr)
	}
}

// TestWithParamsWinsOverATriggersOwn: a caller can override a trigger's
// answer without editing the trigger.
func TestWithParamsWinsOverATriggersOwn(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	out := filepath.Join(t.TempDir(), "smoke")

	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("smoke", exec.Command("sh", "-c", "echo smoked > "+out)).
		When(senro.ParamIs("suite", "smoke"))

	ev := &trigger.Event{Kind: trigger.Schedule, Schedule: "0 3 * * *"}
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trigger.OnSchedule("0 3 * * *", trigger.Params{"suite": "full"})),
		senro.WithParams(senro.Params{"suite": "smoke"}))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, statErr := os.Stat(out); statErr != nil {
		t.Errorf("WithParams did not override the trigger's own: %v", statErr)
	}
}

// TestTheRunManifestRecordsWhatTriggeredTheRun. A run that nobody can
// attribute afterwards is a run nobody can trust, and the ledger is the
// wrong place for it: run.started is a published schema pinned by golden
// fixtures, and provenance is not something a client folds into a RunState.
func TestTheRunManifestRecordsWhatTriggeredTheRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")

	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(dir), senro.WithRunID("20260812T101500-abcdef0123"),
		senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pushEvent("main"), trigger.OnPush(trigger.Branches("main"))))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	m, err := senro.ReadRunManifest(dir)
	if err != nil {
		t.Fatalf("ReadRunManifest: %v", err)
	}
	if m.RunID != "20260812T101500-abcdef0123" {
		t.Errorf("RunID = %q", m.RunID)
	}
	if m.Pipeline != "triggered" {
		t.Errorf("Pipeline = %q, want triggered", m.Pipeline)
	}
	if m.StartedAt.IsZero() {
		t.Error("StartedAt is zero")
	}
	if m.Trigger == nil {
		t.Fatal("the manifest records no trigger for a triggered run")
	}
	if m.Trigger.Kind != trigger.Push {
		t.Errorf("Kind = %q", m.Trigger.Kind)
	}
	if m.Trigger.Ref != "refs/heads/main" {
		t.Errorf("Ref = %q", m.Trigger.Ref)
	}
	if m.Trigger.Repo != "acme/app" {
		t.Errorf("Repo = %q", m.Trigger.Repo)
	}
	if m.Trigger.Matched != "push(branches=[main])" {
		t.Errorf("Matched = %q, want the declaration that claimed the event", m.Trigger.Matched)
	}
}

// TestTheRunManifestCarriesTheModeAndBase, which are the two things the
// design says a trigger exists to feed the affected-set computation. senro
// computes no affected set; it hands these on.
func TestTheRunManifestCarriesTheModeAndBase(t *testing.T) {
	cases := []struct {
		name string
		ev   *trigger.Event
		tr   trigger.Trigger
		mode trigger.Mode
		base trigger.Base
	}{
		{"push to the default branch", pushEvent("main"), trigger.OnPush(),
			trigger.ModeAll, trigger.Base{From: "aaaa", To: "bbbb"}},
		{"push to any other branch", pushEvent("topic"), trigger.OnPush(),
			trigger.ModeAffected, trigger.Base{From: "aaaa", To: "bbbb"}},
		{"pull request", &trigger.Event{
			Kind: trigger.PullRequest, Branch: "main", Action: "opened",
			Base: trigger.Base{From: "base1", To: "head1"},
		}, trigger.OnPullRequest(), trigger.ModeAffected,
			trigger.Base{From: "base1", To: "head1"}},
		{"tag", &trigger.Event{Kind: trigger.Tag, Tag: "v2.0.0"},
			trigger.OnTag(trigger.Semver(">=1.0.0")), trigger.ModeAll, trigger.Base{}},
	}
	for _, c := range cases {
		dir := filepath.Join(t.TempDir(), "run")
		err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
			senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
			senro.WithTrigger(c.ev, c.tr))
		if err != nil {
			t.Fatalf("%s: Run: %v", c.name, err)
		}
		m, err := senro.ReadRunManifest(dir)
		if err != nil {
			t.Fatalf("%s: ReadRunManifest: %v", c.name, err)
		}
		if m.Trigger.Mode != c.mode {
			t.Errorf("%s: Mode = %q, want %q", c.name, m.Trigger.Mode, c.mode)
		}
		if m.Trigger.Base != c.base {
			t.Errorf("%s: Base = %+v, want %+v", c.name, m.Trigger.Base, c.base)
		}
	}
}

// TestTheRunManifestNeverCarriesAParamValue. WithParams promises a value
// lands in nothing durable, and run.json is durable and has no redactor in
// front of it. The manifest records why the run started, not what it was
// given.
func TestTheRunManifestNeverCarriesAParamValue(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	const value = "hunter2hunter2"

	ev := &trigger.Event{Kind: trigger.Manual, Params: map[string]string{"from-event": value}}
	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trigger.OnManual(trigger.Params{"from-trigger": value})))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, senro.ManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if strings.Contains(string(b), value) {
		t.Errorf("run.json carries a param value:\n%s", b)
	}
}

// TestARunWithNoTriggerStillGetsAManifest: one layout, so a reader never has
// to ask whether this run happens to have a manifest.
func TestARunWithNoTriggerStillGetsAManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	m, err := senro.ReadRunManifest(dir)
	if err != nil {
		t.Fatalf("ReadRunManifest: %v", err)
	}
	if m.Trigger != nil {
		t.Errorf("Trigger = %+v, want none for a run nobody triggered", m.Trigger)
	}
	if m.RunID == "" {
		t.Error("RunID is empty")
	}
}

// TestTheManifestRecordsTheFileCountAndNotThePaths. A monorepo push carries
// thousands of paths; provenance is not a diff.
func TestTheManifestRecordsTheFileCountAndNotThePaths(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	ev := pushEvent("main")
	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trigger.OnPush()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	m, err := senro.ReadRunManifest(dir)
	if err != nil {
		t.Fatalf("ReadRunManifest: %v", err)
	}
	if m.Trigger.Files != 1 {
		t.Errorf("Files = %d, want 1", m.Trigger.Files)
	}
	b, _ := os.ReadFile(filepath.Join(dir, senro.ManifestFile))
	if strings.Contains(string(b), "services/api/main.go") {
		t.Errorf("run.json carries the changed paths:\n%s", b)
	}

	// A provider that supplied no list is -1, not 0: "nobody said" and
	// "nothing changed" are the same distinction Event.Files draws.
	dir2 := filepath.Join(t.TempDir(), "run")
	pr := &trigger.Event{Kind: trigger.PullRequest, Branch: "main", Action: "opened"}
	if err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(dir2), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(pr, trigger.OnPullRequest())); err != nil {
		t.Fatalf("Run: %v", err)
	}
	m2, err := senro.ReadRunManifest(dir2)
	if err != nil {
		t.Fatalf("ReadRunManifest: %v", err)
	}
	if m2.Trigger.Files != -1 {
		t.Errorf("Files = %d, want -1 for a provider that supplied no list", m2.Trigger.Files)
	}
}

// TestTheManifestIsWrittenForPeopleToRead. encoding/json escapes <, > and &
// by default, which turns the one declaration a release pipeline cares about
// into "tag(semver=[>=1.0.0])". There is no HTML anywhere near this
// file.
func TestTheManifestIsWrittenForPeopleToRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	ev := &trigger.Event{Kind: trigger.Tag, Tag: "v1.2.0"}
	err := senro.Run(context.Background(), triggerPipeline(t, filepath.Join(t.TempDir(), "ran")),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithTrigger(ev, trigger.OnTag(trigger.Semver(">=1.0.0"))))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, senro.ManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(b), "semver=[>=1.0.0]") {
		t.Errorf("run.json does not read plainly:\n%s", b)
	}
}

// TestReadRunManifestOnADirectoryWithoutOneIsAnError, and says so, because
// a run directory from a build before manifests existed is the likely cause.
func TestReadRunManifestOnADirectoryWithoutOneIsAnError(t *testing.T) {
	if _, err := senro.ReadRunManifest(t.TempDir()); err == nil {
		t.Fatal("ReadRunManifest accepted a directory with no run.json")
	}
}
