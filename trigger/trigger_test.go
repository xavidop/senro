// The GitHub fixtures under testdata are real webhook payloads (from
// go-playground/webhooks and octokit/webhooks payload-examples), trimmed to
// the fields this build reads and wrapped in the envelope LoadEvent
// documents; github-push-tag.json is the new-branch payload with only the
// ref changed to refs/tags/v1.2.0, since octokit ships no created-tag
// example. A payload written from memory would have plausible field names no
// test written from the same memory could catch.
package trigger_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/trigger"
)

// load reads a testdata fixture through the same public entry point a
// pipeline's main uses, so every parsing test exercises the envelope too.
func load(t *testing.T, name string) *trigger.Event {
	t.Helper()
	ev, err := trigger.LoadEvent(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("LoadEvent(%s): %v", name, err)
	}
	if ev == nil {
		t.Fatalf("LoadEvent(%s) returned no event and no error", name)
	}
	return ev
}

// writeEvent puts an envelope in a temp file and returns its path, for the
// cases a fixture would be more ceremony than the one field under test.
func writeEvent(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "event.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write event: %v", err)
	}
	return p
}

// TestGitHubPushToABranchIsParsedFromTheRealPayload pins the field names
// against a real webhook body; a hand-written fixture would agree with a
// misremembered name.
func TestGitHubPushToABranchIsParsedFromTheRealPayload(t *testing.T) {
	ev := load(t, "github-push-branch.json")

	if ev.Kind != trigger.Push {
		t.Errorf("Kind = %q, want %q", ev.Kind, trigger.Push)
	}
	if ev.Provider != "github" {
		t.Errorf("Provider = %q, want github", ev.Provider)
	}
	if ev.Ref != "refs/heads/master" {
		t.Errorf("Ref = %q", ev.Ref)
	}
	if ev.Branch != "master" {
		t.Errorf("Branch = %q, want master", ev.Branch)
	}
	if ev.Repo != "binkkatal/sample_app" {
		t.Errorf("Repo = %q", ev.Repo)
	}
	if ev.DefaultBranch != "master" {
		t.Errorf("DefaultBranch = %q, want master", ev.DefaultBranch)
	}
	if ev.Deleted {
		t.Error("Deleted is true for a push that deleted nothing")
	}
	want := []string{".razorops.yaml", "app/controllers/application_controller.rb"}
	if !equalStrings(ev.Files, want) {
		t.Errorf("Files = %v, want %v", ev.Files, want)
	}
	if ev.Base.From != "737d38c599c1b2991664dfc6155d6bf516fcce36" {
		t.Errorf("Base.From = %q, want the push's own before", ev.Base.From)
	}
	if ev.Base.To != "fd489864e7642b48eaad6e3f155c10e46810ec72" {
		t.Errorf("Base.To = %q, want the push's own after", ev.Base.To)
	}
}

// TestGitHubPushToATagIsATagNotAPush pins the one shape decision GitHub
// forces: there is no "tag" webhook event, a pushed tag is a push whose ref
// is refs/tags/..., and a pipeline asking OnTag must still get it.
func TestGitHubPushToATagIsATagNotAPush(t *testing.T) {
	ev := load(t, "github-push-tag.json")

	if ev.Kind != trigger.Tag {
		t.Fatalf("Kind = %q, want %q for a push to refs/tags/", ev.Kind, trigger.Tag)
	}
	if ev.Tag != "v1.2.0" {
		t.Errorf("Tag = %q, want v1.2.0 with no refs/tags/ prefix", ev.Tag)
	}
	if ev.Branch != "" {
		t.Errorf("Branch = %q, want empty: a tag is not on a branch", ev.Branch)
	}
}

// TestACreatedRefHasNoBaseToDiffAgainst covers GitHub's all-zero before on a
// ref that did not exist a moment ago. Carrying it verbatim would hand an
// affected-set computation forty zeros and let it try to diff against them.
func TestACreatedRefHasNoBaseToDiffAgainst(t *testing.T) {
	ev := load(t, "github-push-new-branch.json")

	if ev.Base.From != "" {
		t.Errorf("Base.From = %q, want empty for a created ref", ev.Base.From)
	}
	if ev.Base.To != "6113728f27ae82c7b1a177c8d03f9e96e0adf246" {
		t.Errorf("Base.To = %q, want the new commit", ev.Base.To)
	}
}

// TestGitHubPullRequestIsParsedFromTheRealPayload pins the pull_request
// field names and the base/head decision.
func TestGitHubPullRequestIsParsedFromTheRealPayload(t *testing.T) {
	ev := load(t, "github-pull-request-opened.json")

	if ev.Kind != trigger.PullRequest {
		t.Errorf("Kind = %q, want %q", ev.Kind, trigger.PullRequest)
	}
	if ev.Action != "opened" {
		t.Errorf("Action = %q, want opened", ev.Action)
	}
	if ev.Number != 2 {
		t.Errorf("Number = %d, want 2", ev.Number)
	}
	if ev.Base.From != "f95f852bd8fca8fcc58a9a2d6c842781e32a215e" {
		t.Errorf("Base.From = %q, want the base sha", ev.Base.From)
	}
	if ev.Base.To != "ec26c3e57ca3a959ca5aad62de7213c562f8c821" {
		t.Errorf("Base.To = %q, want the head sha", ev.Base.To)
	}
}

// TestPullRequestBranchIsTheBaseBranch pins the choice Branches depends on:
// Branch is the base ref, and filtering on the head would run main's
// pipeline for every feature branch that ever opened a pull request.
func TestPullRequestBranchIsTheBaseBranch(t *testing.T) {
	ev := load(t, "github-pull-request-opened.json")

	if ev.Branch != "master" {
		t.Errorf("Branch = %q, want master (the BASE branch), not the head branch", ev.Branch)
	}
	if !matched(t, ev, trigger.OnPullRequest(trigger.Branches("master"))) {
		t.Error("Branches(master) did not match a pull request whose base is master")
	}
	if matched(t, ev, trigger.OnPullRequest(trigger.Branches("changes"))) {
		t.Error("Branches matched the HEAD branch; it must test the base")
	}
}

// TestAPullRequestCarriesNoChangedFileList is the negative half of
// Event.Files' nil-versus-empty contract, and what makes Paths able to
// refuse rather than silently never match.
func TestAPullRequestCarriesNoChangedFileList(t *testing.T) {
	ev := load(t, "github-pull-request-opened.json")

	if ev.Files != nil {
		t.Errorf("Files = %v, want nil: GitHub puts no file list in a pull_request payload", ev.Files)
	}
}

// TestAPushWithNoCommitsHasAnEmptyButPresentFileList is the positive half:
// GitHub did supply the list for a push, and it was empty. Nil here would
// make Paths refuse a perfectly well-formed event.
func TestAPushWithNoCommitsHasAnEmptyButPresentFileList(t *testing.T) {
	ev := load(t, "github-push-tag-deleted.json")

	if ev.Files == nil {
		t.Fatal("Files is nil for a push, which reads as \"the provider did not say\"")
	}
	if len(ev.Files) != 0 {
		t.Errorf("Files = %v, want empty", ev.Files)
	}
}

// TestNoTriggerMatchesADeletedRef: GitHub sends a push for a removed branch
// or tag, and there is nothing to build at a ref that no longer exists.
func TestNoTriggerMatchesADeletedRef(t *testing.T) {
	ev := load(t, "github-push-tag-deleted.json")

	if ev.Kind != trigger.Tag || !ev.Deleted {
		t.Fatalf("fixture is not a deleted tag: kind %q deleted %v", ev.Kind, ev.Deleted)
	}
	_, err := trigger.Select(ev, trigger.OnTag())
	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select on a deleted tag = %v, want ErrNoMatch", err)
	}
}

// TestBranchesFiltersAPush is the matcher's own reason to exist: a push to
// main runs the pipeline and a push to somebody's branch does not.
func TestBranchesFiltersAPush(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "feature/login", Files: []string{}}
	if matched(t, ev, trigger.OnPush(trigger.Branches("main"))) {
		t.Error("Branches(main) matched a push to feature/login")
	}

	ev.Branch = "main"
	if !matched(t, ev, trigger.OnPush(trigger.Branches("main"))) {
		t.Error("Branches(main) did not match a push to main")
	}
}

// TestBranchesUsesSenrosOneGlobSyntax pins that a branch pattern means the
// same thing a workspace exclude or an Inputs pattern does, rather than a
// second syntax that looks similar until it does not.
func TestBranchesUsesSenrosOneGlobSyntax(t *testing.T) {
	cases := []struct {
		pattern string
		branch  string
		want    bool
	}{
		{"main", "main", true},
		{"main", "maintenance", false},
		{"release/*", "release/1.0", true},
		{"release/*", "release/1.0/hotfix", false},
		{"release/**", "release/1.0/hotfix", true},
		{"feat/**", "feat/a/b/c", true},
		// A pattern with no "/" is the whole name, not something that
		// reaches into subdirectories: senro's documented rule.
		{"main", "feat/main", false},
		{"*", "main", true},
		{"*", "feat/x", false},
	}
	for _, c := range cases {
		ev := &trigger.Event{Kind: trigger.Push, Branch: c.branch}
		got := matched(t, ev, trigger.OnPush(trigger.Branches(c.pattern)))
		if got != c.want {
			t.Errorf("Branches(%q) against branch %q = %v, want %v", c.pattern, c.branch, got, c.want)
		}
	}
}

// TestBranchesWithSeveralPatternsMatchesAnyOfThem: the patterns are an OR,
// the way a list of branches reads.
func TestBranchesWithSeveralPatternsMatchesAnyOfThem(t *testing.T) {
	tr := trigger.OnPush(trigger.Branches("main", "release/*"))
	for _, b := range []string{"main", "release/9"} {
		ev := &trigger.Event{Kind: trigger.Push, Branch: b}
		if !matched(t, ev, tr) {
			t.Errorf("branch %q did not match %s", b, tr)
		}
	}
	ev := &trigger.Event{Kind: trigger.Push, Branch: "wip"}
	if matched(t, ev, tr) {
		t.Errorf("branch wip matched %s", tr)
	}
}

// TestPathsFiltersOnTheEventsOwnFileList, and on nothing else: no working
// tree is consulted, which is what lets this run before a checkout exists.
func TestPathsFiltersOnTheEventsOwnFileList(t *testing.T) {
	tr := trigger.OnPush(trigger.Paths("services/**"))

	hit := &trigger.Event{Kind: trigger.Push, Files: []string{"README.md", "services/api/main.go"}}
	if !matched(t, hit, tr) {
		t.Error("Paths(services/**) did not match a push that changed services/api/main.go")
	}

	miss := &trigger.Event{Kind: trigger.Push, Files: []string{"README.md", "docs/index.md"}}
	if matched(t, miss, tr) {
		t.Error("Paths(services/**) matched a push that changed nothing under services/")
	}

	// Present and empty is a real answer: nothing changed, so nothing under
	// services/ changed.
	none := &trigger.Event{Kind: trigger.Push, Files: []string{}}
	if matched(t, none, tr) {
		t.Error("Paths matched a push whose file list is present and empty")
	}
}

// TestPathsRefusesAnEventWithNoFileList: a wiring mistake and a no-match
// must not look alike. OnPullRequest(Paths(...)) can never be answered from
// a GitHub payload, so it says so instead of quietly never running.
func TestPathsRefusesAnEventWithNoFileList(t *testing.T) {
	ev := load(t, "github-pull-request-opened.json")

	m, err := trigger.Select(ev, trigger.OnPullRequest(trigger.Paths("services/**")))
	if err == nil {
		t.Fatalf("Select = %+v, want an error: the event carries no changed-file list", m)
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select reported ErrNoMatch for an unanswerable question: %v", err)
	}
	if !strings.Contains(err.Error(), "changed-file list") {
		t.Errorf("the error must say what is missing; got %q", err)
	}
}

// TestActionsFiltersAPullRequest: "opened" and "synchronize" are worth a
// build, "labeled" is not, and the default with no Actions is everything.
func TestActionsFiltersAPullRequest(t *testing.T) {
	tr := trigger.OnPullRequest(trigger.Actions("opened", "synchronize"))

	opened := load(t, "github-pull-request-opened.json")
	if !matched(t, opened, tr) {
		t.Error("Actions(opened, synchronize) did not match an opened pull request")
	}
	sync := load(t, "github-pull-request-synchronize.json")
	if sync.Action != "synchronize" {
		t.Fatalf("fixture action = %q", sync.Action)
	}
	if !matched(t, sync, tr) {
		t.Error("Actions did not match a synchronize")
	}

	labeled := &trigger.Event{Kind: trigger.PullRequest, Action: "labeled"}
	if matched(t, labeled, tr) {
		t.Error("Actions(opened, synchronize) matched a labeled")
	}
	if !matched(t, labeled, trigger.OnPullRequest()) {
		t.Error("OnPullRequest with no Actions must match every action")
	}
}

// TestATriggerOnlyMatchesItsOwnKind: every other test hands a trigger the
// kind it asked for, so all would pass if the kind were never checked. The
// tag row matters most: a pushed tag arrives as a GitHub push, and OnPush
// claiming it would run the branch pipeline for every release.
func TestATriggerOnlyMatchesItsOwnKind(t *testing.T) {
	events := map[trigger.Kind]*trigger.Event{
		trigger.Push:        {Kind: trigger.Push, Branch: "main", Files: []string{}},
		trigger.PullRequest: {Kind: trigger.PullRequest, Branch: "main", Action: "opened"},
		trigger.Tag:         {Kind: trigger.Tag, Tag: "v1.0.0", Files: []string{}},
		trigger.Schedule:    {Kind: trigger.Schedule, Schedule: "0 3 * * *"},
		trigger.Manual:      {Kind: trigger.Manual, Branch: "main"},
	}
	triggers := map[trigger.Kind]trigger.Trigger{
		trigger.Push:        trigger.OnPush(),
		trigger.PullRequest: trigger.OnPullRequest(),
		trigger.Tag:         trigger.OnTag(),
		trigger.Schedule:    trigger.OnSchedule("0 3 * * *"),
		trigger.Manual:      trigger.OnManual(),
	}
	for declared, tr := range triggers {
		for arrived, ev := range events {
			got := matched(t, ev, tr)
			want := declared == arrived
			if got != want {
				t.Errorf("%s matched a %s event = %v, want %v", tr, arrived, got, want)
			}
		}
	}
}

// TestANonVersionTagIsRejectedRatherThanReadAsZero needs ">=0.0.0": against
// ">=1.0.0" a rejected tag and one read as 0.0.0 both fail to match, so only
// a constraint every real version satisfies can tell them apart. The stake:
// "latest" read as zero would satisfy a range and deploy.
func TestANonVersionTagIsRejectedRatherThanReadAsZero(t *testing.T) {
	tr := trigger.OnTag(trigger.Semver(">=0.0.0"))

	for _, tag := range []string{"v0.0.0", "0.0.0", "v1.0.0", "v99.1.2"} {
		ev := &trigger.Event{Kind: trigger.Tag, Tag: tag}
		if !matched(t, ev, tr) {
			t.Errorf("Semver(\">=0.0.0\") did not match the real version %q", tag)
		}
	}
	for _, tag := range []string{"release-2024", "latest", "", "v1.0", "v1", "v1.0.0.0", "vx.y.z", "main"} {
		ev := &trigger.Event{Kind: trigger.Tag, Tag: tag}
		if matched(t, ev, tr) {
			t.Errorf("Semver(\">=0.0.0\") matched %q, which is not a version: it was read as zero", tag)
		}
	}
}

// TestSemverRejectsATagThatIsNotAVersion: the ugly cases, against a release
// constraint.
func TestSemverRejectsATagThatIsNotAVersion(t *testing.T) {
	cases := []struct {
		tag  string
		want bool
	}{
		{"v1.0.0", true},
		{"1.0.0", true},
		{"v2.3.4", true},
		{"v1.0.1", true},
		// Below 1.0.0 by semver's own ordering: a release candidate for a
		// version is not that version.
		{"v1.0.0-rc.1", false},
		{"v0.9.9", false},
		// Not versions at all. None of these may be read as 0.0.0.
		{"release-2024", false},
		{"", false},
		{"latest", false},
		{"v1.0", false},
		{"v1", false},
		{"v1.0.0.0", false},
		{"v01.0.0", false},
		{"vx.y.z", false},
		{"1.0.0-", false},
	}
	tr := trigger.OnTag(trigger.Semver(">=1.0.0"))
	for _, c := range cases {
		ev := &trigger.Event{Kind: trigger.Tag, Tag: c.tag}
		if got := matched(t, ev, tr); got != c.want {
			t.Errorf("Semver(\">=1.0.0\") against tag %q = %v, want %v", c.tag, got, c.want)
		}
	}
}

// TestSemverComparisonOperators covers the rest of the operator set and a
// two-sided range, which is the shape a "1.x only" release pipeline needs.
func TestSemverComparisonOperators(t *testing.T) {
	cases := []struct {
		constraint string
		tag        string
		want       bool
	}{
		{">1.0.0", "v1.0.0", false},
		{">1.0.0", "v1.0.1", true},
		{"<2.0.0", "v1.9.9", true},
		{"<2.0.0", "v2.0.0", false},
		{"<=2.0.0", "v2.0.0", true},
		{"!=1.4.2", "v1.4.2", false},
		{"!=1.4.2", "v1.4.3", true},
		{"=1.4.2", "v1.4.2", true},
		{"1.4.2", "v1.4.2", true},
		{"1.4.2", "v1.4.3", false},
		{">=1.0.0 <2.0.0", "v1.5.0", true},
		{">=1.0.0 <2.0.0", "v2.0.0", false},
		{">=1.0.0, <2.0.0", "v0.9.0", false},
		// Prerelease ordering within a range.
		{">=1.0.0-rc.1", "v1.0.0-rc.2", true},
		{">=1.0.0-rc.2", "v1.0.0-rc.1", false},
		{">=1.0.0-rc.1", "v1.0.0", true},
	}
	for _, c := range cases {
		ev := &trigger.Event{Kind: trigger.Tag, Tag: c.tag}
		got := matched(t, ev, trigger.OnTag(trigger.Semver(c.constraint)))
		if got != c.want {
			t.Errorf("Semver(%q) against %q = %v, want %v", c.constraint, c.tag, got, c.want)
		}
	}
}

// TestSemverWithAnUnparseableConstraintIsAWiringError: the tag is fine, the
// declaration is not, and that is not something to discover by never
// releasing again.
func TestSemverWithAnUnparseableConstraintIsAWiringError(t *testing.T) {
	for _, bad := range []string{"", "  ", ">=banana", ">=1.0", "~>1.0.0"} {
		ev := &trigger.Event{Kind: trigger.Tag, Tag: "v1.2.3"}
		_, err := trigger.Select(ev, trigger.OnTag(trigger.Semver(bad)))
		if err == nil {
			t.Errorf("Semver(%q) was accepted", bad)
			continue
		}
		if errors.Is(err, trigger.ErrNoMatch) {
			t.Errorf("Semver(%q) reported ErrNoMatch rather than a wiring error: %v", bad, err)
		}
	}
}

// TestAMatcherAskedOfAKindThatCannotAnswerIsAWiringError. A tag has no
// branch, so OnTag(Branches(...)) can never be true: silently never matching
// is the failure mode this exists to prevent.
func TestAMatcherAskedOfAKindThatCannotAnswerIsAWiringError(t *testing.T) {
	cases := []struct {
		name string
		tr   trigger.Trigger
		ev   *trigger.Event
	}{
		{"Branches on a tag", trigger.OnTag(trigger.Branches("main")),
			&trigger.Event{Kind: trigger.Tag, Tag: "v1.0.0"}},
		{"Semver on a push", trigger.OnPush(trigger.Semver(">=1.0.0")),
			&trigger.Event{Kind: trigger.Push, Branch: "main"}},
		{"Actions on a push", trigger.OnPush(trigger.Actions("opened")),
			&trigger.Event{Kind: trigger.Push, Branch: "main"}},
		{"Branches on a schedule", trigger.OnSchedule("0 3 * * *", trigger.Branches("main")),
			&trigger.Event{Kind: trigger.Schedule, Schedule: "0 3 * * *"}},
		{"Paths on a schedule", trigger.OnSchedule("0 3 * * *", trigger.Paths("a/**")),
			&trigger.Event{Kind: trigger.Schedule, Schedule: "0 3 * * *"}},
	}
	for _, c := range cases {
		_, err := trigger.Select(c.ev, c.tr)
		if err == nil {
			t.Errorf("%s was accepted", c.name)
			continue
		}
		if errors.Is(err, trigger.ErrNoMatch) {
			t.Errorf("%s reported ErrNoMatch rather than a wiring error: %v", c.name, err)
		}
	}
}

// TestAMatcherWithNoPatternsIsAWiringError: Branches() reads as "any
// branch", which is what OnPush already means, so it is always a mistake.
func TestAMatcherWithNoPatternsIsAWiringError(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "main", Files: []string{}}
	for name, tr := range map[string]trigger.Trigger{
		"Branches()": trigger.OnPush(trigger.Branches()),
		"Paths()":    trigger.OnPush(trigger.Paths()),
		"Actions()":  trigger.OnPullRequest(trigger.Actions()),
	} {
		if _, err := trigger.Select(ev, tr); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestADeclarationErrorIsReportedEvenWhenAnEarlierTriggerMatches. A wiring
// mistake that only surfaces on the days it is unlucky is worse than no
// check at all.
func TestADeclarationErrorIsReportedEvenWhenAnEarlierTriggerMatches(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "main"}
	_, err := trigger.Select(ev,
		trigger.OnPush(trigger.Branches("main")), // this one matches
		trigger.OnTag(trigger.Semver("nonsense")),
	)
	if err == nil {
		t.Fatal("Select matched the first trigger and never noticed the broken second one")
	}
	if !strings.Contains(err.Error(), "nonsense") {
		t.Errorf("the error must name the broken declaration; got %q", err)
	}
}

// TestTheFirstMatchingTriggerWins pins declaration order, which is what
// makes the Params a match carries predictable.
func TestTheFirstMatchingTriggerWins(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "main"}
	m, err := trigger.Select(ev,
		trigger.OnPush(trigger.Params{"which": "first"}),
		trigger.OnPush(trigger.Params{"which": "second"}),
	)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if m.Params["which"] != "first" {
		t.Errorf("Params[which] = %q, want first", m.Params["which"])
	}
}

// TestNoMatchIsErrNoMatchAndNamesTheEvent: the sentinel a caller maps to
// exit 78, with enough text for an operator to see which event was turned
// away.
func TestNoMatchIsErrNoMatchAndNamesTheEvent(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Ref: "refs/heads/wip", Branch: "wip"}
	m, err := trigger.Select(ev, trigger.OnPush(trigger.Branches("main")))
	if m != nil {
		t.Errorf("Select returned a match as well as %v", err)
	}
	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want ErrNoMatch", err)
	}
	if !strings.Contains(err.Error(), "refs/heads/wip") {
		t.Errorf("the error must name the event; got %q", err)
	}
}

// TestNoMatchExplainsWhyEachTriggerWasRejected: the sentinel names the event
// (TestNoMatchIsErrNoMatchAndNamesTheEvent), but an operator staring at "no
// trigger matched" still has to guess why. Every declared trigger's own
// rejection reason must be in the error: which predicate it failed, or that
// it only answers a different kind of event.
func TestNoMatchExplainsWhyEachTriggerWasRejected(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Ref: "refs/heads/wip", Branch: "wip"}
	_, err := trigger.Select(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnPullRequest(trigger.Actions("opened")),
	)
	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want ErrNoMatch", err)
	}
	if !strings.Contains(err.Error(), "branches=[main]") {
		t.Errorf("error must name the predicate that rejected the push trigger; got %q", err)
	}
	if !strings.Contains(err.Error(), "pull_request") || !strings.Contains(err.Error(), "push") {
		t.Errorf("error must say the pull_request trigger only answers pull_request events, not push; got %q", err)
	}
}

// TestNoMatchExplainsADeletedRef: Deleted rejects before any predicate runs
// (a push to a since-deleted branch has nothing to build), and that reason
// must say so rather than blaming a predicate that never ran.
func TestNoMatchExplainsADeletedRef(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Ref: "refs/heads/wip", Branch: "wip", Deleted: true}
	_, err := trigger.Select(ev, trigger.OnPush(trigger.Branches("wip")))
	if !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want ErrNoMatch", err)
	}
	// Not just the event's own "(deleted)" tag: the trigger's own line must
	// say deletion, specifically, is why THIS trigger did not claim it,
	// rather than blaming the branches predicate that never ran.
	got := err.Error()
	line := "push(branches=[wip]):"
	i := strings.Index(got, line)
	if i < 0 {
		t.Fatalf("error must contain a per-trigger line starting %q; got %q", line, got)
	}
	if !strings.Contains(got[i+len(line):], "deleted") {
		t.Errorf("the push trigger's own reason must say the ref was deleted; got %q", got)
	}
}

// TestAnEventWithNoTriggersDeclaredIsAWiringError: being handed an event and
// nothing to compare it against is a half-wired pipeline, not a no-match.
func TestAnEventWithNoTriggersDeclaredIsAWiringError(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "main"}
	_, err := trigger.Select(ev)
	if err == nil {
		t.Fatal("Select accepted an event with no triggers")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Errorf("Select reported ErrNoMatch rather than a wiring error: %v", err)
	}
}

// TestANilEventIsNeitherAMatchNorAnError is the local loop: ./pipeline with
// no --trigger-event runs, because a dispatcher that forgets the flag should
// over-run visibly rather than never run silently.
func TestANilEventIsNeitherAMatchNorAnError(t *testing.T) {
	m, err := trigger.Select(nil, trigger.OnPush(trigger.Branches("main")))
	if err != nil {
		t.Fatalf("Select(nil) = %v, want no error", err)
	}
	if m != nil {
		t.Errorf("Select(nil) = %+v, want no match", m)
	}
}

// TestModeIsAllOnlyWhereTheDesignSaysItIs. Mode is one of the two things a
// trigger exists to feed the affected-set computation, and it is carried,
// never computed here.
func TestModeIsAllOnlyWhereTheDesignSaysItIs(t *testing.T) {
	cases := []struct {
		name string
		ev   *trigger.Event
		tr   trigger.Trigger
		want trigger.Mode
	}{
		{"push to the default branch", &trigger.Event{
			Kind: trigger.Push, Branch: "main", DefaultBranch: "main",
		}, trigger.OnPush(), trigger.ModeAll},
		{"push to any other branch", &trigger.Event{
			Kind: trigger.Push, Branch: "feature", DefaultBranch: "main",
		}, trigger.OnPush(), trigger.ModeAffected},
		{"push whose event never says what the default branch is", &trigger.Event{
			Kind: trigger.Push, Branch: "main",
		}, trigger.OnPush(), trigger.ModeAffected},
		{"pull request", &trigger.Event{
			Kind: trigger.PullRequest, Branch: "main", Action: "opened",
		}, trigger.OnPullRequest(), trigger.ModeAffected},
		{"tag", &trigger.Event{
			Kind: trigger.Tag, Tag: "v1.0.0",
		}, trigger.OnTag(), trigger.ModeAll},
		{"schedule", &trigger.Event{
			Kind: trigger.Schedule, Schedule: "0 3 * * *",
		}, trigger.OnSchedule("0 3 * * *"), trigger.ModeAll},
		{"manual", &trigger.Event{
			Kind: trigger.Manual,
		}, trigger.OnManual(), trigger.ModeAll},
	}
	for _, c := range cases {
		m, err := trigger.Select(c.ev, c.tr)
		if err != nil {
			t.Fatalf("%s: Select: %v", c.name, err)
		}
		if m.Mode != c.want {
			t.Errorf("%s: Mode = %q, want %q", c.name, m.Mode, c.want)
		}
	}
}

// TestTheMatchCarriesTheBaseTheProviderGave, unchanged: this package
// computes no affected set and shells out to no git, it carries the two ends
// so that the computation has them when it is built.
func TestTheMatchCarriesTheBaseTheProviderGave(t *testing.T) {
	ev := load(t, "github-pull-request-synchronize.json")
	m, err := trigger.Select(ev, trigger.OnPullRequest())
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if m.Base != ev.Base {
		t.Errorf("Match.Base = %+v, want the event's own %+v", m.Base, ev.Base)
	}
	if m.Base.From == "" || m.Base.To == "" {
		t.Errorf("Match.Base = %+v, want both ends of the comparison", m.Base)
	}
}

// TestATriggersParamsWinOverTheEventsOwn: the pipeline author's declaration
// is more specific than whatever the dispatcher happened to send.
func TestATriggersParamsWinOverTheEventsOwn(t *testing.T) {
	ev := &trigger.Event{
		Kind:   trigger.Manual,
		Params: map[string]string{"mode": "affected", "reason": "rebuild"},
	}
	m, err := trigger.Select(ev, trigger.OnManual(trigger.Params{"mode": "all"}))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if m.Params["mode"] != "all" {
		t.Errorf("Params[mode] = %q, want all: the trigger's own must win", m.Params["mode"])
	}
	if m.Params["reason"] != "rebuild" {
		t.Errorf("Params[reason] = %q, want the event's own to survive", m.Params["reason"])
	}
	// The merge must not write back into the event it was given.
	if ev.Params["mode"] != "affected" {
		t.Errorf("Select mutated the event's own Params: %v", ev.Params)
	}
}

// TestOnScheduleSelectsOneOfSeveralSchedules is why OnSchedule takes a cron
// string at all: two crontab lines pointing at one binary, each selecting
// its own work.
func TestOnScheduleSelectsOneOfSeveralSchedules(t *testing.T) {
	nightly := trigger.OnSchedule("0 3 * * *", trigger.Params{"suite": "full"})
	hourly := trigger.OnSchedule("0 * * * *", trigger.Params{"suite": "smoke"})

	ev := &trigger.Event{Kind: trigger.Schedule, Schedule: "0 * * * *"}
	m, err := trigger.Select(ev, nightly, hourly)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if m.Params["suite"] != "smoke" {
		t.Errorf("Params[suite] = %q, want smoke", m.Params["suite"])
	}

	unknown := &trigger.Event{Kind: trigger.Schedule, Schedule: "*/5 * * * *"}
	if _, err := trigger.Select(unknown, nightly, hourly); !errors.Is(err, trigger.ErrNoMatch) {
		t.Errorf("Select on an unclaimed schedule = %v, want ErrNoMatch", err)
	}
}

// TestOnScheduleComparesWhitespaceInsensitivelyAndNothingElse. senro parses
// no cron: "0 3 * * *" and "0 3 * * 0-6" mean the same thing to cron and
// deliberately not to this.
func TestOnScheduleComparesWhitespaceInsensitivelyAndNothingElse(t *testing.T) {
	tr := trigger.OnSchedule("0 3 * * *")

	spaced := &trigger.Event{Kind: trigger.Schedule, Schedule: "  0   3 * * *  "}
	if !matched(t, spaced, tr) {
		t.Error("a schedule that differs only in whitespace did not match")
	}
	equivalent := &trigger.Event{Kind: trigger.Schedule, Schedule: "0 3 * * 0-6"}
	if matched(t, equivalent, tr) {
		t.Error("a cron-equivalent but textually different schedule matched; senro parses no cron")
	}
}

// TestOnScheduleWithNoExpressionIsAWiringError.
func TestOnScheduleWithNoExpressionIsAWiringError(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Schedule, Schedule: "0 3 * * *"}
	if _, err := trigger.Select(ev, trigger.OnSchedule("  ")); err == nil {
		t.Error("OnSchedule with a blank expression was accepted")
	}
}

// TestAZeroTriggerIsRefused: it declares nothing, so matching against it
// would be matching against silence.
func TestAZeroTriggerIsRefused(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "main"}
	if _, err := trigger.Select(ev, trigger.Trigger{}); err == nil {
		t.Error("Select accepted a zero Trigger")
	}
}

// TestTriggerStringDescribesTheDeclaration, which is what a run's provenance
// record names and what an error quotes.
func TestTriggerStringDescribesTheDeclaration(t *testing.T) {
	got := trigger.OnPush(trigger.Branches("main", "release/*"), trigger.Paths("services/**")).String()
	want := "push(branches=[main release/*], paths=[services/**])"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if bare := trigger.OnTag().String(); bare != "tag" {
		t.Errorf("String() of a bare OnTag = %q, want tag", bare)
	}
}

// TestTriggerStringNeverNamesAParamValue. senro.WithParams promises a
// parameter value lands in nothing durable, and this string is written to
// runs/<id>/run.json and quoted in errors that reach a CI log, neither of
// which the run's redactor sits in front of.
func TestTriggerStringNeverNamesAParamValue(t *testing.T) {
	s := trigger.OnSchedule("0 3 * * *", trigger.Params{"token": "hunter2hunter2"}).String()
	if strings.Contains(s, "hunter2hunter2") {
		t.Errorf("String() = %q, which carries a param value into a durable record", s)
	}
	if strings.Contains(s, "token") {
		t.Errorf("String() = %q, which names a param at all", s)
	}
}

// TestLoadEventReadsAnEventFromStandardInput covers the "-" path, which is
// how a dispatcher pipes a webhook body straight in without a temp file.
func TestLoadEventReadsAnEventFromStandardInput(t *testing.T) {
	body := `{"provider":"senro","event":"manual","payload":{"ref":"refs/heads/main"}}`
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	if _, err := w.WriteString(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = w.Close()

	saved := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = saved; _ = r.Close() }()

	ev, err := trigger.LoadEvent("-")
	if err != nil {
		t.Fatalf("LoadEvent(-): %v", err)
	}
	if ev == nil || ev.Kind != trigger.Manual {
		t.Fatalf("LoadEvent(-) = %+v, want a manual event", ev)
	}
	if ev.Branch != "main" {
		t.Errorf("Branch = %q, want main derived from the ref", ev.Branch)
	}
}

// TestLoadEventWithNoPathIsNoEventAndNoError: the local loop.
func TestLoadEventWithNoPathIsNoEventAndNoError(t *testing.T) {
	ev, err := trigger.LoadEvent("")
	if err != nil {
		t.Fatalf("LoadEvent(\"\") = %v, want no error", err)
	}
	if ev != nil {
		t.Errorf("LoadEvent(\"\") = %+v, want nil", ev)
	}
}

// TestAnUnreadableOrUnknownEventIsAnError, one case per way of being wrong.
// Every one of these must be distinguishable from ErrNoMatch, because they
// mean the opposite thing.
func TestAnUnreadableOrUnknownEventIsAnError(t *testing.T) {
	cases := map[string]string{
		"not json":                  `{not json`,
		"empty file":                ``,
		"no provider":               `{"event":"push","payload":{"ref":"refs/heads/main"}}`,
		"unknown provider":          `{"provider":"gerrit","event":"push","payload":{}}`,
		"no event type":             `{"provider":"github","payload":{}}`,
		"unknown github event":      `{"provider":"github","event":"issues","payload":{}}`,
		"unknown gitlab event":      `{"provider":"gitlab","event":"Pipeline Hook","payload":{}}`,
		"unknown senro event":       `{"provider":"senro","event":"cron","payload":{}}`,
		"no payload":                `{"provider":"github","event":"push"}`,
		"push with no ref":          `{"provider":"github","event":"push","payload":{"before":"a"}}`,
		"push with a strange ref":   `{"provider":"github","event":"push","payload":{"ref":"HEAD"}}`,
		"pull request no action":    `{"provider":"github","event":"pull_request","payload":{"number":1}}`,
		"schedule with no schedule": `{"provider":"senro","event":"schedule","payload":{"params":{}}}`,
		"misspelt neutral field":    `{"provider":"senro","event":"manual","payload":{"branches":"main"}}`,
	}
	for name, body := range cases {
		ev, err := trigger.LoadEvent(writeEvent(t, body))
		if err == nil {
			t.Errorf("%s: LoadEvent accepted it and returned %+v", name, ev)
			continue
		}
		if errors.Is(err, trigger.ErrNoMatch) {
			t.Errorf("%s: reported ErrNoMatch, which means the opposite thing: %v", name, err)
		}
	}
}

// TestAnEnvelopeErrorSaysWhichPartIsMissing: asserting only "is an error"
// leaves the messages free to drift into saying the wrong thing, and these
// are the sentences somebody debugging a dispatcher reads.
func TestAnEnvelopeErrorSaysWhichPartIsMissing(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"no provider", `{"event":"push","payload":{"ref":"refs/heads/main"}}`,
			"names no provider"},
		{"no event type", `{"provider":"github","payload":{}}`,
			"no event type"},
		{"no payload", `{"provider":"github","event":"push"}`,
			"no payload"},
		{"unknown provider", `{"provider":"gerrit","event":"push","payload":{}}`,
			`unknown provider "gerrit"`},
		// The funnel names the provider and event; the parser's own sentence
		// says what it does understand.
		{"unknown github event", `{"provider":"github","event":"issues","payload":{}}`,
			`the github "issues" event`},
		{"unknown github event says what it does parse",
			`{"provider":"github","event":"issues","payload":{}}`, `"pull_request"`},
		{"unknown gitlab event", `{"provider":"gitlab","event":"Pipeline Hook","payload":{}}`,
			`the gitlab "Pipeline Hook" event`},
		{"unknown gitlab event says what it does parse",
			`{"provider":"gitlab","event":"Pipeline Hook","payload":{}}`, `"merge_request"`},
	}
	for _, c := range cases {
		_, err := trigger.LoadEvent(writeEvent(t, c.body))
		if err == nil {
			t.Errorf("%s: accepted", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %q, want it to contain %q", c.name, err, c.want)
		}
	}
}

// TestLoadEventOnAMissingFileIsAnError, and names the path, because a typo
// in a dispatcher's template is the likely cause.
func TestLoadEventOnAMissingFileIsAnError(t *testing.T) {
	p := filepath.Join(t.TempDir(), "nope.json")
	_, err := trigger.LoadEvent(p)
	if err == nil {
		t.Fatal("LoadEvent accepted a missing file")
	}
	if !strings.Contains(err.Error(), "nope.json") {
		t.Errorf("the error must name the path; got %q", err)
	}
}

// TestTheNeutralShapeCarriesAFileListWhenTheCallerHasOne: the way out of
// Paths' refusal for a caller who did fetch the list from the API.
func TestTheNeutralShapeCarriesAFileListWhenTheCallerHasOne(t *testing.T) {
	body := `{"provider":"senro","event":"manual","payload":` +
		`{"branch":"main","files":["services/api/main.go"]}}`
	ev, err := trigger.LoadEvent(writeEvent(t, body))
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if !matched(t, ev, trigger.OnManual(trigger.Paths("services/**"))) {
		t.Error("Paths did not match a neutral event that supplied its own file list")
	}
}

// matched runs one trigger against one event and fails the test on anything
// that is not a clean yes or no, so a case that was meant to be a no-match
// cannot pass by erroring instead.
func matched(t *testing.T, ev *trigger.Event, tr trigger.Trigger) bool {
	t.Helper()
	m, err := trigger.Select(ev, tr)
	switch {
	case err == nil && m != nil:
		return true
	case errors.Is(err, trigger.ErrNoMatch):
		return false
	case err != nil:
		t.Fatalf("Select(%s): unexpected error: %v", tr, err)
	}
	t.Fatalf("Select(%s) returned neither a match nor an error", tr)
	return false
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
