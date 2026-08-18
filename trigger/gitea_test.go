package trigger_test

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/xavidop/senro/trigger"
)

// The testdata/gitea-*.json payloads are captured Gitea webhook bodies
// (woodpecker-ci/woodpecker's gitea fixtures), trimmed to the fields this
// build reads and cross-checked against Gitea's own api.PushPayload,
// api.CreatePayload and api.PullRequestPayload. Two values are set rather than
// captured: the truncated fixture's total_commits exceeds the commits sent,
// which is what Gitea does past ui.FEED_MAX_COMMIT_NUM, and the synchronized
// fixture's pull_request.id differs from its number, because id is the field a
// parser must NOT read and a fixture where the two agree could not catch one
// that did.

// ─────────────────────────────────────────────────────────────────────────────
// The built-in is the public extension point, not a shortcut past it.
// ─────────────────────────────────────────────────────────────────────────────

// TestGiteaIsTheSameProviderAThirdPartyWouldHandIn: provider "gitea" in an
// envelope and trigger.Gitea() handed in by hand must parse to the same Event
// byte for byte, or the built-in has a private path past the public interface.
func TestGiteaIsTheSameProviderAThirdPartyWouldHandIn(t *testing.T) {
	for _, name := range []string{
		"gitea-push-branch.json",
		"gitea-push-new-branch.json",
		"gitea-push-truncated.json",
		"gitea-push-tag.json",
		"gitea-push-tag-deleted.json",
		"gitea-create-tag.json",
		"gitea-pull-request-opened.json",
		"gitea-pull-request-synchronized.json",
	} {
		t.Run(name, func(t *testing.T) {
			builtin, err := trigger.LoadEvent("testdata/" + name)
			if err != nil {
				t.Fatalf("LoadEvent through the built-in name: %v", err)
			}

			body, err := os.ReadFile("testdata/" + name)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			const asThirdParty = "gitea-by-hand"
			byHand, err := trigger.ReadEvent(
				strings.NewReader(strings.Replace(string(body), `"provider": "gitea"`,
					`"provider": "`+asThirdParty+`"`, 1)),
				renamed{Provider: trigger.Gitea(), name: asThirdParty})
			if err != nil {
				t.Fatalf("LoadEvent through trigger.Gitea() by hand: %v", err)
			}

			if builtin.Provider != "gitea" {
				t.Errorf("the built-in filled in provider %q, want \"gitea\"", builtin.Provider)
			}
			if byHand.Provider != asThirdParty {
				t.Errorf("the by-hand path filled in provider %q, want %q", byHand.Provider, asThirdParty)
			}
			builtin.Provider, byHand.Provider = "", ""

			a, err := json.Marshal(builtin)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			b, err := json.Marshal(byHand)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(a) != string(b) {
				t.Errorf("provider \"gitea\" parsed to\n\t%s\ntrigger.Gitea() by hand parsed to\n\t%s\n"+
					"they must be the same event: the built-in has no path past the public interface", a, b)
			}
		})
	}
}

// TestGiteaMayNotBeShadowed: same rule as "github" and "senro"; a silently
// replaced built-in would make one event file mean different things in two
// binaries.
func TestGiteaMayNotBeShadowed(t *testing.T) {
	_, err := trigger.LoadEvent(envelope(t, "gitea", "push", `{"ref": "refs/heads/main"}`),
		shadow{name: "gitea"})
	if err == nil {
		t.Fatal("LoadEvent accepted a provider claiming the built-in name \"gitea\"")
	}
	if !strings.Contains(err.Error(), "gitea") {
		t.Errorf("the error does not name the provider it refused:\n%v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The payloads, field name by field name.
// ─────────────────────────────────────────────────────────────────────────────

func TestGiteaPushToABranchIsParsedFromTheRealPayload(t *testing.T) {
	ev := loadGitea(t, "gitea-push-branch.json")
	if ev.Kind != trigger.Push {
		t.Errorf("kind = %q, want push", ev.Kind)
	}
	if ev.Repo != "Test-CI/multi-line-secrets" {
		t.Errorf("repo = %q, want it read from repository.full_name", ev.Repo)
	}
	if ev.Ref != "refs/heads/main" || ev.Branch != "main" {
		t.Errorf("ref = %q, branch = %q, want refs/heads/main and main", ev.Ref, ev.Branch)
	}
	if ev.DefaultBranch != "main" {
		t.Errorf("default branch = %q, want it read from repository.default_branch", ev.DefaultBranch)
	}
	if ev.Deleted {
		t.Error("an ordinary push was read as a deletion")
	}
	want := []string{"aa", "aaa"}
	if got := ev.Files; !equalStrings(got, want) {
		t.Errorf("files = %v, want %v deduplicated across every commit and sorted", got, want)
	}
	if ev.Base.From != "6efcf5b7c98f3e7a491675164b7a2e7acac27941" ||
		ev.Base.To != "29be01c073851cf0db0c6a466e396b725a670453" {
		t.Errorf("base = %+v, want before..after", ev.Base)
	}
	// A push to the default branch builds everything; the rule is the event's,
	// and it applies to a Gitea event unchanged.
	if ev.Mode() != trigger.ModeAll {
		t.Errorf("mode = %q, want all for a push to the default branch", ev.Mode())
	}
}

// TestGiteaHasNoDeletedFlagAndSaysSoFromTheSHAs: a Gitea push body has no
// created or deleted booleans, only the all-zero SHA at one end.
func TestGiteaHasNoDeletedFlagAndSaysSoFromTheSHAs(t *testing.T) {
	created := loadGitea(t, "gitea-push-new-branch.json")
	if created.Deleted {
		t.Error("a created branch was read as a deletion")
	}
	if created.Base.From != "" {
		t.Errorf("base.from = %q, want empty: the all-zero SHA is not a commit anything can diff against",
			created.Base.From)
	}
	if created.Base.To == "" {
		t.Error("base.to is empty for a created branch, which does have a commit at that end")
	}

	deleted := loadGitea(t, "gitea-push-tag-deleted.json")
	if !deleted.Deleted {
		t.Error("a push whose after is the all-zero SHA was not read as a deletion")
	}
	if deleted.Base.To != "" {
		t.Errorf("base.to = %q, want empty", deleted.Base.To)
	}
	// Nothing matches a deleted ref, whatever the trigger asks.
	if matched(t, deleted, trigger.OnTag()) || matched(t, deleted, trigger.OnPush()) {
		t.Error("a trigger matched a ref that no longer exists")
	}
}

// TestGiteaPushToATagIsATagAndNotAPush: Gitea sends a push for a new tag as
// well as the create, with the full ref in it, so the ref decides.
func TestGiteaPushToATagIsATagAndNotAPush(t *testing.T) {
	ev := loadGitea(t, "gitea-push-tag.json")
	if ev.Kind != trigger.Tag {
		t.Errorf("kind = %q, want tag: a push whose ref is refs/tags/... is a tag", ev.Kind)
	}
	if ev.Tag != "v1.0.0" {
		t.Errorf("tag = %q, want v1.0.0 with no refs/tags/ prefix and the v left alone", ev.Tag)
	}
	if ev.Branch != "" {
		t.Errorf("branch = %q, want empty for a tag", ev.Branch)
	}
	if !matched(t, ev, trigger.OnTag(trigger.Semver(">=1.0.0"))) {
		t.Error("Semver did not match v1.0.0 against >=1.0.0")
	}
}

// TestGiteaCreateOfATagIsATagWithNoFileList: the create carries the tag's
// commit but says nothing about changed files, where the push Gitea sends for
// the same tag says "none". The two must not be collapsed: nil is "nobody
// said" and empty is "nothing changed".
func TestGiteaCreateOfATagIsATagWithNoFileList(t *testing.T) {
	create := loadGitea(t, "gitea-create-tag.json")
	if create.Kind != trigger.Tag {
		t.Errorf("kind = %q, want tag", create.Kind)
	}
	// Gitea sends the short name in a create, so senro builds the full ref.
	if create.Tag != "v1.0.0" || create.Ref != "refs/tags/v1.0.0" {
		t.Errorf("tag = %q, ref = %q, want v1.0.0 and refs/tags/v1.0.0 built from it",
			create.Tag, create.Ref)
	}
	if create.Repo != "gordon/hello-world" || create.DefaultBranch != "main" {
		t.Errorf("event = %+v, want the repository read from the payload", create)
	}
	if create.Base.To != "ef98532add3b2feb7a137426bba1248724367df5" {
		t.Errorf("base.to = %q, want the create's sha", create.Base.To)
	}
	if create.Base.From != "" {
		t.Errorf("base.from = %q, want empty: a create names nothing to diff against",
			create.Base.From)
	}
	if create.Files != nil {
		t.Errorf("files = %v, want nil: a create says nothing about changed files", create.Files)
	}

	push := loadGitea(t, "gitea-push-tag.json")
	if push.Files == nil || len(push.Files) != 0 {
		t.Errorf("the push for the same tag has files = %v, want an empty non-nil list: it did "+
			"send a commit list, and it was empty", push.Files)
	}
	if !matched(t, create, trigger.OnTag(trigger.Semver(">=1.0.0"))) {
		t.Error("Semver did not match a tag that arrived as a create")
	}
}

// TestAGiteaCreateOfABranchIsRefused: Gitea sends a push for the same new
// branch carrying its commits and their changed files, and deciding "a branch
// appeared" from the create's weaker evidence would be a second path to the
// same conclusion, running the pipeline twice for one branch.
func TestAGiteaCreateOfABranchIsRefused(t *testing.T) {
	body := `{"provider":"gitea","event":"create","payload":{"sha":"28c3613a","ref":"feature/x",` +
		`"ref_type":"branch","repository":{"full_name":"a/b","default_branch":"main"}}}`
	ev, err := trigger.LoadEvent(writeEvent(t, body))
	if err == nil {
		t.Fatalf("LoadEvent accepted a create of a branch and returned %+v", ev)
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("LoadEvent = %v, want an ordinary error rather than a no-match", err)
	}
	// The refusal has to name what to send instead, or it is only a rejection.
	for _, want := range []string{"feature/x", "push"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q:\n%v", want, err)
		}
	}
}

func TestGiteaPullRequestCarriesGiteasOwnActionWords(t *testing.T) {
	ev := loadGitea(t, "gitea-pull-request-opened.json")
	if ev.Kind != trigger.PullRequest {
		t.Errorf("kind = %q, want pull_request", ev.Kind)
	}
	if ev.Action != "opened" {
		t.Errorf("action = %q, want Gitea's own word", ev.Action)
	}
	// This payload carries the number only at the top level.
	if ev.Number != 1 {
		t.Errorf("number = %d, want 1 from the payload's own number", ev.Number)
	}
	// The BASE branch, as the GitHub parser reports it.
	if ev.Branch != "main" || ev.Ref != "refs/heads/main" {
		t.Errorf("branch = %q, ref = %q, want the base branch main", ev.Branch, ev.Ref)
	}
	if ev.Repo != "gordon/hello-world" {
		t.Errorf("repo = %q, want it read from repository.full_name", ev.Repo)
	}
	if ev.Base.From != "9353195a19e45482665306e466c832c46560532d" ||
		ev.Base.To != "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c" {
		t.Errorf("base = %+v, want base.sha..head.sha", ev.Base)
	}
	if ev.Files != nil {
		t.Errorf("files = %v, want nil: Gitea supplies no changed-file list here, and nil is how "+
			"Paths tells that from \"nothing changed\"", ev.Files)
	}
	if ev.Mode() != trigger.ModeAffected {
		t.Errorf("mode = %q, want affected", ev.Mode())
	}
}

// TestGiteaPullRequestNumberIsNotTheDatabaseID, the same trap GitLab's iid is:
// pull_request.id is the instance-wide key and means nothing to a person.
func TestGiteaPullRequestNumberIsNotTheDatabaseID(t *testing.T) {
	ev := loadGitea(t, "gitea-pull-request-synchronized.json")
	if ev.Action != "synchronized" {
		t.Fatalf("action = %q, want synchronized", ev.Action)
	}
	if ev.Number != 2 {
		t.Errorf("number = %d, want the number (2) and not the id (93)", ev.Number)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The commit list Gitea truncates.
// ─────────────────────────────────────────────────────────────────────────────

// TestATruncatedGiteaPushSaysNothingRatherThanSayingTooLittle: Gitea caps the
// commits array at ui.FEED_MAX_COMMIT_NUM (5 by default) while total_commits
// keeps the real number, so a bigger push carries an incomplete file list;
// Paths against it could silently answer "no match" when the truth is "match",
// and nil Files turns that into the error it is.
func TestATruncatedGiteaPushSaysNothingRatherThanSayingTooLittle(t *testing.T) {
	ev := loadGitea(t, "gitea-push-truncated.json")
	if ev.Files != nil {
		t.Fatalf("files = %v, want nil when total_commits exceeds the commits sent", ev.Files)
	}
	_, err := trigger.Select(ev, trigger.OnPush(trigger.Paths("**")))
	if err == nil {
		t.Fatal("Select accepted Paths against a truncated commit list")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
	// Everything that does not depend on the file list still works.
	if !matched(t, ev, trigger.OnPush(trigger.Branches("main"))) {
		t.Error("Branches stopped working on a push whose commit list was truncated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Every built-in matcher, on a Gitea event.
// ─────────────────────────────────────────────────────────────────────────────

// TestEveryBuiltInMatcherWorksOnAGiteaEvent: not one of the four matchers
// learns where the event came from.
func TestEveryBuiltInMatcherWorksOnAGiteaEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		trig    trigger.Trigger
		want    bool
	}{
		{"branches", "gitea-push-branch.json", trigger.OnPush(trigger.Branches("main")), true},
		{"branches that do not match", "gitea-push-branch.json",
			trigger.OnPush(trigger.Branches("release/*")), false},
		{"branches on a new branch", "gitea-push-new-branch.json",
			trigger.OnPush(trigger.Branches("feature/*")), true},
		{"paths", "gitea-push-branch.json", trigger.OnPush(trigger.Paths("aa*")), true},
		{"paths that do not match", "gitea-push-branch.json",
			trigger.OnPush(trigger.Paths("services/**")), false},
		{"actions", "gitea-pull-request-opened.json",
			trigger.OnPullRequest(trigger.Actions("opened", "synchronized")), true},
		{"actions that do not match", "gitea-pull-request-opened.json",
			trigger.OnPullRequest(trigger.Actions("closed")), false},
		{"branches on a pull request", "gitea-pull-request-opened.json",
			trigger.OnPullRequest(trigger.Branches("main")), true},
		{"semver on a pushed tag", "gitea-push-tag.json",
			trigger.OnTag(trigger.Semver(">=1.0.0")), true},
		{"semver on a created tag", "gitea-create-tag.json",
			trigger.OnTag(trigger.Semver(">=1.0.0")), true},
		{"semver below the constraint", "gitea-create-tag.json",
			trigger.OnTag(trigger.Semver(">=2.0.0")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := matched(t, loadGitea(t, tc.fixture), tc.trig); got != tc.want {
				t.Errorf("%s matched %s = %v, want %v", tc.trig, tc.fixture, got, tc.want)
			}
		})
	}
}

// TestPathsAgainstAGiteaPullRequestIsAnError, for the same reason as a GitHub
// pull request: the payload carries no changed-file list.
func TestPathsAgainstAGiteaPullRequestIsAnError(t *testing.T) {
	_, err := trigger.Select(loadGitea(t, "gitea-pull-request-opened.json"),
		trigger.OnPullRequest(trigger.Paths("services/**")))
	if err == nil {
		t.Fatal("Select accepted Paths against a pull request carrying no changed-file list")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Everything that goes wrong is an error.
// ─────────────────────────────────────────────────────────────────────────────

func TestAMalformedOrUnknownGiteaEventIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name, body, says string
	}{
		{"unknown event", `{"provider":"gitea","event":"issues","payload":{}}`,
			"pull_request"},
		{"payload that is not json", `{"provider":"gitea","event":"push","payload":"nope"}`,
			"gitea"},
		{"push with no ref", `{"provider":"gitea","event":"push","payload":{"after":"a"}}`,
			"no ref"},
		{"push with a ref that is neither", `{"provider":"gitea","event":"push","payload":{"ref":"HEAD"}}`,
			"HEAD"},
		{"pull request with no action",
			`{"provider":"gitea","event":"pull_request","payload":{"number":1,"pull_request":{"base":{"ref":"main"}}}}`,
			"action"},
		{"pull request with no base branch",
			`{"provider":"gitea","event":"pull_request","payload":{"action":"opened","number":1,"pull_request":{}}}`,
			"base branch"},
		{"create with no ref_type",
			`{"provider":"gitea","event":"create","payload":{"ref":"v1.0.0","sha":"a"}}`,
			"ref_type"},
		{"create of something that is neither",
			`{"provider":"gitea","event":"create","payload":{"ref":"v1","ref_type":"note","sha":"a"}}`,
			"note"},
		{"a push body sent as a create",
			`{"provider":"gitea","event":"create","payload":{"ref":"refs/heads/main","after":"a"}}`,
			"ref_type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := trigger.LoadEvent(writeEvent(t, tc.body))
			if err == nil {
				t.Fatalf("LoadEvent accepted it and returned %+v", ev)
			}
			if errors.Is(err, trigger.ErrNoMatch) {
				t.Errorf("LoadEvent = %v, want an ordinary error rather than a no-match", err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the error does not say %q:\n%v", tc.says, err)
			}
			if !strings.Contains(err.Error(), "gitea") {
				t.Errorf("the error does not name the provider:\n%v", err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func loadGitea(t *testing.T, name string) *trigger.Event {
	t.Helper()
	ev, err := trigger.LoadEvent("testdata/" + name)
	if err != nil {
		t.Fatalf("LoadEvent(%s): %v", name, err)
	}
	return ev
}
