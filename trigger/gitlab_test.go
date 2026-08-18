package trigger_test

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/xavidop/senro/trigger"
)

// The testdata/gitlab-*.json payloads are GitLab's own published webhook
// examples (gitlab-org/gitlab webhook_events.md), trimmed to the fields this
// build reads, cross-checked against go-playground/webhooks' captured
// bodies. object_attributes.id is kept beside iid although nothing reads it,
// because it is the field a parser must NOT read, and a fixture without it
// could not catch one that did. The new-branch, deleted and truncated
// fixtures each derive one field from GitLab's own source
// (lib/gitlab/data_builder/push.rb): the all-zero SHA at one end, or a
// total_commits_count exceeding the commits sent.

// ─────────────────────────────────────────────────────────────────────────────
// The built-in is the public extension point, not a shortcut past it.
// ─────────────────────────────────────────────────────────────────────────────

// renamed wraps a Provider under a different name, the only way to hand a
// built-in back to LoadEvent as a third party's (claiming "gitlab" is
// refused).
type renamed struct {
	trigger.Provider
	name string
}

func (r renamed) Name() string { return r.name }

// TestGitLabIsTheSameProviderAThirdPartyWouldHandIn: provider "gitlab" in an
// envelope and trigger.GitLab() handed in by hand must parse to the same
// Event byte for byte, or the built-in has a private path past the public
// interface.
func TestGitLabIsTheSameProviderAThirdPartyWouldHandIn(t *testing.T) {
	for _, name := range []string{
		"gitlab-push-branch.json",
		"gitlab-push-new-branch.json",
		"gitlab-push-branch-deleted.json",
		"gitlab-push-truncated.json",
		"gitlab-tag-push.json",
		"gitlab-tag-push-deleted.json",
		"gitlab-merge-request-open.json",
		"gitlab-merge-request-update.json",
	} {
		t.Run(name, func(t *testing.T) {
			builtin, err := trigger.LoadEvent("testdata/" + name)
			if err != nil {
				t.Fatalf("LoadEvent through the built-in name: %v", err)
			}

			// The same file with the envelope's provider renamed, parsed by
			// the same trigger.Provider handed in the way a stranger's is.
			body, err := os.ReadFile("testdata/" + name)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			const asThirdParty = "gitlab-by-hand"
			byHand, err := trigger.ReadEvent(
				strings.NewReader(strings.Replace(string(body), `"provider": "gitlab"`,
					`"provider": "`+asThirdParty+`"`, 1)),
				renamed{Provider: trigger.GitLab(), name: asThirdParty})
			if err != nil {
				t.Fatalf("LoadEvent through trigger.GitLab() by hand: %v", err)
			}

			// Provenance is the one field that must differ; everything else
			// is the same event or the built-in took a shortcut.
			if builtin.Provider != "gitlab" {
				t.Errorf("the built-in filled in provider %q, want \"gitlab\"", builtin.Provider)
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
				t.Errorf("provider \"gitlab\" parsed to\n\t%s\ntrigger.GitLab() by hand parsed to\n\t%s\n"+
					"they must be the same event: the built-in has no path past the public interface", a, b)
			}
		})
	}
}

// TestGitLabMayNotBeShadowed: same rule as "github" and "senro"; a silently
// replaced built-in would make one event file mean different things in two
// binaries.
func TestGitLabMayNotBeShadowed(t *testing.T) {
	_, err := trigger.LoadEvent(envelope(t, "gitlab", "push", `{"ref": "refs/heads/main"}`),
		shadow{name: "gitlab"})
	if err == nil {
		t.Fatal("LoadEvent accepted a provider claiming the built-in name \"gitlab\"")
	}
	if !strings.Contains(err.Error(), "gitlab") {
		t.Errorf("the error does not name the provider it refused:\n%v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The payloads, field name by field name.
// ─────────────────────────────────────────────────────────────────────────────

func TestGitLabPushToABranchIsParsedFromTheRealPayload(t *testing.T) {
	ev := loadGitLab(t, "gitlab-push-branch.json")
	if ev.Kind != trigger.Push {
		t.Errorf("kind = %q, want push", ev.Kind)
	}
	if ev.Repo != "mike/diaspora" {
		t.Errorf("repo = %q, want it read from project.path_with_namespace", ev.Repo)
	}
	if ev.Ref != "refs/heads/master" || ev.Branch != "master" {
		t.Errorf("ref = %q, branch = %q, want refs/heads/master and master", ev.Ref, ev.Branch)
	}
	if ev.DefaultBranch != "master" {
		t.Errorf("default branch = %q, want it read from project.default_branch", ev.DefaultBranch)
	}
	if ev.Deleted {
		t.Error("an ordinary push was read as a deletion")
	}
	want := []string{"CHANGELOG", "app/controller/application.rb"}
	if got := ev.Files; !equalStrings(got, want) {
		t.Errorf("files = %v, want %v deduplicated across every commit and sorted", got, want)
	}
	if ev.Base.From != "95790bf891e76fee5e1747ab589903a6a1f80f22" ||
		ev.Base.To != "da1560886d4f094c3e6c9ef40349f7d38b5d27d7" {
		t.Errorf("base = %+v, want before..after", ev.Base)
	}
	// A push to the default branch builds everything; the rule is the
	// event's, and it applies to a GitLab event unchanged.
	if ev.Mode() != trigger.ModeAll {
		t.Errorf("mode = %q, want all for a push to the default branch", ev.Mode())
	}
}

// TestGitLabHasNoDeletedFlagAndSaysSoFromTheSHAs: a GitLab push body has no
// created or deleted booleans, only the all-zero SHA at one end.
func TestGitLabHasNoDeletedFlagAndSaysSoFromTheSHAs(t *testing.T) {
	created := loadGitLab(t, "gitlab-push-new-branch.json")
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

	for _, name := range []string{"gitlab-push-branch-deleted.json", "gitlab-tag-push-deleted.json"} {
		ev := loadGitLab(t, name)
		if !ev.Deleted {
			t.Errorf("%s: a push whose after is the all-zero SHA was not read as a deletion", name)
		}
		if ev.Base.To != "" {
			t.Errorf("%s: base.to = %q, want empty", name, ev.Base.To)
		}
		// Nothing matches a deleted ref, whatever the trigger asks.
		if matched(t, ev, trigger.OnPush()) || matched(t, ev, trigger.OnTag()) {
			t.Errorf("%s: a trigger matched a ref that no longer exists", name)
		}
	}
}

func TestGitLabTagPushIsATagAndNotAPush(t *testing.T) {
	ev := loadGitLab(t, "gitlab-tag-push.json")
	if ev.Kind != trigger.Tag {
		t.Errorf("kind = %q, want tag: a tag_push whose ref is refs/tags/... is a tag", ev.Kind)
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

// TestGitLabPushAndTagPushAreOneShape: GitLab has a separate tag_push event
// where GitHub has only a push whose ref is a tag, but the BODY is the same
// shape and the ref still decides. A tag_push whose ref is a branch, which a
// misconfigured dispatcher can produce, is a push.
func TestGitLabPushAndTagPushAreOneShape(t *testing.T) {
	for _, event := range []string{"push", "tag_push", "Push Hook", "Tag Push Hook"} {
		ev, err := trigger.LoadEvent(envelope(t, "gitlab", event,
			`{"ref": "refs/tags/v2.0.0", "after": "82b3d5ae55f7080f1e6022629cdb57bfae7cccc7",
			  "commits": [], "total_commits_count": 0,
			  "project": {"path_with_namespace": "a/b", "default_branch": "main"}}`))
		if err != nil {
			t.Fatalf("%s: LoadEvent: %v", event, err)
		}
		if ev.Kind != trigger.Tag || ev.Tag != "v2.0.0" {
			t.Errorf("%s: event = %+v, want tag v2.0.0", event, ev)
		}
	}
}

func TestGitLabMergeRequestIsAPullRequest(t *testing.T) {
	ev := loadGitLab(t, "gitlab-merge-request-open.json")
	if ev.Kind != trigger.PullRequest {
		t.Errorf("kind = %q, want pull_request: senro has one vocabulary", ev.Kind)
	}
	if ev.Number != 16 {
		t.Errorf("number = %d, want the iid (16) and not the id (93)", ev.Number)
	}
	if ev.Action != "open" {
		t.Errorf("action = %q, want GitLab's own word: it is not GitHub's \"opened\"", ev.Action)
	}
	// The TARGET branch, as the GitHub parser reports the base branch.
	if ev.Branch != "main" || ev.Ref != "refs/heads/main" {
		t.Errorf("branch = %q, ref = %q, want the target branch main", ev.Branch, ev.Ref)
	}
	if ev.Repo != "flightjs/flight-management" {
		t.Errorf("repo = %q, want it read from project.path_with_namespace", ev.Repo)
	}
	if ev.Base.To != "e59094b8de0f2f91abbe4760a52d9137260252d8" {
		t.Errorf("base.to = %q, want object_attributes.last_commit.id", ev.Base.To)
	}
	if ev.Base.From != "" {
		t.Errorf("base.from = %q, want empty: a merge request body names no commit on the target branch",
			ev.Base.From)
	}
	if ev.Files != nil {
		t.Errorf("files = %v, want nil: GitLab supplies no changed-file list here, and nil is how "+
			"Paths tells that from \"nothing changed\"", ev.Files)
	}
	if ev.Mode() != trigger.ModeAffected {
		t.Errorf("mode = %q, want affected", ev.Mode())
	}
}

// TestGitLabMergeRequestOldrevIsNotABase: oldrev is the previous head of the
// SOURCE branch, so diffing from it would look like an affected set and be a
// narrower one.
func TestGitLabMergeRequestOldrevIsNotABase(t *testing.T) {
	ev := loadGitLab(t, "gitlab-merge-request-update.json")
	if ev.Action != "update" {
		t.Fatalf("action = %q, want update", ev.Action)
	}
	if ev.Base.From != "" {
		t.Errorf("base.from = %q, want empty even though the payload carries an oldrev", ev.Base.From)
	}
	if ev.Base.To != "562e173be03b8ff2efb05345d12df18815438a4b" {
		t.Errorf("base.to = %q, want the last commit", ev.Base.To)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The commit list GitLab truncates.
// ─────────────────────────────────────────────────────────────────────────────

// TestATruncatedGitLabPushSaysNothingRatherThanSayingTooLittle: GitLab caps
// the commits array at 20, so a bigger push carries an incomplete file list;
// Paths against it could silently answer "no match" when the truth is
// "match", and nil Files turns that into the error it is.
func TestATruncatedGitLabPushSaysNothingRatherThanSayingTooLittle(t *testing.T) {
	ev := loadGitLab(t, "gitlab-push-truncated.json")
	if ev.Files != nil {
		t.Fatalf("files = %v, want nil when total_commits_count exceeds the commits sent", ev.Files)
	}
	_, err := trigger.Select(ev, trigger.OnPush(trigger.Paths("**")))
	if err == nil {
		t.Fatal("Select accepted Paths against a truncated commit list")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
	// Everything that does not depend on the file list still works.
	if !matched(t, ev, trigger.OnPush(trigger.Branches("master"))) {
		t.Error("Branches stopped working on a push whose commit list was truncated")
	}
}

// TestAnUntruncatedGitLabPushStillReportsAnEmptyList: empty and non-nil is
// "GitLab said, and nothing changed", which a tag push is. It must not
// collapse into the nil that means "nobody said".
func TestAnUntruncatedGitLabPushStillReportsAnEmptyList(t *testing.T) {
	ev := loadGitLab(t, "gitlab-tag-push.json")
	if ev.Files == nil {
		t.Fatal("files = nil for a tag push, want an empty non-nil list")
	}
	if len(ev.Files) != 0 {
		t.Errorf("files = %v, want empty", ev.Files)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Every built-in matcher, on a GitLab event.
// ─────────────────────────────────────────────────────────────────────────────

// TestEveryBuiltInMatcherWorksOnAGitLabEvent: not one of the four matchers
// learns where the event came from.
func TestEveryBuiltInMatcherWorksOnAGitLabEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		trig    trigger.Trigger
		want    bool
	}{
		{"branches", "gitlab-push-branch.json", trigger.OnPush(trigger.Branches("master")), true},
		{"branches that do not match", "gitlab-push-branch.json",
			trigger.OnPush(trigger.Branches("release/*")), false},
		{"branches on a new branch", "gitlab-push-new-branch.json",
			trigger.OnPush(trigger.Branches("feature/*")), true},
		{"paths", "gitlab-push-branch.json", trigger.OnPush(trigger.Paths("app/**")), true},
		{"paths that do not match", "gitlab-push-branch.json",
			trigger.OnPush(trigger.Paths("services/**")), false},
		{"actions", "gitlab-merge-request-open.json",
			trigger.OnPullRequest(trigger.Actions("open", "update")), true},
		{"actions that do not match", "gitlab-merge-request-open.json",
			trigger.OnPullRequest(trigger.Actions("merge")), false},
		{"branches on a merge request", "gitlab-merge-request-open.json",
			trigger.OnPullRequest(trigger.Branches("main")), true},
		{"semver", "gitlab-tag-push.json", trigger.OnTag(trigger.Semver(">=1.0.0")), true},
		{"semver below the constraint", "gitlab-tag-push.json",
			trigger.OnTag(trigger.Semver(">=2.0.0")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := matched(t, loadGitLab(t, tc.fixture), tc.trig); got != tc.want {
				t.Errorf("%s matched %s = %v, want %v", tc.trig, tc.fixture, got, tc.want)
			}
		})
	}
}

// TestPathsAgainstAGitLabMergeRequestIsAnError, for the same reason as a
// GitHub pull request: the payload carries no changed-file list.
func TestPathsAgainstAGitLabMergeRequestIsAnError(t *testing.T) {
	_, err := trigger.Select(loadGitLab(t, "gitlab-merge-request-open.json"),
		trigger.OnPullRequest(trigger.Paths("services/**")))
	if err == nil {
		t.Fatal("Select accepted Paths against a merge request carrying no changed-file list")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Everything that goes wrong is an error.
// ─────────────────────────────────────────────────────────────────────────────

func TestAMalformedOrUnknownGitLabEventIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name, body, says string
	}{
		{"unknown event", `{"provider":"gitlab","event":"Pipeline Hook","payload":{"object_kind":"pipeline"}}`,
			"pipeline"},
		{"payload that is not json", `{"provider":"gitlab","event":"push","payload":"nope"}`,
			"gitlab"},
		{"push with no ref", `{"provider":"gitlab","event":"push","payload":{"after":"a"}}`,
			"no ref"},
		{"push with a ref that is neither", `{"provider":"gitlab","event":"push","payload":{"ref":"HEAD"}}`,
			"HEAD"},
		{"merge request with no action",
			`{"provider":"gitlab","event":"merge_request","payload":{"object_attributes":{"iid":1,"target_branch":"main"}}}`,
			"action"},
		{"merge request with no target branch",
			`{"provider":"gitlab","event":"merge_request","payload":{"object_attributes":{"iid":1,"action":"open"}}}`,
			"target branch"},
		{"an event the body contradicts",
			`{"provider":"gitlab","event":"push","payload":{"object_kind":"merge_request","ref":"refs/heads/main"}}`,
			"merge_request"},
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
			if !strings.Contains(err.Error(), "gitlab") {
				t.Errorf("the error does not name the provider:\n%v", err)
			}
		})
	}
}

// TestAGitLabEventWhoseBodyAgreesWithTheEnvelopeIsFine, the other half of the
// contradiction check: object_kind and the envelope naming the same thing in
// either spelling is not a disagreement.
func TestAGitLabEventWhoseBodyAgreesWithTheEnvelopeIsFine(t *testing.T) {
	for _, event := range []string{"push", "Push Hook", "tag_push", "Tag Push Hook"} {
		body := `{"provider":"gitlab","event":"` + event + `","payload":` +
			`{"object_kind":"push","ref":"refs/heads/main","commits":[],"total_commits_count":0}}`
		if _, err := trigger.LoadEvent(writeEvent(t, body)); err != nil {
			t.Errorf("%s: LoadEvent: %v", event, err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func loadGitLab(t *testing.T, name string) *trigger.Event {
	t.Helper()
	ev, err := trigger.LoadEvent("testdata/" + name)
	if err != nil {
		t.Fatalf("LoadEvent(%s): %v", name, err)
	}
	return ev
}
