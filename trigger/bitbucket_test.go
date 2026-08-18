package trigger_test

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/xavidop/senro/trigger"
)

// The testdata/bitbucket-*.json payloads are Atlassian's own published
// examples (support.atlassian.com "Event payloads", the repo:push sample and
// the Pull Request entity sample), trimmed to the fields this build reads and
// cross-checked against go-playground/webhooks' captured bodies. Each pull
// request fixture keeps "state" and each push change keeps "closed" and its
// commits although nothing reads them: those are the fields a parser must NOT
// read (the action comes from the event key, a deletion from a null new, and a
// Bitbucket commit carries no paths at all), and a fixture without them could
// not catch one that did.

// ─────────────────────────────────────────────────────────────────────────────
// The built-in is the public extension point, not a shortcut past it.
// ─────────────────────────────────────────────────────────────────────────────

// TestBitbucketIsTheSameProviderAThirdPartyWouldHandIn: provider "bitbucket"
// in an envelope and trigger.Bitbucket() handed in by hand must parse to the
// same Event byte for byte, or the built-in has a private path past the public
// interface.
func TestBitbucketIsTheSameProviderAThirdPartyWouldHandIn(t *testing.T) {
	for _, name := range []string{
		"bitbucket-push-branch.json",
		"bitbucket-push-new-branch.json",
		"bitbucket-push-branch-deleted.json",
		"bitbucket-push-tag.json",
		"bitbucket-pull-request-created.json",
		"bitbucket-pull-request-fulfilled.json",
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
			const asThirdParty = "bitbucket-by-hand"
			byHand, err := trigger.ReadEvent(
				strings.NewReader(strings.Replace(string(body), `"provider": "bitbucket"`,
					`"provider": "`+asThirdParty+`"`, 1)),
				renamed{Provider: trigger.Bitbucket(), name: asThirdParty})
			if err != nil {
				t.Fatalf("LoadEvent through trigger.Bitbucket() by hand: %v", err)
			}

			if builtin.Provider != "bitbucket" {
				t.Errorf("the built-in filled in provider %q, want \"bitbucket\"", builtin.Provider)
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
				t.Errorf("provider \"bitbucket\" parsed to\n\t%s\ntrigger.Bitbucket() by hand parsed to\n\t%s\n"+
					"they must be the same event: the built-in has no path past the public interface", a, b)
			}
		})
	}
}

// TestBitbucketMayNotBeShadowed: same rule as "github" and "senro"; a silently
// replaced built-in would make one event file mean different things in two
// binaries.
func TestBitbucketMayNotBeShadowed(t *testing.T) {
	// A payload the built-in parses happily, so the only thing that can make
	// this an error is the refusal itself.
	_, err := trigger.LoadEvent(envelope(t, "bitbucket", "repo:push",
		`{"push":{"changes":[{"new":{"type":"branch","name":"main","target":{"hash":"a1b2"}}}]}}`),
		shadow{name: "bitbucket"})
	if err == nil {
		t.Fatal("LoadEvent accepted a provider claiming the built-in name \"bitbucket\"")
	}
	if !strings.Contains(err.Error(), "bitbucket") {
		t.Errorf("the error does not name the provider it refused:\n%v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The payloads, field name by field name.
// ─────────────────────────────────────────────────────────────────────────────

func TestBitbucketPushToABranchIsParsedFromTheRealPayload(t *testing.T) {
	ev := loadBitbucket(t, "bitbucket-push-branch.json")
	if ev.Kind != trigger.Push {
		t.Errorf("kind = %q, want push", ev.Kind)
	}
	if ev.Repo != "team_name/repo_name" {
		t.Errorf("repo = %q, want it read from repository.full_name", ev.Repo)
	}
	// Bitbucket names the branch, it does not send a ref, so senro builds one.
	if ev.Ref != "refs/heads/main" || ev.Branch != "main" {
		t.Errorf("ref = %q, branch = %q, want refs/heads/main built from the change's name",
			ev.Ref, ev.Branch)
	}
	if ev.Deleted {
		t.Error("an ordinary push was read as a deletion")
	}
	if ev.Base.From != "1e65c05c1d5171631d92438a13901ca7dae9618c" ||
		ev.Base.To != "709d658dc5b6d6afcd46049c2f332ee3f515a67d" {
		t.Errorf("base = %+v, want old..new target hashes", ev.Base)
	}
}

// TestABitbucketPushCarriesNoChangedFileList: a Bitbucket commit has a hash, a
// message and an author and no paths, so senro never learns what changed. Nil
// is how Paths tells that from "nothing changed"; an empty list here would
// silently answer "no match" for every path filter.
func TestABitbucketPushCarriesNoChangedFileList(t *testing.T) {
	ev := loadBitbucket(t, "bitbucket-push-branch.json")
	if ev.Files != nil {
		t.Fatalf("files = %v, want nil: the payload's commits carry no paths", ev.Files)
	}
	_, err := trigger.Select(ev, trigger.OnPush(trigger.Paths("**")))
	if err == nil {
		t.Fatal("Select accepted Paths against a push carrying no changed-file list")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
	// Everything that does not depend on the file list still works.
	if !matched(t, ev, trigger.OnPush(trigger.Branches("main"))) {
		t.Error("Branches did not match a push to main")
	}
}

// TestABitbucketPushIsNeverModeAll: the repository object names no default
// branch, so senro cannot tell a push to it from any other push and says the
// conservative thing rather than guessing that "main" is the trunk.
func TestABitbucketPushIsNeverModeAll(t *testing.T) {
	ev := loadBitbucket(t, "bitbucket-push-branch.json")
	if ev.DefaultBranch != "" {
		t.Errorf("default branch = %q, want empty: the payload does not name one", ev.DefaultBranch)
	}
	if ev.Mode() != trigger.ModeAffected {
		t.Errorf("mode = %q, want affected for an event that names no default branch", ev.Mode())
	}
}

// TestBitbucketSaysCreatedAndDeletedWithANullEnd: Bitbucket has no all-zero
// SHA and no deleted flag senro reads; a created ref has a null old and a
// deleted one a null new.
func TestBitbucketSaysCreatedAndDeletedWithANullEnd(t *testing.T) {
	created := loadBitbucket(t, "bitbucket-push-new-branch.json")
	if created.Deleted {
		t.Error("a created branch was read as a deletion")
	}
	if created.Base.From != "" {
		t.Errorf("base.from = %q, want empty: a created ref has nothing to diff against",
			created.Base.From)
	}
	if created.Base.To == "" {
		t.Error("base.to is empty for a created branch, which does have a commit at that end")
	}

	deleted := loadBitbucket(t, "bitbucket-push-branch-deleted.json")
	if !deleted.Deleted {
		t.Error("a push whose new is null was not read as a deletion")
	}
	if deleted.Base.To != "" {
		t.Errorf("base.to = %q, want empty", deleted.Base.To)
	}
	if deleted.Branch != "feature/booking-validation" {
		t.Errorf("branch = %q, want it read from the old ref when the new one is null",
			deleted.Branch)
	}
	// Nothing matches a deleted ref, whatever the trigger asks.
	if matched(t, deleted, trigger.OnPush()) {
		t.Error("a trigger matched a ref that no longer exists")
	}
}

func TestBitbucketTagPushIsATagAndNotAPush(t *testing.T) {
	ev := loadBitbucket(t, "bitbucket-push-tag.json")
	if ev.Kind != trigger.Tag {
		t.Errorf("kind = %q, want tag: a change whose new.type is a tag is a tag", ev.Kind)
	}
	if ev.Tag != "v1.0.0" || ev.Ref != "refs/tags/v1.0.0" {
		t.Errorf("tag = %q, ref = %q, want v1.0.0 and refs/tags/v1.0.0", ev.Tag, ev.Ref)
	}
	if ev.Branch != "" {
		t.Errorf("branch = %q, want empty for a tag", ev.Branch)
	}
	if !matched(t, ev, trigger.OnTag(trigger.Semver(">=1.0.0"))) {
		t.Error("Semver did not match v1.0.0 against >=1.0.0")
	}
}

// TestABitbucketPushOfSeveralRefsIsRefused: one delivery can carry several
// changes (a branch and its tag, or git push --all) where an Event is one ref.
// Reading the first would drop the rest silently, so the refusal names the
// count and how to split the envelope.
func TestABitbucketPushOfSeveralRefsIsRefused(t *testing.T) {
	_, err := trigger.LoadEvent("testdata/bitbucket-push-two-changes.json")
	if err == nil {
		t.Fatal("LoadEvent accepted a push that moved two refs at once")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("LoadEvent = %v, want an ordinary error rather than a no-match", err)
	}
	for _, want := range []string{"2 refs", "push.changes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q:\n%v", want, err)
		}
	}
}

// TestBitbucketPullRequestActionComesFromTheEventKey: the body has no action
// field at all, so the X-Event-Key's own suffix is the action, carried through
// in Bitbucket's words and not translated into GitHub's.
func TestBitbucketPullRequestActionComesFromTheEventKey(t *testing.T) {
	for _, tc := range []struct {
		fixture, action string
	}{
		{"bitbucket-pull-request-created.json", "created"},
		{"bitbucket-pull-request-fulfilled.json", "fulfilled"},
	} {
		t.Run(tc.action, func(t *testing.T) {
			ev := loadBitbucket(t, tc.fixture)
			if ev.Kind != trigger.PullRequest {
				t.Errorf("kind = %q, want pull_request: senro has one vocabulary", ev.Kind)
			}
			if ev.Action != tc.action {
				t.Errorf("action = %q, want %q from the event key and not the payload's state",
					ev.Action, tc.action)
			}
			if ev.Number != 16 {
				t.Errorf("number = %d, want the pull request id (16)", ev.Number)
			}
			// The DESTINATION branch, as the GitHub parser reports the base.
			if ev.Branch != "main" || ev.Ref != "refs/heads/main" {
				t.Errorf("branch = %q, ref = %q, want the destination branch main", ev.Branch, ev.Ref)
			}
			if ev.Base.From != "ce5965ddd289" || ev.Base.To != "d3022fc0ca3d" {
				t.Errorf("base = %+v, want destination..source, abbreviated as Bitbucket sent them",
					ev.Base)
			}
			if ev.Files != nil {
				t.Errorf("files = %v, want nil: Bitbucket supplies no changed-file list here, and "+
					"nil is how Paths tells that from \"nothing changed\"", ev.Files)
			}
			if ev.Mode() != trigger.ModeAffected {
				t.Errorf("mode = %q, want affected", ev.Mode())
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Every built-in matcher, on a Bitbucket event.
// ─────────────────────────────────────────────────────────────────────────────

// TestEveryBuiltInMatcherWorksOnABitbucketEvent: not one of the four matchers
// learns where the event came from. Paths is absent because no Bitbucket
// payload carries a file list at all; see the test above.
func TestEveryBuiltInMatcherWorksOnABitbucketEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		trig    trigger.Trigger
		want    bool
	}{
		{"branches", "bitbucket-push-branch.json", trigger.OnPush(trigger.Branches("main")), true},
		{"branches that do not match", "bitbucket-push-branch.json",
			trigger.OnPush(trigger.Branches("release/*")), false},
		{"branches on a new branch", "bitbucket-push-new-branch.json",
			trigger.OnPush(trigger.Branches("feature/*")), true},
		{"actions", "bitbucket-pull-request-created.json",
			trigger.OnPullRequest(trigger.Actions("created", "updated")), true},
		{"actions that do not match", "bitbucket-pull-request-created.json",
			trigger.OnPullRequest(trigger.Actions("fulfilled")), false},
		{"branches on a pull request", "bitbucket-pull-request-created.json",
			trigger.OnPullRequest(trigger.Branches("main")), true},
		{"semver", "bitbucket-push-tag.json", trigger.OnTag(trigger.Semver(">=1.0.0")), true},
		{"semver below the constraint", "bitbucket-push-tag.json",
			trigger.OnTag(trigger.Semver(">=2.0.0")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := matched(t, loadBitbucket(t, tc.fixture), tc.trig); got != tc.want {
				t.Errorf("%s matched %s = %v, want %v", tc.trig, tc.fixture, got, tc.want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Everything that goes wrong is an error.
// ─────────────────────────────────────────────────────────────────────────────

func TestAMalformedOrUnknownBitbucketEventIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name, body, says string
	}{
		{"unknown event", `{"provider":"bitbucket","event":"repo:fork","payload":{}}`,
			"repo:push"},
		{"an event spelled without its prefix",
			`{"provider":"bitbucket","event":"push","payload":{"push":{"changes":[]}}}`,
			"X-Event-Key"},
		{"payload that is not json", `{"provider":"bitbucket","event":"repo:push","payload":"nope"}`,
			"bitbucket"},
		{"push with no changes",
			`{"provider":"bitbucket","event":"repo:push","payload":{"push":{"changes":[]}}}`,
			"no change"},
		{"push whose change names neither end",
			`{"provider":"bitbucket","event":"repo:push","payload":{"push":{"changes":[{"new":null,"old":null}]}}}`,
			"neither a new nor an old"},
		{"push on a mercurial reference",
			`{"provider":"bitbucket","event":"repo:push","payload":{"push":{"changes":[` +
				`{"new":{"type":"named_branch","name":"default","target":{"hash":"a"}}}]}}}`,
			"Mercurial"},
		{"push whose change has no type",
			`{"provider":"bitbucket","event":"repo:push","payload":{"push":{"changes":[` +
				`{"new":{"name":"main","target":{"hash":"a"}}}]}}}`,
			"branch or a tag"},
		{"pull request with no destination branch",
			`{"provider":"bitbucket","event":"pullrequest:created","payload":{"pullrequest":{"id":1}}}`,
			"destination branch"},
		{"a push whose body is a pull request",
			`{"provider":"bitbucket","event":"repo:push","payload":{"pullrequest":{"id":1}}}`,
			"pullrequest"},
		{"a pull request whose body is a push",
			`{"provider":"bitbucket","event":"pullrequest:created","payload":{"push":{"changes":[]}}}`,
			"push"},
		{"a body that is neither",
			`{"provider":"bitbucket","event":"repo:push","payload":{"repository":{"full_name":"a/b"}}}`,
			"carries no"},
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
			if !strings.Contains(err.Error(), "bitbucket") {
				t.Errorf("the error does not name the provider:\n%v", err)
			}
		})
	}
}

// TestABitbucketPullRequestEventThisBuildHasNotHeardOfStillParses: the event
// key's suffix is the action, so an approval or a comment reaches the same
// PullRequest kind with its own word, exactly as GitHub's "labeled" does.
// Naming the actions you want is what narrows it.
func TestABitbucketPullRequestEventThisBuildHasNotHeardOfStillParses(t *testing.T) {
	body := `{"provider":"bitbucket","event":"pullrequest:approved","payload":{"pullrequest":` +
		`{"id":3,"destination":{"branch":{"name":"main"}}}}}`
	ev, err := trigger.LoadEvent(writeEvent(t, body))
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if ev.Kind != trigger.PullRequest || ev.Action != "approved" {
		t.Errorf("event = %+v, want a pull_request whose action is approved", ev)
	}
	if matched(t, ev, trigger.OnPullRequest(trigger.Actions("created", "updated"))) {
		t.Error("Actions matched an approval it did not name")
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func loadBitbucket(t *testing.T, name string) *trigger.Event {
	t.Helper()
	ev, err := trigger.LoadEvent("testdata/" + name)
	if err != nil {
		t.Fatalf("LoadEvent(%s): %v", name, err)
	}
	return ev
}
