package trigger

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Bitbucket Cloud's own names for what this build reads, in the X-Event-Key
// spelling. The body carries no event name at all, so the envelope's is the
// only one there is.
const (
	bbPushEvent        = "repo:push"
	bbPullRequestEvent = "pullrequest:"
	bbCreated          = "pullrequest:created"
	bbUpdated          = "pullrequest:updated"
	bbFulfilled        = "pullrequest:fulfilled"
)

// The two top-level objects a body carries, which is the whole of what can
// contradict the envelope; see checkBitbucketBody.
const (
	bbPushObject        = "push"
	bbPullRequestObject = "pullrequest"
)

// The reference types a change reports for a git repository. Bitbucket's
// Mercurial words ("named_branch", "bookmark") reach neither of senro's kinds
// and are refused rather than guessed at.
const (
	bbBranch       = "branch"
	bbTag          = "tag"
	bbAnnotatedTag = "annotated_tag"
)

// Bitbucket is the built-in Bitbucket Cloud provider, an ordinary Provider
// with no private path past fromProvider; see GitHub and Provider. Wrapping it
// under another name is how a dispatcher labels two workspaces apart, or fills
// in what a Bitbucket body cannot say:
//
//	type withTrunk struct{ trigger.Provider }
//
//	func (withTrunk) Name() string { return "bitbucket-acme" }
//	func (w withTrunk) Parse(event string, payload []byte) (*trigger.Event, error) {
//		ev, err := w.Provider.Parse(event, payload)
//		if err == nil {
//			ev.DefaultBranch = "main"
//		}
//		return ev, err
//	}
func Bitbucket() Provider { return bitbucket{} }

// bitbucket parses Bitbucket Cloud webhook payloads. The field names are the
// webhook body's own; testdata holds real payloads trimmed to exactly these.
//
// What differs from GitHub: a pull request body carries no action, so the
// action is the event key's own suffix ("created", "updated", "fulfilled"),
// carried through untranslated; a push carries an array of changes, one per
// ref updated, where senro's event is one ref, so a delivery that moved
// several is refused rather than half read; a created or deleted ref is a null
// old or new rather than an all-zero SHA; no payload here carries a
// changed-file list, so Files is always nil; and the repository object names
// no default branch, so a Bitbucket push is always Mode affected.
type bitbucket struct{}

// Name is the value an envelope's "provider" field carries for Bitbucket.
func (bitbucket) Name() string { return providerBitbucket }

// Parse turns one Bitbucket Cloud webhook payload into an Event. event is the
// value of the X-Event-Key header, which the envelope carries because the body
// does not.
func (bitbucket) Parse(event string, payload []byte) (*Event, error) {
	switch {
	case event == bbPushEvent:
		return parseBitbucketPush(payload)
	case strings.HasPrefix(event, bbPullRequestEvent):
		return parseBitbucketPullRequest(event, payload)
	default:
		return nil, fmt.Errorf("this build parses %q (a branch or, when the change is on a tag, "+
			"a tag) and every %q event (%q, %q, %q and the rest), in the X-Event-Key spelling",
			bbPushEvent, bbPullRequestEvent+"...", bbCreated, bbUpdated, bbFulfilled)
	}
}

// bbHook is the part of a Bitbucket payload this build reads. Both of the
// body's top-level objects are here and both are pointers: the body names no
// event of its own, so which one is present is the only cross-check there is.
type bbHook struct {
	Push *struct {
		Changes []bbChange `json:"changes"`
	} `json:"push"`
	PullRequest *bbPullRequest `json:"pullrequest"`
	Repository  bbRepository   `json:"repository"`
}

// bbChange is one ref a push updated. new is null for a deleted ref and old is
// null for a created one, which is the whole of what Bitbucket says about
// either: there is no created or deleted flag and no all-zero SHA.
type bbChange struct {
	New *bbRef `json:"new"`
	Old *bbRef `json:"old"`
}

type bbRef struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Target struct {
		Hash string `json:"hash"`
	} `json:"target"`
}

// bbPullRequest is the part of Bitbucket's pull request object this build
// reads. id is the number a person sees: Bitbucket numbers pull requests per
// repository and has no second, instance-wide key here.
type bbPullRequest struct {
	ID     int `json:"id"`
	Source struct {
		Commit struct {
			Hash string `json:"hash"`
		} `json:"commit"`
	} `json:"source"`
	Destination struct {
		Branch struct {
			Name string `json:"name"`
		} `json:"branch"`
		Commit struct {
			Hash string `json:"hash"`
		} `json:"commit"`
	} `json:"destination"`
}

// bbRepository is Bitbucket's repository object. full_name is the
// "workspace/repository" that Repo means. It names no default branch: see
// bitbucket.
type bbRepository struct {
	FullName string `json:"full_name"`
}

func parseBitbucketPush(payload []byte) (*Event, error) {
	var p bbHook
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the %s payload: %w", bbPushEvent, err)
	}
	if err := checkBitbucketBody(bbPushEvent, bbPushObject, p); err != nil {
		return nil, err
	}
	changes := p.Push.Changes
	switch {
	case len(changes) == 0:
		return nil, fmt.Errorf("the %s payload names no change, so nothing says which ref was "+
			"pushed", bbPushEvent)
	case len(changes) > 1:
		// One delivery, several refs (git push --all, or a branch and its tag
		// together), against an Event that is one ref. Reading the first would
		// drop the rest silently.
		return nil, fmt.Errorf("the %s payload moved %d refs in one delivery and an event is one "+
			"ref; split the envelope into one event per push.changes entry, for example: "+
			"jq -c '.payload.push.changes[] as $c | .payload.push.changes = [$c]'",
			bbPushEvent, len(changes))
	}
	c := changes[0]
	ref, deleted := c.New, false
	if ref == nil {
		ref, deleted = c.Old, true
	}
	switch {
	case ref == nil:
		return nil, fmt.Errorf("the %s payload's change names neither a new nor an old ref, so "+
			"nothing says what was pushed", bbPushEvent)
	case ref.Name == "":
		return nil, fmt.Errorf("the %s payload's change names no branch or tag", bbPushEvent)
	}
	ev := &Event{
		Kind:    Push,
		Repo:    p.Repository.FullName,
		Deleted: deleted,
		// Files is deliberately nil, not empty: a Bitbucket commit carries a
		// hash, a message and an author and no paths at all, so senro never
		// learns what changed and Paths must be able to tell that from
		// "nothing changed". DefaultBranch is left empty for the same reason,
		// and an event that does not name one leaves every push Mode affected.
		Files: nil,
		Base:  Base{From: bbTarget(c.Old), To: bbTarget(c.New)},
	}
	switch ref.Type {
	case bbBranch:
		ev.Ref, ev.Branch = refHeads+ref.Name, ref.Name
	case bbTag, bbAnnotatedTag:
		ev.Kind, ev.Ref, ev.Tag = Tag, refTags+ref.Name, ref.Name
	case "":
		return nil, fmt.Errorf("the %s payload's change does not say whether %q is a branch or a "+
			"tag", bbPushEvent, ref.Name)
	default:
		return nil, fmt.Errorf("the %s payload's change is on a %q, which is a Mercurial "+
			"reference; senro reads Bitbucket's git repositories, whose changes are on a %q, %q "+
			"or %q", bbPushEvent, ref.Type, bbBranch, bbTag, bbAnnotatedTag)
	}
	return ev, nil
}

func parseBitbucketPullRequest(event string, payload []byte) (*Event, error) {
	var p bbHook
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the %s payload: %w", event, err)
	}
	if err := checkBitbucketBody(event, bbPullRequestObject, p); err != nil {
		return nil, err
	}
	// Bitbucket's own word, and the event key is the only place it is: the
	// body carries no action field. Not GitHub's "opened" or "synchronize".
	action := strings.TrimPrefix(event, bbPullRequestEvent)
	if action == "" {
		return nil, fmt.Errorf("the event key names no action; Bitbucket's own are %q, %q, %q, "+
			"\"rejected\", \"approved\" and the rest, which are the words an Actions matcher for "+
			"Bitbucket is written with", "created", "updated", "fulfilled")
	}
	pr := p.PullRequest
	if pr.Destination.Branch.Name == "" {
		return nil, fmt.Errorf("the %s payload names no destination branch, which is the branch "+
			"a Branches matcher tests", event)
	}
	return &Event{
		Kind:   PullRequest,
		Repo:   p.Repository.FullName,
		Action: action,
		Number: pr.ID,
		// The DESTINATION branch, as the GitHub parser reports the base one.
		Ref:    refHeads + pr.Destination.Branch.Name,
		Branch: pr.Destination.Branch.Name,
		// Deliberately nil, not empty: Bitbucket supplies no changed-file list
		// here, and Paths must be able to tell that from "nothing changed".
		Files: nil,
		// Both ends, as GitHub's base and head. Bitbucket abbreviates a hash
		// on a pull request to 12 characters and senro reports what the event
		// said, so whatever consumes a Base resolves it.
		Base: Base{From: pr.Destination.Commit.Hash, To: pr.Source.Commit.Hash},
	}, nil
}

// checkBitbucketBody refuses a body carrying the other one of Bitbucket's two
// top-level objects: the payload names no event of its own, so which object is
// present is the only cross-check there is, and parsing whichever was read
// first would turn a dispatcher's copy-paste mistake into the wrong pipeline
// running silently. GitLab's checkObjectKind is the same decision.
func checkBitbucketBody(event, want string, p bbHook) error {
	var got string
	switch {
	case want == bbPushObject && p.Push != nil, want == bbPullRequestObject && p.PullRequest != nil:
		return nil
	case p.Push != nil:
		got = bbPushObject
	case p.PullRequest != nil:
		got = bbPullRequestObject
	default:
		return fmt.Errorf("the %s payload carries no %q object, which is where Bitbucket puts "+
			"what happened", event, want)
	}
	return fmt.Errorf("the envelope calls this a %q event but the payload carries a %q object and "+
		"no %q one; one of the two is wrong and guessing which would run the wrong pipeline",
		event, got, want)
}

// bbTarget is the commit at one end of a change, empty when that end is null
// (a created or a deleted ref), which is truthfully nothing to diff against.
func bbTarget(r *bbRef) string {
	if r == nil {
		return ""
	}
	return realSHA(r.Target.Hash)
}
