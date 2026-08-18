package trigger

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Gitea's own names for the three events this build reads, in the
// X-Gitea-Event spelling. The body carries no event name, so the envelope's is
// the only one there is.
const (
	gtPushEvent        = "push"
	gtPullRequestEvent = "pull_request"
	gtCreateEvent      = "create"
)

// The ref_type words a create payload uses, which are the only thing saying
// what its ref names.
const (
	gtBranch = "branch"
	gtTag    = "tag"
)

// Gitea is the built-in Gitea provider, an ordinary Provider with no private
// path past fromProvider; see GitHub and Provider. Wrapping it under another
// name is how a dispatcher labels two Gitea instances apart or teaches it an
// event this does not parse; see examples/extensions/gitlabcomment, which does
// exactly that for GitLab.
func Gitea() Provider { return gitea{} }

// gitea parses Gitea webhook payloads. The field names are the webhook body's
// own; testdata holds real payloads trimmed to exactly these.
//
// What differs from GitHub: there is no deleted or created flag, only the
// all-zero SHA at one end of the push, as GitLab does it; a pull request
// carries Gitea's own action vocabulary ("opened", "synchronized", "closed"),
// through untranslated, and its number is under pull_request.number, the
// sibling id being the instance-wide key; the commit list is truncated with
// the real count in total_commits, see untruncatedFiles; and Gitea sends a
// create alongside the push for a new tag, whose ref is the SHORT name.
type gitea struct{}

// Name is the value an envelope's "provider" field carries for Gitea.
func (gitea) Name() string { return providerGitea }

// Parse turns one Gitea webhook payload into an Event. event is the value of
// the X-Gitea-Event header, which the envelope carries because the body does
// not.
func (gitea) Parse(event string, payload []byte) (*Event, error) {
	switch event {
	case gtPushEvent:
		return parseGiteaPush(payload)
	case gtPullRequestEvent:
		return parseGiteaPullRequest(payload)
	case gtCreateEvent:
		return parseGiteaCreate(payload)
	default:
		return nil, fmt.Errorf("this build parses %q (a branch or, when the ref is refs/tags/..., "+
			"a tag), %q and %q (a tag)", gtPushEvent, gtPullRequestEvent, gtCreateEvent)
	}
}

// gtPush is the part of a Gitea push payload this build reads. Gitea sends one
// for a tag as well as a branch, with the full ref in both, so the ref decides
// which of senro's Push and Tag this is, exactly as GitHub's does.
type gtPush struct {
	Ref     string       `json:"ref"`
	Before  string       `json:"before"`
	After   string       `json:"after"`
	Commits []hookCommit `json:"commits"`
	// TotalCommits is how many commits the push really had, not how many are
	// in Commits; see untruncatedFiles.
	TotalCommits int          `json:"total_commits"`
	Repository   gtRepository `json:"repository"`
}

// gtPullRequest is the part of a Gitea pull_request payload this build reads.
// There is no changed-file list in it, as there is none in GitHub's; see
// Paths.
type gtPullRequest struct {
	Action      string `json:"action"`
	Number      int    `json:"number"`
	PullRequest struct {
		// Number is the per-repository number a person sees. The sibling "id"
		// is the instance-wide database key and is deliberately not read.
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			// Ref is the base BRANCH name ("main"), not a full ref.
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"base"`
	} `json:"pull_request"`
	Repository gtRepository `json:"repository"`
}

// gtCreate is the part of a Gitea create payload this build reads. Its ref is
// the short name ("v1.0.0") where every other Gitea payload here carries the
// full one, which is why ref_type has to say what it names.
type gtCreate struct {
	SHA        string       `json:"sha"`
	Ref        string       `json:"ref"`
	RefType    string       `json:"ref_type"`
	Repository gtRepository `json:"repository"`
}

// gtRepository is Gitea's repository object, GitHub's field names exactly.
type gtRepository struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
}

func parseGiteaPush(payload []byte) (*Event, error) {
	var p gtPush
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the push payload: %w", err)
	}
	ev := &Event{
		Kind:          Push,
		Repo:          p.Repository.FullName,
		Ref:           p.Ref,
		DefaultBranch: p.Repository.DefaultBranch,
		Files:         untruncatedFiles(p.TotalCommits, p.Commits),
		Base:          Base{From: realSHA(p.Before), To: realSHA(p.After)},
		// No deleted flag exists to read: the all-zero SHA at the after end is
		// the whole of what a Gitea push says about a removed ref, as GitLab.
		Deleted: isNullSHA(p.After),
	}
	switch {
	case strings.HasPrefix(p.Ref, refHeads):
		ev.Branch = strings.TrimPrefix(p.Ref, refHeads)
	case strings.HasPrefix(p.Ref, refTags):
		ev.Kind = Tag
		ev.Tag = strings.TrimPrefix(p.Ref, refTags)
	case p.Ref == "":
		return nil, fmt.Errorf("the push payload names no ref")
	default:
		return nil, fmt.Errorf("the push payload's ref %q is neither a branch (%s...) "+
			"nor a tag (%s...)", p.Ref, refHeads, refTags)
	}
	return ev, nil
}

func parseGiteaPullRequest(payload []byte) (*Event, error) {
	var p gtPullRequest
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the pull_request payload: %w", err)
	}
	a := p.PullRequest
	if p.Action == "" {
		return nil, fmt.Errorf("the pull_request payload names no action; Gitea's own are " +
			"\"opened\", \"closed\", \"reopened\", \"edited\", \"synchronized\" and the label, " +
			"assignee and review ones, which are the words an Actions matcher for Gitea is " +
			"written with")
	}
	if a.Base.Ref == "" {
		return nil, fmt.Errorf("the pull_request payload names no base branch, which is the " +
			"branch a Branches matcher tests")
	}
	// Older Gitea payloads carry the number only at the top level.
	n := a.Number
	if n == 0 {
		n = p.Number
	}
	return &Event{
		Kind: PullRequest,
		Repo: p.Repository.FullName,
		// Gitea's own word, not GitHub's; see gitea.
		Action: p.Action,
		Number: n,
		// The BASE branch, as the GitHub parser reports it.
		Ref:           refHeads + a.Base.Ref,
		Branch:        a.Base.Ref,
		DefaultBranch: p.Repository.DefaultBranch,
		// Deliberately nil, not empty: Gitea supplies no changed-file list
		// here, and Paths must be able to tell that from "nothing changed".
		Files: nil,
		Base:  Base{From: realSHA(a.Base.SHA), To: realSHA(a.Head.SHA)},
	}, nil
}

// parseGiteaCreate reads the create Gitea sends for a new tag. It reads only a
// tag: Gitea sends a push for the same new branch carrying its commits and
// their changed files, and a second path deciding "a branch appeared" from
// weaker evidence is the divergence this build refuses. Forward one or the
// other for a tag too, or one tag runs the pipeline twice.
func parseGiteaCreate(payload []byte) (*Event, error) {
	var p gtCreate
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the create payload: %w", err)
	}
	switch p.RefType {
	case gtTag:
	case gtBranch:
		return nil, fmt.Errorf("this is the creation of the branch %q, and senro reads a branch "+
			"from the %q Gitea sends for the same ref, which carries its commits and their "+
			"changed files; only a create whose ref_type is %q is parsed here",
			p.Ref, gtPushEvent, gtTag)
	case "":
		return nil, fmt.Errorf("the create payload names no ref_type, which is the only thing "+
			"saying whether %q is a branch or a tag", p.Ref)
	default:
		return nil, fmt.Errorf("the create payload's ref_type is %q; Gitea's own are %q and %q",
			p.RefType, gtBranch, gtTag)
	}
	if p.Ref == "" {
		return nil, fmt.Errorf("the create payload names no ref")
	}
	return &Event{
		Kind: Tag,
		Repo: p.Repository.FullName,
		// Gitea sends the short name here, so senro's full ref is built from
		// it rather than read.
		Ref:           refTags + p.Ref,
		Tag:           p.Ref,
		DefaultBranch: p.Repository.DefaultBranch,
		// Deliberately nil, not empty: a create says nothing about changed
		// files, where the push Gitea sends for the same tag says "none".
		Files: nil,
		// To only: a create names the commit the tag points at and nothing to
		// diff it against.
		Base: Base{To: realSHA(p.SHA)},
	}, nil
}
