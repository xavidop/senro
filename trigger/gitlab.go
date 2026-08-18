package trigger

import (
	"encoding/json"
	"fmt"
	"strings"
)

// GitLab's own names for the three events this build reads, in the spelling
// the body carries. The X-Gitlab-Event header spells the same three "Push
// Hook", "Tag Push Hook" and "Merge Request Hook", and normaliseGitLabEvent
// brings both to these.
const (
	glPush         = "push"
	glTagPush      = "tag_push"
	glMergeRequest = "merge_request"
)

// GitLab is the built-in GitLab provider, an ordinary Provider with no
// private path past fromProvider; see GitHub and Provider. Wrapping it under
// another name is how a dispatcher labels two GitLab instances apart or
// teaches it an event this does not parse:
//
//	type withComments struct{ trigger.Provider }
//
//	func (w withComments) Name() string { return "gitlab-comment" }
//	func (w withComments) Parse(event string, payload []byte) (*trigger.Event, error) {
//		if strings.Contains(event, "Note") {
//			return parseTheComment(payload)
//		}
//		return w.Provider.Parse(event, payload)
//	}
//
// See examples/extensions/gitlabcomment, which is exactly that.
func GitLab() Provider { return gitLab{} }

// gitLab parses GitLab webhook payloads. The field names are the webhook
// body's own; testdata holds real payloads trimmed to exactly these.
//
// What differs from GitHub: the body names its own event (object_kind), and
// a body that contradicts the envelope is refused rather than parsed as
// whichever was read first; there is no deleted or created flag, only the
// all-zero SHA at one end of the push; a merge request lives under
// object_attributes with its own action vocabulary ("open", "update",
// "merge"), carried through untranslated; and the commit list is truncated
// at 20 with the real count in total_commits_count, see untruncatedFiles.
type gitLab struct{}

// Name is the value an envelope's "provider" field carries for GitLab.
func (gitLab) Name() string { return providerGitLab }

// Parse turns one GitLab webhook payload into an Event. event is GitLab's
// own name for what happened, in either spelling: the X-Gitlab-Event
// header's ("Push Hook") or the body's object_kind ("push").
func (gitLab) Parse(event string, payload []byte) (*Event, error) {
	name := normaliseGitLabEvent(event)
	switch name {
	case glPush, glTagPush:
		return parseGitLabPush(name, payload)
	case glMergeRequest:
		return parseGitLabMergeRequest(payload)
	default:
		return nil, fmt.Errorf("this build parses %q, %q and %q (the X-Gitlab-Event spellings "+
			"%q, %q and %q), and %q is none of them",
			glPush, glTagPush, glMergeRequest,
			"Push Hook", "Tag Push Hook", "Merge Request Hook", name)
	}
}

// normaliseGitLabEvent turns "Merge Request Hook" into "merge_request", so
// the header spelling and the object_kind spelling reach the same case.
func normaliseGitLabEvent(event string) string {
	s := normaliseSpace(strings.ToLower(event))
	s = strings.TrimSuffix(s, " hook")
	return strings.ReplaceAll(s, " ", "_")
}

// glPushHook is the part of a GitLab push or tag_push payload this build
// reads. One struct for both: the bodies are the same shape and the ref
// decides which of senro's Push and Tag this is.
type glPushHook struct {
	ObjectKind string       `json:"object_kind"`
	Ref        string       `json:"ref"`
	Before     string       `json:"before"`
	After      string       `json:"after"`
	Commits    []hookCommit `json:"commits"`
	// TotalCommitsCount is how many commits the push really had, not how
	// many are in Commits; see untruncatedFiles.
	TotalCommitsCount int       `json:"total_commits_count"`
	Project           glProject `json:"project"`
}

// glMergeRequestHook is the part of a GitLab merge_request payload this
// build reads; everything that matters is under object_attributes.
type glMergeRequestHook struct {
	ObjectKind       string `json:"object_kind"`
	ObjectAttributes struct {
		// IID is the per-project number a person sees. The sibling "id" is
		// the instance-wide database key and is deliberately not read.
		IID          int    `json:"iid"`
		Action       string `json:"action"`
		TargetBranch string `json:"target_branch"`
		SourceBranch string `json:"source_branch"`
		LastCommit   struct {
			ID string `json:"id"`
		} `json:"last_commit"`
	} `json:"object_attributes"`
	Project glProject `json:"project"`
}

// glProject is GitLab's project object. path_with_namespace is the
// "group/project" that Repo means; GitHub spells the same thing full_name.
type glProject struct {
	PathWithNamespace string `json:"path_with_namespace"`
	DefaultBranch     string `json:"default_branch"`
}

func parseGitLabPush(event string, payload []byte) (*Event, error) {
	var p glPushHook
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the push payload: %w", err)
	}
	// push and tag_push are one shape, so either object_kind is consistent
	// with either event name.
	if err := checkObjectKind(event, p.ObjectKind, glPush, glTagPush); err != nil {
		return nil, err
	}
	ev := &Event{
		Kind:          Push,
		Repo:          p.Project.PathWithNamespace,
		Ref:           p.Ref,
		DefaultBranch: p.Project.DefaultBranch,
		Files:         untruncatedFiles(p.TotalCommitsCount, p.Commits),
		Base:          Base{From: realSHA(p.Before), To: realSHA(p.After)},
		// No deleted flag exists to read: the all-zero SHA at the after end
		// is the whole of what GitLab says about a removed ref.
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

func parseGitLabMergeRequest(payload []byte) (*Event, error) {
	var p glMergeRequestHook
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the merge_request payload: %w", err)
	}
	if err := checkObjectKind(glMergeRequest, p.ObjectKind, glMergeRequest); err != nil {
		return nil, err
	}
	a := p.ObjectAttributes
	if a.Action == "" {
		return nil, fmt.Errorf("the merge_request payload names no action; GitLab's own are " +
			"\"open\", \"close\", \"reopen\", \"update\", \"merge\" and the approval ones, which " +
			"are the words an Actions matcher for GitLab is written with")
	}
	if a.TargetBranch == "" {
		return nil, fmt.Errorf("the merge_request payload names no target branch, which is the " +
			"branch a Branches matcher tests")
	}
	return &Event{
		Kind: PullRequest,
		Repo: p.Project.PathWithNamespace,
		// GitLab's own word, not GitHub's; see gitLab.
		Action: a.Action,
		Number: a.IID,
		// The TARGET branch, as the GitHub parser reports the base branch.
		Ref:           refHeads + a.TargetBranch,
		Branch:        a.TargetBranch,
		DefaultBranch: p.Project.DefaultBranch,
		// Deliberately nil, not empty: GitLab supplies no changed-file list
		// here, and Paths must be able to tell that from "nothing changed".
		Files: nil,
		// To only. object_attributes.oldrev is the previous head of the
		// SOURCE branch, not a commit on the target, so a diff from it would
		// cover only the last push. The body names no target-branch commit
		// at all, so From stays empty and the consumer decides.
		Base: Base{To: realSHA(a.LastCommit.ID)},
	}, nil
}

// checkObjectKind refuses a body whose object_kind contradicts the event the
// envelope named: parsing whichever was read first would turn a dispatcher's
// copy-paste mistake into the wrong pipeline running silently. An absent
// object_kind is not a disagreement; a trimmed payload still parses.
func checkObjectKind(event, got string, want ...string) error {
	if got == "" {
		return nil
	}
	for _, w := range want {
		if got == w {
			return nil
		}
	}
	return fmt.Errorf("the envelope calls this a %q event but the payload's own object_kind "+
		"says %q; one of the two is wrong and guessing which would run the wrong pipeline",
		event, got)
}
