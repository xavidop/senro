package trigger

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// The field names below are the webhook payload's own; testdata holds real
// payloads trimmed to exactly these.
const (
	refHeads = "refs/heads/"
	refTags  = "refs/tags/"
)

// GitHub is the built-in GitHub provider, an ordinary Provider dispatched
// exactly the way a stranger's is. A caller who needs it under another name
// wraps it:
//
//	type ghe struct{ trigger.Provider }
//	func (ghe) Name() string { return "github-enterprise" }
//
//	ev, err := trigger.LoadEvent(*eventPath, ghe{trigger.GitHub()})
func GitHub() Provider { return gitHub{} }

// gitHub parses GitHub webhook payloads.
type gitHub struct{}

// Name is the value an envelope's "provider" field carries for GitHub.
func (gitHub) Name() string { return providerGitHub }

// Parse turns one GitHub webhook payload into an Event. event is the value of
// the X-GitHub-Event header, which the envelope carries because the body does
// not.
func (gitHub) Parse(event string, payload []byte) (*Event, error) {
	return parseGitHub(event, payload)
}

// ghPush is the part of a GitHub push payload this build reads. A tag push
// arrives as a push whose ref is refs/tags/..., so Kind is decided from the
// ref, not the event name; the "create" event GitHub also sends for the same
// tag carries strictly less and is deliberately not read.
type ghPush struct {
	Ref        string       `json:"ref"`
	Before     string       `json:"before"`
	After      string       `json:"after"`
	Created    bool         `json:"created"`
	Deleted    bool         `json:"deleted"`
	Commits    []hookCommit `json:"commits"`
	Repository ghRepository `json:"repository"`
}

// ghPullRequest is the part of a GitHub pull_request payload this build
// reads. There is no changed-file list in it: GitHub does not put one in the
// payload, and fetching it is an API call this build does not make. See
// Paths.
type ghPullRequest struct {
	Action      string `json:"action"`
	Number      int    `json:"number"`
	PullRequest struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"base"`
	} `json:"pull_request"`
	Repository ghRepository `json:"repository"`
}

type ghRepository struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
}

// parseGitHub turns one GitHub webhook payload into an Event. The provider
// and the event name are already in the error fromProvider wraps this with,
// so nothing here repeats them.
func parseGitHub(event string, payload []byte) (*Event, error) {
	switch event {
	case "push":
		return parseGitHubPush(payload)
	case "pull_request":
		return parseGitHubPullRequest(payload)
	default:
		return nil, fmt.Errorf("this build parses \"push\" " +
			"(a branch or, when the ref is refs/tags/..., a tag) and \"pull_request\"")
	}
}

func parseGitHubPush(payload []byte) (*Event, error) {
	var p ghPush
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the push payload: %w", err)
	}
	ev := &Event{
		Kind:          Push,
		Repo:          p.Repository.FullName,
		Ref:           p.Ref,
		Deleted:       p.Deleted,
		DefaultBranch: p.Repository.DefaultBranch,
		Files:         changedFiles(p.Commits),
		Base:          Base{From: realSHA(p.Before), To: realSHA(p.After)},
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

func parseGitHubPullRequest(payload []byte) (*Event, error) {
	var p ghPullRequest
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the pull_request payload: %w", err)
	}
	if p.Action == "" {
		return nil, fmt.Errorf("the pull_request payload names no action")
	}
	n := p.PullRequest.Number
	if n == 0 {
		n = p.Number
	}
	return &Event{
		Kind:          PullRequest,
		Repo:          p.Repository.FullName,
		Action:        p.Action,
		Number:        n,
		Ref:           refHeads + p.PullRequest.Base.Ref,
		Branch:        p.PullRequest.Base.Ref,
		DefaultBranch: p.Repository.DefaultBranch,
		// Deliberately nil, not empty: GitHub supplies no changed-file list
		// here, and Paths must be able to tell that from "nothing changed".
		Files: nil,
		Base:  Base{From: realSHA(p.PullRequest.Base.SHA), To: realSHA(p.PullRequest.Head.SHA)},
	}, nil
}

// hookCommit is the changed-path triple GitHub, GitLab and Gitea all spell
// the same way in a push payload, GitLab's and Gitea's being descended from
// GitHub's. One type, so one union rule serves all three.
type hookCommit struct {
	Added    []string `json:"added"`
	Removed  []string `json:"removed"`
	Modified []string `json:"modified"`
}

// changedFiles is the union of every commit's added, removed and modified
// paths, deduplicated and sorted. Every commit, not head_commit alone, or a
// file touched only in an earlier commit would be missed. Always non-nil,
// even with no commits: nil is reserved for a provider that said nothing
// (see Event.Files).
func changedFiles(commits []hookCommit) []string {
	seen := make(map[string]bool)
	out := make([]string, 0)
	add := func(paths []string) {
		for _, f := range paths {
			if f == "" || seen[f] {
				continue
			}
			seen[f] = true
			out = append(out, f)
		}
	}
	for _, c := range commits {
		add(c.Added)
		add(c.Removed)
		add(c.Modified)
	}
	sort.Strings(out)
	return out
}

// untruncatedFiles is changedFiles unless the source sent fewer commits than
// it says the push had, in which case NIL. GitLab caps the array at 20 and
// Gitea at ui.FEED_MAX_COMMIT_NUM (5 by default), and an incomplete list
// handed to Paths could answer "no match" when the truth is "match", so a
// truncated list becomes "the provider did not say" and Paths errors rather
// than silently skipping a build. GitHub sends no count to check against.
func untruncatedFiles(total int, commits []hookCommit) []string {
	if total > len(commits) {
		return nil
	}
	return changedFiles(commits)
}

// isNullSHA reports whether s is git's all-zero object name, sent for the end
// of a push that has no commit there (before on a created ref, after on a
// deleted one). It is not a commit and must never reach a Base. Any length:
// a SHA-256 repository's is 64 characters, and Gitea serves those.
func isNullSHA(s string) bool { return s != "" && strings.Trim(s, "0") == "" }

// realSHA blanks the all-zero SHA: an empty Base.From truthfully means
// nothing to diff against, where the null SHA would look like a commit.
// Shared with the GitLab and Gitea parsers, which send the same value.
func realSHA(s string) string {
	if isNullSHA(s) {
		return ""
	}
	return s
}
