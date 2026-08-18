// Package gitlabcomment teaches senro to run for a comment on a GitLab merge
// request, written the way a provider in somebody else's repository would
// be: it imports github.com/xavidop/senro/trigger and nothing else of
// senro's (extension_static_test.go checks that; extension_e2e_test.go
// drives it through a real run).
//
// GitLab sends a Note Hook when somebody comments, and "/retest" in a
// comment is a real CI trigger no push corresponds to. That is the shape of
// most extensions worth writing: one more event, layered on the built-in.
// Parse handles a comment and delegates everything else to trigger.GitLab(),
// then enriches what comes back with the author in Params, the field that
// exists for what senro's Event has no field for.
//
// Using it:
//
//	ev, err := trigger.LoadEvent(*eventPath, gitlabcomment.Provider{})
//	if err != nil {
//		return err
//	}
//	err = senro.Run(ctx, pipeline, senro.WithTrigger(ev,
//		trigger.OnPullRequest(
//			trigger.Branches("main"),
//			gitlabcomment.Command("retest"),
//		),
//	))
//
// The structs below name only the Note Hook fields this example reads, taken
// from the payload example in gitlab-org/gitlab webhook_events.md; check
// GitLab's own reference before relying on this in production.
package gitlabcomment

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/xavidop/senro/trigger"
)

const refHeads = "refs/heads/"

// Provider parses GitLab Note Hooks, and hands every other GitLab event to
// the built-in parser. It is a trigger.Provider; the zero value is ready to
// use.
type Provider struct {
	// Base parses a payload that is not a comment. Nil means
	// trigger.GitLab(); the field exists so a test can substitute one.
	Base trigger.Provider
}

// Name is the value an event envelope's "provider" field must carry to
// reach this parser. Deliberately not "gitlab": trigger refuses a provider
// claiming a built-in name.
func (Provider) Name() string { return "gitlab-comment" }

// Parse turns one GitLab webhook payload into a trigger.Event. event is
// GitLab's own name in either spelling ("Note Hook" or "note"). Anything
// that is not a comment goes to Base, so this provider is a superset of the
// built-in, not a competitor.
func (p Provider) Parse(event string, payload []byte) (*trigger.Event, error) {
	if normalise(event) != "note" {
		ev, err := p.base().Parse(event, payload)
		if err != nil {
			return nil, err
		}
		// The one thing the built-in does not carry, added on the way past.
		setAuthor(ev, authorOf(payload))
		return ev, nil
	}
	return parseNote(payload)
}

func (p Provider) base() trigger.Provider {
	if p.Base != nil {
		return p.Base
	}
	return trigger.GitLab()
}

// normalise turns "Note Hook" into "note", so the header spelling and the
// object_kind spelling reach the same case.
func normalise(event string) string {
	s := strings.ToLower(strings.Join(strings.Fields(event), " "))
	s = strings.TrimSuffix(s, " hook")
	return strings.ReplaceAll(s, " ", "_")
}

// noteHook is the part of a GitLab Note Hook this example reads.
type noteHook struct {
	ObjectAttributes struct {
		// NoteableType is what was commented on: "MergeRequest", "Commit",
		// "Issue" or "Snippet". Only the first is a thing to build.
		NoteableType string `json:"noteable_type"`
		Note         string `json:"note"`
		Action       string `json:"action"`
	} `json:"object_attributes"`
	MergeRequest struct {
		IID          int    `json:"iid"`
		TargetBranch string `json:"target_branch"`
		SourceBranch string `json:"source_branch"`
		LastCommit   struct {
			ID string `json:"id"`
		} `json:"last_commit"`
	} `json:"merge_request"`
	Project struct {
		PathWithNamespace string `json:"path_with_namespace"`
		DefaultBranch     string `json:"default_branch"`
	} `json:"project"`
	User struct {
		Username string `json:"username"`
	} `json:"user"`
}

func parseNote(payload []byte) (*trigger.Event, error) {
	var p noteHook
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the note payload: %w", err)
	}
	a := p.ObjectAttributes
	if a.NoteableType != "MergeRequest" {
		// An error, not a silent no-match: senro keeps three outcomes, and
		// so does a provider written for it.
		return nil, fmt.Errorf("this provider reads comments on a merge request, and this one is "+
			"on a %q", a.NoteableType)
	}
	if p.MergeRequest.TargetBranch == "" {
		return nil, fmt.Errorf("the note payload names no merge_request.target_branch, which is " +
			"the branch a Branches matcher tests")
	}
	params := map[string]string{
		// GitLab's own word for what happened to the comment: "create" for a
		// new one, "update" for an edit.
		"note_action": a.Action,
		"comment":     a.Note,
	}
	if cmd := command(a.Note); cmd != "" {
		params["command"] = cmd
	}
	if p.MergeRequest.SourceBranch != "" {
		params["source_branch"] = p.MergeRequest.SourceBranch
	}
	if p.MergeRequest.IID != 0 {
		params["merge_request"] = strconv.Itoa(p.MergeRequest.IID)
	}
	if p.User.Username != "" {
		params["author"] = p.User.Username
	}
	return &trigger.Event{
		Kind: trigger.PullRequest,
		Repo: p.Project.PathWithNamespace,
		// The comment is what happened, so that is the Action; the command
		// inside it is a question Actions cannot ask, which Command is for.
		Action: "comment",
		Number: p.MergeRequest.IID,
		// The TARGET branch, as senro's own parsers report a base branch.
		Ref:           refHeads + p.MergeRequest.TargetBranch,
		Branch:        p.MergeRequest.TargetBranch,
		DefaultBranch: p.Project.DefaultBranch,
		// Deliberately nil, not empty: a comment says nothing about which
		// files changed, and trigger.Paths must be able to tell that from
		// "nothing changed".
		Files:  nil,
		Base:   trigger.Base{To: p.MergeRequest.LastCommit.ID},
		Params: params,
	}, nil
}

// command is the slash command a comment opens with, without its slash:
// "/retest please" is "retest". Empty for an ordinary comment, which is a
// real event that simply matches no trigger.
func command(note string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(note), "\n")
	if !strings.HasPrefix(line, "/") {
		return ""
	}
	word, _, _ := strings.Cut(strings.TrimPrefix(line, "/"), " ")
	return strings.TrimSpace(word)
}

// authorOf pulls the username out of whichever place the payload keeps it:
// a push body has top-level user_username, a merge request body has
// user.username.
func authorOf(payload []byte) string {
	var p struct {
		UserUsername string `json:"user_username"`
		User         struct {
			Username string `json:"username"`
		} `json:"user"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return ""
	}
	if p.UserUsername != "" {
		return p.UserUsername
	}
	return p.User.Username
}

// setAuthor puts the username in Params, the field that exists for what a
// source carries that senro's vocabulary does not.
func setAuthor(ev *trigger.Event, username string) {
	if ev == nil || username == "" {
		return
	}
	if ev.Params == nil {
		ev.Params = map[string]string{}
	}
	ev.Params["author"] = username
}

// Command is a trigger.Matcher for the slash command a comment opens with:
// a pipeline that reruns itself when somebody asks.
//
//	trigger.OnPullRequest(trigger.Branches("main"), gitlabcomment.Command("retest"))
//
// It renders in a run's provenance record exactly as a built-in matcher
// would. A comment with no command is an ordinary "no" rather than the error
// Author gives: "nobody asked for anything" is an answer, where "this event
// does not say who" is the absence of one.
func Command(commands ...string) trigger.Option {
	return trigger.Matcher{
		Name:  "command",
		Args:  commands,
		Kinds: []trigger.Kind{trigger.PullRequest},
		Match: func(ev *trigger.Event) (bool, error) {
			for _, c := range commands {
				if strings.TrimPrefix(c, "/") == ev.Params["command"] {
					return true, nil
				}
			}
			return false, nil
		},
	}
}

// Author is a trigger.Matcher for the field only this provider fills in.
//
//	trigger.OnPush(trigger.Branches("main"), gitlabcomment.Author("dependabot"))
//
// It applies to a push and a merge request as well as a comment, because
// Parse attaches the author on the way past the built-in parser too.
func Author(usernames ...string) trigger.Option {
	return trigger.Matcher{
		Name:  "author",
		Args:  usernames,
		Kinds: []trigger.Kind{trigger.Push, trigger.PullRequest, trigger.Tag},
		Match: func(ev *trigger.Event) (bool, error) {
			who, ok := ev.Params["author"]
			if !ok {
				// Neither true nor false: this event does not say. The same
				// answer trigger.Paths gives.
				return false, fmt.Errorf("gitlabcomment.Author%v was asked of a %s event that "+
					"carries no author", usernames, ev.Kind)
			}
			for _, u := range usernames {
				if u == who {
					return true, nil
				}
			}
			return false, nil
		},
	}
}
