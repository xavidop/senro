package trigger

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// LoadEvent reads the event this process was handed.
//
//	ev, err := trigger.LoadEvent(*eventPath)
//
// path is a file, "-" for standard input, or "" for no event at all. The
// empty case is not an error: senro.Run given a nil event does no gating, so
// a dispatcher that forgets the flag over-runs visibly rather than silently
// never running. This package never looks at os.Args; main parses the flag.
//
// The file format is an envelope naming where the event came from, wrapped
// around the provider's own payload verbatim:
//
//	{
//	  "provider": "github",
//	  "event": "push",
//	  "payload": { ... the webhook body, verbatim ... }
//	}
//
// provider and event are both required (a GitHub body does not say which
// event it is; that is the X-GitHub-Event header). provider "github" takes
// "push" or "pull_request"; a push to refs/tags/... is a Tag. provider
// "gitlab" takes "push", "tag_push" or "merge_request", in that spelling or
// the X-Gitlab-Event header's; a merge request is senro's PullRequest kind
// with GitLab's own action words. provider "bitbucket" takes "repo:push" and
// every "pullrequest:..." event key, whose suffix is the action. provider
// "gitea" takes "push", "pull_request" and "create". Each translates into
// senro's one vocabulary; see GitLab, Bitbucket and Gitea for what differs.
//
// providers are event sources of the caller's own, joining the built-ins:
//
//	ev, err := trigger.LoadEvent(*eventPath, deploybus.Provider{})
//
// An envelope naming one is handed to it verbatim; see Provider. A provider
// claiming a name a built-in answers to is refused rather than allowed to
// shadow it.
//
// provider "senro" is the provider-neutral shape for invocations with no
// webhook behind them:
//
//	{"provider": "senro", "event": "schedule",
//	 "payload": {"schedule": "0 3 * * *", "params": {"mode": "all"}}}
//
//	{"provider": "senro", "event": "manual",
//	 "payload": {"ref": "refs/heads/main", "params": {"reason": "rebuild"}}}
//
// Its payload fields are Event's own, lowercased with underscores; branch is
// derived from ref when not given. files present and empty means "nothing
// changed"; absent means "nobody said", which Paths treats as an error.
// Anything else is an error naming what this build understands.
func LoadEvent(path string, providers ...Provider) (*Event, error) {
	switch path {
	case "":
		return nil, nil
	case "-":
		ev, err := ReadEvent(os.Stdin, providers...)
		if err != nil {
			return nil, fmt.Errorf("trigger: reading the event from standard input: %w", err)
		}
		return ev, nil
	}
	f, err := os.Open(path) // #nosec G304 -- the path is the operator's own, from a flag main parsed
	if err != nil {
		return nil, fmt.Errorf("trigger: %w", err)
	}
	defer func() { _ = f.Close() }()
	ev, err := ReadEvent(f, providers...)
	if err != nil {
		return nil, fmt.Errorf("trigger: %s: %w", path, err)
	}
	return ev, nil
}

// ReadEvent parses one event from r, for a caller that already has the bytes:
// a webhook body still in memory, or a test. See LoadEvent for the format and
// Provider for what providers are.
func ReadEvent(r io.Reader, providers ...Provider) (*Event, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return parseEnvelope(b, providers)
}

// envelope is the on-disk shape: which provider, which of its events, and
// that provider's own payload untouched. One shape for every provider.
type envelope struct {
	Provider string          `json:"provider"`
	Event    string          `json:"event"`
	Payload  json.RawMessage `json:"payload"`
}

func parseEnvelope(b []byte, providers []Provider) (*Event, error) {
	// Every supplied provider is checked before the event is read, so a
	// duplicate or reserved name is reported whether or not this event
	// names it; see indexProviders.
	custom, err := indexProviders(providers)
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return nil, errors.New("the event is empty")
	}
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("the event is not JSON this build understands: %w", err)
	}
	switch {
	case env.Provider == "":
		return nil, errNoProvider
	case env.Event == "":
		return nil, fmt.Errorf("the event names provider %q but no event type", env.Provider)
	case len(env.Payload) == 0:
		return nil, fmt.Errorf("the %s %q event carries no payload", env.Provider, env.Event)
	}
	// Built-ins first (indexProviders already refused their names), and both
	// go through fromProvider: one funnel, so every guarantee Provider
	// documents is held against senro's own parsers too.
	p := builtIn(env.Provider)
	if p == nil {
		p = custom[env.Provider]
	}
	if p == nil {
		return nil, fmt.Errorf("unknown provider %q: %s", env.Provider, knownProviders(custom))
	}
	return fromProvider(p, env.Event, env.Payload)
}

// neutralPayload is the provider-neutral shape. Deliberately not Event
// itself: Event has a Kind the envelope already carries, and a Base a caller
// should not be able to contradict the ref with.
type neutralPayload struct {
	Ref           string            `json:"ref"`
	Branch        string            `json:"branch"`
	Tag           string            `json:"tag"`
	Repo          string            `json:"repo"`
	DefaultBranch string            `json:"default_branch"`
	Schedule      string            `json:"schedule"`
	Files         []string          `json:"files"`
	Params        map[string]string `json:"params"`
}

// senroShape is the provider-neutral built-in, an ordinary Provider so it
// reaches Event through the same fromProvider everything else does.
// Unexported: there is no second instance of senro's own shape to wrap.
type senroShape struct{}

func (senroShape) Name() string { return providerSenro }

func (senroShape) Parse(event string, payload []byte) (*Event, error) {
	return parseNeutral(event, payload)
}

func parseNeutral(event string, payload []byte) (*Event, error) {
	var kind Kind
	switch event {
	case "schedule":
		kind = Schedule
	case "manual":
		kind = Manual
	default:
		return nil, fmt.Errorf("this build understands \"schedule\" and \"manual\"")
	}
	var p neutralPayload
	if err := strictUnmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the payload: %w", err)
	}
	if kind == Schedule && strings.TrimSpace(p.Schedule) == "" {
		return nil, errors.New(`it must say which schedule it is, ` +
			`for example {"schedule": "0 3 * * *"}`)
	}
	ev := &Event{
		Kind:          kind,
		Repo:          p.Repo,
		Ref:           p.Ref,
		Branch:        p.Branch,
		Tag:           p.Tag,
		DefaultBranch: p.DefaultBranch,
		Schedule:      p.Schedule,
		Files:         p.Files,
		Params:        p.Params,
	}
	if ev.Branch == "" {
		ev.Branch = strings.TrimPrefix(ev.Ref, refHeads)
		if ev.Branch == ev.Ref {
			ev.Branch = ""
		}
	}
	if ev.Tag == "" && strings.HasPrefix(ev.Ref, refTags) {
		ev.Tag = strings.TrimPrefix(ev.Ref, refTags)
	}
	return ev, nil
}

// strictUnmarshal refuses a field the target does not have, so "branches"
// where "branch" was meant is a message rather than a silent no-match. Only
// for senro's own shape: a provider's payload is theirs and carries fields
// this build ignores on purpose.
func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}
