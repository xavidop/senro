// Package trigger decides whether an incoming event is this pipeline's
// business. A pipeline binary is its own matcher: whatever dispatches runs
// hands it an event and reads its exit code, which keeps a dispatcher
// stateless.
//
//	ev, err := trigger.LoadEvent(*eventPath) // a file, "-" for stdin, "" for none
//	if err != nil {
//		return err
//	}
//	err = senro.Run(ctx, pipeline, senro.WithTrigger(ev,
//		trigger.OnPush(trigger.Branches("main"), trigger.Paths("services/**")),
//		trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
//		trigger.OnTag(trigger.Semver(">=1.0.0")),
//		trigger.OnSchedule("0 3 * * *", trigger.Params{"mode": "all"}),
//	))
//	if errors.Is(err, trigger.ErrNoMatch) {
//		os.Exit(78)
//	}
//
// Three outcomes, never two: a match runs the pipeline; no match is
// ErrNoMatch (no run directory, no events, exit 78 by convention); and an
// event that cannot be parsed, or is asked a question it carries no answer
// to (Paths with no changed-file list), is an ordinary error, because
// somebody wired this wrong and silence would hide it.
//
// This package never exits and never reads os.Args: senro is embeddable, so
// the event arrives through LoadEvent with a path main parsed itself, and no
// match is an error value main maps to an exit code.
//
// GitHub, GitLab, Bitbucket Cloud and Gitea webhook payloads are parsed, plus
// a provider-neutral shape for manual and scheduled invocations (see
// LoadEvent). None is privileged: all are Providers, and any other source is
// the same two-method interface, implementable from any repository, producing
// the same Event matched by the same matchers. See Provider and Matcher.
// OnSchedule matches an event that says "this is the 03:00 run"; senro has
// no scheduler of its own.
package trigger

import (
	"errors"
	"fmt"
	"strings"
)

// Kind is what happened. It is the first thing a trigger tests, and the only
// one every trigger tests: OnPush matches Push events and nothing else.
type Kind string

// The kinds this build understands. A GitHub push to refs/tags/... is a Tag,
// not a Push, because that is the question a pipeline is actually asking.
const (
	Push        Kind = "push"
	PullRequest Kind = "pull_request"
	Tag         Kind = "tag"
	Schedule    Kind = "schedule"
	Manual      Kind = "manual"
)

// Mode is how much of the repository a matched run covers. senro does not
// compute an affected set; Mode and Base are carried from the event to the
// run for the computation that does.
type Mode string

// The modes. A pull request builds what its changes affect; a push to the
// default branch, a tag and a scheduled run build everything.
const (
	ModeAll      Mode = "all"
	ModeAffected Mode = "affected"
)

// Base is the two ends of the comparison an affected-set computation would
// make. A push supplies before and after; a pull request supplies the base
// branch's commit and the head. A tag, schedule or manual run has no "since"
// and Base is zero. From is empty for a push that created a ref: GitHub
// sends the all-zero SHA there, which nothing can be diffed against.
type Base struct {
	// From is the commit to diff against: a push's before, a pull
	// request's base.
	From string `json:"from,omitempty"`
	// To is the commit to diff: a push's after, a pull request's head.
	To string `json:"to,omitempty"`
}

// Event is one thing that happened, in provider-neutral form. A provider's
// own payload is parsed into this once (see LoadEvent) and every matcher
// reads only this, so push, pull request and tag share one matching path
// rather than three that drift.
type Event struct {
	// Kind is what happened. Always set on an event this package produced.
	Kind Kind `json:"kind"`

	// Provider names where the event came from: "github" for a webhook
	// payload, "senro" for the provider-neutral shape. Provenance only;
	// nothing matches on it.
	Provider string `json:"provider,omitempty"`

	// Repo is the repository, "owner/name" for GitHub.
	Repo string `json:"repo,omitempty"`

	// Ref is the full git ref: "refs/heads/main", "refs/tags/v1.2.3".
	Ref string `json:"ref,omitempty"`

	// Branch is the branch a Branches matcher tests: the branch pushed to,
	// or for a pull request the BASE branch, not the head ("does this PR
	// target main", the same question GitHub Actions' branches filter
	// answers).
	Branch string `json:"branch,omitempty"`

	// Tag is the tag name a Semver matcher tests, with no refs/tags/ prefix
	// and the leading "v" left exactly as the tag has it.
	Tag string `json:"tag,omitempty"`

	// Action is a pull request's action: "opened", "synchronize", "closed".
	Action string `json:"action,omitempty"`

	// Number is a pull request's number. Provenance only.
	Number int `json:"number,omitempty"`

	// Deleted reports a push that removed the ref. No trigger matches one.
	Deleted bool `json:"deleted,omitempty"`

	// DefaultBranch is what makes a push to it Mode all rather than
	// affected. An event that does not say leaves every push affected.
	DefaultBranch string `json:"default_branch,omitempty"`

	// Schedule is the cron expression a scheduled event says it is the run
	// for, matched against OnSchedule's own by string.
	Schedule string `json:"schedule,omitempty"`

	// Files is the event's changed-file list; Paths filters on it and never
	// on the working tree. Nil means "the provider did not say", and Paths
	// against it is an error rather than an indistinguishable no-match;
	// empty and non-nil means "the provider said, and nothing changed".
	Files []string `json:"files,omitempty"`

	// Base is what an affected-set computation would diff. See Base.
	Base Base `json:"base,omitzero"`

	// Params are parameters the event itself carried. A matched trigger's
	// own Params take precedence; see Match.
	Params map[string]string `json:"params,omitempty"`
}

// Params are parameters a matched trigger contributes to the run, an option
// to every constructor here:
//
//	trigger.OnSchedule("0 3 * * *", trigger.Params{"mode": "all"})
//
// They land in the run's senro.Params, where a condition reads them.
// senro.WithParams still wins over them.
type Params map[string]string

func (p Params) applyTrigger(t *Trigger) {
	if t.params == nil {
		t.params = make(Params, len(p))
	}
	for k, v := range p {
		t.params[k] = v
	}
}

// Option narrows a trigger, or adds to what a match carries. The method is
// unexported deliberately: every question a trigger asks must be renderable
// into the run's provenance record and error messages, which a bare
// interface could not promise. Matcher is the public way in and carries the
// name and arguments alongside the function.
type Option interface {
	applyTrigger(*Trigger)
}

// Trigger is one declaration of what a pipeline runs for. The constructors
// below are the only way to build one; a zero Trigger matches nothing.
type Trigger struct {
	kind   Kind
	preds  []predicate
	params Params

	// err is a declaration that cannot be honoured (a Semver that does not
	// parse, a matcher asked of the wrong kind). Held rather than returned
	// so constructors stay inline, and reported by Select before anything
	// matches, so a mistake in the third trigger is never masked by the
	// first one matching.
	err error
}

// predicate is one question a trigger asks of an event: one shape for all of
// them, so a new matcher adds a constructor and nothing else.
type predicate struct {
	// name and args are what String and the run's provenance record show.
	name string
	args []string
	// kinds are the event kinds this question has an answer for; a Semver
	// asked of a push is a declaration that can never be true, worth a
	// message.
	kinds []Kind
	// match answers for one event. An error means the event carries nothing
	// to answer with; see Event.Files.
	match func(*Event) (bool, error)
}

// OnPush declares that this pipeline runs for a push to a branch. A push to
// a tag is OnTag, not this.
//
//	trigger.OnPush(trigger.Branches("main", "release/*"))
//
// With no options it matches every branch push. It never matches a push that
// deleted the branch.
func OnPush(opts ...Option) Trigger { return newTrigger(Push, opts) }

// OnPullRequest declares that this pipeline runs for a pull request.
//
//	trigger.OnPullRequest(trigger.Actions("opened", "synchronize"))
//
// With no options it matches every action, including ones that are rarely
// worth a build ("labeled", "assigned"), so naming the actions you want is
// usually right. Branches tests the pull request's BASE branch; see
// Event.Branch.
func OnPullRequest(opts ...Option) Trigger { return newTrigger(PullRequest, opts) }

// OnTag declares that this pipeline runs for a pushed tag.
//
//	trigger.OnTag(trigger.Semver(">=1.0.0"))
//
// With no options it matches every tag. It never matches a deleted one.
func OnTag(opts ...Option) Trigger { return newTrigger(Tag, opts) }

// OnSchedule declares that this pipeline runs for the scheduled invocation
// named by cron.
//
//	trigger.OnSchedule("0 3 * * *", trigger.Params{"mode": "all"})
//
// senro neither schedules nor parses cron: the string is compared to the
// event's own, whitespace-normalised, so each OnSchedule selects its own
// work when two schedules point at one binary.
func OnSchedule(cron string, opts ...Option) Trigger {
	t := newTrigger(Schedule, opts)
	norm := normaliseSpace(cron)
	if norm == "" {
		t.errf("OnSchedule needs a cron expression to match the event's own against")
		return t
	}
	t.preds = append(t.preds, predicate{
		name:  "schedule",
		args:  []string{norm},
		kinds: []Kind{Schedule},
		match: func(ev *Event) (bool, error) {
			return normaliseSpace(ev.Schedule) == norm, nil
		},
	})
	return t
}

// OnManual declares that this pipeline runs for a manual invocation, which
// arrives as the provider-neutral event shape LoadEvent documents.
func OnManual(opts ...Option) Trigger { return newTrigger(Manual, opts) }

func newTrigger(k Kind, opts []Option) Trigger {
	t := Trigger{kind: k}
	for _, o := range opts {
		if o == nil {
			t.errf("a nil option")
			continue
		}
		o.applyTrigger(&t)
	}
	return t
}

// errf records the first declaration error and keeps the rest, so a trigger
// with two mistakes reports the first one rather than the last.
func (t *Trigger) errf(format string, a ...any) {
	if t.err != nil {
		return
	}
	t.err = fmt.Errorf("trigger: %s: %s", t.kindLabel(), fmt.Sprintf(format, a...))
}

func (t Trigger) kindLabel() string {
	switch t.kind {
	case Push:
		return "OnPush"
	case PullRequest:
		return "OnPullRequest"
	case Tag:
		return "OnTag"
	case Schedule:
		return "OnSchedule"
	case Manual:
		return "OnManual"
	default:
		return "trigger"
	}
}

// add attaches one predicate, refusing it when this trigger's kind has no
// answer for the question. Every matcher goes through here, so "which
// matchers apply to which kinds" is one table and not five.
func (t *Trigger) add(p predicate) {
	for _, k := range p.kinds {
		if k == t.kind {
			t.preds = append(t.preds, p)
			return
		}
	}
	t.errf("%s applies to %s, not to a %s event", p.name, kindList(p.kinds), t.kind)
}

func kindList(ks []Kind) string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return strings.Join(out, " or ")
}

// String describes the trigger the way its declaration reads, used in the
// run's provenance record and error messages:
//
//	push(branches=[main release/*], paths=[services/**])
//
// Deliberately without the Params: this string goes into run.json and CI
// logs, neither of which the run's redactor sits in front of, and
// senro.WithParams promises a parameter value never lands anywhere durable.
func (t Trigger) String() string {
	if len(t.preds) == 0 {
		return string(t.kind)
	}
	parts := make([]string, 0, len(t.preds))
	for _, p := range t.preds {
		parts = append(parts, fmt.Sprintf("%s=[%s]", p.name, strings.Join(p.args, " ")))
	}
	return fmt.Sprintf("%s(%s)", t.kind, strings.Join(parts, ", "))
}

// matches answers whether ev is this trigger's business: the kind first,
// then every predicate, all of which must hold.
func (t Trigger) matches(ev *Event) (bool, error) {
	if ev.Kind != t.kind {
		return false, nil
	}
	// GitHub still sends a push for a deleted branch or tag, and a ref that
	// no longer exists has nothing to build.
	if ev.Deleted {
		return false, nil
	}
	for _, p := range t.preds {
		ok, err := p.match(ev)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// Match is what a matched event produced: which trigger claimed it, and the
// three things it carries into the run.
type Match struct {
	// Event is the event that matched.
	Event *Event
	// Trigger is the first declared trigger that claimed it.
	Trigger Trigger
	// Mode is how much of the repository this run covers. See Mode.
	Mode Mode
	// Base is what an affected-set computation would diff. See Base.
	Base Base
	// Params are the event's own parameters with the matched trigger's laid
	// over them.
	Params Params
}

// ErrNoMatch reports that the event is not this pipeline's business. It is
// not a failure: the pipeline decided, and decided no. The convention is
// exit 78 (EX_CONFIG), so a dispatcher can tell "nothing to do here" from
// success and failure. Recover it with errors.Is on Select's or senro.Run's
// error.
var ErrNoMatch = errors.New("no trigger matched the event")

// Select reports which of ts claims ev, in declaration order: the first
// match wins. Every trigger's declaration is checked before any is matched,
// so a mistake in the third trigger is reported even when the first would
// have matched. A nil ev returns (nil, nil): "this process was not handed an
// event", which senro.WithTrigger treats as no gating at all.
func Select(ev *Event, ts ...Trigger) (*Match, error) {
	if ev == nil {
		return nil, nil
	}
	if len(ts) == 0 {
		return nil, errors.New("trigger: an event was supplied but no trigger was declared, " +
			"so there is nothing to match it against")
	}
	for _, t := range ts {
		if t.err != nil {
			return nil, t.err
		}
		if t.kind == "" {
			return nil, errors.New("trigger: a zero Trigger declares nothing; " +
				"build one with OnPush, OnPullRequest, OnTag, OnSchedule or OnManual")
		}
	}
	if ev.Kind == "" {
		return nil, errors.New("trigger: the event says nothing about what happened (empty kind)")
	}
	var reasons []string
	for _, t := range ts {
		ok, err := t.matches(ev)
		if err != nil {
			return nil, err
		}
		if ok {
			return &Match{
				Event:   ev,
				Trigger: t,
				Mode:    modeFor(ev),
				Base:    ev.Base,
				Params:  mergeParams(ev.Params, t.params),
			}, nil
		}
		reasons = append(reasons, fmt.Sprintf("  %s: %s", t.String(), t.rejection(ev)))
	}
	return nil, fmt.Errorf("trigger: %w: %s\n%s", ErrNoMatch, describe(ev), strings.Join(reasons, "\n"))
}

// rejection names why t, specifically, did not claim ev: a kind it does not
// answer, a deleted ref (which matches nothing, before any predicate runs),
// or the first predicate that said no. matches() already stops at the first
// failing predicate for the same reason: an operator wants the one true
// cause, not every predicate that would also have failed.
func (t Trigger) rejection(ev *Event) string {
	if ev.Kind != t.kind {
		return fmt.Sprintf("only answers %s events, this was %s", t.kind, ev.Kind)
	}
	if ev.Deleted {
		return "the ref was deleted, which never matches"
	}
	for _, p := range t.preds {
		ok, err := p.match(ev)
		if err != nil || !ok {
			return fmt.Sprintf("rejected by %s=[%s]", p.name, strings.Join(p.args, " "))
		}
	}
	return "did not match"
}

// describe names the event for an operator reading a log: what happened and
// to what, never the whole payload.
func describe(ev *Event) string {
	var b strings.Builder
	b.WriteString(string(ev.Kind))
	switch {
	case ev.Ref != "":
		b.WriteString(" " + ev.Ref)
	case ev.Schedule != "":
		b.WriteString(" " + ev.Schedule)
	}
	if ev.Action != "" {
		b.WriteString(" (" + ev.Action + ")")
	}
	if ev.Deleted {
		b.WriteString(" (deleted)")
	}
	return b.String()
}

// Mode reports how much of the repository a run for this event covers: a
// property of what happened, not of which trigger claimed it, which is why
// Match.Mode is exactly this value. Exported because an affected-set
// computation needs the answer before there is a Match (see
// github.com/xavidop/senro/change). A nil Event is ModeAll: no event is the
// local loop, and the local loop builds everything.
func (ev *Event) Mode() Mode {
	if ev == nil {
		return ModeAll
	}
	return modeFor(ev)
}

// modeFor is the one place the mode rule lives: a pull request and a push
// off the default branch build what changed; everything else builds all. An
// event that does not name the default branch cannot be a push to it as far
// as this can tell, so it is affected.
func modeFor(ev *Event) Mode {
	switch ev.Kind {
	case PullRequest:
		return ModeAffected
	case Push:
		if ev.DefaultBranch != "" && ev.Branch == ev.DefaultBranch {
			return ModeAll
		}
		return ModeAffected
	default:
		return ModeAll
	}
}

// mergeParams lays over on top of base without mutating either; nil when
// there is nothing to carry.
func mergeParams(base map[string]string, over Params) Params {
	if len(base) == 0 && len(over) == 0 {
		return nil
	}
	out := make(Params, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

// normaliseSpace collapses runs of whitespace and trims the ends.
// Whitespace only: this package does not parse cron, so "0 3 * * *" and
// "0 3 * * 0-6" are deliberately not equal.
func normaliseSpace(s string) string { return strings.Join(strings.Fields(s), " ") }
