package trigger

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xavidop/senro/internal/workspace"
)

// Branches narrows a trigger to the branches whose names match any of
// patterns.
//
//	trigger.OnPush(trigger.Branches("main", "release/*"))
//
// Patterns use senro's one glob syntax (the same one workspace excludes and
// Inputs use): "*" and "?" match within a segment, "**" spans segments, so
// "release/*" is release/1.0 but not release/1.0/hotfix. On a pull request
// this tests the BASE branch; see Event.Branch.
func Branches(patterns ...string) Option {
	return optionFunc(func(t *Trigger) {
		if len(patterns) == 0 {
			t.errf("Branches needs at least one pattern")
			return
		}
		t.add(predicate{
			name:  "branches",
			args:  patterns,
			kinds: []Kind{Push, PullRequest, Manual},
			match: func(ev *Event) (bool, error) {
				return matchAny(patterns, ev.Branch), nil
			},
		})
	})
}

// Paths narrows a trigger to events that changed at least one file matching
// any of patterns, using Branches' syntax.
//
//	trigger.OnPush(trigger.Branches("main"), trigger.Paths("services/**"))
//
// A filter on the event's own changed-file list (Event.Files), never the
// working tree; not an affected-set computation, which is precise and runs
// inside a run, where this is cheap and avoids starting one. An event with
// no changed-file list is an error, not a no-match: a GitHub pull_request
// payload carries none (the list is a separate API call), so
// OnPullRequest(Paths(...)) fails loudly. Supply the list through the
// provider-neutral event's files field if you have fetched it.
func Paths(patterns ...string) Option {
	return optionFunc(func(t *Trigger) {
		if len(patterns) == 0 {
			t.errf("Paths needs at least one pattern")
			return
		}
		t.add(predicate{
			name:  "paths",
			args:  patterns,
			kinds: []Kind{Push, PullRequest, Tag, Manual},
			match: func(ev *Event) (bool, error) {
				if ev.Files == nil {
					return false, fmt.Errorf(
						"trigger: Paths%v was asked of a %s event that carries no changed-file list, "+
							"so it can be neither true nor false; a GitHub pull_request payload never "+
							"carries one, and this build does not call the API to fetch it",
						patterns, ev.Kind)
				}
				for _, f := range ev.Files {
					if matchAny(patterns, f) {
						return true, nil
					}
				}
				return false, nil
			},
		})
	})
}

// Actions narrows a pull request trigger to the named actions.
//
//	trigger.OnPullRequest(trigger.Actions("opened", "synchronize"))
//
// Exact names, not patterns: the set is the provider's own and closed, so a
// glob would only be a way to typo one silently. GitHub's "synchronize"
// fires when a PR branch gets new commits; a Provider carries its source's
// own names through unchanged (GitLab says "open" and "update").
func Actions(actions ...string) Option {
	return optionFunc(func(t *Trigger) {
		if len(actions) == 0 {
			t.errf("Actions needs at least one action")
			return
		}
		t.add(predicate{
			name:  "actions",
			args:  actions,
			kinds: []Kind{PullRequest},
			match: func(ev *Event) (bool, error) {
				for _, a := range actions {
					if a == ev.Action {
						return true, nil
					}
				}
				return false, nil
			},
		})
	})
}

// Semver narrows a tag trigger to tags that are semantic versions satisfying
// constraint.
//
//	trigger.OnTag(trigger.Semver(">=1.0.0"))
//
// A constraint is one or more comparisons, separated by spaces or commas,
// all of which must hold: ">=1.0.0", ">=1.0.0 <2.0.0". The operators are
// >=, <=, >, <, !=, = and ==; a bare version means =.
//
// A tag that is not a version is not a match and not an error: "latest" is
// rejected rather than read as 0.0.0. A leading "v" is accepted; everything
// else follows semver 2.0.0. Prereleases order the way semver says
// (1.0.0-rc.1 is below 1.0.0), and there is no separate exclude-prereleases
// rule on top. A constraint that does not parse is a wiring error, reported
// when the trigger is matched.
func Semver(constraint string) Option {
	return optionFunc(func(t *Trigger) {
		cs, err := parseConstraints(constraint)
		if err != nil {
			t.errf("Semver(%q): %s", constraint, err)
			return
		}
		t.add(predicate{
			name:  "semver",
			args:  []string{constraint},
			kinds: []Kind{Tag},
			match: func(ev *Event) (bool, error) {
				v, ok := parseVersion(ev.Tag)
				if !ok {
					return false, nil
				}
				for _, c := range cs {
					if !c.satisfied(v) {
						return false, nil
					}
				}
				return true, nil
			},
		})
	})
}

// Matcher is a question of your own that a trigger asks of an event, an
// Option like the built-in four:
//
//	func Author(who ...string) trigger.Option {
//		return trigger.Matcher{
//			Name:  "author",
//			Args:  who,
//			Kinds: []trigger.Kind{trigger.Push, trigger.PullRequest},
//			Match: func(ev *trigger.Event) (bool, error) {
//				return slices.Contains(who, ev.Params["author"]), nil
//			},
//		}
//	}
//
// A struct rather than a bare interface so every question stays renderable
// into the run's provenance record: Name and Args are exactly what String
// renders, so a custom matcher appears there like a built-in one. A custom
// matcher pairs naturally with a custom Provider that puts extra facts in
// Params.
//
// Match is called once per event, on the goroutine that called Select,
// before any run exists; it must not block, and never asks about the working
// tree or the network. True is a match, false is not, and an error is
// neither: return one when the event carries nothing to answer with, as
// Paths does. A panic is caught and becomes an error naming the matcher, so
// "not my business" (exit 78) and "somebody wired this wrong" stay
// distinguishable.
type Matcher struct {
	// Name is what String and the run's provenance record call this
	// question. Required.
	Name string

	// Args are what the question was asked with. Provenance only; they land
	// in run.json and CI logs, which the redactor does not sit in front of,
	// so a credential must not go in one.
	Args []string

	// Kinds are the event kinds this question has an answer for; any other
	// kind is a declaration error. Empty means every kind.
	Kinds []Kind

	// Match answers for one event. Required. See the type's doc for what
	// true, false and an error each mean.
	Match func(*Event) (bool, error)
}

func (m Matcher) applyTrigger(t *Trigger) {
	switch {
	case m.Name == "":
		t.errf("a Matcher needs a Name: it is what this question is called in the run's " +
			"provenance record and in every error message about it")
		return
	case m.Match == nil:
		t.errf("the %q Matcher has no Match function, so there is no question for it to answer", m.Name)
		return
	}
	p := predicate{
		name:  m.Name,
		args:  append([]string(nil), m.Args...),
		kinds: append([]Kind(nil), m.Kinds...),
		match: guarded(m.Name, m.Match),
	}
	if len(m.Kinds) == 0 {
		// Every kind: a matcher that named no kinds made no claim for add to
		// check.
		t.preds = append(t.preds, p)
		return
	}
	t.add(p)
}

// guarded turns a panic in somebody else's matcher into an ordinary error;
// see Matcher.
func guarded(name string, f func(*Event) (bool, error)) func(*Event) (bool, error) {
	return func(ev *Event) (ok bool, err error) {
		defer func() {
			if r := recover(); r != nil {
				ok, err = false, fmt.Errorf("trigger: the %s matcher panicked: %v", name, r)
			}
		}()
		return f(ev)
	}
}

// optionFunc adapts a function to Option, so every matcher here is one
// closure and there is no per-matcher type.
type optionFunc func(*Trigger)

func (f optionFunc) applyTrigger(t *Trigger) { f(t) }

// matchAny reports whether any pattern matches s. workspace.MatchGlob is
// senro's one glob syntax, already behind workspace excludes and cache
// inputs; a second implementation is how the two drift.
func matchAny(patterns []string, s string) bool {
	if s == "" {
		return false
	}
	for _, p := range patterns {
		if workspace.MatchGlob(p, s) {
			return true
		}
	}
	return false
}

// errNoConstraint is what an empty Semver argument reports. A separate value
// so the message is written once and the test can name it.
var errNoConstraint = errors.New("a constraint is required, for example \">=1.0.0\"")

// constraint is one comparison: an operator and the version it compares
// against.
type constraint struct {
	op string
	v  version
}

func (c constraint) satisfied(v version) bool {
	cmp := compareVersions(v, c.v)
	switch c.op {
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	case "!=":
		return cmp != 0
	default: // "=" and "=="
		return cmp == 0
	}
}

// parseConstraints splits a constraint string on whitespace and commas and
// parses each comparison. All are ANDed and there is no "or": an "or"
// spelled with a comma is the classic way to write a range that accidentally
// matches everything.
func parseConstraints(s string) ([]constraint, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	if len(fields) == 0 {
		return nil, errNoConstraint
	}
	out := make([]constraint, 0, len(fields))
	for _, f := range fields {
		op := "="
		rest := f
		for _, cand := range []string{">=", "<=", "==", "!=", ">", "<", "="} {
			if strings.HasPrefix(f, cand) {
				op = cand
				rest = strings.TrimPrefix(f, cand)
				break
			}
		}
		if op == "==" {
			op = "="
		}
		v, ok := parseVersion(strings.TrimSpace(rest))
		if !ok {
			return nil, fmt.Errorf("%q is not a version", rest)
		}
		out = append(out, constraint{op: op, v: v})
	}
	return out, nil
}
