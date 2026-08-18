package trigger

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// The providers this build ships with; constants because the envelope
// dispatch, the known-providers error and the reserved-name check must agree
// on the spelling.
const (
	providerGitHub    = "github"
	providerGitLab    = "gitlab"
	providerBitbucket = "bitbucket"
	providerGitea     = "gitea"
	providerSenro     = "senro"
)

// builtIn returns the shipped Provider of that name, or nil. Every built-in
// is reached through the same fromProvider a stranger's provider is; there
// is deliberately no private path for a built-in to drift down.
func builtIn(name string) Provider {
	switch name {
	case providerGitHub:
		return GitHub()
	case providerGitLab:
		return GitLab()
	case providerBitbucket:
		return Bitbucket()
	case providerGitea:
		return Gitea()
	case providerSenro:
		return senroShape{}
	default:
		return nil
	}
}

// reserved is every name a built-in already answers to, which is every name a
// caller's own provider may not claim.
var reserved = []string{
	providerGitHub, providerGitLab, providerBitbucket, providerGitea, providerSenro,
}

func isReserved(name string) bool {
	for _, r := range reserved {
		if name == r {
			return true
		}
	}
	return false
}

// Provider parses one event source's own payload into senro's Event: a Gitea
// event this build does not parse, an internal deployment bus. Hand one to
// LoadEvent or ReadEvent and it joins the built-ins, which are dispatched
// through this same interface. A caller that already has the event in its
// own form needs no Provider at all: Event is an ordinary struct, and
// building one and handing it to senro.WithTrigger has always worked.
//
// The contract: event is the envelope's "event" field (the source's own name
// for what happened) and payload is the source's body, verbatim. The Event
// returned must carry one of the five Kinds this build has, or nothing could
// ever match it and the mistake is reported. Provider is filled in from Name
// if left empty. Files' nil is load-bearing: nil means "this source did not
// say what changed" (Paths errors on it), empty and non-nil means "it said,
// and nothing changed"; never substitute one for the other.
//
// Everything that goes wrong is an error, never a nil Event with a nil error
// and never a silent no-match: an event nobody wanted and an event nobody
// wired correctly must not look the same. A Provider that returns nothing or
// panics is itself reported as an error naming the provider.
type Provider interface {
	// Name is the value the envelope's "provider" field carries for this
	// source. It must not be one this build already answers to (see
	// reserved): a provider that replaced one would make the same event file
	// mean different things in different binaries.
	Name() string

	// Parse turns one payload into an Event; see the interface's doc.
	Parse(event string, payload []byte) (*Event, error)
}

// allKinds is every Kind a trigger can be declared for, and therefore every
// Kind a Provider may return; one list so the check and its message agree.
var allKinds = []Kind{Push, PullRequest, Tag, Schedule, Manual}

func knownKind(k Kind) bool {
	for _, want := range allKinds {
		if k == want {
			return true
		}
	}
	return false
}

func kindNames() string {
	out := make([]string, len(allKinds))
	for i, k := range allKinds {
		out[i] = string(k)
	}
	return strings.Join(out, ", ")
}

// indexProviders checks the providers a caller supplied and keys them by
// name. All of them, before the event is looked at, so a duplicate name is
// reported whether or not the event happens to name it.
func indexProviders(ps []Provider) (map[string]Provider, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	out := make(map[string]Provider, len(ps))
	for i, p := range ps {
		if p == nil {
			return nil, fmt.Errorf("trigger: provider %d is nil", i+1)
		}
		name := p.Name()
		switch {
		case name == "":
			return nil, fmt.Errorf("trigger: provider %d (%T) names itself nothing; "+
				"its Name is the value an event's \"provider\" field must carry", i+1, p)
		case isReserved(name):
			return nil, fmt.Errorf("trigger: a provider may not be called %q: this build already "+
				"parses it, and one that replaced it would make the same event file mean "+
				"different things in different binaries", name)
		}
		if prev, dup := out[name]; dup {
			return nil, fmt.Errorf("trigger: two providers are both called %q (%T and %T), "+
				"so an event naming it would be parsed by whichever won", name, prev, p)
		}
		out[name] = p
	}
	return out, nil
}

// knownProviders names everything this process can parse, for the error a
// person debugging a dispatcher actually reads.
func knownProviders(custom map[string]Provider) string {
	var b strings.Builder
	b.WriteString(`this build parses "github", "gitlab", "bitbucket" and "gitea" webhook ` +
		`payloads and senro's own "senro" shape for a manual or scheduled run`)
	if len(custom) == 0 {
		return b.String()
	}
	names := make([]string, 0, len(custom))
	for name := range custom {
		names = append(names, fmt.Sprintf("%q", name))
	}
	sort.Strings(names)
	b.WriteString(", and this process was given " + strings.Join(names, ", "))
	return b.String()
}

// fromProvider runs one Provider and holds what comes back to the contract
// Provider documents. Every provider goes through here, built-ins included.
// The recover is the same decision notify.Renderer's is: a panic in foreign
// code becomes an ordinary error naming the provider, so "not my business"
// (exit 78) and "somebody wired this wrong" stay distinguishable.
func fromProvider(p Provider, event string, payload []byte) (ev *Event, err error) {
	name := p.Name()
	defer func() {
		if r := recover(); r != nil {
			ev, err = nil, fmt.Errorf("the %s provider panicked parsing a %q event: %v", name, event, r)
		}
	}()

	ev, err = p.Parse(event, payload)
	if err != nil {
		return nil, fmt.Errorf("the %s %q event: %w", name, event, err)
	}
	switch {
	case ev == nil:
		return nil, fmt.Errorf("the %s provider returned no event and no error for a %q event, "+
			"which is neither a parse nor a refusal", name, event)
	case ev.Kind == "":
		return nil, fmt.Errorf("the %s provider parsed a %q event into an event that says nothing "+
			"about what happened (empty kind); it must be one of: %s", name, event, kindNames())
	case !knownKind(ev.Kind):
		return nil, fmt.Errorf("the %s provider parsed a %q event into kind %q, which no trigger "+
			"can be declared for and which nothing could therefore ever match; it must be one "+
			"of: %s", name, event, ev.Kind, kindNames())
	}
	if ev.Provider == "" {
		ev.Provider = name
	}
	return ev, nil
}

// errNoProvider is the envelope's missing-provider message, named because
// the sentence is long.
var errNoProvider = errors.New(`the event names no provider: it needs ` +
	`{"provider": "github"|"gitlab"|"bitbucket"|"gitea"|"senro", "event": ..., "payload": {...}}`)
