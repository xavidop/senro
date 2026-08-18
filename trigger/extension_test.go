package trigger_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xavidop/senro/trigger"
)

// bus is a provider for an event shape senro has never seen: an internal
// deployment bus whose payload looks nothing like a forge webhook. It lives
// in trigger_test and touches only the exported API, the same thing somebody
// else's repository would have.
type bus struct{}

func (bus) Name() string { return "bus" }

func (bus) Parse(event string, payload []byte) (*trigger.Event, error) {
	var p struct {
		Repo    string            `json:"repo"`
		Branch  string            `json:"branch"`
		Tag     string            `json:"tag"`
		Trunk   string            `json:"trunk"`
		Touched []string          `json:"touched"`
		Review  string            `json:"review"`
		By      string            `json:"by"`
		Extra   map[string]string `json:"extra"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the bus payload: %w", err)
	}
	ev := &trigger.Event{
		Repo:          p.Repo,
		Branch:        p.Branch,
		Tag:           p.Tag,
		DefaultBranch: p.Trunk,
		Files:         p.Touched,
		Action:        p.Review,
		Params:        p.Extra,
	}
	switch event {
	case "landed":
		ev.Kind = trigger.Push
		ev.Ref = "refs/heads/" + p.Branch
	case "proposed":
		ev.Kind = trigger.PullRequest
		ev.Ref = "refs/heads/" + p.Branch
	case "released":
		ev.Kind = trigger.Tag
		ev.Ref = "refs/tags/" + p.Tag
	default:
		return nil, fmt.Errorf("unknown bus event %q: this provider parses "+
			"\"landed\", \"proposed\" and \"released\"", event)
	}
	if p.By != "" {
		if ev.Params == nil {
			ev.Params = map[string]string{}
		}
		ev.Params["author"] = p.By
	}
	return ev, nil
}

// envelope wraps a provider payload in the file format LoadEvent documents,
// so every test goes through the entry point a pipeline's main uses.
func envelope(t *testing.T, provider, event, payload string) string {
	t.Helper()
	return writeEvent(t, fmt.Sprintf(`{"provider": %q, "event": %q, "payload": %s}`,
		provider, event, payload))
}

// ─────────────────────────────────────────────────────────────────────────────
// A provider senro has never heard of.
// ─────────────────────────────────────────────────────────────────────────────

// TestACustomProviderParsesAnEventThisBuildHasNeverSeen: an event shape
// written outside senro becomes the same trigger.Event the built-ins
// produce, treated the same downstream.
func TestACustomProviderParsesAnEventThisBuildHasNeverSeen(t *testing.T) {
	path := envelope(t, "bus", "landed",
		`{"repo": "acme/app", "branch": "main", "trunk": "main",
		  "touched": ["services/api/main.go"], "by": "ada"}`)

	ev, err := trigger.LoadEvent(path, bus{})
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if ev.Kind != trigger.Push {
		t.Errorf("kind = %q, want push", ev.Kind)
	}
	if ev.Provider != "bus" {
		t.Errorf("provider = %q, want the provider's own name, filled in without the provider having to", ev.Provider)
	}
	if ev.Branch != "main" || ev.Repo != "acme/app" {
		t.Errorf("event = %+v, want branch main of acme/app", ev)
	}
	// The mode rule is the event's, not the provider's.
	if ev.Mode() != trigger.ModeAll {
		t.Errorf("mode = %q, want all for a push to the default branch", ev.Mode())
	}

	m, err := trigger.Select(ev, trigger.OnPush(trigger.Branches("main")))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if m == nil || m.Event != ev {
		t.Fatalf("Select = %+v, want the event it was given", m)
	}
	if m.Mode != trigger.ModeAll {
		t.Errorf("match mode = %q, want all", m.Mode)
	}
}

// TestEveryBuiltInMatcherWorksOnACustomProvidersEvent: the four shipped
// matchers took no GitHub-specific shortcut.
func TestEveryBuiltInMatcherWorksOnACustomProvidersEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		event   string
		payload string
		trig    trigger.Trigger
		want    bool
	}{
		{
			name:    "branches",
			event:   "landed",
			payload: `{"branch": "release/1.2", "trunk": "main", "touched": []}`,
			trig:    trigger.OnPush(trigger.Branches("release/*")),
			want:    true,
		},
		{
			name:    "branches that do not match",
			event:   "landed",
			payload: `{"branch": "wip", "trunk": "main", "touched": []}`,
			trig:    trigger.OnPush(trigger.Branches("main")),
			want:    false,
		},
		{
			name:    "paths",
			event:   "landed",
			payload: `{"branch": "main", "touched": ["services/api/main.go", "README.md"]}`,
			trig:    trigger.OnPush(trigger.Paths("services/**")),
			want:    true,
		},
		{
			name:    "actions",
			event:   "proposed",
			payload: `{"branch": "main", "review": "opened"}`,
			trig:    trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
			want:    true,
		},
		{
			name:    "semver",
			event:   "released",
			payload: `{"tag": "v1.4.0"}`,
			trig:    trigger.OnTag(trigger.Semver(">=1.0.0")),
			want:    true,
		},
		{
			name:    "semver below the constraint",
			event:   "released",
			payload: `{"tag": "v0.9.0"}`,
			trig:    trigger.OnTag(trigger.Semver(">=1.0.0")),
			want:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := trigger.LoadEvent(envelope(t, "bus", tc.event, tc.payload), bus{})
			if err != nil {
				t.Fatalf("LoadEvent: %v", err)
			}
			m, err := trigger.Select(ev, tc.trig)
			switch {
			case tc.want && err != nil:
				t.Fatalf("Select: %v", err)
			case tc.want && m == nil:
				t.Fatal("Select returned no match and no error")
			case !tc.want && !errors.Is(err, trigger.ErrNoMatch):
				t.Fatalf("Select = (%v, %v), want ErrNoMatch", m, err)
			}
		})
	}
}

// TestPathsAgainstACustomProviderThatSaidNothingIsStillAnError: nil Files is
// "nobody said", which a new provider must not be able to soften into a
// silent no-match.
func TestPathsAgainstACustomProviderThatSaidNothingIsStillAnError(t *testing.T) {
	ev, err := trigger.LoadEvent(envelope(t, "bus", "landed", `{"branch": "main"}`), bus{})
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	_, err = trigger.Select(ev, trigger.OnPush(trigger.Paths("services/**")))
	if err == nil {
		t.Fatal("Select accepted Paths against an event carrying no changed-file list")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
}

// TestAnUnknownProviderNamesEveryProviderThisProcessHas: the error must list
// the custom providers too, not only the shipped ones.
func TestAnUnknownProviderNamesEveryProviderThisProcessHas(t *testing.T) {
	_, err := trigger.LoadEvent(envelope(t, "gerrit", "push", `{}`), bus{})
	if err == nil {
		t.Fatal("LoadEvent accepted an event from a provider nothing can parse")
	}
	for _, want := range []string{
		"gerrit", "github", "gitlab", "bitbucket", "gitea", "senro", "bus",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q:\n%v", want, err)
		}
	}
}

// TestABuiltInProviderStillParsesWhenCustomOnesAreGiven: adding a provider
// widens the set, it does not replace it.
func TestABuiltInProviderStillParsesWhenCustomOnesAreGiven(t *testing.T) {
	ev, err := trigger.LoadEvent("testdata/github-push-branch.json", bus{})
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if ev.Provider != "github" || ev.Kind != trigger.Push {
		t.Errorf("event = %+v, want the built-in github parser's own result", ev)
	}
}

// shadow claims a name this build already owns.
type shadow struct{ name string }

func (s shadow) Name() string { return s.name }
func (shadow) Parse(string, []byte) (*trigger.Event, error) {
	return &trigger.Event{Kind: trigger.Manual}, nil
}

// TestACustomProviderMayNotShadowABuiltIn: silently overriding "github"
// would mean one event file parsing differently in two binaries.
func TestACustomProviderMayNotShadowABuiltIn(t *testing.T) {
	for _, name := range []string{"github", "gitlab", "bitbucket", "gitea", "senro"} {
		t.Run(name, func(t *testing.T) {
			_, err := trigger.LoadEvent(envelope(t, name, "push", `{"ref": "refs/heads/main"}`),
				shadow{name: name})
			if err == nil {
				t.Fatalf("LoadEvent accepted a provider claiming the built-in name %q", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("the error does not name the provider it refused:\n%v", err)
			}
		})
	}
}

func TestTwoProvidersWithTheSameNameIsAnError(t *testing.T) {
	_, err := trigger.LoadEvent(envelope(t, "bus", "landed", `{"branch": "main"}`),
		bus{}, shadow{name: "bus"})
	if err == nil {
		t.Fatal("LoadEvent accepted two providers both called bus")
	}
	if !strings.Contains(err.Error(), "bus") {
		t.Errorf("the error does not name the duplicate:\n%v", err)
	}
}

func TestAProviderWithNoNameIsAnError(t *testing.T) {
	_, err := trigger.LoadEvent(envelope(t, "bus", "landed", `{"branch": "main"}`), shadow{name: ""})
	if err == nil {
		t.Fatal("LoadEvent accepted a provider that names itself nothing")
	}
}

func TestANilProviderIsAnError(t *testing.T) {
	_, err := trigger.LoadEvent(envelope(t, "bus", "landed", `{"branch": "main"}`), nil)
	if err == nil {
		t.Fatal("LoadEvent accepted a nil provider")
	}
}

// broken is a provider that gets it wrong in each of the ways a provider can.
type broken struct{ how string }

func (broken) Name() string { return "broken" }

func (b broken) Parse(string, []byte) (*trigger.Event, error) {
	switch b.how {
	case "error":
		return nil, errors.New("the deployment bus payload is not what it says it is")
	case "nothing":
		return nil, nil
	case "unknown-kind":
		return &trigger.Event{Kind: trigger.Kind("deployment")}, nil
	case "no-kind":
		return &trigger.Event{Branch: "main"}, nil
	default:
		panic("the deployment bus provider dereferenced nil")
	}
}

// TestAProviderThatGetsItWrongIsAnErrorAndNotANoMatch: a provider's mistake
// is an ordinary error every time, never something a dispatcher could read
// as "nothing to do".
func TestAProviderThatGetsItWrongIsAnErrorAndNotANoMatch(t *testing.T) {
	for _, tc := range []struct {
		how  string
		says string
	}{
		{"error", "not what it says it is"},
		{"nothing", "broken"},
		{"unknown-kind", "deployment"},
		{"no-kind", "broken"},
		{"panic", "panicked"},
	} {
		t.Run(tc.how, func(t *testing.T) {
			ev, err := trigger.LoadEvent(envelope(t, "broken", "whatever", `{}`), broken{how: tc.how})
			if err == nil {
				t.Fatalf("LoadEvent accepted it and returned %+v", ev)
			}
			if errors.Is(err, trigger.ErrNoMatch) {
				t.Errorf("LoadEvent = %v, want an ordinary error rather than a no-match", err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the error does not say %q:\n%v", tc.says, err)
			}
		})
	}
}

// TestReadEventTakesProvidersToo, for a caller that already has the bytes.
func TestReadEventTakesProvidersToo(t *testing.T) {
	body := `{"provider": "bus", "event": "released", "payload": {"tag": "v2.0.0"}}`
	ev, err := trigger.ReadEvent(strings.NewReader(body), bus{})
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if ev.Kind != trigger.Tag || ev.Tag != "v2.0.0" {
		t.Errorf("event = %+v, want tag v2.0.0", ev)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A matcher senro has never heard of.
// ─────────────────────────────────────────────────────────────────────────────

// byAuthor is the kind of matcher a custom provider makes worth having: it
// asks a question about a field only that provider fills in.
func byAuthor(who ...string) trigger.Option {
	return trigger.Matcher{
		Name:  "author",
		Args:  who,
		Kinds: []trigger.Kind{trigger.Push, trigger.PullRequest},
		Match: func(ev *trigger.Event) (bool, error) {
			for _, w := range who {
				if ev.Params["author"] == w {
					return true, nil
				}
			}
			return false, nil
		},
	}
}

// TestACustomMatcherNarrowsATrigger: Matcher is the public way in, carrying
// the name and arguments provenance needs plus the one function only the
// caller can write.
func TestACustomMatcherNarrowsATrigger(t *testing.T) {
	mine := envelope(t, "bus", "landed", `{"branch": "main", "by": "dependabot[bot]"}`)
	theirs := envelope(t, "bus", "landed", `{"branch": "main", "by": "ada"}`)

	trig := trigger.OnPush(trigger.Branches("main"), byAuthor("dependabot[bot]"))

	ev, err := trigger.LoadEvent(mine, bus{})
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if _, err := trigger.Select(ev, trig); err != nil {
		t.Fatalf("Select on the author it wanted: %v", err)
	}

	other, err := trigger.LoadEvent(theirs, bus{})
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if _, err := trigger.Select(other, trig); !errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select on another author = %v, want ErrNoMatch", err)
	}
}

// TestACustomMatcherShowsUpInTheTriggersOwnDescription: a matcher nobody can
// see in the provenance record is a run nobody can explain.
func TestACustomMatcherShowsUpInTheTriggersOwnDescription(t *testing.T) {
	got := trigger.OnPush(trigger.Branches("main"), byAuthor("ada")).String()
	want := "push(branches=[main], author=[ada])"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// TestACustomMatcherWithNoKindsAppliesToEveryKind: leaving Kinds empty is how
// a matcher says it has an answer for anything, which a question about
// Params does.
func TestACustomMatcherWithNoKindsAppliesToEveryKind(t *testing.T) {
	any := trigger.Matcher{
		Name:  "always",
		Match: func(*trigger.Event) (bool, error) { return true, nil },
	}
	for _, trig := range []trigger.Trigger{
		trigger.OnPush(any), trigger.OnPullRequest(any), trigger.OnTag(any),
		trigger.OnManual(any), trigger.OnSchedule("0 3 * * *", any),
	} {
		if !strings.Contains(trig.String(), "always") {
			t.Errorf("%s did not accept a Matcher with no Kinds", trig)
		}
	}
}

// TestACustomMatcherDeclaredWrongIsReportedAtSelect, the same way a built-in
// mistake is: held on the trigger and reported before any trigger matches.
func TestACustomMatcherDeclaredWrongIsReportedAtSelect(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  trigger.Option
		says string
	}{
		{
			name: "no name",
			opt:  trigger.Matcher{Match: func(*trigger.Event) (bool, error) { return true, nil }},
			says: "Name",
		},
		{
			name: "no match function",
			opt:  trigger.Matcher{Name: "author"},
			says: "Match",
		},
		{
			name: "a kind this trigger is not",
			opt: trigger.Matcher{
				Name:  "author",
				Kinds: []trigger.Kind{trigger.PullRequest},
				Match: func(*trigger.Event) (bool, error) { return true, nil },
			},
			says: "pull_request",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := trigger.Select(pushEventForMatcher(), trigger.OnPush(tc.opt))
			if err == nil {
				t.Fatal("Select accepted a matcher that cannot be honoured")
			}
			if errors.Is(err, trigger.ErrNoMatch) {
				t.Errorf("Select = %v, want an ordinary error rather than a no-match", err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the error does not say %q:\n%v", tc.says, err)
			}
		})
	}
}

// TestACustomMatcherThatCannotAnswerIsAnErrorNotANoMatch mirrors Paths: an
// unanswerable question is neither true nor false.
func TestACustomMatcherThatCannotAnswerIsAnErrorNotANoMatch(t *testing.T) {
	_, err := trigger.Select(pushEventForMatcher(), trigger.OnPush(trigger.Matcher{
		Name: "author",
		Match: func(*trigger.Event) (bool, error) {
			return false, errors.New("this event carries no author")
		},
	}))
	if err == nil {
		t.Fatal("Select swallowed a matcher's error")
	}
	if errors.Is(err, trigger.ErrNoMatch) {
		t.Fatalf("Select = %v, want an ordinary error rather than a no-match", err)
	}
	if !strings.Contains(err.Error(), "no author") {
		t.Errorf("the error does not carry the matcher's own message:\n%v", err)
	}
}

// TestACustomMatcherThatPanicsIsAnErrorNotACrash: a dispatcher reads the
// exit code, and a panic is an exit status nobody wired a meaning to.
func TestACustomMatcherThatPanicsIsAnErrorNotACrash(t *testing.T) {
	_, err := trigger.Select(pushEventForMatcher(), trigger.OnPush(trigger.Matcher{
		Name:  "author",
		Match: func(*trigger.Event) (bool, error) { panic("a third party's matcher dereferenced nil") },
	}))
	if err == nil {
		t.Fatal("Select returned no error for a matcher that panicked")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("the error does not say the matcher panicked:\n%v", err)
	}
	if !strings.Contains(err.Error(), "dereferenced nil") {
		t.Errorf("the error does not carry the panic value:\n%v", err)
	}
}

func pushEventForMatcher() *trigger.Event {
	return &trigger.Event{
		Kind: trigger.Push, Provider: "bus", Repo: "acme/app",
		Ref: "refs/heads/main", Branch: "main", DefaultBranch: "main",
		Files: []string{"a.go"},
	}
}
