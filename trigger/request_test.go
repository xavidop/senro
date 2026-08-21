package trigger_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/xavidop/senro/trigger"
)

const hookSecret = "shh"

// rawPayload is the provider's OWN body, dug out of a testdata envelope.
// That is what a webhook actually delivers: the envelope is senro's file
// format, and a server never sees one.
func rawPayload(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	var env struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("reading the fixture envelope: %v", err)
	}
	if len(env.Payload) == 0 {
		t.Fatalf("%s carries no payload", name)
	}
	return env.Payload
}

func ghSign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(hookSecret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func delivery(body []byte, header, value string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	r.Header.Set(header, value)
	return r
}

// The assertion that matters most: a delivery parsed straight off the wire
// is the SAME Event the same delivery produces through a file. If the two
// ever disagree, a trigger tested against a fixture would behave differently
// in production, which is the whole risk of having two entry points.
func TestFromRequestAgreesWithLoadEvent(t *testing.T) {
	cases := []struct {
		fixture string
		header  string
		event   string
	}{
		{"github-push-branch.json", "X-GitHub-Event", "push"},
		{"github-pull-request-opened.json", "X-GitHub-Event", "pull_request"},
		{"github-push-tag.json", "X-GitHub-Event", "push"},
		{"gitlab-push-branch.json", "X-Gitlab-Event", "Push Hook"},
		{"gitea-push-branch.json", "X-Gitea-Event", "push"},
	}
	for _, c := range cases {
		t.Run(c.fixture, func(t *testing.T) {
			viaFile, err := trigger.LoadEvent("testdata/" + c.fixture)
			if err != nil {
				t.Fatalf("LoadEvent: %v", err)
			}

			body := rawPayload(t, c.fixture)
			r := delivery(body, c.header, c.event)
			switch c.header {
			case "X-Gitlab-Event":
				r.Header.Set("X-Gitlab-Token", hookSecret)
			default:
				r.Header.Set("X-Hub-Signature-256", ghSign(body))
			}

			viaWire, err := trigger.FromRequest(r, trigger.Secret(hookSecret))
			if err != nil {
				t.Fatalf("FromRequest: %v", err)
			}
			if !reflect.DeepEqual(viaFile, viaWire) {
				t.Errorf("the wire and the file disagree:\n wire = %+v\n file = %+v", viaWire, viaFile)
			}
		})
	}
}

// Gitea sends X-GitHub-Event alongside its own header for compatibility.
// Reading it as GitHub is a real bug with quiet symptoms: Gitea's pull
// request actions are its own words, so a trigger would just never match.
func TestAGiteaDeliveryIsNotReadAsGitHub(t *testing.T) {
	body := rawPayload(t, "gitea-pull-request-synchronized.json")
	r := delivery(body, "X-Gitea-Event", "pull_request")
	r.Header.Set("X-GitHub-Event", "pull_request") // what Gitea really sends
	r.Header.Set("X-Gitea-Signature", hex.EncodeToString(hmacOf(body)))

	ev, err := trigger.FromRequest(r, trigger.Secret(hookSecret))
	if err != nil {
		t.Fatalf("FromRequest: %v", err)
	}
	if ev.Provider != "gitea" {
		t.Fatalf("Provider = %q, want gitea: X-Gitea-Event must be read before X-GitHub-Event", ev.Provider)
	}
	// Gitea's own word, which GitHub spells "synchronize".
	if ev.Action != "synchronized" {
		t.Errorf("Action = %q, want Gitea's own %q", ev.Action, "synchronized")
	}
}

func hmacOf(body []byte) []byte {
	mac := hmac.New(sha256.New, []byte(hookSecret))
	mac.Write(body)
	return mac.Sum(nil)
}

// Gitea signs in its own header, and newer builds also send GitHub's. Either
// has to satisfy verification, or half the Gitea installations in the world
// are refused.
func TestGiteaAcceptsEitherSignatureHeader(t *testing.T) {
	body := rawPayload(t, "gitea-push-branch.json")
	for _, header := range []string{"X-Gitea-Signature", "X-Hub-Signature-256"} {
		t.Run(header, func(t *testing.T) {
			r := delivery(body, "X-Gitea-Event", "push")
			if header == "X-Gitea-Signature" {
				r.Header.Set(header, hex.EncodeToString(hmacOf(body)))
			} else {
				r.Header.Set(header, ghSign(body))
			}
			if _, err := trigger.FromRequest(r, trigger.Secret(hookSecret)); err != nil {
				t.Errorf("a delivery signed in %s was refused: %v", header, err)
			}
		})
	}
}

// GitLab does not sign: it sends the secret back. A build that expected an
// HMAC would refuse every GitLab delivery there is.
func TestGitLabIsVerifiedByItsToken(t *testing.T) {
	body := rawPayload(t, "gitlab-push-branch.json")

	r := delivery(body, "X-Gitlab-Event", "Push Hook")
	r.Header.Set("X-Gitlab-Token", hookSecret)
	if _, err := trigger.FromRequest(r, trigger.Secret(hookSecret)); err != nil {
		t.Fatalf("a delivery with the right token was refused: %v", err)
	}

	wrong := delivery(body, "X-Gitlab-Event", "Push Hook")
	wrong.Header.Set("X-Gitlab-Token", "not-the-secret")
	if _, err := trigger.FromRequest(wrong, trigger.Secret(hookSecret)); !errors.Is(err, trigger.ErrUnsigned) {
		t.Errorf("a delivery with the wrong token gave %v, want ErrUnsigned", err)
	}
}

// Every way of getting the signature wrong is refused, and refused the same
// way: which of them it was is not something to confirm to a stranger.
func TestAnIncorrectlySignedDeliveryIsRefused(t *testing.T) {
	body := rawPayload(t, "github-push-branch.json")
	other := hmac.New(sha256.New, []byte("not-the-secret"))
	other.Write(body)

	for _, tc := range []struct{ name, sig string }{
		{"absent", ""},
		{"wrong secret", "sha256=" + hex.EncodeToString(other.Sum(nil))},
		{"not hex", "sha256=zzzz"},
		{"empty after the prefix", "sha256="},
		{"a different body's signature", ghSign([]byte(`{"ref":"refs/heads/other"}`))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := delivery(body, "X-GitHub-Event", "push")
			if tc.sig != "" {
				r.Header.Set("X-Hub-Signature-256", tc.sig)
			}
			_, err := trigger.FromRequest(r, trigger.Secret(hookSecret))
			if !errors.Is(err, trigger.ErrUnsigned) {
				t.Errorf("got %v, want ErrUnsigned", err)
			}
		})
	}
}

// Forgetting to verify must not be what a short call does. An endpoint that
// checks nothing runs the pipeline for anybody who can reach it.
func TestFromRequestRefusesToSkipVerificationSilently(t *testing.T) {
	body := rawPayload(t, "github-push-branch.json")
	r := delivery(body, "X-GitHub-Event", "push")

	_, err := trigger.FromRequest(r)
	if err == nil {
		t.Fatal("FromRequest with no Secret and no Unverified must be an error")
	}
	if !strings.Contains(err.Error(), "Unverified") {
		t.Errorf("the error must name the explicit opt-out: %v", err)
	}
	// And it is not ErrUnsigned: this is the caller's mistake, not a
	// stranger's, and a handler must not answer 401 to its own bug.
	if errors.Is(err, trigger.ErrUnsigned) {
		t.Error("a missing Secret is a wiring error, not a failed verification")
	}
}

// Saying both is a mistake, and picking a winner is how an endpoint quietly
// stops checking.
func TestSecretAndUnverifiedTogetherIsAnError(t *testing.T) {
	body := rawPayload(t, "github-push-branch.json")
	r := delivery(body, "X-GitHub-Event", "push")
	r.Header.Set("X-Hub-Signature-256", ghSign(body))

	if _, err := trigger.FromRequest(r, trigger.Secret(hookSecret), trigger.Unverified()); err == nil {
		t.Fatal("Secret and Unverified together must be an error")
	}
}

// Bitbucket signs nothing, so a secret has nothing to check. Saying so is
// better than pretending to have verified something.
func TestBitbucketSaysWhyASecretCannotHelp(t *testing.T) {
	body := rawPayload(t, "bitbucket-push-branch.json")

	r := delivery(body, "X-Event-Key", "repo:push")
	_, err := trigger.FromRequest(r, trigger.Secret(hookSecret))
	if !errors.Is(err, trigger.ErrUnsigned) {
		t.Fatalf("got %v, want ErrUnsigned", err)
	}
	if !strings.Contains(err.Error(), "Unverified") {
		t.Errorf("the error must say what to do instead: %v", err)
	}

	// With the opt-out it parses like anything else.
	ok := delivery(body, "X-Event-Key", "repo:push")
	if _, err := trigger.FromRequest(ok, trigger.Unverified()); err != nil {
		t.Errorf("an unverified Bitbucket delivery was refused: %v", err)
	}
}

// A delivery carrying none of the four headers names no source, so there is
// nothing to parse it as. A handler answers 400, not 401.
func TestADeliveryFromNowhereIsRefused(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`))
	_, err := trigger.FromRequest(r, trigger.Secret(hookSecret))
	if !errors.Is(err, trigger.ErrUnknownSource) {
		t.Errorf("got %v, want ErrUnknownSource", err)
	}
}

// A source of your own has no header this build could know, so As says what
// to parse it as. The rest of the pipeline never learns the difference.
func TestAsReachesACustomProvider(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/webhook",
		strings.NewReader(`{"repo":"acme/app","tag":"v1.4.0","sha":"abc123"}`))
	r.Header.Set("X-Bus-Event", "release")

	ev, err := trigger.FromRequest(r,
		trigger.As("deploy-bus", r.Header.Get("X-Bus-Event")),
		trigger.WithProviders(busProvider{}),
		trigger.Unverified())
	if err != nil {
		t.Fatalf("FromRequest: %v", err)
	}
	if ev.Kind != trigger.Tag || ev.Tag != "v1.4.0" {
		t.Errorf("Kind/Tag = %v/%q, want tag/v1.4.0", ev.Kind, ev.Tag)
	}
	if ev.Provider != "deploy-bus" {
		t.Errorf("Provider = %q, want deploy-bus", ev.Provider)
	}
	// And every built-in matcher works on it, which is the point of the
	// neutral Event.
	m, err := trigger.Select(ev, trigger.OnTag(trigger.Semver(">=1.0.0")))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if m == nil {
		t.Error("OnTag(Semver) did not match a custom provider's tag event")
	}
}

// senro has no signature scheme for a source it has never seen, so it says
// so rather than claiming to have checked one.
func TestACustomProviderCannotBeVerifiedBySenro(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{"tag":"v1"}`))
	_, err := trigger.FromRequest(r,
		trigger.As("deploy-bus", "release"),
		trigger.WithProviders(busProvider{}),
		trigger.Secret(hookSecret))
	if !errors.Is(err, trigger.ErrUnsigned) {
		t.Fatalf("got %v, want ErrUnsigned", err)
	}
	if !strings.Contains(err.Error(), "yourself") {
		t.Errorf("the error must say who has to verify it: %v", err)
	}
}

// An unbounded read is a way to exhaust a process reachable from the
// internet.
func TestADeliveryOverTheLimitIsRefused(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(strings.Repeat("x", 4096)))
	r.Header.Set("X-GitHub-Event", "push")
	_, err := trigger.FromRequest(r, trigger.Unverified(), trigger.MaxBody(1024))
	if err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Errorf("got %v, want a size refusal", err)
	}
}

// Parse is the primitive under FromRequest, for a server whose headers are
// already in hand. It has to agree with the file path too.
func TestParseAgreesWithLoadEvent(t *testing.T) {
	viaFile, err := trigger.LoadEvent("testdata/github-push-branch.json")
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	viaParse, err := trigger.Parse("github", "push", rawPayload(t, "github-push-branch.json"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !reflect.DeepEqual(viaFile, viaParse) {
		t.Errorf("Parse and LoadEvent disagree:\n parse = %+v\n file  = %+v", viaParse, viaFile)
	}
}

func TestParseReportsWhatIsMissing(t *testing.T) {
	body := rawPayload(t, "github-push-branch.json")
	for _, tc := range []struct{ name, provider, event, want string }{
		{"no provider", "", "push", "provider"},
		{"no event", "github", "", "no event type"},
		{"unknown provider", "nope", "push", "unknown provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := trigger.Parse(tc.provider, tc.event, body)
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	if _, err := trigger.Parse("github", "push", nil); err == nil {
		t.Error("an empty payload must be an error")
	}
}

// busProvider is a source senro has never seen, the shape the trigger docs
// use as the worked example.
type busProvider struct{}

func (busProvider) Name() string { return "deploy-bus" }

func (busProvider) Parse(event string, payload []byte) (*trigger.Event, error) {
	if event != "release" {
		return nil, errors.New("deploy-bus reads a release event")
	}
	var p struct {
		Repo string `json:"repo"`
		Tag  string `json:"tag"`
		SHA  string `json:"sha"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	if p.Tag == "" {
		return nil, errors.New("the release payload names no tag")
	}
	return &trigger.Event{
		Kind: trigger.Tag, Repo: p.Repo,
		Ref: "refs/tags/" + p.Tag, Tag: p.Tag,
		Base: trigger.Base{To: p.SHA},
	}, nil
}
