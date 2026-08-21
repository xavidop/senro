package trigger

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Running senro as a server.
//
// LoadEvent reads an envelope somebody wrote to disk. FromRequest is the
// same thing for a process that IS the webhook endpoint: it reads the
// delivery's own headers, verifies its signature and parses its body, with
// no file and no envelope in between.
//
//	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
//		ev, err := trigger.FromRequest(r, trigger.Secret(hookSecret))
//		if err != nil { ... }
//		go run(ev)
//		w.WriteHeader(http.StatusAccepted)
//	})
//
// The knowledge it holds is the part every server would otherwise have to
// rediscover from four sets of provider documentation: which header names
// the event, which header carries the signature, and that the four do not
// agree about any of it.

// DefaultMaxBody bounds a delivery FromRequest will read. GitHub's own limit
// is 25MB; an unbounded read is a way to exhaust a process reachable from
// the internet.
const DefaultMaxBody = 25 << 20

// ErrUnsigned reports a delivery that failed verification, or that arrived
// with no signature at all when one was required.
//
// One error for both, deliberately: a handler answers 401 either way, and
// telling a caller WHICH of the two it got is telling a stranger how close
// their guess was.
var ErrUnsigned = errors.New("trigger: the delivery is not correctly signed")

// ErrUnknownSource reports a delivery carrying none of the headers this
// build recognises. A handler answers 400: nothing about it names an event
// source, so there is nothing to parse it as.
var ErrUnknownSource = errors.New("trigger: the delivery names no event source this build knows")

// RequestOption configures FromRequest. Separate from Option, which is a
// matcher on an event that already exists.
type RequestOption interface{ applyRequest(*requestConfig) }

type requestConfig struct {
	secret     string
	unverified bool
	providers  []Provider
	maxBody    int64
	provider   string
	event      string
}

type requestOptionFunc func(*requestConfig)

func (f requestOptionFunc) applyRequest(c *requestConfig) { f(c) }

// Secret is the shared secret this delivery is verified against.
//
// What that means is the source's own choice, and the three differ:
//
//   - GitHub signs the raw body, HMAC-SHA256, in X-Hub-Signature-256.
//   - Gitea signs the same way, in X-Gitea-Signature (and, on newer builds,
//     X-Hub-Signature-256 as well). Either is accepted.
//   - GitLab sends the secret ITSELF, in X-Gitlab-Token. Nothing is signed,
//     so this is only as good as the transport: use HTTPS.
//   - Bitbucket Cloud signs nothing and sends no token. There is nothing for
//     a secret to check, so a Bitbucket delivery needs Unverified and an
//     answer to "who else can reach this endpoint".
//
// Handing the wrong secret, or none, is ErrUnsigned.
func Secret(secret string) RequestOption {
	return requestOptionFunc(func(c *requestConfig) { c.secret = secret })
}

// Unverified accepts a delivery without checking any signature.
//
// It has to be said out loud, because the signature is the whole of the
// authentication: an unverified endpoint runs your pipeline for anybody who
// can reach it. Two cases genuinely need it, and both come with homework:
//
//   - Bitbucket Cloud, which signs nothing. Restrict the endpoint by network
//     instead: Bitbucket publishes its egress ranges.
//   - A proxy or gateway that already verified the delivery and did not pass
//     the signature through.
//
// Unverified and Secret together is an error rather than a precedence rule:
// a call saying both "check this" and "check nothing" is a mistake, and
// guessing which half was meant is how an endpoint silently stops checking.
func Unverified() RequestOption {
	return requestOptionFunc(func(c *requestConfig) { c.unverified = true })
}

// WithProviders adds event sources of your own, joining the built-ins,
// exactly as the variadic providers of LoadEvent do.
//
// A source senro has never seen has no header FromRequest could recognise
// it by, so pair this with As, which says what to parse the delivery as.
func WithProviders(ps ...Provider) RequestOption {
	return requestOptionFunc(func(c *requestConfig) { c.providers = append(c.providers, ps...) })
}

// As parses the delivery as the named provider's named event, instead of
// working both out from the headers.
//
// For a source of your own, whose headers this build cannot recognise:
//
//	trigger.FromRequest(r,
//		trigger.As("deploy-bus", r.Header.Get("X-Bus-Event")),
//		trigger.WithProviders(deploybus.Provider{}),
//		trigger.Unverified())
//
// It also pins a built-in: a proxy that rewrites headers, or an endpoint you
// know only ever receives one source. Signature verification still follows
// the named provider's own rules.
func As(provider, event string) RequestOption {
	return requestOptionFunc(func(c *requestConfig) { c.provider, c.event = provider, event })
}

// MaxBody bounds how much of a delivery is read. DefaultMaxBody by default.
func MaxBody(n int64) RequestOption {
	return requestOptionFunc(func(c *requestConfig) { c.maxBody = n })
}

// FromRequest builds an Event from a webhook delivery: work out which source
// sent it, verify it, and parse its body.
//
//	ev, err := trigger.FromRequest(r, trigger.Secret(os.Getenv("HOOK_SECRET")))
//	switch {
//	case errors.Is(err, trigger.ErrUnsigned):
//		http.Error(w, "unauthorized", http.StatusUnauthorized)
//	case errors.Is(err, trigger.ErrUnknownSource):
//		http.Error(w, "unrecognised delivery", http.StatusBadRequest)
//	case err != nil:
//		http.Error(w, "could not read the delivery", http.StatusBadRequest)
//	}
//
// It reads r.Body, so the body is consumed. Verification is REQUIRED: with
// neither Secret nor Unverified the call is an error, because an endpoint
// that checks nothing runs your pipeline for anybody who can reach it, and
// that must not be what forgetting an argument gets you.
//
// The Event it returns is the same Event LoadEvent produces from the same
// delivery, so a trigger declared for a file works unchanged against a
// server. What it does NOT do is decide anything about concurrency: see
// Select, and run the pipeline yourself.
func FromRequest(r *http.Request, opts ...RequestOption) (*Event, error) {
	cfg := requestConfig{maxBody: DefaultMaxBody}
	for _, o := range opts {
		if o == nil {
			continue
		}
		o.applyRequest(&cfg)
	}
	switch {
	case cfg.secret != "" && cfg.unverified:
		return nil, errors.New(
			"trigger: FromRequest was given both Secret and Unverified; one says check the " +
				"delivery and the other says do not, and guessing which was meant is how an " +
				"endpoint silently stops checking")
	case cfg.secret == "" && !cfg.unverified:
		return nil, errors.New(
			"trigger: FromRequest needs Secret(...) to verify the delivery with. A webhook " +
				"endpoint that verifies nothing runs your pipeline for anybody who can reach it. " +
				"If the delivery genuinely cannot be signed (Bitbucket Cloud) or was already " +
				"verified in front of this process, say trigger.Unverified() in so many words")
	case r == nil || r.Body == nil:
		return nil, errors.New("trigger: FromRequest was given no request body")
	}

	provider, event := cfg.provider, cfg.event
	if provider == "" {
		var ok bool
		provider, event, ok = sourceOf(r.Header)
		if !ok {
			return nil, ErrUnknownSource
		}
	}

	// Read before verifying: every signature this package checks is over the
	// raw bytes, so they have to exist first. Bounded, because this is a
	// process something on the internet is talking to.
	body, err := io.ReadAll(io.LimitReader(r.Body, cfg.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("trigger: reading the delivery: %w", err)
	}
	if int64(len(body)) > cfg.maxBody {
		return nil, fmt.Errorf("trigger: the delivery is larger than %d bytes", cfg.maxBody)
	}

	if !cfg.unverified {
		if err := verify(provider, r.Header, body, cfg.secret); err != nil {
			return nil, err
		}
	}
	return Parse(provider, event, body, cfg.providers...)
}

// SourceOf reports which source sent a delivery and its own name for what
// happened, read from the header each source names its events in.
//
// It is what FromRequest uses, exported for a process that forwards a
// delivery rather than handling it: a dispatcher execing a pipeline binary
// needs the pair to write an Envelope with.
//
//	provider, event, ok := trigger.SourceOf(r.Header)
//	if !ok { http.Error(w, "unrecognised delivery", http.StatusBadRequest); return }
//	file, err := trigger.Envelope(provider, event, body)
//
// ok is false for a delivery carrying none of the four headers.
func SourceOf(h http.Header) (provider, event string, ok bool) {
	return sourceOf(h)
}

// Envelope wraps a source's raw body in the file format LoadEvent reads, so
// a process holding an HTTP delivery can hand it to a pipeline binary
// through --trigger-event.
//
// A webhook body alone is NOT that format and never was: no GitHub, GitLab,
// Bitbucket or Gitea body says which event it is, which is why the envelope
// exists and why both halves are arguments here. Writing a raw body to the
// file gets "the event names no provider" from the pipeline, at the far end
// of an exec where nobody is looking.
//
// A process that runs the pipeline itself needs none of this: use
// FromRequest and skip the file.
func Envelope(provider, event string, payload []byte) ([]byte, error) {
	switch {
	case provider == "":
		return nil, errors.New("trigger: Envelope needs the name of the source that sent this")
	case event == "":
		return nil, fmt.Errorf("trigger: Envelope needs %s's own name for what happened", provider)
	case len(payload) == 0:
		return nil, fmt.Errorf("trigger: the %s %q delivery carries no payload", provider, event)
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("trigger: the %s %q delivery is not JSON", provider, event)
	}
	return json.Marshal(envelope{Provider: provider, Event: event, Payload: payload})
}

// sourceOf works out which source sent a delivery, from the header each one
// names its own events in.
//
// Gitea is checked FIRST and the order is load-bearing: Gitea sends
// X-GitHub-Event alongside its own header for compatibility, so checking
// GitHub first reads every Gitea delivery as a GitHub one. They are not
// interchangeable: Gitea has a "create" event GitHub's parser refuses, and
// its pull request actions are its own words ("synchronized", where GitHub
// says "synchronize"), so the mistake is a trigger that silently never
// matches.
func sourceOf(h http.Header) (provider, event string, ok bool) {
	for _, c := range []struct{ header, provider string }{
		{"X-Gitea-Event", providerGitea},
		{"X-GitHub-Event", providerGitHub},
		{"X-Gitlab-Event", providerGitLab},
		{"X-Event-Key", providerBitbucket},
	} {
		if v := h.Get(c.header); v != "" {
			return c.provider, v, true
		}
	}
	return "", "", false
}

// verify checks the delivery against the source's own scheme. Each is that
// source's documented one; none of the four agrees with another.
func verify(provider string, h http.Header, body []byte, secret string) error {
	switch provider {
	case providerGitHub:
		return checkHMAC(h.Get("X-Hub-Signature-256"), body, secret)

	case providerGitea:
		// Newer Gitea sends GitHub's header too. Either satisfies it; both
		// are the same HMAC over the same bytes.
		if err := checkHMAC(h.Get("X-Gitea-Signature"), body, secret); err == nil {
			return nil
		}
		return checkHMAC(h.Get("X-Hub-Signature-256"), body, secret)

	case providerGitLab:
		// GitLab signs nothing: it sends the secret back. Constant-time all
		// the same, because a timing oracle on a shared secret is still a
		// timing oracle on a shared secret.
		if tok := h.Get("X-Gitlab-Token"); tok != "" &&
			hmac.Equal([]byte(tok), []byte(secret)) {
			return nil
		}
		return ErrUnsigned

	case providerBitbucket:
		return fmt.Errorf(
			"%w: Bitbucket Cloud signs nothing and sends no token, so a secret has nothing to "+
				"check here. Restrict the endpoint by network instead (Bitbucket publishes its "+
				"egress ranges) and say trigger.Unverified()", ErrUnsigned)

	default:
		// A provider of the caller's own, reached through As. senro does not
		// know its scheme, so it cannot claim to have checked one.
		return fmt.Errorf(
			"%w: senro has no signature scheme for provider %q. Verify the delivery yourself "+
				"before calling FromRequest, then say trigger.Unverified()", ErrUnsigned, provider)
	}
}

// checkHMAC verifies an HMAC-SHA256 over the raw body, hex-encoded, with or
// without GitHub's "sha256=" prefix.
func checkHMAC(sig string, body []byte, secret string) error {
	sig = strings.TrimPrefix(sig, "sha256=")
	if sig == "" {
		return ErrUnsigned
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return ErrUnsigned
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return ErrUnsigned
	}
	return nil
}
