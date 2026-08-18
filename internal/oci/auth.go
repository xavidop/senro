package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// senro implements ONE registry authentication flow: the token challenge
// the distribution specification describes. A registry answers 401 with
//
//	WWW-Authenticate: Bearer realm="https://auth.example.com/token",service="example.com",scope="repository:acme/senro-cache:pull"
//
// and the client fetches a token from the realm (presenting its credential
// as HTTP Basic there and nowhere else), then repeats the request with a
// Bearer token. Identical at every hosted registry; what differs is where
// the credential comes from, and senro does not participate in that:
// credentials arrive already resolved from configuration.
//
// Every other scheme is refused by name rather than half-implemented,
// including Basic: four lines to send, shipped untested against any real
// server. See ErrUnsupportedAuth.

// authenticator holds one credential and the tokens it has been given.
type authenticator struct {
	username string
	password string
	http     *http.Client
	scrub    *strings.Replacer

	mu     sync.Mutex
	tokens map[string]cachedToken
}

// cachedToken is one token and the moment it stops being worth sending.
type cachedToken struct {
	value   string
	expires time.Time
}

// tokenSkew is how long before its stated expiry a token is treated as gone.
// A token that expires between being chosen here and arriving at the registry
// costs an extra round trip rather than a failure, since a 401 is answered by
// asking for another one, but avoiding it is free.
const tokenSkew = 10 * time.Second

// defaultTokenLifetime is what a token endpoint that states no lifetime is
// assumed to have granted. The specification's own default.
const defaultTokenLifetime = 60 * time.Second

// cached returns a token for scope that is still worth sending, or "".
func (a *authenticator) cached(scope string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	tok, ok := a.tokens[scope]
	if !ok || time.Now().Add(tokenSkew).After(tok.expires) {
		return ""
	}
	return tok.value
}

func (a *authenticator) remember(scope, value string, lifetime time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tokens == nil {
		a.tokens = make(map[string]cachedToken)
	}
	a.tokens[scope] = cachedToken{value: value, expires: time.Now().Add(lifetime)}
}

// answer turns a registry's challenge into a token to send back, or refuses
// the challenge in a message that says what was asked for.
//
// want is the scope the operation needs, used when the challenge names none
// of its own. When the challenge does name one, that is what is asked for:
// the registry knows better than this package which access its own request
// requires, and a client that insisted on its own guess would fail against
// any registry that scopes differently.
func (a *authenticator) answer(ctx context.Context, challenge, want, registry string) (string, error) {
	scheme, params, err := parseChallenge(challenge)
	if err != nil {
		return "", fmt.Errorf("%w: the registry at %s refused the request and %w",
			ErrDenied, registry, err)
	}
	if !strings.EqualFold(scheme, "bearer") {
		return "", fmt.Errorf(
			"%w: the registry at %s asked for %q. senro implements the OCI token flow "+
				"(Bearer) and nothing else. A registry that serves Basic directly needs a token "+
				"endpoint in front of it, or a different shared cache backend",
			ErrUnsupportedAuth, registry, scheme)
	}
	realm := params["realm"]
	if realm == "" {
		return "", fmt.Errorf(
			"%w: the registry at %s asked for a Bearer token but named no realm to get one from",
			ErrUnsupportedAuth, registry)
	}
	scope := params["scope"]
	if scope == "" {
		scope = want
	}

	token, lifetime, err := a.fetch(ctx, realm, params["service"], scope)
	if err != nil {
		return "", err
	}
	// Cached under the scope the OPERATION asked for, which is the key every
	// later request looks up. The scope the challenge named may be broader or
	// spelled differently, and filing it under that would mean a cache that
	// never hits.
	a.remember(want, token, lifetime)
	return token, nil
}

// fetch asks the token endpoint for a token.
func (a *authenticator) fetch(
	ctx context.Context, realm, service, scope string,
) (string, time.Duration, error) {
	u, err := url.Parse(realm)
	if err != nil {
		return "", 0, fmt.Errorf("%w: the token realm %q is not a URL: %w",
			ErrUnsupportedAuth, realm, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", 0, fmt.Errorf("%w: the token realm %q is not an http URL",
			ErrUnsupportedAuth, realm)
	}
	query := u.Query()
	if service != "" {
		query.Set("service", service)
	}
	if scope != "" {
		query.Set("scope", scope)
	}
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", 0, fmt.Errorf("oci: asking %s for a token: %w", u.Host, err)
	}
	// The credential goes to the token endpoint and to nothing else, in one
	// header, on one request. It is never attached to a request to the
	// registry itself, which is the point of the flow: what the registry sees
	// is a short-lived token scoped to one repository.
	if a.username != "" || a.password != "" {
		req.SetBasicAuth(a.username, a.password)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("oci: asking %s for a token: %w", u.Host, a.clean(err))
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// A token endpoint that refuses is a credential problem, and saying
		// so is the difference between somebody checking their password and
		// somebody looking for a network fault.
		kind := error(nil)
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			kind = ErrDenied
		}
		if kind != nil {
			return "", 0, fmt.Errorf("%w: %s refused the credentials for scope %q (%d)",
				kind, u.Host, scope, resp.StatusCode)
		}
		return "", 0, fmt.Errorf("oci: %s answered %d when asked for a token for scope %q",
			u.Host, resp.StatusCode, scope)
	}

	var doc struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	// Bounded for the same reason a manifest read is: this is a network
	// answering, and it may be answering with something else entirely.
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("oci: reading the token from %s: %w", u.Host, a.clean(err))
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return "", 0, fmt.Errorf("oci: %s answered with something that is not a token document: %w",
			u.Host, err)
	}
	token := doc.Token
	if token == "" {
		// Both spellings are in the wild, and several registries send only
		// the second one.
		token = doc.AccessToken
	}
	if token == "" {
		return "", 0, fmt.Errorf("oci: %s answered with a token document holding no token", u.Host)
	}
	lifetime := defaultTokenLifetime
	if doc.ExpiresIn > 0 {
		lifetime = time.Duration(doc.ExpiresIn) * time.Second
	}
	return token, lifetime, nil
}

func (a *authenticator) clean(err error) error {
	msg := a.scrub.Replace(err.Error())
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

// parseChallenge reads a WWW-Authenticate header into its scheme and its
// parameters.
//
// Only the first challenge in the header is read. A registry that offers two
// is offering senro one it implements or one it does not, and the first is
// what every client in the ecosystem acts on.
func parseChallenge(header string) (scheme string, params map[string]string, err error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", nil, errors.New("sent no WWW-Authenticate challenge to answer")
	}
	scheme, rest, _ := strings.Cut(header, " ")
	params = make(map[string]string)

	for rest != "" {
		rest = strings.TrimLeft(rest, " \t,")
		if rest == "" {
			break
		}
		key, after, ok := strings.Cut(rest, "=")
		if !ok {
			break
		}
		key = strings.TrimSpace(key)
		if strings.HasPrefix(after, `"`) {
			// A quoted value may hold commas, which the scope parameter
			// routinely does ("repository:x:pull,push"), so it is read to its
			// closing quote rather than to the next separator.
			value, remainder, closed := cutQuoted(after[1:])
			if !closed {
				break
			}
			params[strings.ToLower(key)] = value
			rest = remainder
			continue
		}
		value, remainder, _ := strings.Cut(after, ",")
		params[strings.ToLower(key)] = strings.TrimSpace(value)
		rest = remainder
	}
	return scheme, params, nil
}

// cutQuoted reads to the closing quote, honouring backslash escapes.
func cutQuoted(s string) (value, rest string, closed bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 < len(s) {
				i++
				b.WriteByte(s[i])
			}
		case '"':
			return b.String(), s[i+1:], true
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String(), "", false
}
