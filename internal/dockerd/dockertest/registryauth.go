package dockertest

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"
)

// The credentials the test registry accepts.
//
// They are a test fixture and nothing else: they never leave this machine, and
// the point of having them at all is that an unauthenticated or wrongly scoped
// request has something real to fail against.
const (
	// RegistryUser can pull and push.
	RegistryUser     = "senrotestpusher"
	RegistryPassword = "senrotestpushersecret"
	// RegistryReadOnlyUser can pull and nothing else. It is how a test asks
	// the registry to refuse a push for a reason a real deployment produces:
	// a credential scoped to reads.
	RegistryReadOnlyUser     = "senrotestpuller"
	RegistryReadOnlyPassword = "senrotestpullersecret"
)

// tokenIssuer is the authorization server the test registry's challenge
// points at. The registry never contacts it: it publishes the realm and
// then verifies the JWT on its own. That division keeps this a usable
// oracle even though the issuer is written here: the issuer hands out
// exactly the scope asked for, and the REGISTRY decides whether that scope
// covers the request, so a client that asks wrongly is refused by the
// registry rather than waved through by a fake.
type tokenIssuer struct {
	srv *httptest.Server
	// key signs tokens; leaf is the certificate that carries its public key,
	// and it is sent inside the JWT header so the registry can chain it back
	// to caPEM, which is the only thing the container is given.
	key     *rsa.PrivateKey
	leafDER []byte
	caPEM   string
	issuer  string
	service string
}

// newTokenIssuer generates a fresh signing chain and starts the server.
//
// A chain rather than one self-signed certificate because the registry
// verifies the JWT's x5c header as an ordinary certificate chain: the leaf
// that signs tokens must not be the certificate authority it chains to.
func newTokenIssuer(issuer, service string) (*tokenIssuer, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generating the token CA key: %w", err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "senro test token CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("creating the token CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parsing the token CA certificate: %w", err)
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generating the token signing key: %w", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "senro test token signer"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("creating the token signing certificate: %w", err)
	}

	ti := &tokenIssuer{
		key:     leafKey,
		leafDER: leafDER,
		caPEM:   string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		issuer:  issuer,
		service: service,
	}
	ti.srv = httptest.NewServer(http.HandlerFunc(ti.serve))
	return ti, nil
}

// realm is the URL a client is sent to for a token.
func (ti *tokenIssuer) realm() string { return ti.srv.URL + "/token" }

func (ti *tokenIssuer) close() {
	if ti.srv != nil {
		ti.srv.Close()
	}
}

// serve issues one token, or refuses the credentials.
//
// The scope granted is exactly the scope requested, narrowed by what the user
// is allowed to do. Nothing here decides whether the token is good enough for
// the request that follows: that is the registry's job, and leaving it there
// is the whole reason this is worth testing against.
func (ti *tokenIssuer) serve(w http.ResponseWriter, r *http.Request) {
	user, password, hasBasic := r.BasicAuth()
	var mayPull, mayPush bool
	switch {
	case !hasBasic:
		// Anonymous. A real public registry grants pull on its public
		// repositories; this one grants nothing at all, so a test that
		// forgets its credentials is refused by the registry rather than
		// passing on a permission nobody meant to give it.
		user = "anonymous"
	case user == RegistryUser && password == RegistryPassword:
		mayPull, mayPush = true, true
	case user == RegistryReadOnlyUser && password == RegistryReadOnlyPassword:
		mayPull = true
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED","message":"bad credentials"}]}`))
		return
	}

	query := r.URL.Query()
	access := make([]tokenAccess, 0, len(query["scope"]))
	for _, scope := range query["scope"] {
		for _, one := range strings.Fields(scope) {
			entry, ok := parseScope(one)
			if !ok {
				continue
			}
			entry.Actions = allowedActions(entry.Actions, mayPull, mayPush)
			access = append(access, entry)
		}
	}

	// The audience is whatever the client asked for rather than this server's
	// own service name, deliberately: a client that forgets to send the
	// service then gets a token the REGISTRY rejects, which is the failure a
	// real deployment would produce, instead of one this fixture invented.
	token, err := ti.sign(user, query.Get("service"), access)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":        token,
		"access_token": token,
		"expires_in":   300,
		"issued_at":    time.Now().UTC().Format(time.RFC3339),
	})
}

// tokenAccess is one entry of a token's access claim.
type tokenAccess struct {
	Type    string   `json:"type"`
	Name    string   `json:"name"`
	Actions []string `json:"actions"`
}

// parseScope reads "repository:acme/senro-cache:pull,push".
func parseScope(scope string) (tokenAccess, bool) {
	parts := strings.SplitN(scope, ":", 3)
	if len(parts) != 3 {
		return tokenAccess{}, false
	}
	return tokenAccess{
		Type:    parts[0],
		Name:    parts[1],
		Actions: strings.Split(parts[2], ","),
	}, true
}

func allowedActions(asked []string, mayPull, mayPush bool) []string {
	granted := make([]string, 0, len(asked))
	for _, action := range asked {
		if (action == "pull" && mayPull) || (action != "pull" && mayPush) {
			granted = append(granted, action)
		}
	}
	return granted
}

// sign renders the JWT the registry will verify.
//
// The signing certificate travels in the header's x5c, so the container needs
// nothing but the CA certificate to check it, and a test run never has to
// install a key anywhere.
func (ti *tokenIssuer) sign(subject, audience string, access []tokenAccess) (string, error) {
	header := map[string]any{
		"typ": "JWT",
		"alg": "RS256",
		"x5c": []string{base64.StdEncoding.EncodeToString(ti.leafDER)},
	}
	now := time.Now()
	claims := map[string]any{
		"iss":    ti.issuer,
		"sub":    subject,
		"aud":    audience,
		"exp":    now.Add(5 * time.Minute).Unix(),
		"nbf":    now.Add(-time.Minute).Unix(),
		"iat":    now.Unix(),
		"jti":    fmt.Sprintf("senro-test-%d", now.UnixNano()),
		"access": access,
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := base64.RawURLEncoding.EncodeToString(headerJSON) + "." +
		base64.RawURLEncoding.EncodeToString(claimsJSON)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, ti.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
