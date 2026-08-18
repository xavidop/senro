// Package oci is a small, standard-library-only client for the subset of
// the OCI distribution API a content-addressed cache needs: push a blob,
// pull a blob, ask whether one is there, and write the small manifest that
// names it. A registry SDK would bring a few hundred packages to save about
// four hundred lines; the API is HTTP and JSON with one documented
// challenge-response (see auth.go).
//
// Nothing here knows what a cache is: it moves opaque bytes under digests
// the caller chose, and verification is the caller's (internal/remotecache).
//
// Deliberately not implemented, because a cache asks for none of it:
// catalog and tag listing, deletion, chunked uploads, cross-repository blob
// mounts, and the referrers API. Pull and push on one repository is the
// whole permission senro needs, a property worth keeping.
package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/redact"
)

var (
	// ErrNotFound means the registry answered, authoritatively, that it
	// holds no such blob or manifest. Deliberately narrow: a refused
	// connection or rejected token stays an ordinary error, or a broken
	// configuration becomes a cache that never hits and never says why. A
	// missing repository IS a miss: the first run against a fresh cache
	// repository gets NAME_UNKNOWN before anything was ever pushed.
	ErrNotFound = errors.New("oci: not found")

	// ErrDenied means the request was understood and refused: no credentials,
	// wrong credentials, an expired token, or a token whose scope does not
	// cover the operation. Separate from a plain error so a caller can say
	// "check your credentials" rather than "something went wrong".
	ErrDenied = errors.New("oci: access denied")

	// ErrUnsupportedAuth means the registry asked for an authentication
	// scheme senro does not implement. See auth.go for what is implemented
	// and why it is only one thing.
	ErrUnsupportedAuth = errors.New("oci: unsupported authentication scheme")
)

// MediaTypeImageManifest is the manifest media type senro writes and asks for.
const MediaTypeImageManifest = "application/vnd.oci.image.manifest.v1+json"

// maxAttempts bounds how many times one operation is sent. Retries cover a
// connection that failed before the registry formed an opinion and a "try
// again" answer (429, 5xx); every operation is idempotent, so neither can
// produce a wrong answer. 401, 403 and 404 are answers, never retried.
// Three because the caller degrades to no cache on failure, and a build
// waiting out a long schedule for a dead cache is what degradation exists
// to prevent.
const maxAttempts = 3

// retryBackoff is the pause before attempt n+1. Short, and fixed rather than
// exponential, for the reason maxAttempts is small: the budget here is a
// blip, not an outage.
const retryBackoff = 250 * time.Millisecond

// DefaultTimeout bounds a single request. Generous, because the largest blob
// a senro cache moves is a workspace snapshot and a cold CI runner's upstream
// link is not always fast, but finite, because an operation that hangs
// forever is worse for a build than one that fails.
const DefaultTimeout = 5 * time.Minute

// Config is everything needed to reach one repository.
type Config struct {
	// Registry is the host and optional port, such as "ghcr.io" or
	// "registry.internal:5000". Not a URL: a registry is named by its
	// authority in every reference anybody writes, and accepting a URL here
	// would invite a path that has no meaning in the API.
	Registry string
	// Repository is the path inside the registry that holds the cache, such
	// as "acme/senro-cache". Lowercase, as the distribution specification
	// requires.
	Repository string

	// Username and Password are the credential presented to the registry's
	// token endpoint. Both empty means anonymous, which works against a
	// registry that demands nothing and is refused by one that does.
	//
	// For a registry whose credentials are issued by another service, resolve
	// them first and pass the result: "AWS" and the output of
	// `aws ecr get-login-password` for Elastic Container Registry,
	// "oauth2accesstoken" and an access token for Artifact Registry. senro
	// runs no credential helper and contacts no metadata service.
	Username string
	Password string

	// PlainHTTP talks to the registry over http rather than https. For a
	// registry on a trusted network that serves no certificate, and for the
	// container this repository's own tests run against. Off by default,
	// because a credential sent in clear text to a host on the internet is a
	// leaked credential.
	PlainHTTP bool

	// Timeout bounds one request including reading its body. Zero means
	// DefaultTimeout.
	Timeout time.Duration

	// Transport is the round tripper to use. Zero means a private transport
	// with this package's own settings; it exists so a test can inject
	// failures a real registry will not produce on demand.
	Transport http.RoundTripper
}

// Client talks to one repository in one registry.
type Client struct {
	registry   string
	repository string
	scheme     string
	http       *http.Client
	auth       *authenticator
	// scrub removes the credential from any text on its way into an error.
	// Belt and braces: nothing here puts a secret in a message deliberately,
	// and this is what makes that true of text the REGISTRY sent back as well.
	scrub *strings.Replacer
}

// New validates a config and prepares a client. It performs no I/O: a
// configuration error is reported now, at startup, rather than on the first
// cache lookup where it would be indistinguishable from a cold cache.
func New(cfg Config) (*Client, error) {
	registry := strings.TrimSpace(cfg.Registry)
	switch {
	case registry == "":
		return nil, errors.New("oci: no registry")
	case cfg.Repository == "":
		return nil, errors.New("oci: no repository")
	case strings.Contains(registry, "://"):
		return nil, fmt.Errorf(
			"oci: registry %q is a URL; name the host alone, such as \"ghcr.io\" or "+
				"\"registry.internal:5000\", and set PlainHTTP for a registry served over http",
			registry)
	case strings.Contains(registry, "@"):
		// A credential in the host is a credential in every error message,
		// every log line and every event that names the registry.
		return nil, errors.New("oci: the registry must not carry a username or password; " +
			"put credentials in the username and password fields")
	case strings.ContainsAny(registry, "/ "):
		return nil, fmt.Errorf("oci: registry %q is not a host", registry)
	}
	if err := validRepository(cfg.Repository); err != nil {
		return nil, err
	}

	scheme := "https"
	if cfg.PlainHTTP {
		scheme = "http"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	transport := cfg.Transport
	if transport == nil {
		transport = newTransport()
	}
	scrub := scrubPairs(cfg)
	hc := &http.Client{Transport: transport, Timeout: timeout}
	return &Client{
		registry:   registry,
		repository: cfg.Repository,
		scheme:     scheme,
		http:       hc,
		auth: &authenticator{
			username: cfg.Username,
			password: cfg.Password,
			http:     hc,
			scrub:    scrub,
		},
		scrub: scrub,
	}, nil
}

// validRepository refuses anything that is not a repository name.
//
// The distribution specification's grammar, and it is enforced here rather
// than left to the registry because a repository name becomes a URL path: a
// caller that took "senro/../../v2/_catalog" from configuration and joined it
// onto a base would be building a request nobody asked for.
func validRepository(repo string) error {
	if strings.HasPrefix(repo, "/") || strings.HasSuffix(repo, "/") {
		return fmt.Errorf("oci: repository %q must not begin or end with a slash", repo)
	}
	for _, component := range strings.Split(repo, "/") {
		if !validRepositoryComponent(component) {
			return fmt.Errorf(
				"oci: repository %q is not a repository name: each part must be lowercase "+
					"letters, digits, and single separators (.  _  __  -), such as "+
					"\"acme/senro-cache\"", repo)
		}
	}
	return nil
}

func validRepositoryComponent(c string) bool {
	if c == "" {
		return false
	}
	// A component begins and ends with an alphanumeric, and separators appear
	// only between them.
	if !isAlnum(rune(c[0])) || !isAlnum(rune(c[len(c)-1])) {
		return false
	}
	for _, r := range c {
		if !isAlnum(r) && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func isAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}

// scrubPairs builds the replacer that keeps the credential out of error
// text. A value under redact.MinLength is deliberately NOT scrubbed, the
// same ruling internal/redact and internal/s3 make: a "secret" of "s" would
// replace half of every message. A backstop, not the mechanism: nothing
// here puts a credential in a message on purpose, and what this covers is
// text the REGISTRY sent back, about to land in somebody's log.
func scrubPairs(cfg Config) *strings.Replacer {
	var pairs []string
	if len(cfg.Password) >= redact.MinLength {
		pairs = append(pairs, cfg.Password, "[redacted registry password]")
	}
	return strings.NewReplacer(pairs...)
}

// newTransport is a private transport rather than http.DefaultTransport, so
// this package's connection behaviour is not shared with, or altered by,
// anything else in the process.
func newTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	// A cache moves many small objects and a few very large ones, mostly to
	// one host. Keeping connections warm avoids a TLS handshake per object.
	t.MaxIdleConnsPerHost = 32
	return t
}

// String names the repository without naming the credentials. It is what a
// degraded run prints, so it must be safe to put in front of anyone.
func (c *Client) String() string {
	return "oci repository " + c.repository + " at " + c.registry
}

// Repository is the repository this client addresses.
func (c *Client) Repository() string { return c.repository }

// BlobURL is the absolute URL one blob resolves to. Exported for the tests
// that pin the API paths, and for an error message that has to say precisely
// what was requested.
func (c *Client) BlobURL(digest string) string {
	return c.url("blobs/" + digest)
}

func (c *Client) url(suffix string) string {
	return c.scheme + "://" + c.registry + "/v2/" + c.repository + "/" + suffix
}

// HasBlob reports whether the blob exists and how many bytes it holds.
//
// A missing blob is (0, false, nil), not an error: "is it there" has "no" as
// a legitimate answer. Anything that stops the registry from answering at all
// is still an error.
func (c *Client) HasBlob(ctx context.Context, digest string) (int64, bool, error) {
	if err := validDigest(digest); err != nil {
		return 0, false, err
	}
	resp, err := c.send(ctx, request{
		method: http.MethodHead,
		url:    c.BlobURL(digest),
		scope:  c.scope(false),
		what:   "blob " + short(digest),
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	size := int64(-1)
	if v := resp.Header.Get("Content-Length"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("oci: HEAD blob %s: unreadable Content-Length %q",
				short(digest), v)
		}
		size = n
	}
	if size < 0 {
		return 0, false, fmt.Errorf(
			"oci: HEAD blob %s: the registry reported no Content-Length", short(digest))
	}
	return size, true, nil
}

// GetBlob returns the blob's bytes. The caller closes them.
//
// The body is NOT verified here: this package does not know what the bytes
// are supposed to be, and a registry is free to serve a blob from a redirect
// to storage that has its own opinions. Verification is the caller's, and in
// this repository it is unconditional; see internal/remotecache.
func (c *Client) GetBlob(ctx context.Context, digest string) (io.ReadCloser, error) {
	if err := validDigest(digest); err != nil {
		return nil, err
	}
	resp, err := c.send(ctx, request{
		method: http.MethodGet,
		url:    c.BlobURL(digest),
		scope:  c.scope(false),
		what:   "blob " + short(digest),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// PutBlob stores size bytes read from body under digest.
//
// body must be seekable on purpose: an upload is two requests, and a retry
// of the pair has to send the same bytes from the top.
//
// Concurrent pushes of the same digest are safe (two runners finishing the
// same step): each gets its own session, both send identical bytes, and
// whichever lands second finds the blob already present. Nothing to
// coordinate because nothing could differ.
func (c *Client) PutBlob(ctx context.Context, digest string, body io.ReadSeeker, size int64) error {
	if err := validDigest(digest); err != nil {
		return err
	}
	// The retry is around the WHOLE upload rather than around each of its two
	// requests: an upload session is single use, so re-sending the final PUT
	// on its own would arrive at a session the registry has already closed.
	// Starting again is both correct and cheap, since a session is created by
	// an empty POST.
	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		retryable, err := c.upload(ctx, digest, body, size)
		if err == nil {
			return nil
		}
		last = err
		if !retryable || attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryBackoff):
		}
	}
	return last
}

// upload performs one whole two-step blob upload.
func (c *Client) upload(
	ctx context.Context, digest string, body io.ReadSeeker, size int64,
) (retryable bool, err error) {
	what := "blob " + short(digest)
	session, retryable, err := c.attempt(ctx, request{
		method: http.MethodPost,
		url:    c.url("blobs/uploads/"),
		scope:  c.scope(true),
		what:   what,
	})
	if err != nil {
		return retryable, err
	}
	location := session.Header.Get("Location")
	_, _ = io.Copy(io.Discard, session.Body)
	_ = session.Body.Close()
	if location == "" {
		return false, fmt.Errorf(
			"oci: POST %s: the registry started an upload but published no Location", what)
	}
	// The Location is frequently a path rather than an absolute URL, and it
	// carries state the registry needs, so it is resolved against the request
	// that produced it rather than rebuilt.
	base, err := url.Parse(c.url("blobs/uploads/"))
	if err != nil {
		return false, fmt.Errorf("oci: PUT %s: %w", what, err)
	}
	next, err := base.Parse(location)
	if err != nil {
		return false, fmt.Errorf("oci: PUT %s: the registry published an unusable Location: %w",
			what, c.clean(err))
	}
	query := next.Query()
	query.Set("digest", digest)
	next.RawQuery = query.Encode()

	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("oci: PUT %s: rewinding the body: %w", what, err)
	}
	resp, retryable, err := c.attempt(ctx, request{
		method:      http.MethodPut,
		url:         next.String(),
		scope:       c.scope(true),
		what:        what,
		body:        body,
		size:        size,
		contentType: "application/octet-stream",
	})
	if err != nil {
		return retryable, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return false, nil
}

// GetManifest returns the manifest stored under ref, which is a tag or a
// digest. The bytes are returned exactly as the registry served them, because
// a manifest's identity is the digest of its bytes and re-encoding one would
// change it.
func (c *Client) GetManifest(ctx context.Context, ref string) ([]byte, error) {
	if err := validReference(ref); err != nil {
		return nil, err
	}
	resp, err := c.send(ctx, request{
		method: http.MethodGet,
		url:    c.url("manifests/" + ref),
		scope:  c.scope(false),
		what:   "manifest " + ref,
		accept: MediaTypeImageManifest,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	// A manifest is a small JSON document. The bound exists because the bytes
	// come off a network that may be serving something else entirely, and
	// reading an unbounded body into memory on that basis is how a cache
	// lookup becomes an out-of-memory kill.
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("oci: GET manifest %s: %w", ref, c.clean(err))
	}
	if len(b) > maxManifestBytes {
		return nil, fmt.Errorf("oci: GET manifest %s: larger than a manifest can be", ref)
	}
	return b, nil
}

// maxManifestBytes bounds how much of a manifest is read. The specification
// puts the ceiling at 4MiB; senro's own are a few hundred bytes.
const maxManifestBytes = 4 << 20

// HasManifest reports whether a manifest is stored under ref.
func (c *Client) HasManifest(ctx context.Context, ref string) (bool, error) {
	if err := validReference(ref); err != nil {
		return false, err
	}
	resp, err := c.send(ctx, request{
		method: http.MethodHead,
		url:    c.url("manifests/" + ref),
		scope:  c.scope(false),
		what:   "manifest " + ref,
		accept: MediaTypeImageManifest,
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return true, nil
}

// PutManifest stores a manifest under ref.
//
// Concurrent writes of the same ref are safe when the bytes are the same,
// which is the only way senro writes one: the manifest is derived from the
// object it names, so two machines storing the same object write the same
// manifest, and which of them won is not a question anybody has to answer.
func (c *Client) PutManifest(ctx context.Context, ref, mediaType string, body []byte) error {
	if err := validReference(ref); err != nil {
		return err
	}
	resp, err := c.send(ctx, request{
		method:      http.MethodPut,
		url:         c.url("manifests/" + ref),
		scope:       c.scope(true),
		what:        "manifest " + ref,
		body:        bytesSeeker(body),
		size:        int64(len(body)),
		contentType: mediaType,
	})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// scope is the access one operation needs, in the form a token endpoint
// expects. Reads ask for pull; writes ask for pull and push, because a push
// that has to read the registry's answer needs both and every registry issues
// them together.
func (c *Client) scope(write bool) string {
	if write {
		return "repository:" + c.repository + ":pull,push"
	}
	return "repository:" + c.repository + ":pull"
}

// request is one operation to perform.
type request struct {
	method string
	url    string
	// scope is the access this operation needs, used when the registry's
	// challenge does not name one of its own.
	scope string
	// what names the thing being operated on, for error messages. Never a
	// URL: a URL in a message invites somebody to paste it somewhere.
	what        string
	accept      string
	contentType string
	body        io.ReadSeeker
	size        int64
}

// send performs one operation, retrying the failures that are safe to retry.
func (c *Client) send(ctx context.Context, req request) (*http.Response, error) {
	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, retryable, err := c.attempt(ctx, req)
		if err == nil {
			return resp, nil
		}
		last = err
		if !retryable || attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryBackoff):
		}
	}
	return nil, last
}

// attempt performs one logical request, including answering an authentication
// challenge once. The second return says whether sending it again could
// plausibly produce a different answer.
func (c *Client) attempt(ctx context.Context, req request) (*http.Response, bool, error) {
	resp, err := c.once(ctx, req, c.auth.cached(req.scope))
	if err != nil {
		// A transport failure means the registry never formed an opinion, so
		// asking again is both safe and often right.
		return nil, true, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return c.classify(req, resp)
	}

	// The registry wants to be told who this is. Everything about which
	// scheme is answered, and which are refused, is in auth.go.
	challenge := resp.Header.Get("Www-Authenticate")
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	token, err := c.auth.answer(ctx, challenge, req.scope, c.registry)
	if err != nil {
		return nil, false, err
	}
	resp, err = c.once(ctx, req, token)
	if err != nil {
		return nil, true, err
	}
	return c.classify(req, resp)
}

// once sends exactly one HTTP request.
func (c *Client) once(
	ctx context.Context, req request, token string,
) (*http.Response, error) {
	// readOnly, never the caller's reader directly: net/http closes a
	// request body that is an io.ReadCloser, and an *os.File closed by the
	// transport cannot be rewound for the retry, failing with "file already
	// closed" the first time a busy registry answers 503. Hiding the Closer
	// makes net/http wrap the reader itself. The S3 backend shipped this
	// exact bug.
	var body io.Reader
	if req.body != nil {
		if _, err := req.body.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("oci: %s %s: rewinding the body: %w", req.method, req.what, err)
		}
		body = readOnly{req.body}
	}
	hr, err := http.NewRequestWithContext(ctx, req.method, req.url, body)
	if err != nil {
		return nil, fmt.Errorf("oci: %s %s: %w", req.method, req.what, c.clean(err))
	}
	if req.body != nil {
		hr.ContentLength = req.size
	}
	if req.accept != "" {
		hr.Header.Set("Accept", req.accept)
	}
	if req.contentType != "" {
		hr.Header.Set("Content-Type", req.contentType)
	}
	if token != "" {
		hr.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(hr)
	if err != nil {
		return nil, fmt.Errorf("oci: %s %s: %w", req.method, req.what, c.clean(err))
	}
	return resp, nil
}

// classify turns a response into either a success or an error that says what
// the registry said, sorted so a caller can tell a miss from a refusal.
func (c *Client) classify(req request, resp *http.Response) (*http.Response, bool, error) {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, false, nil
	}
	defer func() { _ = resp.Body.Close() }()

	// A HEAD carries no body, so the code has to come from the status alone
	// there. Everything else returns the API's error document, whose code is
	// much more useful than the status: BLOB_UNKNOWN and NAME_UNKNOWN are
	// both 404 and mean different things to somebody reading a log.
	var doc struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	_ = json.Unmarshal(b, &doc)

	var code, message string
	if len(doc.Errors) > 0 {
		code, message = doc.Errors[0].Code, doc.Errors[0].Message
	}
	if code == "" {
		switch resp.StatusCode {
		case http.StatusNotFound:
			code = "NOT_FOUND"
		case http.StatusForbidden:
			code = "DENIED"
		case http.StatusUnauthorized:
			code = "UNAUTHORIZED"
		}
	}

	err := &Error{
		Op:     req.method,
		What:   req.what,
		Status: resp.StatusCode,
		Code:   code,
		// The message is scrubbed because it is text the registry wrote and
		// this package is about to put into somebody's log.
		Message: c.scrub.Replace(strings.TrimSpace(message)),
	}
	switch resp.StatusCode {
	case http.StatusNotFound:
		err.kind = ErrNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		err.kind = ErrDenied
	}
	return nil, retryableStatus(resp.StatusCode) || racedWithAnotherWriter(req, code), err
}

func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// racedWithAnotherWriter reports whether a 4xx on a manifest PUT is the
// kind another attempt can fix: concurrent savers of one cache key push the
// same blobs, and a registry still committing one intermittently answers
// the manifest with 400 DIGEST_INVALID.
//
// A 4xx normally means "wrong now, wrong next time", which is why
// retryableStatus must not include any of them. These three codes are the
// exception, and only on a manifest write: each says the registry does not
// YET hold a blob another machine is putting there. Scoped to method AND
// code, so a genuinely malformed manifest still fails immediately.
func racedWithAnotherWriter(req request, code string) bool {
	if req.method != http.MethodPut || !strings.Contains(req.what, "manifest ") {
		return false
	}
	switch code {
	case "DIGEST_INVALID", "BLOB_UNKNOWN", "MANIFEST_BLOB_UNKNOWN":
		return true
	}
	return false
}

// clean strips the credential out of an error the transport produced. A
// url.Error prints the whole URL, and while nothing here ever puts a
// credential in one, this is what makes that a guarantee rather than a
// property of the current code.
func (c *Client) clean(err error) error {
	msg := c.scrub.Replace(err.Error())
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

// Error is one refusal from the registry.
type Error struct {
	Op      string // "GET", "PUT", "HEAD", "POST"
	What    string // "blob sha256:1234abcd", "manifest sha256-..."
	Status  int
	Code    string // the API's own error code, e.g. "BLOB_UNKNOWN"
	Message string
	// kind is the sentinel this error matches, if any. Not exported: a caller
	// compares with errors.Is rather than reading it.
	kind error
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "oci: %s %s: %d", e.Op, e.What, e.Status)
	if e.Code != "" {
		b.WriteString(" " + e.Code)
	}
	if e.Message != "" {
		b.WriteString(": " + e.Message)
	}
	return b.String()
}

func (e *Error) Is(target error) bool { return target == e.kind }

// validDigest refuses anything that is not a digest this package can address.
//
// Every function that turns a digest into a URL calls this first: a digest
// reaches this package from event logs, plans and command-line arguments, all
// of which are untrusted input, and "sha256:../../v2/_catalog" must never
// become a path.
func validDigest(digest string) error {
	const prefix = "sha256:"
	ok := len(digest) == len(prefix)+64 && strings.HasPrefix(digest, prefix)
	if ok {
		for _, r := range digest[len(prefix):] {
			if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
				ok = false
				break
			}
		}
	}
	if !ok {
		return fmt.Errorf("%w: %q is not a sha256 digest", ErrNotFound, digest)
	}
	return nil
}

// validReference refuses anything that is not a tag or a digest, for the same
// reason validDigest does.
func validReference(ref string) error {
	if validDigest(ref) == nil {
		return nil
	}
	// The specification's tag grammar: an alphanumeric or underscore, then up
	// to 127 more of those plus dot and dash.
	bad := len(ref) == 0 || len(ref) > 128
	if !bad {
		for i, r := range ref {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			case (r == '.' || r == '-') && i > 0:
			default:
				bad = true
			}
			if bad {
				break
			}
		}
	}
	if bad {
		return fmt.Errorf("%w: %q is not a tag or a digest", ErrNotFound, ref)
	}
	return nil
}

// short is a digest's first eight hex digits, for error messages. Never an
// address.
func short(digest string) string {
	_, hex, ok := strings.Cut(digest, ":")
	if !ok || len(hex) < 8 {
		return digest
	}
	return hex[:8]
}

// readOnly hides everything but Read, so a reader handed to net/http cannot
// be closed by it. See once.
type readOnly struct{ io.Reader }

func bytesSeeker(b []byte) io.ReadSeeker { return &byteSeeker{b: b} }

// byteSeeker is bytes.Reader's Read and Seek, written out rather than
// imported, so this package's request bodies are one type whose behaviour on
// a retry is stated here rather than inferred.
type byteSeeker struct {
	b []byte
	i int64
}

func (r *byteSeeker) Read(p []byte) (int, error) {
	if r.i >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += int64(n)
	return n, nil
}

func (r *byteSeeker) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.i + offset
	case io.SeekEnd:
		abs = int64(len(r.b)) + offset
	default:
		return 0, errors.New("oci: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("oci: negative position")
	}
	r.i = abs
	return abs, nil
}
