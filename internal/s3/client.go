// Package s3 is a small, standard-library-only client for the subset of the
// S3 API a content-addressed cache needs: get, put, and head of one key.
//
// Not an SDK: the root module stays thin, Signature Version 4 is a
// documented HMAC chain (sign.go, pinned against AWS's worked examples),
// and the credentials arrive already resolved. An SDK would bring hundreds
// of packages to save about three hundred lines.
//
// "S3" means the protocol, not the vendor: tested against MinIO, works
// against Amazon S3, R2, B2, Ceph and GCS's interoperability endpoint.
//
// Nothing here knows what a cache is: opaque bytes under opaque keys, with
// verification the caller's job (see internal/remotecache).
package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
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
	// ErrNotFound means the store answered, authoritatively, that there is
	// no object at that key. Deliberately narrow: only a 404 naming a
	// missing KEY produces it. A missing bucket or rejected signature is an
	// ordinary error, or a broken configuration would read as a cache that
	// silently never hits.
	ErrNotFound = errors.New("s3: object not found")

	// ErrDenied means the request was understood and refused (bad signature,
	// expired token, policy), so a caller can say "check your credentials".
	ErrDenied = errors.New("s3: access denied")
)

// maxAttempts bounds how many times one operation is sent.
//
// Retries cover a connection that failed before the server formed an
// opinion, and 429/5xx; every request is idempotent, so neither can produce
// a wrong answer. A 403 or 404 is an answer, never retried. Three, because
// the caller degrades to no cache on failure and should not wait out a long
// schedule for a cache that is down.
const maxAttempts = 3

// retryBackoff is the pause before attempt n+1. Short and fixed: the budget
// here is a blip, not an outage.
const retryBackoff = 250 * time.Millisecond

// Config is everything needed to reach one bucket.
type Config struct {
	// Endpoint is the service URL, e.g. "https://s3.eu-west-1.amazonaws.com"
	// or "http://127.0.0.1:9000". Any path on it becomes a prefix, so an
	// endpoint behind a reverse proxy at /storage works.
	Endpoint string
	// Region scopes the signature; a mismatch surfaces as an authorization
	// failure from the service.
	Region string
	Bucket string

	AccessKeyID     string
	SecretAccessKey string
	// SessionToken is set for temporary credentials (an OIDC-assumed role in
	// CI).
	SessionToken string

	// PathStyle puts the bucket in the path (endpoint/bucket/key) rather
	// than the host (bucket.endpoint/key). Self-hosted implementations need
	// it; Amazon expects the other. remotecache.Config defaults it from the
	// endpoint.
	PathStyle bool

	// Timeout bounds one request including reading its body. Zero means
	// DefaultTimeout.
	Timeout time.Duration

	// Transport is the round tripper. Zero means a private transport; set so
	// a test can inject failures a real server will not produce on demand.
	Transport http.RoundTripper

	// now is the clock signatures are stamped with. Zero means time.Now.
	now func() time.Time
}

// DefaultTimeout bounds a single request. Generous, since a workspace
// tarball over a cold CI runner's link is slow; finite, since a hang is
// worse for a build than a failure.
const DefaultTimeout = 5 * time.Minute

// Client talks to one bucket.
type Client struct {
	base      *url.URL
	bucket    string
	region    string
	cred      credentials
	pathStyle bool
	http      *http.Client
	now       func() time.Time
	// scrub removes the credentials from any text on its way into an error,
	// including text the SERVER sent back.
	scrub *strings.Replacer
}

// New validates a config and prepares a client. No I/O: a configuration
// error is reported at startup, not on a first lookup where it would be
// indistinguishable from a cold cache.
func New(cfg Config) (*Client, error) {
	switch {
	case cfg.Endpoint == "":
		return nil, errors.New("s3: no endpoint")
	case cfg.Bucket == "":
		return nil, errors.New("s3: no bucket")
	case cfg.Region == "":
		return nil, errors.New("s3: no region")
	case cfg.AccessKeyID == "":
		return nil, errors.New("s3: no access key id")
	case cfg.SecretAccessKey == "":
		return nil, errors.New("s3: no secret access key")
	}

	base, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3: endpoint is not a URL: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("s3: endpoint scheme %q is not http or https", base.Scheme)
	}
	if base.Host == "" {
		return nil, errors.New("s3: endpoint has no host")
	}
	if base.User != nil {
		// A credential in a URL is a credential in every error message and
		// log line that names the endpoint.
		return nil, errors.New("s3: endpoint must not carry a username or password; " +
			"put credentials in the access key and secret fields")
	}
	base.Path = strings.TrimSuffix(base.Path, "/")
	base.RawQuery, base.Fragment = "", ""

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	transport := cfg.Transport
	if transport == nil {
		transport = newTransport()
	}

	scrubs := scrubPairs(cfg)

	return &Client{
		base:   base,
		bucket: cfg.Bucket,
		region: cfg.Region,
		cred: credentials{
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			SessionToken:    cfg.SessionToken,
		},
		pathStyle: cfg.PathStyle,
		http:      &http.Client{Transport: transport, Timeout: timeout},
		now:       now,
		scrub:     scrubs,
	}, nil
}

// scrubPairs builds the replacer that keeps credentials out of error text.
//
// A value shorter than redact.MinLength is NOT scrubbed, the same ruling
// internal/redact makes: a one-letter "secret" would replace half of every
// message, and no real credential is that short. This is a backstop, not
// the mechanism: the secret is an HMAC key that never travels, and the
// endpoint refuses userinfo; what this covers is text the SERVER sent back.
func scrubPairs(cfg Config) *strings.Replacer {
	var pairs []string
	for name, value := range map[string]string{
		"[redacted secret access key]": cfg.SecretAccessKey,
		"[redacted session token]":     cfg.SessionToken,
	} {
		if len(value) >= redact.MinLength {
			pairs = append(pairs, value, name)
		}
	}
	return strings.NewReplacer(pairs...)
}

// newTransport is a private transport rather than http.DefaultTransport, so
// this package's connection behaviour is not shared with, or altered by,
// anything else in the process.
func newTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	// Mostly one host; warm connections avoid a TLS handshake per object.
	t.MaxIdleConnsPerHost = 32
	return t
}

// String names the store without naming the credentials; a degraded run
// prints it, so it must be safe in front of anyone.
func (c *Client) String() string {
	return "s3 bucket " + c.bucket + " at " + c.base.Host
}

// Bucket is the bucket this client addresses.
func (c *Client) Bucket() string { return c.bucket }

// KeyURL is the absolute URL one key resolves to. Exported for the tests
// pinning path style against virtual-host style, and for precise errors.
func (c *Client) KeyURL(key string) string { return c.keyURL(key).String() }

func (c *Client) keyURL(key string) *url.URL {
	u := *c.base
	if c.pathStyle {
		u.Path = c.base.Path + "/" + c.bucket + "/" + strings.TrimPrefix(key, "/")
	} else {
		u.Host = c.bucket + "." + c.base.Host
		u.Path = c.base.Path + "/" + strings.TrimPrefix(key, "/")
	}
	// RawPath uses this package's own encoding so what is signed and what is
	// sent are the same string, and KeyURL renders what goes on the wire.
	u.RawPath = escapePath(u.Path)
	return &u
}

// Get returns the object's body. The caller closes it, and the caller
// verifies it; see internal/remotecache.
func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.send(ctx, http.MethodGet, key, c.keyURL(key), nil, 0, emptyPayloadHash)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Head reports whether the object exists and how many bytes it holds. A
// missing object is (0, false, nil), not an error; anything that stops the
// store from answering at all still is.
func (c *Client) Head(ctx context.Context, key string) (int64, bool, error) {
	resp, err := c.send(ctx, http.MethodHead, key, c.keyURL(key), nil, 0, emptyPayloadHash)
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
			return 0, false, fmt.Errorf("s3: HEAD %s: unreadable Content-Length %q", key, v)
		}
		size = n
	}
	if size < 0 {
		return 0, false, fmt.Errorf("s3: HEAD %s: the store reported no Content-Length", key)
	}
	return size, true, nil
}

// Put stores size bytes read from body under key.
//
// body must be seekable, on purpose: the request is signed over the
// SHA-256 of the exact bytes it carries, so the body is read once to hash
// and again to send. A signed payload turns bytes altered in flight into a
// signature failure at the store rather than a corrupted object surfacing
// days later, and makes a retry safe.
//
// Concurrent Puts of the same key with the same bytes are safe: a single
// PUT is atomic at the store, and identical content makes the winner moot.
func (c *Client) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	hash, err := hashSeeker(body)
	if err != nil {
		return fmt.Errorf("s3: PUT %s: %w", key, err)
	}
	resp, err := c.send(ctx, http.MethodPut, key, c.keyURL(key), body, size, hash)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// PutBytes stores b: for the small objects a cache stores by the thousand,
// a byte slice reads better than a reader and costs nothing.
func (c *Client) PutBytes(ctx context.Context, key string, b []byte) error {
	return c.Put(ctx, key, bytesSeeker(b), int64(len(b)))
}

// emptyPayloadHash is the SHA-256 of no bytes, which every request without a
// body signs.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// send performs one operation, retrying the failures that are safe to retry.
//
// u is the prepared URL and key is only the label errors name it by. The two
// are separate because a bucket-level request (see List) addresses no key at
// all, and threading its URL through here is what lets it share this retry
// loop, this signing, and this error classification rather than growing a
// second copy of them.
func (c *Client) send(
	ctx context.Context, method, key string, u *url.URL,
	body io.ReadSeeker, size int64, payloadHash string,
) (*http.Response, error) {
	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if body != nil {
			if _, err := body.Seek(0, io.SeekStart); err != nil {
				return nil, fmt.Errorf("s3: %s %s: rewinding the body: %w", method, key, err)
			}
		}
		resp, retryable, err := c.attempt(ctx, method, key, u, body, size, payloadHash)
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

// attempt performs exactly one request. The second return says whether
// sending it again could plausibly produce a different answer.
func (c *Client) attempt(
	ctx context.Context, method, key string, u *url.URL,
	body io.Reader, size int64, payloadHash string,
) (*http.Response, bool, error) {
	// readOnly, never the caller's reader directly: net/http closes a body
	// that is an io.ReadCloser, and an *os.File closed by the transport
	// cannot be rewound for the retry, failing "file already closed" the
	// first time a busy store answers 503. Hiding the Closer makes net/http
	// wrap the reader in its own NopCloser.
	var reqBody io.Reader
	if body != nil {
		reqBody = readOnly{body}
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
	if err != nil {
		return nil, false, fmt.Errorf("s3: %s %s: %w", method, key, err)
	}
	if body != nil {
		req.ContentLength = size
	}
	// RawPath must be restored after the parse: this package's encoding is
	// stricter than net/url's and the signature is taken over it.
	req.URL.Path, req.URL.RawPath = u.Path, u.RawPath
	sign(req, c.cred, c.region, payloadHash, c.now())

	resp, err := c.http.Do(req)
	if err != nil {
		// The store never formed an opinion; asking again is safe.
		return nil, true, fmt.Errorf("s3: %s %s: %w", method, key, c.clean(err))
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, false, nil
	}
	defer func() { _ = resp.Body.Close() }()
	return nil, retryableStatus(resp.StatusCode), c.statusError(method, key, resp)
}

func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// statusError turns a refusal into an error that says what the store said,
// classified so a caller can tell a miss from a misconfiguration.
func (c *Client) statusError(method, key string, resp *http.Response) error {
	// HEAD carries no body, so its code comes from the status alone. GET and
	// PUT return S3's XML error document, whose Code matters: "NoSuchBucket"
	// and "NoSuchKey" are both 404 and mean different things to a cache.
	var doc struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	_ = xml.Unmarshal(b, &doc)

	code := doc.Code
	if code == "" {
		switch resp.StatusCode {
		case http.StatusNotFound:
			code = "NotFound"
		case http.StatusForbidden:
			code = "AccessDenied"
		case http.StatusUnauthorized:
			code = "Unauthorized"
		}
	}

	err := &Error{
		Op:     method,
		Key:    key,
		Status: resp.StatusCode,
		Code:   code,
		// Scrubbed: text the store wrote, about to land in somebody's log.
		Message: c.scrub.Replace(strings.TrimSpace(doc.Message)),
	}
	switch {
	case resp.StatusCode == http.StatusNotFound && (code == "NoSuchKey" || code == "NotFound"):
		err.kind = ErrNotFound
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized:
		err.kind = ErrDenied
	}
	return err
}

// clean strips the credentials out of an error the transport produced: a
// url.Error prints the whole URL, and this makes "no credential in it" a
// guarantee rather than a property of the current code.
func (c *Client) clean(err error) error {
	msg := c.scrub.Replace(err.Error())
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

// Error is one refusal from the store.
type Error struct {
	Op      string // "GET", "PUT", "HEAD"
	Key     string
	Status  int
	Code    string // the store's own error code, e.g. "NoSuchBucket"
	Message string
	// kind is the sentinel this error matches, if any; compared with
	// errors.Is, not read.
	kind error
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "s3: %s %s: %d", e.Op, e.Key, e.Status)
	if e.Code != "" {
		b.WriteString(" " + e.Code)
	}
	if e.Message != "" {
		b.WriteString(": " + e.Message)
	}
	return b.String()
}

func (e *Error) Is(target error) bool { return target == e.kind }

// hashSeeker reads body to the end to hash it and rewinds. The caller sends
// the same bytes afterwards.
func hashSeeker(body io.ReadSeeker) (string, error) {
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(h, body); err != nil {
		return "", err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// readOnly hides everything but Read, so a reader handed to net/http cannot
// be closed by it. See attempt.
type readOnly struct{ io.Reader }

func bytesSeeker(b []byte) io.ReadSeeker { return &byteSeeker{b: b} }

// byteSeeker is bytes.Reader's Read and Seek written out, so this package's
// request bodies are one type whose retry behaviour is stated here.
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
		return 0, errors.New("s3: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("s3: negative position")
	}
	r.i = abs
	return abs, nil
}
