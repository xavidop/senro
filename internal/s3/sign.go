package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

// service is the only AWS service this package signs for. A constant, not a
// parameter: a wrong-service signature rejects in a way that reads like a
// credential problem.
const service = "s3"

// scheme names the signing algorithm in the Authorization header and in the
// string to sign. Both must say the same thing.
const scheme = "AWS4-HMAC-SHA256"

// terminator ends the credential scope. Fixed by the SigV4 specification.
const terminator = "aws4_request"

// UnsignedPayload is what x-amz-content-sha256 carries when a body's hash
// is not known in advance. This package never uses it: every upload is
// spooled somewhere seekable so the body CAN be hashed and signed, turning
// bytes altered in flight into a signature failure rather than a corrupted
// object. Named so the absence is a stated decision, not an omission.
const UnsignedPayload = "UNSIGNED-PAYLOAD"

// credentials are what SigV4 needs. SecretAccessKey is used as an HMAC key
// and never travels: see TestSignNeverPutsTheSecretKeyOnTheWire.
type credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	// SessionToken is set for temporary credentials (an assumed role, which
	// is how CI usually authenticates). It goes out as x-amz-security-token
	// AND is signed: sending it unsigned is rejected.
	SessionToken string
}

// sign adds the SigV4 Authorization header to req, along with the headers
// the signature covers.
//
// payloadSHA256 is the lowercase hex SHA-256 of the exact bytes the body
// will deliver; the caller computes it because only the caller knows
// whether the body can be read twice. now is a parameter so a test can pin
// a signature against a published vector.
func sign(req *http.Request, cred credentials, region, payloadSHA256 string, now time.Time) {
	now = now.UTC()
	stamp := now.Format("20060102T150405Z")
	day := now.Format("20060102")

	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payloadSHA256)
	if cred.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", cred.SessionToken)
	}

	canonicalHeaders, signedHeaders := canonicalizeHeaders(req)

	// The path is rewritten to this package's strict encoding and signed
	// from that same string, so signature and wire cannot drift: net/http
	// would otherwise send url.EscapedPath, whose RFC 3986 rules disagree
	// with SigV4's on characters such as "$".
	path := req.URL.Path
	if path == "" {
		path = "/"
	}
	escaped := escapePath(path)
	req.URL.RawPath = escaped

	canonicalRequest := strings.Join([]string{
		req.Method,
		escaped,
		canonicalQuery(req),
		canonicalHeaders,
		signedHeaders,
		payloadSHA256,
	}, "\n")

	scope := day + "/" + region + "/" + service + "/" + terminator
	stringToSign := strings.Join([]string{
		scheme,
		stamp,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	key := signingKey(cred.SecretAccessKey, day, region)
	signature := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))

	req.Header.Set("Authorization", scheme+" "+
		"Credential="+cred.AccessKeyID+"/"+scope+", "+
		"SignedHeaders="+signedHeaders+", "+
		"Signature="+signature)
}

// canonicalizeHeaders returns the canonical headers block and the
// semicolon-joined list of names it covers. Every header on the request is
// signed, plus host (which net/http carries outside the header map), so a
// header added at a call site is covered automatically rather than
// travelling unprotected.
func canonicalizeHeaders(req *http.Request) (block, names string) {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	values := map[string]string{"host": host}
	for name, vs := range req.Header {
		lower := strings.ToLower(name)
		if lower == "authorization" {
			continue // being computed; it cannot cover itself
		}
		trimmed := make([]string, 0, len(vs))
		for _, v := range vs {
			trimmed = append(trimmed, collapseSpaces(v))
		}
		values[lower] = strings.Join(trimmed, ",")
	}

	sorted := make([]string, 0, len(values))
	for name := range values {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	var b strings.Builder
	for _, name := range sorted {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(values[name])
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(sorted, ";")
}

// canonicalQuery renders the query string in SigV4's canonical form: sorted
// by name and then by value, every component strictly encoded.
func canonicalQuery(req *http.Request) string {
	q := req.URL.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(q))
	for _, k := range keys {
		vs := append([]string(nil), q[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, escapeQuery(k)+"="+escapeQuery(v))
		}
	}
	return strings.Join(parts, "&")
}

// collapseSpaces trims a header value and reduces every run of spaces inside
// it to one, which is what the specification requires of an unquoted value.
func collapseSpaces(v string) string {
	return strings.Join(strings.Fields(v), " ")
}

// escapePath encodes a key path for both the wire and the signature: every
// byte outside RFC 3986's unreserved set is percent-encoded, "/" kept as a
// segment separator. Not net/url's: url.PathEscape leaves "$", "+", "=" and
// ":" unescaped, and SigV4 requires them encoded; one character encoded
// differently in the signature than on the wire is an unexplained rejection.
func escapePath(s string) string { return escapeBytes(s, true) }

// escapeQuery is escapePath for a query component, where "/" has no
// structural meaning and is encoded like anything else.
func escapeQuery(s string) string { return escapeBytes(s, false) }

func escapeBytes(s string, keepSlash bool) string {
	const upperhex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '-' || c == '_' || c == '.' || c == '~':
			b.WriteByte(c)
		case c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(upperhex[c>>4])
			b.WriteByte(upperhex[c&0x0f])
		}
	}
	return b.String()
}

// signingKey derives the request key from the secret. The chain is what makes
// a leaked signature useless outside its day, region and service.
func signingKey(secret, day, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), []byte(day))
	k = hmacSHA256(k, []byte(region))
	k = hmacSHA256(k, []byte(service))
	return hmacSHA256(k, []byte(terminator))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
