package attachsrv

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// This file is the entire access boundary for a TCP attach listener.
//
// A unix socket has two independent guards: a mode and directory the
// operating system enforces, and a peer credential the kernel captures at
// connect time and the peer cannot forge. Neither has any meaning over TCP:
// there is no uid behind a socket off a network interface and no file mode
// to enforce. What is left is a secret the client presents and the server
// checks, so that check has to be right.

// minTokenLength is the shortest credential a TCP listener will start with.
// attach.Listen generates 32 bytes of crypto/rand as 43 base64url
// characters; the floor exists so a caller reaching attachsrv directly
// cannot bind behind a guessable secret. Refused at bind time rather than
// checked diligently: a diligent check of a guessable secret is the
// appearance of protection, not protection.
const minTokenLength = 32

// AuthFailureBurst is how many failed authentications a TCP listener will
// evaluate before refusing them unevaluated; see failureCredit for what
// the bound is for, and why it is not a defence against guessing.
const AuthFailureBurst = 20

// authFailureRefill is how long one unit of failure budget takes to come
// back. One per second: a client that mistypes a token twice is never
// throttled in practice, while a program looping on the port is held to
// roughly one attempt a second forever.
const authFailureRefill = time.Second

// ErrTokenRequired reports that a TCP listener was asked to bind without a
// usable bearer token. Wrapped rather than returned bare so a caller can
// tell this apart from a transport-level bind failure with errors.Is.
var ErrTokenRequired = errors.New("attachsrv: a TCP listener requires a bearer token of at least " +
	fmt.Sprint(minTokenLength) + " characters")

// ErrTLSRequired reports that a TCP listener was asked to bind an address
// that is not loopback, without a TLS configuration. See listenTCP for why
// there is no flag that turns this off.
var ErrTLSRequired = errors.New("attachsrv: a TCP listener on an address that is not loopback requires TLS")

// unauthorizedBody is the entire response an unauthenticated request gets,
// byte for byte, whether it presented no credential, a wrong one, or a
// well-formed one for another run. It says nothing, deliberately: a
// refusal naming the run, the pipeline or the product would confirm to a
// scanner that they had found a senro engine. "No such run" and "wrong
// token" must be the same answer, and one answer is the cheapest way to
// guarantee it.
const unauthorizedBody = "unauthorized\n"

// throttledBody is the equivalent for a request refused unevaluated.
// Distinguishable from unauthorizedBody, harmlessly: it reveals only how
// often somebody has recently failed, which they already know.
const throttledBody = "too many failed attempts\n"

// tokenDigest hashes a presented credential to a fixed 32 bytes. Not to
// hide the token at rest (it is in memory either way): it is what makes
// the comparison in accepts constant-time END TO END.
// subtle.ConstantTimeCompare is constant-time only across EQUAL lengths,
// returning 0 immediately for unequal ones without reading a byte, so a
// caller presenting candidates of varying length could time the difference
// and learn the secret's length. Hashing both sides makes the operands
// always sha256.Size bytes, whatever arrived on the wire. See
// TestTheComparedValueIsAlwaysAFixedLengthDigest.
func tokenDigest(presented string) [sha256.Size]byte {
	return sha256.Sum256([]byte(presented))
}

// tokenGuard holds one run's credential, as a digest, and answers one
// question about it. Separate from the http.Handler that uses it so the
// decision can be tested as a decision: the same split CheckPeer and
// peerCheckedListener use.
type tokenGuard struct {
	want [sha256.Size]byte
}

func newTokenGuard(token string) *tokenGuard {
	return &tokenGuard{want: tokenDigest(token)}
}

// accepts reports whether presented is this run's token.
//
// subtle.ConstantTimeCompare, never ==: Go's string equality returns as
// soon as it finds a differing byte, which over enough samples maps the
// secret one byte at a time. That this really goes through subtle, in this
// package's production code and nowhere else, is checked mechanically; see
// TestTheTokenComparisonGoesThroughConstantTimeCompare.
func (g *tokenGuard) accepts(presented string) bool {
	got := tokenDigest(presented)
	return subtle.ConstantTimeCompare(got[:], g.want[:]) == 1
}

// failureCredit is a token bucket over FAILED authentications.
//
// Not a defence against guessing: 32 bytes of crypto/rand is not guessable
// at any rate, and no bucket size changes that arithmetic. What it bounds
// is COST: without it, anything that can reach the port can make this
// process hash a credential, allocate a request and write a response
// indefinitely, for free.
//
// Global to the server rather than per remote address, on purpose: a
// per-address bucket is trivially evaded with more addresses, so it would
// only ever throttle the client that was not attacking. The cost is that a
// flood can make a MISTYPED token come back 429 rather than 401, a
// confusing message rather than a lost capability: a CORRECT token is
// never checked against this bucket at all. See
// TestFailedAuthenticationIsBoundedAndDoesNotLockOutTheOperator.
type failureCredit struct {
	mu        sync.Mutex
	available float64
	last      time.Time
	// now is a seam for tests, nil meaning time.Now. Nothing outside this
	// package's own tests ever sets it.
	now func() time.Time
}

func newFailureCredit() *failureCredit {
	return &failureCredit{available: AuthFailureBurst}
}

// spend takes one unit of budget for a failed authentication and reports
// whether there was one to take. Only ever called on the failure path.
func (c *failureCredit) spend() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	nowFn := c.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	if !c.last.IsZero() {
		c.available += now.Sub(c.last).Seconds() / authFailureRefill.Seconds()
		if c.available > AuthFailureBurst {
			c.available = AuthFailureBurst
		}
	}
	c.last = now

	if c.available < 1 {
		return false
	}
	c.available--
	return true
}

// tokenAuth puts a tokenGuard in front of the WHOLE mux, not the handlers
// that looked dangerous: a per-route guard is one somebody forgets to
// apply to the next route, and GET /api/logs is a file-read primitive,
// GET /api/stream is the run's whole event history, POST /api/shell is a
// command prompt. No endpoint here should be reachable unauthenticated,
// including one added after this comment, so the check sits where a new
// route cannot route around it.
type tokenAuth struct {
	guard  *tokenGuard
	credit *failureCredit
	// rejected counts refusals, read through Server.AuthRejected. Counted
	// rather than logged per occurrence: a log line per rejected request is
	// a resource an unauthenticated peer can consume for free.
	rejected *uint64
	next     http.Handler
}

func (a *tokenAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a.guard.accepts(bearerCredential(r)) {
		a.next.ServeHTTP(w, r)
		return
	}
	atomic.AddUint64(a.rejected, 1)
	if !a.credit.spend() {
		writeFixedRefusal(w, http.StatusTooManyRequests, throttledBody)
		return
	}
	writeFixedRefusal(w, http.StatusUnauthorized, unauthorizedBody)
}

// writeFixedRefusal writes a response whose every observable byte is fixed
// by status and body alone: no request echo, no timing-dependent content,
// nothing telling one refusal from another with the same status.
//
// Connection: close, so every failed attempt costs a fresh TCP (and, over
// TLS, a fresh handshake) rather than being pipelined down an open
// connection: a real part of the bound failureCredit describes.
func writeFixedRefusal(w http.ResponseWriter, status int, body string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("WWW-Authenticate", "Bearer")
	h.Set("Connection", "close")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// bearerCredential pulls the presented credential out of an Authorization
// header, or returns "" if there is nothing usable there.
//
// "" rather than a distinct "absent" result, so a missing header, another
// scheme and a wrong token converge on the same comparison and the same
// response: omitting the header teaches exactly what guessing wrong does.
//
// The Authorization header and nothing else: no ?token= query parameter,
// which would put the credential in shell history, proxy logs and a
// browser's address bar, and no cookie, which would make the endpoint
// reachable by a cross-site request.
func bearerCredential(r *http.Request) string {
	const scheme = "Bearer "
	v := r.Header.Get("Authorization")
	if len(v) <= len(scheme) || !strings.EqualFold(v[:len(scheme)], scheme) {
		return ""
	}
	return v[len(scheme):]
}

// hostIsLoopback reports whether host names an address unreachable from
// off this machine.
//
// The empty host is the wildcard bind (":8080"), which listens on every
// interface and is emphatically NOT loopback: the easiest way to expose a
// control channel by accident, so the case this most has to get right.
//
// A name counts as loopback only if EVERY address it resolves to is: a
// hosts file must not be able to talk this into treating a routable
// address as local. A name that does not resolve is not loopback either.
func hostIsLoopback(host string) (bool, error) {
	if host == "" {
		return false, nil
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback(), nil
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return false, fmt.Errorf("attachsrv: resolving bind host %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return false, nil
	}
	for _, a := range addrs {
		ip := net.ParseIP(a)
		if ip == nil || !ip.IsLoopback() {
			return false, nil
		}
	}
	return true, nil
}

// bindIsLoopback is hostIsLoopback over a whole host:port bind string.
func bindIsLoopback(bind string) (bool, error) {
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		return false, fmt.Errorf("attachsrv: %q is not a host:port address: %w", bind, err)
	}
	return hostIsLoopback(host)
}
