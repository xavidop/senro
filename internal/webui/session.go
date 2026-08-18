package webui

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"sync/atomic"
)

// This file is the browser's side of the access boundary, built on one
// fact: the run's own credential must never reach the page. A bearer token
// in a URL is in history, Referer headers and screenshots; in localStorage
// or a readable cookie it is one cross-site script from stolen; in WASM
// memory the same, one step removed. So the senro process holds the run's
// token and adds it to forwarded requests (proxy.go); the browser holds a
// credential for THIS UI server only, minted here, granting exactly what
// this server is willing to do.

// credentialBytes is how many bytes of crypto/rand back each credential:
// 32, matching a run's own bearer token; the browser's door should not be
// weaker than the engine's.
const credentialBytes = 32

// sessionCookie is the name of the cookie carrying the browser's
// credential. Not __Host- prefixed: the prefix requires Secure, Secure
// requires https, and this server is plaintext loopback; a cookie some
// browser silently drops is a UI that silently does not load, and the
// prefix would only guard against subdomains 127.0.0.1 does not have.
const sessionCookie = "senro_ui_session"

// handoffPrefix is the path that trades the one-time nonce for the session
// cookie. Short because it is a URL a person may see in their terminal.
const handoffPrefix = "/h/"

// credential is a random secret compared in constant time, the same shape
// as attachsrv's tokenGuard: both sides are hashed to a fixed length
// first, so the comparison is constant-time end to end and cannot be timed
// for the secret's LENGTH.
type credential struct {
	value  string
	digest [sha256.Size]byte
}

func newCredential() (credential, error) {
	b := make([]byte, credentialBytes)
	if _, err := rand.Read(b); err != nil {
		return credential{}, err
	}
	v := base64.RawURLEncoding.EncodeToString(b)
	return credential{value: v, digest: sha256.Sum256([]byte(v))}, nil
}

// accepts reports whether presented is this credential.
func (c credential) accepts(presented string) bool {
	got := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(got[:], c.digest[:]) == 1
}

// handoff is the one-time nonce that turns a URL somebody opened into a
// browser session. One time, enforced by an atomic swap: the guarantee
// must hold against two tabs opening the same URL at the same instant.
//
// This is the one place a credential touches a URL. The nonce is in the
// shell's scrollback and possibly the browser's history, but by the time
// it could be read out of either it is already spent, and spending it
// again gets the same fixed 404 as an unknown path. The alternatives cost
// more: a typed token is in the clipboard and the page's DOM, and a
// credential-free loopback server can be read by any local process or any
// DNS-rebound page.
type handoff struct {
	cred     credential
	consumed atomic.Bool
}

// claim reports whether presented is the unspent nonce, and spends it. A
// second caller with the identical value gets false.
func (h *handoff) claim(presented string) bool {
	if !h.cred.accepts(presented) {
		return false
	}
	return h.consumed.CompareAndSwap(false, true)
}

// setSession writes the session cookie.
//
// HttpOnly: an injected script cannot exfiltrate a credential it cannot
// see, and the module does not need it (the browser attaches it to
// same-origin requests itself). SameSite=Strict: a request from any other
// page carries no cookie and is refused by requireSession, which is what
// keeps a stray internet tab from reading this run even though it can
// reach 127.0.0.1. Path=/ and no Domain: host-only. No Expires/Max-Age: a
// session cookie the browser drops, since the credential is meaningless
// after the senro process exits. Secure is deliberately absent; see
// sessionCookie.
func setSession(w http.ResponseWriter, value string) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}

// presentedSession pulls the session credential out of a request, or ""
// when there is none: a missing cookie and a wrong one converge on the
// same comparison and the same refusal, as attachsrv's bearerCredential
// does.
func presentedSession(r *http.Request) string {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return ""
	}
	return c.Value
}
