package webui

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
)

// This file is the one place the browser UI can act on a run rather than
// watch it, and a POST is a different proposition from a GET.
//
// The read routes can fail OPEN on a missing browser header: reading is
// gated by the session cookie, which SameSite=Strict keeps other pages
// from sending. That fails for a state-changing request on loopback:
// SameSite is about the SITE, and a site does not include the port, so
// http://127.0.0.1:9999 is same-site with this server and its fetches DO
// carry the cookie. Sec-Fetch-Site catches that case, but checkFetchSite
// deliberately allows the header's ABSENCE: right for a GET, wrong for a
// request that cancels a run.
//
// So control adds a check the read routes do not have, and it fails
// CLOSED: an Origin header exactly matching this server's own, or no
// forwarding happens. See checkOrigin.

// controllableOps is every control operation this server will forward: an
// explicit list, not "whatever api declares". Every name here acts on THIS
// run and nothing else; an op that reaches further must be added here
// deliberately, weighed against this page's threat model, rather than
// becoming browser-reachable the moment it is declared in api.
// TestEveryDeclaredOpHasABrowserRuling forces that decision.
//
// POST /api/shell is not an op and is not here: a browser page does not
// get a shell.
var controllableOps = map[string]bool{
	api.OpRunCancel:       true,
	api.OpRunPause:        true,
	api.OpRunResume:       true,
	api.OpStepRetry:       true,
	api.OpStepSkip:        true,
	api.OpBreakpointSet:   true,
	api.OpBreakpointClear: true,
	api.OpRunRerunFrom:    true,
	api.OpAnalysisAccept:  true,
	api.OpAnalysisReject:  true,

	// ws.snapshot forwards, but present.StepActions draws no button for it:
	// whether a step mounts a workspace at all is not in the folded state
	// that function is a pure function of, so the page cannot tell a step
	// the engine would accept it for from one it would answer no_workspace,
	// and that file's rule is that a button which produces a refusal teaches
	// an operator to distrust the buttons. Forwarded regardless, because the
	// ruling here is about the THREAT model: this op acts on the run the
	// operator is already watching, changes nothing about its outcome, and
	// cannot reach past it.
	api.OpWSSnapshot: true,
}

// maxControlRequestBytes bounds what this server will read from the
// browser. Tighter than attachsrv's 64KiB: the only body this endpoint has
// a use for is a frame carrying an op name and at most one short argument.
const maxControlRequestBytes = 4 * 1024

// checkOrigin reports whether a state-changing request came from this
// server's own page. Fail-closed, unlike checkFetchSite: a request with no
// Origin at all is refused. Every browser sends Origin on a POST, so this
// only turns away non-browsers, which have the attach server itself; the
// check cannot be defeated by a client declining to describe itself.
//
// Compared against allowedHosts, the same allowlist checkHost uses, so the
// two cannot drift. The scheme is always http because Listen binds
// plaintext loopback with no option to change it.
func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	host, ok := strings.CutPrefix(strings.ToLower(origin), "http://")
	if !ok {
		return false
	}
	return s.allowedHosts[host]
}

// handleControl forwards one control frame to the attach server. The
// session cookie is already required by the middleware; this adds the
// origin check, a bound on the body, and a ruling on the op itself.
//
// It deliberately does NOT re-validate the frame's arguments:
// attachsrv.controlArgAllowlist and internal/engine/control.go already do,
// independently, and a third copy of that table would be a third thing to
// drift. This server rules on WHO may ask and WHICH op; the engine's own
// boundary rules on what the arguments may contain.
func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(r) {
		http.Error(w, refusalBody, http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxControlRequestBytes+1))
	if err != nil {
		http.Error(w, "senro ui: could not read the control request\n", http.StatusBadRequest)
		return
	}
	if len(body) > maxControlRequestBytes {
		http.Error(w, "senro ui: control request too large\n", http.StatusRequestEntityTooLarge)
		return
	}

	var req api.Frame
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "senro ui: control request is not a frame\n", http.StatusBadRequest)
		return
	}
	if !controllableOps[req.Type] {
		// Named, unlike the boundary refusals: whoever sees this holds a
		// valid session and is the operator, so "not an op this page may
		// ask for" is worth telling them.
		http.Error(w, "senro ui: the browser UI does not forward that control operation\n", http.StatusForbidden)
		return
	}

	// Re-encoded rather than forwarded byte for byte: what crosses is a
	// frame this server constructed from fields it recognises, so a
	// response-only field or unknown key cannot ride along to the engine.
	out, err := json.Marshal(api.Frame{
		V:       req.V,
		Kind:    api.KindReq,
		ID:      req.ID,
		Type:    req.Type,
		Payload: req.Payload,
	})
	if err != nil {
		http.Error(w, "senro ui: could not encode the control request\n", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	s.forwardControl(w, r, out)
}

// controlHeaderBudget bounds the wait for the attach server's answer to a
// control request, deliberately LONGER than that server's own 30s
// controlTimeout. Equal timers raced, and this side usually won, turning a
// perfectly alive request into an opaque 502; waiting longer lets
// attachsrv answer its own timeout with a frame that says the engine did
// not respond, the true and useful statement.
var controlHeaderBudget = 60 * time.Second

// forwardControl POSTs one already-validated frame upstream and copies the
// answer back. A sibling of forward rather than a parameter on it: forward
// is GET-only by construction, and keeping "readable" and "actionable"
// apart means the read path has no method parameter to get wrong. The
// shared four-line header rule is restated rather than factored out.
func (s *Server) forwardControl(w http.ResponseWriter, r *http.Request, body []byte) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.upstreamBase+"/api/control", bytes.NewReader(body))
	if err != nil {
		http.Error(w, "senro ui: bad upstream request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if s.upstream.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.upstream.Token)
	}

	resp, err := s.controlClient.Do(req)
	if err != nil {
		http.Error(w, "senro ui: the attach server did not answer", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	// Bounded, unlike forward's io.Copy: a control response is one frame,
	// and reading past it would let an upstream make this process allocate
	// without limit.
	_, _ = io.Copy(w, io.LimitReader(resp.Body, maxControlRequestBytes))
}
