package attachsrv_test

import (
	"net/http"
	"testing"
)

// The positive half of the accept-time peer check, through the real
// Listen, asserting explicitly that a legitimate connection is served and
// PeerRejected stays zero. On its own it would pass even with the check
// disabled entirely, which is why
// TestARejectedPeerNeverReachesAHandlerAndIsCounted exists alongside it:
// together they prove a legitimate connection is let through AND an
// illegitimate one is stopped before net/http sees it.
func TestASameUIDClientCompletesARealRequestThroughThePeerCheck(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	for _, e := range twoStepEvents() {
		ts.hub.Emit(e)
	}

	resp, err := ts.client.Get(ts.url("/api/state"))
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 — a same-uid client must complete a real request through the accept-time peer check", resp.StatusCode)
	}

	if got := ts.srv.PeerRejected(); got != 0 {
		t.Errorf("PeerRejected() = %d, want 0 — this connection was never illegitimate", got)
	}
}
