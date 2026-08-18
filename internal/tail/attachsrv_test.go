package tail_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/tail"
)

// This file drives the resume loop against a REAL attach server over TCP
// with a real bearer token, so the rules asserted with a fake elsewhere are
// also the ones a live engine answers; the browser's wasm client differs
// only in its Getter. It also checks end to end that the state a client
// folds equals the state the server holds, both through api.RunState.Apply.

// testToken is 43 characters, matching what attach.Listen generates and
// comfortably over attachsrv's own minimum.
const testToken = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"

// httpGetter is tail.Getter over net/http, presenting the run's bearer
// token exactly as internal/source's LiveSource does: an Authorization
// header, never a query parameter.
type httpGetter struct {
	base   string
	token  string
	client *http.Client

	// before lets a test change the world underneath the client at a
	// precise point; overflow is otherwise a race nobody can time.
	mu     sync.Mutex
	before func(path string)
}

func (g *httpGetter) Get(ctx context.Context, path string) (int, io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+g.token)
	resp, err := g.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	g.mu.Lock()
	hook := g.before
	g.mu.Unlock()
	if hook != nil {
		hook(path)
	}
	return resp.StatusCode, resp.Body, nil
}

func (g *httpGetter) hook(f func(path string)) {
	g.mu.Lock()
	g.before = f
	g.mu.Unlock()
}

// liveServer is one attach server with a hub a test can emit into.
type liveServer struct {
	hub    *attachsrv.Hub
	getter *httpGetter
}

func newLiveServer(t *testing.T, ringSize int) *liveServer {
	t.Helper()
	hub := attachsrv.NewHub(ringSize)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind:    "127.0.0.1:0",
		Network: attachsrv.NetworkTCP,
		Token:   testToken,
		Dir:     t.TempDir(),
		Hub:     hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	t.Cleanup(func() { _ = hub.Close() })

	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return &liveServer{
		hub: hub,
		getter: &httpGetter{
			base:   "http://" + srv.Addr(),
			token:  testToken,
			client: &http.Client{Transport: tr}, // no blanket Timeout: the stream body stays open
		},
	}
}

// emit pushes one event, assigning the next sequence number.
func (ls *liveServer) emit(seq uint64, e api.Event) {
	e.V = api.Version
	e.Seq = seq
	e.Run = "r1"
	if e.TS.IsZero() {
		e.TS = time.Unix(0, int64(seq)*int64(time.Millisecond)).UTC()
	}
	ls.hub.Emit(e)
}

// A whole run folded by a client against a real server must EQUAL the run
// as the server folded it, possible only because both call
// api.RunState.Apply.
func TestClientStateEqualsServerStateOverARealAttach(t *testing.T) {
	shortenReconnect(t)
	ls := newLiveServer(t, 256)

	// History before the client connects, so the snapshot is not empty and
	// the resume pairing is load-bearing.
	ls.emit(1, api.Event{Type: api.RunStarted, Payload: mustPayload(t, map[string]any{
		"pipeline": "demo", "engine_version": "test", "started_at": "2024-01-01T00:00:00Z",
	})})
	ls.emit(2, api.Event{Type: api.StepCreated, Step: "build", Payload: mustPayload(t, map[string]any{"kind": "shell"})})
	ls.emit(3, api.Event{Type: api.StepStarted, Step: "build"})

	// The mutex is handed to tail.Run: guarding only the pointer would
	// leave every read of Seq and Steps racing the fold (see tail.Fold).
	var mu sync.Mutex
	var state *api.RunState
	done := make(chan error, 1)
	go func() {
		done <- tail.Run(context.Background(), &tail.HTTPBackend{Getter: ls.getter}, tail.Fold{
			Lock:       &mu,
			OnSnapshot: func(st *api.RunState) { state = st },
			OnFold:     func(*api.RunState) {},
		})
	}()

	// Wait for the subscription so the remaining events go down the live
	// stream, not into the snapshot; the subscriber count keeps this
	// deterministic.
	waitForSubscriber(t, ls.hub)

	ls.emit(4, api.Event{Type: api.StepLogAppended, Step: "build", Payload: mustPayload(t, map[string]any{
		"stream": "stdout", "offset": 0, "len": 12,
	})})
	ls.emit(5, api.Event{Type: api.StepRetried, Step: "build", Attempt: 2, Payload: mustPayload(t, map[string]any{"attempt": 2})})
	ls.emit(6, api.Event{Type: api.StepStarted, Step: "build", Attempt: 2})
	ls.emit(7, api.Event{Type: api.StepFinished, Step: "build", Attempt: 2, Payload: mustPayload(t, map[string]any{
		"state": string(api.StateRecovered), "exit_code": 0,
	})})
	// A type this build folds nothing for: ignored, not rejected, and its
	// Seq still advances.
	ls.emit(8, api.Event{Type: api.Type("something.from.the.future")})
	ls.emit(9, api.Event{Type: api.RunFinished, Payload: mustPayload(t, map[string]any{
		"status": string(api.RunSucceeded),
	})})

	waitForSeq(t, func() uint64 {
		mu.Lock()
		defer mu.Unlock()
		if state == nil {
			return 0
		}
		return state.Seq
	}, 9)

	_ = ls.hub.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("tail.Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tail.Run did not return after the hub closed")
	}

	mu.Lock()
	got := state
	mu.Unlock()
	want := ls.hub.State()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the client's fold and the server's fold disagree.\nclient: %+v\nserver: %+v", got, want)
	}
	// Spot-check a rule only the real fold implements, so a DeepEqual over
	// two empty states would still be caught.
	st := got.Steps["build"]
	if st == nil || st.State != api.StateRecovered || st.Attempt != 2 {
		t.Fatalf("step build = %+v, want the retried attempt folded as recovered", st)
	}
	if got.Seq != 9 {
		t.Errorf("Seq = %d, want 9: an unknown event type must still advance the sequence", got.Seq)
	}
}

// The overflow path against a real server, with a real 410: the hook moves
// the ring past the client between snapshot and subscription.
func TestRealOverflowIsRecoveredFromByReSnapshotting(t *testing.T) {
	shortenReconnect(t)
	const ring = 2
	ls := newLiveServer(t, ring)

	ls.emit(1, api.Event{Type: api.RunStarted, Payload: mustPayload(t, map[string]any{"pipeline": "demo"})})

	// After the first snapshot, bury the resume point under enough newer
	// events that the ring evicts it.
	var once sync.Once
	seq := uint64(1)
	ls.getter.hook(func(path string) {
		if path != tail.StatePath {
			return
		}
		once.Do(func() {
			for i := 0; i < ring*4; i++ {
				seq++
				ls.emit(seq, api.Event{Type: api.StepCreated, Step: fmt.Sprintf("s%d", i),
					Payload: mustPayload(t, map[string]any{"kind": "shell"})})
			}
			ls.getter.hook(nil)
		})
	})

	var mu sync.Mutex
	var snapshots []uint64
	var state *api.RunState
	done := make(chan error, 1)
	go func() {
		done <- tail.Run(context.Background(), &tail.HTTPBackend{Getter: ls.getter}, tail.Fold{
			Lock: &mu,
			OnSnapshot: func(st *api.RunState) {
				snapshots = append(snapshots, st.Seq)
				state = st
			},
			OnFold: func(*api.RunState) {},
		})
	}()

	waitForSubscriber(t, ls.hub)
	_ = ls.hub.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("tail.Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tail.Run did not return after the hub closed")
	}

	mu.Lock()
	snaps := append([]uint64(nil), snapshots...)
	got := state
	mu.Unlock()

	if len(snaps) < 2 {
		t.Fatalf("snapshots = %v, want at least two: the client did not re-snapshot after the ring moved past it", snaps)
	}
	if snaps[0] != 1 {
		t.Errorf("first snapshot Seq = %d, want 1", snaps[0])
	}
	if snaps[len(snaps)-1] <= snaps[0] {
		t.Errorf("snapshots = %v, want the recovery snapshot to be ahead of the first", snaps)
	}
	// Recovery means ending up where the server is, having skipped the
	// events the client could never have seen.
	if want := ls.hub.State(); !reflect.DeepEqual(got, want) {
		t.Fatalf("after recovering from overflow the client's fold and the server's disagree.\nclient: %+v\nserver: %+v", got, want)
	}
}

// A request with no credential must not reach a handler, and the loop
// reports the refusal rather than retrying it forever.
func TestAMissingCredentialIsReportedNotRetried(t *testing.T) {
	shortenReconnect(t)
	ls := newLiveServer(t, 16)
	ls.getter.token = "" // presents "Bearer ", which is not this run's token

	err := tail.Run(context.Background(), &tail.HTTPBackend{Getter: ls.getter}, tail.Fold{})
	if err == nil {
		t.Fatal("Run returned nil against a server that refused every request")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("Run error = %v, want it to carry the refusal's status", err)
	}
}

// mustPayload marshals an event body from a map rather than the typed api
// structs: a payload field renamed on the Go side without a wire change
// then fails this test rather than quietly following it.
func mustPayload(t *testing.T, v map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return b
}

// waitForSubscriber blocks until the hub has an attached stream, so a test
// can emit into the live path rather than into the snapshot.
func waitForSubscriber(t *testing.T, hub *attachsrv.Hub) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if hub.SubscriberCount() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no subscriber attached within the deadline")
}

// waitForSeq blocks until read() reports at least want.
func waitForSeq(t *testing.T, read func() uint64, want uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if read() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the client never folded up to seq %d (stuck at %d)", want, read())
}
