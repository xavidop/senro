package webui

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/sink"
)

// This is the one test that runs the thing a browser actually downloads.
// Everything else is checked in Go, and none of it touches whether the
// compiled client BOOTS: whether fetch.go's promises resolve, whether a
// fetch body's ReadableStream yields what the NDJSON decoder expects,
// whether the renderer ever puts the run on the page. Every Go test can
// pass while the browser console shows a stack trace.
//
// Node has WebAssembly, fetch and streams; only the DOM is stubbed (see
// testdata/smoke.mjs). The client is not stubbed: the same binary,
// gunzipped from the same embedded bundle the server serves.
//
// It skips without node, and without a built client; the skip says which
// one is missing.
func TestTheCompiledClientBootsAndRendersTheRun(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("no node on PATH; this test runs the compiled WebAssembly client outside a browser")
	}

	bundle, err := loadBundle()
	if err != nil {
		if errors.Is(err, ErrBundleMissing) {
			t.Skip("this tree has not built the WebAssembly client; run `make wasm`")
		}
		t.Fatalf("loadBundle: %v", err)
	}

	// A real attach server with a real run in it.
	hub := attachsrv.NewHub(64)
	t.Cleanup(func() { _ = hub.Close() })
	const token = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"
	attach, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind:    "127.0.0.1:0",
		Network: attachsrv.NetworkTCP,
		Token:   token,
		Dir:     t.TempDir(),
		Hub:     hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	t.Cleanup(func() { _ = attach.Close() })

	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "smoke-run",
		Payload: []byte(`{"pipeline":"smoke","engine_version":"test"}`)})
	hub.Emit(api.Event{V: api.Version, Seq: 2, Type: api.StepCreated, Run: "smoke-run", Step: "build",
		Payload: []byte(`{"kind":"shell"}`)})
	hub.Emit(api.Event{V: api.Version, Seq: 3, Type: api.StepStarted, Run: "smoke-run", Step: "build"})

	ui, err := Listen(context.Background(), Options{
		Bind:     "127.0.0.1:0",
		Upstream: Upstream{Network: "tcp", Address: attach.Addr(), Token: token},
	})
	if err != nil {
		t.Fatalf("webui.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ui.Close() })

	// The harness needs the client and the toolchain's bootstrap as files on
	// disk. They are taken out of the embedded bundle rather than off the
	// build tree, so what runs here is what the binary would serve.
	dir := t.TempDir()
	wasmPath := filepath.Join(dir, "senro-ui.wasm")
	writeFile(t, wasmPath, gunzip(t, bundle.files[clientAsset].body))
	execPath := filepath.Join(dir, "wasm_exec.js")
	writeFile(t, execPath, bundle.files[execAsset].body)

	// Emitted only once the client has a subscription open, so they can
	// reach the page down GET /api/stream and nowhere else. The harness
	// waits for them specifically: without this, a client whose streaming
	// path was entirely broken would still render the snapshot and pass.
	go func() {
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if hub.SubscriberCount() > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		hub.Emit(api.Event{V: api.Version, Seq: 4, Type: api.StepFinished, Run: "smoke-run", Step: "build",
			Payload: []byte(`{"state":"succeeded","exit_code":0}`)})
		hub.Emit(api.Event{V: api.Version, Seq: 5, Type: api.StepCreated, Run: "smoke-run", Step: "later",
			Payload: []byte(`{"kind":"shell","needs":["build"]}`)})
	}()

	// Stand in for the engine on the hub's control channel.
	//
	// Without this, nothing reads the channel, handleControl waits out its
	// own 30 second budget, and the harness asserts a timeout rather than
	// the feature: a control request that goes through and is answered. It
	// also makes the test fast, since the answer is immediate.
	//
	// Deliberately answers OK without doing anything. This test is about
	// the path from a click in the compiled client to the attach server and
	// back; what an engine does with a pause is internal/engine's own
	// business and is tested there.
	controlSeen := make(chan sink.ControlRequest, 8)
	go func() {
		for req := range hub.Control() {
			select {
			case controlSeen <- req:
			default:
			}
			req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// The real page, so the harness stubs the elements the client actually
	// looks up rather than a list kept alongside it. See smoke.mjs.
	cmd := exec.CommandContext(ctx, node,
		filepath.Join("testdata", "smoke.mjs"), ui.URL(), wasmPath, execPath,
		filepath.Join("assets", "index.html"))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	runErr := cmd.Run()

	t.Logf("node harness output:\n%s", out.String())
	if runErr != nil {
		t.Fatalf("the compiled client did not render the run: %v", runErr)
	}
	if !strings.Contains(out.String(), "SMOKE PASSED") {
		t.Fatalf("the harness did not report success:\n%s", out.String())
	}

	// The harness asserts that the page reported an outcome; this asserts
	// what actually arrived at the other end. Together they close the whole
	// path: a click in the compiled WebAssembly client became a control
	// request, with the right op, at a real attach server.
	select {
	case req := <-controlSeen:
		if req.Op != api.OpRunPause {
			t.Errorf("the attach server received op %q, want %q", req.Op, api.OpRunPause)
		}
		if len(req.Args) != 0 {
			t.Errorf("a run-scoped pause carried args %v, want none", req.Args)
		}
	default:
		t.Error("the harness reported an outcome but no control request reached the attach server")
	}
}

func writeFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func gunzip(t *testing.T, body []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	defer func() { _ = zr.Close() }()
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return out
}
