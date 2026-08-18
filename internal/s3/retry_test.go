package s3_test

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/s3"
)

// failFirstWrites answers the first n write requests with a retryable
// status and lets the rest through to the real store, reproducing on demand
// the 503 SlowDown a busy store produces on its own.
type failFirstWrites struct {
	under     http.RoundTripper
	remaining atomic.Int32
	seen      atomic.Int32
}

func (f *failFirstWrites) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPut || f.remaining.Add(-1) < 0 {
		return f.under.RoundTrip(req)
	}
	f.seen.Add(1)
	const body = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code>` +
		`<Message>Please reduce your request rate.</Message></Error>`
	return &http.Response{
		StatusCode:    http.StatusServiceUnavailable,
		Status:        "503 Service Unavailable",
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"application/xml"}},
		Body:          io.NopCloser(bytes.NewReader([]byte(body))),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

// TestAPutRetriedAfterASlowDownStillSendsTheWholeBody: net/http closes a
// request body that is an io.ReadCloser (an *os.File is one), so the only
// safe answer is to never hand the transport something it may close, or a
// retried upload from a spooled file fails "file already closed".
func TestAPutRetriedAfterASlowDownStillSendsTheWholeBody(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	tr := &failFirstWrites{under: http.DefaultTransport}
	tr.remaining.Store(1)

	c, err := s3.New(s3.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
		PathStyle: true, Timeout: 30 * time.Second, Transport: tr,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}

	// An io.ReadCloser body from a file: what an upload from the local
	// store actually is.
	want := bytes.Repeat([]byte("uploaded from a file on disk\n"), 1000)
	path := filepath.Join(t.TempDir(), "object")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatalf("writing the spool file: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening the spool file: %v", err)
	}
	defer func() { _ = f.Close() }()

	if err := c.Put(ctx, "retried/object", f, int64(len(want))); err != nil {
		t.Fatalf("Put through one 503: %v", err)
	}
	if tr.seen.Load() != 1 {
		t.Fatalf("the transport refused %d writes, want 1; this test did not exercise a retry",
			tr.seen.Load())
	}

	rc, err := c.Get(ctx, "retried/object")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("after a retry the object is %d bytes, want %d", len(got), len(want))
	}
}

// TestTheTransportNeverClosesABodyThisPackageStillOwns is the same rule
// with no store involved: the caller's reader must come back usable.
func TestTheTransportNeverClosesABodyThisPackageStillOwns(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "object")
	if err := os.WriteFile(path, []byte("some bytes"), 0o600); err != nil {
		t.Fatalf("writing: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	defer func() { _ = f.Close() }()

	c, err := s3.New(s3.Config{
		// Nothing listening: every attempt fails and the retry loop runs to
		// exhaustion, where a closed body would show on the second rewind.
		Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "b",
		AccessKeyID: "k", SecretAccessKey: "s", PathStyle: true,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}

	err = c.Put(t.Context(), "k", f, 10)
	if err == nil {
		t.Fatal("Put against an endpoint with nothing listening succeeded")
	}
	if bytes.Contains([]byte(err.Error()), []byte("file already closed")) {
		t.Fatalf("the transport closed a body this package still owns: %v", err)
	}
	// The caller's reader is still usable afterwards, which is the property.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Errorf("the caller's reader is unusable after a failed Put: %v", err)
	}
}
