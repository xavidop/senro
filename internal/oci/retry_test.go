package oci_test

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/oci"
)

// slowDownOnce answers the first upload with 503 and lets everything else
// through to the real registry.
//
// A busy registry does this, and no registry can be asked to do it on demand,
// so it is injected. What is under test is entirely senro's own: whether the
// retry that follows can still send the bytes.
type slowDownOnce struct {
	under   http.RoundTripper
	tripped atomic.Bool
}

func (s *slowDownOnce) RoundTrip(req *http.Request) (*http.Response, error) {
	// The real transport runs first, even for the request whose answer is
	// replaced: a registry answers 503 AFTER net/http has sent (and closed)
	// the body, and a stub that short-circuited the request would never
	// reach the code this is here to catch.
	resp, err := s.under.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if req.Method != http.MethodPut || !strings.Contains(req.URL.Path, "/blobs/uploads/") ||
		!s.tripped.CompareAndSwap(false, true) {
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	const body = `{"errors":[{"code":"TOOMANYREQUESTS","message":"slow down"}]}`
	return &http.Response{
		StatusCode:    http.StatusServiceUnavailable,
		Status:        "503 Service Unavailable",
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

// TestAnUploadRetriedAfterASlowDownStillSendsItsBytes is a regression test
// for the bug the S3 backend shipped: net/http CLOSES a request body that
// is an io.ReadCloser, so a closed *os.File leaves the retry with "file
// already closed", and only against a registry busy enough to answer 503.
// The body is a real *os.File for exactly that reason; a bytes.Reader would
// pass whether or not the bug is present.
func TestAnUploadRetriedAfterASlowDownStillSendsItsBytes(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	content := bytes.Repeat([]byte("a workspace snapshot, uploaded twice\n"), 2000)
	path := filepath.Join(t.TempDir(), "object")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("writing the object: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening the object: %v", err)
	}
	defer func() { _ = f.Close() }()

	c, err := oci.New(oci.Config{
		Registry:   reg.Host,
		Repository: reg.Repository,
		Username:   reg.Username,
		Password:   reg.Password,
		PlainHTTP:  true,
		Timeout:    30 * time.Second,
		Transport:  &slowDownOnce{under: http.DefaultTransport},
	})
	if err != nil {
		t.Fatalf("oci.New: %v", err)
	}

	d := digestOf(content)
	if err := c.PutBlob(ctx, d, f, int64(len(content))); err != nil {
		t.Fatalf("PutBlob through one 503: %v", err)
	}

	// The bytes really arrived, and they are the right ones: the registry
	// verified the digest itself before accepting them, and this reads them
	// back to be sure the retry did not send a truncated body.
	rc, err := c.GetBlob(ctx, d)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the blob back: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("the retried upload stored %d bytes, want %d", len(got), len(content))
	}

	// And the caller's file is still the caller's: closing it here must be the
	// first close it has had.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Errorf("the caller's file is no longer usable after the upload: %v", err)
	}
}
