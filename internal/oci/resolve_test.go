package oci_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/oci"
)

const resolved = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

const wantAccept = "application/vnd.oci.image.index.v1+json, " +
	"application/vnd.oci.image.manifest.v1+json, " +
	"application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.docker.distribution.manifest.v2+json"

// redirect sends every request to srv whatever host it names, so the
// normalised host can be observed without any DNS.
type redirect struct {
	to   string
	seen *atomic.Value
}

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r.seen.Store(req.URL.Scheme + "://" + req.URL.Host + req.URL.Path)
	c := req.Clone(req.Context())
	c.URL.Scheme = "http"
	c.URL.Host = r.to
	return http.DefaultTransport.RoundTrip(c)
}

func TestResolveTagNormalisesReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ ref, plain, want string }{
		{"golang:1.26", "", "https://registry-1.docker.io/v2/library/golang/manifests/1.26"},
		{"docker.io/golang:1.26", "", "https://registry-1.docker.io/v2/library/golang/manifests/1.26"},
		{"docker.io/acme/tool:2", "", "https://registry-1.docker.io/v2/acme/tool/manifests/2"},
		{"acme/tool:2", "", "https://registry-1.docker.io/v2/acme/tool/manifests/2"},
		{"ghcr.io/acme/tool:v1", "", "https://ghcr.io/v2/acme/tool/manifests/v1"},
		{"ghcr.io/acme/tool", "", "https://ghcr.io/v2/acme/tool/manifests/latest"},
		{"localhost:5000/x:y", "yes", "https://localhost:5000/v2/x/manifests/y"},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					t.Errorf("method %s, want HEAD", r.Method)
				}
				if got := r.Header.Get("Accept"); got != wantAccept {
					t.Errorf("Accept = %q", got)
				}
				w.Header().Set("Docker-Content-Digest", resolved)
			}))
			defer srv.Close()
			seen := &atomic.Value{}
			want := tc.want
			cfg := oci.Config{
				Transport: redirect{to: strings.TrimPrefix(srv.URL, "http://"), seen: seen},
				Timeout:   5 * time.Second,
			}
			if tc.plain != "" {
				cfg.PlainHTTP = true
				want = strings.Replace(want, "https://", "http://", 1)
			}
			got, err := oci.ResolveTag(t.Context(), tc.ref, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if got != resolved {
				t.Errorf("digest = %q", got)
			}
			if seen.Load() != want {
				t.Errorf("requested %v, want %v", seen.Load(), want)
			}
		})
	}
}

func TestResolveTagAnswersABearerChallenge(t *testing.T) {
	t.Parallel()
	var tokenHits atomic.Int32
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		tokenHits.Add(1)
		_, _ = w.Write([]byte(`{"token":"t0k","expires_in":300}`))
	})
	mux.HandleFunc("/v2/acme/tool/manifests/v1", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer t0k" {
			w.Header().Set("Www-Authenticate",
				`Bearer realm="`+srv.URL+`/token",service="reg",scope="repository:acme/tool:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Docker-Content-Digest", resolved)
	})
	got, err := oci.ResolveTag(t.Context(), strings.TrimPrefix(srv.URL, "http://")+"/acme/tool:v1",
		oci.Config{PlainHTTP: true, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if got != resolved || tokenHits.Load() != 1 {
		t.Errorf("got %q after %d token fetches", got, tokenHits.Load())
	}
}

func TestResolveTagNotFound(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	ref := strings.TrimPrefix(srv.URL, "http://") + "/acme/tool:gone"
	_, err := oci.ResolveTag(t.Context(), ref, oci.Config{PlainHTTP: true, Timeout: 5 * time.Second})
	if err == nil || err.Error() != "oci: "+ref+": not found" {
		t.Fatalf("got %v", err)
	}
	if !errors.Is(err, oci.ErrNotFound) {
		t.Errorf("%v does not match ErrNotFound", err)
	}
}

func TestResolveTagWithoutADigestHeaderIsAnError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	_, err := oci.ResolveTag(t.Context(), strings.TrimPrefix(srv.URL, "http://")+"/a/b:c",
		oci.Config{PlainHTTP: true, Timeout: 5 * time.Second})
	if err == nil {
		t.Fatal("a missing Docker-Content-Digest was accepted")
	}
}
