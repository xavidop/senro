package oci_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/oci"
)

// TestMain stops the shared registry once every test in this binary has
// finished with it. See dockertest.RunMain.
func TestMain(m *testing.M) { os.Exit(dockertest.RunMain(m)) }

// live returns a client pointed at a real registry, in a repository nobody
// else in this binary is using.
func live(t *testing.T) *oci.Client {
	t.Helper()
	r := dockertest.RequireRegistry(t)
	return clientFor(t, r, r.Username, r.Password)
}

func clientFor(t *testing.T, r dockertest.Registry, user, password string) *oci.Client {
	t.Helper()
	c, err := oci.New(oci.Config{
		Registry:   r.Host,
		Repository: r.Repository,
		Username:   user,
		Password:   password,
		PlainHTTP:  true,
		Timeout:    30 * time.Second,
	})
	if err != nil {
		t.Fatalf("oci.New: %v", err)
	}
	return c
}

func digestOf(b []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(b))
}

func TestPushThenPullReturnsTheSameBlob(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	want := []byte("the bytes a cache would have stored\n")
	d := digestOf(want)
	if err := c.PutBlob(ctx, d, bytes.NewReader(want), int64(len(want))); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	rc, err := c.GetBlob(ctx, d)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the blob: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("GetBlob returned %q, want %q", got, want)
	}

	size, ok, err := c.HasBlob(ctx, d)
	if err != nil {
		t.Fatalf("HasBlob: %v", err)
	}
	if !ok {
		t.Fatal("HasBlob says a blob that was just pushed is not there")
	}
	if size != int64(len(want)) {
		t.Errorf("HasBlob size = %d, want %d", size, len(want))
	}
}

// TestPushingTheSameBlobTwiceIsNotAnError pins the property a cache rests on:
// two machines that finished the same step both push, and neither fails.
func TestPushingTheSameBlobTwiceIsNotAnError(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	body := []byte("stored twice, deliberately")
	d := digestOf(body)
	for i := range 2 {
		if err := c.PutBlob(ctx, d, bytes.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("PutBlob %d: %v", i+1, err)
		}
	}
}

func TestAMissingBlobIsReportedAsNotFound(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	absent := digestOf([]byte("nothing ever stored this"))
	if _, err := c.GetBlob(ctx, absent); !errors.Is(err, oci.ErrNotFound) {
		t.Errorf("GetBlob of an absent blob = %v, want oci.ErrNotFound", err)
	}
	size, ok, err := c.HasBlob(ctx, absent)
	if err != nil {
		t.Fatalf("HasBlob: %v", err)
	}
	if ok {
		t.Errorf("HasBlob says an absent blob is there, at %d bytes", size)
	}
}

// TestTheRegistryRefusesABlobThatIsNotItsDigest is the check that senro sends
// the digest at all and that something other than senro is verifying it. If
// this ever passes, the upload path stopped naming what it was uploading.
func TestTheRegistryRefusesABlobThatIsNotItsDigest(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	body := []byte("these bytes are not what the digest says")
	lie := digestOf([]byte("something else entirely"))
	err := c.PutBlob(ctx, lie, bytes.NewReader(body), int64(len(body)))
	if err == nil {
		t.Fatal("the registry accepted a blob whose bytes are not its digest")
	}
	if errors.Is(err, oci.ErrNotFound) {
		t.Errorf("a rejected upload was reported as a miss: %v", err)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	layer := []byte("a layer the manifest will point at")
	layerDigest := digestOf(layer)
	if err := c.PutBlob(ctx, layerDigest, bytes.NewReader(layer), int64(len(layer))); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	config := []byte("{}")
	configDigest := digestOf(config)
	if err := c.PutBlob(ctx, configDigest, bytes.NewReader(config), int64(len(config))); err != nil {
		t.Fatalf("PutBlob (config): %v", err)
	}

	manifest := fmt.Appendf(nil, `{"schemaVersion":2,`+
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},`+
		`"layers":[{"mediaType":"application/octet-stream","digest":%q,"size":%d}]}`,
		configDigest, len(config), layerDigest, len(layer))

	const tag = "senro-manifest-round-trip"
	if err := c.PutManifest(ctx, tag, oci.MediaTypeImageManifest, manifest); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	got, err := c.GetManifest(ctx, tag)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if !bytes.Equal(got, manifest) {
		t.Errorf("GetManifest returned\n%s\nwant\n%s", got, manifest)
	}
	ok, err := c.HasManifest(ctx, tag)
	if err != nil {
		t.Fatalf("HasManifest: %v", err)
	}
	if !ok {
		t.Error("HasManifest says a manifest that was just pushed is not there")
	}
}

func TestAMissingManifestIsReportedAsNotFound(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	if _, err := c.GetManifest(ctx, "senro-never-pushed"); !errors.Is(err, oci.ErrNotFound) {
		t.Errorf("GetManifest of an absent tag = %v, want oci.ErrNotFound", err)
	}
	ok, err := c.HasManifest(ctx, "senro-never-pushed")
	if err != nil {
		t.Fatalf("HasManifest: %v", err)
	}
	if ok {
		t.Error("HasManifest says an absent manifest is there")
	}
}

// TestTheRegistryRefusesAManifestWhoseBlobIsMissing pins that the registry,
// not senro, is the thing being tested against: a manifest naming a blob
// nobody pushed is refused by it.
func TestTheRegistryRefusesAManifestWhoseBlobIsMissing(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	absent := digestOf([]byte("a layer that was never pushed"))
	manifest := fmt.Appendf(nil, `{"schemaVersion":2,`+
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":30},`+
		`"layers":[{"mediaType":"application/octet-stream","digest":%q,"size":30}]}`, absent, absent)

	if err := c.PutManifest(ctx, "senro-dangling", oci.MediaTypeImageManifest, manifest); err == nil {
		t.Fatal("the registry accepted a manifest naming a blob it does not hold")
	}
}

// TestACredentialThatCannotPushIsDenied uses a real refusal from the registry:
// the token it issues for a read-only user grants pull and nothing else, so
// the push is rejected by the registry's own scope check rather than by
// anything written here.
func TestACredentialThatCannotPushIsDenied(t *testing.T) {
	t.Parallel()
	r := dockertest.RequireRegistry(t)
	ctx := t.Context()

	writer := clientFor(t, r, r.Username, r.Password)
	body := []byte("readable by everyone, writable by one")
	d := digestOf(body)
	if err := writer.PutBlob(ctx, d, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("PutBlob as the writing user: %v", err)
	}

	reader := clientFor(t, r, r.ReadOnlyUsername, r.ReadOnlyPassword)
	rc, err := reader.GetBlob(ctx, d)
	if err != nil {
		t.Fatalf("GetBlob as the reading user: %v", err)
	}
	_ = rc.Close()

	other := []byte("a push the reading user must not be allowed to make")
	err = reader.PutBlob(ctx, digestOf(other), bytes.NewReader(other), int64(len(other)))
	if !errors.Is(err, oci.ErrDenied) {
		t.Errorf("PutBlob with a read-only credential = %v, want oci.ErrDenied", err)
	}
}

func TestAWrongPasswordIsDenied(t *testing.T) {
	t.Parallel()
	r := dockertest.RequireRegistry(t)
	c := clientFor(t, r, r.Username, "not the password")
	ctx := t.Context()

	_, _, err := c.HasBlob(ctx, digestOf([]byte("anything")))
	if !errors.Is(err, oci.ErrDenied) {
		t.Errorf("HasBlob with a wrong password = %v, want oci.ErrDenied", err)
	}
	if err != nil && strings.Contains(err.Error(), "not the password") {
		t.Errorf("the password is in the error text: %v", err)
	}
}

// TestAnAnonymousClientIsDenied pins that the registry these tests run against
// really does demand credentials. A suite that passed against an open
// registry would prove nothing about the authentication senro implements.
func TestAnAnonymousClientIsDenied(t *testing.T) {
	t.Parallel()
	r := dockertest.RequireRegistry(t)
	c := clientFor(t, r, "", "")
	ctx := t.Context()

	body := []byte("anonymous push")
	err := c.PutBlob(t.Context(), digestOf(body), bytes.NewReader(body), int64(len(body)))
	if !errors.Is(err, oci.ErrDenied) {
		t.Errorf("PutBlob with no credentials = %v, want oci.ErrDenied", err)
	}
	if _, _, err := c.HasBlob(ctx, digestOf(body)); !errors.Is(err, oci.ErrDenied) {
		t.Errorf("HasBlob with no credentials = %v, want oci.ErrDenied", err)
	}
}

// TestConcurrentPushesOfOneBlobAllSucceed is the case two runners finishing
// the same step at the same moment produce.
func TestConcurrentPushesOfOneBlobAllSucceed(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	// Large enough that the uploads genuinely overlap rather than finishing
	// one after another inside the loop that starts them.
	body := bytes.Repeat([]byte("senro concurrent upload payload\n"), 40000)
	d := digestOf(body)

	const racers = 8
	errs := make([]error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = c.PutBlob(ctx, d, bytes.NewReader(body), int64(len(body)))
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent PutBlob %d: %v", i, err)
		}
	}
	rc, err := c.GetBlob(ctx, d)
	if err != nil {
		t.Fatalf("GetBlob after the race: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the blob after the race: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("after %d concurrent pushes the blob is %d bytes, want %d",
			racers, len(got), len(body))
	}
}

// TestAnUnreachableRegistryIsAnErrorAndNotAMiss matters because the caller
// above this one turns a miss into "run the step" and an error into "the
// cache is down". A refused connection reported as a miss would leave a
// broken configuration looking like a cache that is merely cold.
func TestAnUnreachableRegistryIsAnErrorAndNotAMiss(t *testing.T) {
	t.Parallel()
	c, err := oci.New(oci.Config{
		// Port 1 on loopback: nothing is listening, and the connection is
		// refused immediately rather than timing out.
		Registry:   "127.0.0.1:1",
		Repository: "senro/cache",
		Username:   "user",
		Password:   "password",
		PlainHTTP:  true,
		Timeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("oci.New: %v", err)
	}
	ctx := t.Context()

	if _, _, err := c.HasBlob(ctx, digestOf([]byte("x"))); err == nil {
		t.Error("HasBlob against an unreachable registry returned no error")
	} else if errors.Is(err, oci.ErrNotFound) {
		t.Errorf("an unreachable registry was reported as a miss: %v", err)
	}
	if _, err := c.GetBlob(ctx, digestOf([]byte("x"))); err == nil {
		t.Error("GetBlob against an unreachable registry returned no error")
	} else if errors.Is(err, oci.ErrNotFound) {
		t.Errorf("an unreachable registry was reported as a miss: %v", err)
	}
}

// TestAnUnsupportedChallengeIsRefusedByName is the whole of senro's position
// on registry authentication: one flow, done properly, and everything else
// refused in a message that says what was asked for. A server is stubbed here
// rather than run because no real registry can be asked to demand Negotiate
// on request, and what is under test is senro's own refusal.
func TestAnUnsupportedChallengeIsRefusedByName(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		challenge string
		want      string
		// kind is the sentinel the refusal has to match. A 401 carrying no
		// challenge at all is a denial rather than an unsupported scheme:
		// there is no scheme in it to support, and what the operator needs to
		// hear is that the registry refused them, not that senro is missing a
		// feature.
		kind error
	}{
		{"basic", `Basic realm="registry"`, "Basic", oci.ErrUnsupportedAuth},
		{"negotiate", "Negotiate", "Negotiate", oci.ErrUnsupportedAuth},
		{"bearer with no realm", "Bearer service=\"x\"", "no realm", oci.ErrUnsupportedAuth},
		{"none", "", "no WWW-Authenticate", oci.ErrDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.challenge != "" {
					w.Header().Set("Www-Authenticate", tc.challenge)
				}
				w.WriteHeader(http.StatusUnauthorized)
			}))
			defer srv.Close()

			c, err := oci.New(oci.Config{
				Registry:   strings.TrimPrefix(srv.URL, "http://"),
				Repository: "senro/cache",
				Username:   "user",
				Password:   "password",
				PlainHTTP:  true,
				Timeout:    5 * time.Second,
			})
			if err != nil {
				t.Fatalf("oci.New: %v", err)
			}
			_, _, err = c.HasBlob(t.Context(), digestOf([]byte("x")))
			if err == nil {
				t.Fatal("a challenge senro does not implement was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name what was asked for.\ngot:  %v\nwant it to mention %q",
					err, tc.want)
			}
			if !errors.Is(err, tc.kind) {
				t.Errorf("%v does not match %v", err, tc.kind)
			}
		})
	}
}

// TestNewRefusesAConfigurationThatCannotWork keeps the mistakes somebody
// typed separate from the conditions of the network: these are reported at
// startup, where the caller fails the run, rather than as a cache that is
// mysteriously always cold.
func TestNewRefusesAConfigurationThatCannotWork(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cfg  oci.Config
		want string
	}{
		{"no registry", oci.Config{Repository: "senro/cache"}, "no registry"},
		{"no repository", oci.Config{Registry: "registry.example.com"}, "no repository"},
		{
			"a URL rather than a host",
			oci.Config{Registry: "https://registry.example.com", Repository: "senro/cache"},
			"host",
		},
		{
			"credentials in the registry",
			oci.Config{Registry: "user:pass@registry.example.com", Repository: "senro/cache"},
			"username or password",
		},
		{
			"an uppercase repository",
			oci.Config{Registry: "registry.example.com", Repository: "Senro/Cache"},
			"repository",
		},
		{
			"a repository that walks out of itself",
			oci.Config{Registry: "registry.example.com", Repository: "senro/../../etc"},
			"repository",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := oci.New(tc.cfg)
			if err == nil {
				t.Fatalf("oci.New(%+v) was accepted", tc.cfg)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("oci.New: %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestStringNamesTheRegistryWithoutTheCredentials pins what a degraded run
// prints. It goes in front of whoever reads a CI log.
func TestStringNamesTheRegistryWithoutTheCredentials(t *testing.T) {
	t.Parallel()
	c, err := oci.New(oci.Config{
		Registry:   "registry.example.com",
		Repository: "acme/senro-cache",
		Username:   "robot$senro",
		Password:   "a-very-secret-registry-password",
	})
	if err != nil {
		t.Fatalf("oci.New: %v", err)
	}
	got := c.String()
	if !strings.Contains(got, "acme/senro-cache") || !strings.Contains(got, "registry.example.com") {
		t.Errorf("String() = %q, want it to name the repository and the registry", got)
	}
	if strings.Contains(got, "a-very-secret-registry-password") {
		t.Errorf("String() = %q, and the password is in it", got)
	}
}

// TestADigestThatIsNotADigestIsNeverPutInAURL closes the path traversal an
// address from an event log or a command line would otherwise open.
func TestADigestThatIsNotADigestIsNeverPutInAURL(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	for _, bad := range []string{
		"",
		"sha256:../../v2/",
		"sha256:not hex",
		"sha512:" + strings.Repeat("a", 64),
		strings.Repeat("a", 64),
	} {
		if _, err := c.GetBlob(ctx, bad); err == nil {
			t.Errorf("GetBlob(%q) was accepted", bad)
		}
		if _, ok, err := c.HasBlob(ctx, bad); ok || err == nil {
			t.Errorf("HasBlob(%q) = %v, %v, want a refusal", bad, ok, err)
		}
	}
}
