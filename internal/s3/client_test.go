package s3_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/s3"
)

// TestMain stops the shared object store once every test in this binary has
// finished with it. See dockertest.RunMain.
func TestMain(m *testing.M) { os.Exit(dockertest.RunMain(m)) }

// live returns a client pointed at a real MinIO, in a bucket nobody else in
// this binary is using.
func live(t *testing.T) *s3.Client {
	t.Helper()
	m := dockertest.RequireMinIO(t)
	c, err := s3.New(s3.Config{
		Endpoint:        m.Endpoint,
		Region:          m.Region,
		Bucket:          m.Bucket,
		AccessKeyID:     m.AccessKey,
		SecretAccessKey: m.SecretKey,
		PathStyle:       true,
		Timeout:         30 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	return c
}

func TestPutThenGetReturnsTheSameBytes(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	want := []byte("the bytes a cache would have stored\n")
	if err := c.PutBytes(ctx, "round/trip", want); err != nil {
		t.Fatalf("PutBytes: %v", err)
	}

	rc, err := c.Get(ctx, "round/trip")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Get returned %q, want %q", got, want)
	}

	size, ok, err := c.Head(ctx, "round/trip")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if !ok {
		t.Fatal("Head says an object that was just stored is not there")
	}
	if size != int64(len(want)) {
		t.Errorf("Head size = %d, want %d", size, len(want))
	}
}

// TestKeysWithCharactersThatHaveToBeEncodedRoundTrip checks the signer's
// path encoding against what a server actually decodes; a mismatch fails
// SignatureDoesNotMatch, which reads like a credentials problem.
func TestKeysWithCharactersThatHaveToBeEncodedRoundTrip(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	for _, key := range []string{
		"plain/key",
		"with space/and more",
		"with+plus",
		"with=equals",
		"with~tilde-and.dots_here",
		"with%25percent",
		"unicode/café",
		"parens(1)",
	} {
		want := []byte("value for " + key)
		if err := c.PutBytes(ctx, key, want); err != nil {
			t.Errorf("PutBytes(%q): %v", key, err)
			continue
		}
		rc, err := c.Get(ctx, key)
		if err != nil {
			t.Errorf("Get(%q): %v", key, err)
			continue
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Errorf("reading %q: %v", key, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestAMissingObjectIsNotFoundRatherThanAnError(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	if _, err := c.Get(ctx, "never/stored"); !errors.Is(err, s3.ErrNotFound) {
		t.Errorf("Get of a missing key = %v, want ErrNotFound", err)
	}
	size, ok, err := c.Head(ctx, "never/stored")
	if err != nil {
		t.Errorf("Head of a missing key returned an error: %v", err)
	}
	if ok || size != 0 {
		t.Errorf("Head of a missing key = (%d, %v), want (0, false)", size, ok)
	}
}

// TestAWrongSecretIsDeniedRatherThanNotFound: a misconfigured credential
// that read as "the cache is empty" would never get fixed.
func TestAWrongSecretIsDeniedRatherThanNotFound(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	good, err := s3.New(s3.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
		PathStyle: true, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	if err := good.PutBytes(ctx, "denied/probe", []byte("x")); err != nil {
		t.Fatalf("seeding an object with valid credentials: %v", err)
	}

	bad, err := s3.New(s3.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: "not-the-right-secret-at-all",
		PathStyle: true, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}

	_, err = bad.Get(ctx, "denied/probe")
	if !errors.Is(err, s3.ErrDenied) {
		t.Errorf("Get with a wrong secret = %v, want ErrDenied", err)
	}
	if errors.Is(err, s3.ErrNotFound) {
		t.Error("a permission failure was reported as a cache miss, which would never get diagnosed")
	}
	if err := bad.PutBytes(ctx, "denied/write", []byte("x")); !errors.Is(err, s3.ErrDenied) {
		t.Errorf("Put with a wrong secret = %v, want ErrDenied", err)
	}
	if _, _, err := bad.Head(ctx, "denied/probe"); !errors.Is(err, s3.ErrDenied) {
		t.Errorf("Head with a wrong secret = %v, want ErrDenied", err)
	}
}

// TestAMissingBucketIsAnErrorRatherThanAMiss: same argument as the wrong
// secret. A typo in the bucket name has to be visible.
func TestAMissingBucketIsAnErrorRatherThanAMiss(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	c, err := s3.New(s3.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: "no-such-bucket-anywhere",
		AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
		PathStyle: true, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	_, err = c.Get(t.Context(), "anything")
	if err == nil {
		t.Fatal("Get against a bucket that does not exist succeeded")
	}
	if errors.Is(err, s3.ErrNotFound) {
		t.Errorf("a missing bucket was reported as a missing object: %v", err)
	}
	if !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Errorf("the error does not name the problem: %v", err)
	}
}

// TestAnUnreachableEndpointFailsPromptly covers the case a run has to survive
// without stalling: nothing is listening.
func TestAnUnreachableEndpointFailsPromptly(t *testing.T) {
	t.Parallel()
	// Port 1 on loopback: never in use, refused immediately rather than
	// timing out like a filtered address.
	c, err := s3.New(s3.Config{
		Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "b",
		AccessKeyID: "AKIA", SecretAccessKey: "secret",
		PathStyle: true, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}

	start := time.Now()
	_, err = c.Get(t.Context(), "k")
	if err == nil {
		t.Fatal("Get against an endpoint with nothing listening succeeded")
	}
	if errors.Is(err, s3.ErrNotFound) {
		t.Errorf("an unreachable endpoint was reported as a missing object: %v", err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("Get took %v to give up on a refused connection", d)
	}
}

// TestConcurrentPutsOfTheSameKeyAllSucceed is the property two machines
// finishing the same step at the same moment depend on.
func TestConcurrentPutsOfTheSameKeyAllSucceed(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	const writers = 8
	body := bytes.Repeat([]byte("racing writers\n"), 4096)

	clients := make([]*s3.Client, writers)
	for i := range clients {
		c, err := s3.New(s3.Config{
			Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
			AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
			PathStyle: true, Timeout: 60 * time.Second,
		})
		if err != nil {
			t.Fatalf("s3.New: %v", err)
		}
		clients[i] = c
	}

	var start sync.WaitGroup
	start.Add(1)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			errs[i] = clients[i].PutBytes(ctx, "race/same-key", body)
		}()
	}
	start.Done()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("writer %d: %v", i, err)
		}
	}

	rc, err := clients[0].Get(ctx, "race/same-key")
	if err != nil {
		t.Fatalf("Get after the race: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("after %d concurrent identical writes the object is %d bytes, want %d",
			writers, len(got), len(body))
	}
}

func TestAnObjectLargerThanOneBufferRoundTrips(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	want := make([]byte, 5<<20)
	for i := range want {
		want[i] = byte(i * 7)
	}
	if err := c.Put(ctx, "big/object", bytes.NewReader(want), int64(len(want))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := c.Get(ctx, "big/object")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("round trip of %d bytes came back as %d and differs", len(want), len(got))
	}
}

// TestErrorsNeverCarryTheSecretKey. An error from here is going into an event
// stream, a log file and quite possibly a CI transcript.
func TestErrorsNeverCarryTheSecretKey(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	const secret = "a-very-recognisable-secret-value"

	c, err := s3.New(s3.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: secret,
		PathStyle: true, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	_, err = c.Get(t.Context(), "whatever")
	if err == nil {
		t.Fatal("a wrong secret was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("the error carries the secret access key: %v", err)
	}
	if strings.Contains(fmt.Sprintf("%+v", err), secret) {
		t.Fatalf("the verbose form of the error carries the secret access key: %+v", err)
	}
}

// TestStringNamesTheStoreWithoutCredentials: this string ends up in the
// message a degraded run prints.
func TestStringNamesTheStoreWithoutCredentials(t *testing.T) {
	t.Parallel()
	c, err := s3.New(s3.Config{
		Endpoint: "https://s3.eu-west-1.amazonaws.com", Region: "eu-west-1", Bucket: "team-cache",
		AccessKeyID: "AKIAEXAMPLE", SecretAccessKey: "sh-secret",
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	got := c.String()
	for _, want := range []string{"team-cache", "s3.eu-west-1.amazonaws.com"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, want it to name %q", got, want)
		}
	}
	for _, forbidden := range []string{"AKIAEXAMPLE", "sh-secret"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("String() = %q, which leaks %q", got, forbidden)
		}
	}
}

func TestNewRefusesAConfigItCannotUse(t *testing.T) {
	t.Parallel()
	base := s3.Config{
		Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "b",
		AccessKeyID: "AKIA", SecretAccessKey: "s",
	}
	for name, mutate := range map[string]func(*s3.Config){
		"no endpoint":   func(c *s3.Config) { c.Endpoint = "" },
		"no bucket":     func(c *s3.Config) { c.Bucket = "" },
		"no region":     func(c *s3.Config) { c.Region = "" },
		"no access key": func(c *s3.Config) { c.AccessKeyID = "" },
		"no secret":     func(c *s3.Config) { c.SecretAccessKey = "" },
		"bad scheme":    func(c *s3.Config) { c.Endpoint = "ftp://s3.example.com" },
		"not a url":     func(c *s3.Config) { c.Endpoint = "://" },
		// Credentials in the endpoint would travel into every error message
		// and every event this package's callers emit.
		"userinfo in the endpoint": func(c *s3.Config) { c.Endpoint = "https://key:secret@s3.example.com" },
	} {
		cfg := base
		mutate(&cfg)
		if _, err := s3.New(cfg); err == nil {
			t.Errorf("New accepted a config with %s", name)
		}
	}
}

func TestNewAcceptsAConfigItCanUse(t *testing.T) {
	t.Parallel()
	if _, err := s3.New(s3.Config{
		Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "b",
		AccessKeyID: "AKIA", SecretAccessKey: "s", SessionToken: "tok",
	}); err != nil {
		t.Errorf("New rejected a usable config: %v", err)
	}
}

// TestKeyURLPutsTheBucketWhereTheStyleSaysItGoes. Path style is what every
// self-hosted and most non-Amazon S3 services need; virtual-host style is
// what Amazon now expects.
func TestKeyURLPutsTheBucketWhereTheStyleSaysItGoes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		pathStyle bool
		want      string
	}{
		{"path style", true, "https://s3.example.com/team-cache/senro/cas/aa"},
		{"virtual host style", false, "https://team-cache.s3.example.com/senro/cas/aa"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := s3.New(s3.Config{
				Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "team-cache",
				AccessKeyID: "AKIA", SecretAccessKey: "s", PathStyle: tc.pathStyle,
			})
			if err != nil {
				t.Fatalf("s3.New: %v", err)
			}
			if got := c.KeyURL("senro/cas/aa"); got != tc.want {
				t.Errorf("KeyURL = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAShortSecretDoesNotMangleEveryMessage: a one-character "secret" would
// replace that letter everywhere and turn a diagnosable error into
// nonsense. Follows internal/redact's MinLength ruling: below it, no scrub.
func TestAShortSecretDoesNotMangleEveryMessage(t *testing.T) {
	t.Parallel()
	c, err := s3.New(s3.Config{
		Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "team-cache",
		AccessKeyID: "k", SecretAccessKey: "s", PathStyle: true, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	_, err = c.Get(t.Context(), "senro/v1/cas/sha256/aa/bb/cc")
	if err == nil {
		t.Fatal("Get against an endpoint with nothing listening succeeded")
	}
	for _, want := range []string{"team-cache", "senro/v1/cas", "connection refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error no longer says %q, so a short secret has mangled it: %v", want, err)
		}
	}
}

// TestASecretLongEnoughToBeOneIsStillScrubbed is the other half: the rule
// above must not become a hole.
func TestASecretLongEnoughToBeOneIsStillScrubbed(t *testing.T) {
	t.Parallel()
	const secret = "an-actual-looking-secret-value"
	c, err := s3.New(s3.Config{
		// No path in this package puts the secret in a URL; this checks the
		// mechanism is wired, not a leak that exists.
		Endpoint: "http://127.0.0.1:1/" + secret, Region: "us-east-1", Bucket: "b",
		AccessKeyID: "k", SecretAccessKey: secret, PathStyle: true, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	_, err = c.Get(t.Context(), "k")
	if err == nil {
		t.Fatal("Get against an endpoint with nothing listening succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("a credential long enough to be a real one was not scrubbed: %v", err)
	}
}
