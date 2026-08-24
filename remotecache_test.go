package senro_test

import (
	"errors"

	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/remotecache"
)

func TestRemoteCacheFromEnvIsAbsentUntilItIsAskedFor(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	got, ok, err := senro.RemoteCacheFromEnv()
	if err != nil {
		t.Fatalf("RemoteCacheFromEnv: %v", err)
	}
	if ok {
		t.Errorf("a machine with nothing configured has a remote cache: %+v", got)
	}
}

func TestRemoteCacheFromEnvReadsAWholeConfiguration(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "s3://team-cache/pipelines")
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", "https://s3.eu-west-1.amazonaws.com")
	t.Setenv("SENRO_REMOTE_CACHE_REGION", "eu-west-1")
	t.Setenv("SENRO_REMOTE_CACHE_TIMEOUT", "45s")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "token")

	got, ok, err := senro.RemoteCacheFromEnv()
	if err != nil {
		t.Fatalf("RemoteCacheFromEnv: %v", err)
	}
	if !ok {
		t.Fatal("RemoteCacheFromEnv found nothing with a full configuration set")
	}
	want := senro.RemoteCache{
		Endpoint: "https://s3.eu-west-1.amazonaws.com", Region: "eu-west-1",
		Bucket: "team-cache", Prefix: "pipelines",
		AccessKeyID: "AKIAEXAMPLE", SecretAccessKey: "secret", SessionToken: "token",
		Timeout: 45 * time.Second,
	}
	if got != want {
		t.Errorf("RemoteCacheFromEnv =\n  %+v\nwant\n  %+v", got, want)
	}
}

// TestRemoteCacheFromEnvTakesTheStandardCredentialNames. CI already puts an
// assumed role's credentials in these three, so a senro-specific spelling
// would mean every pipeline copying them across for no reason.
func TestRemoteCacheFromEnvTakesTheStandardCredentialNames(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "s3://b")
	t.Setenv("SENRO_REMOTE_CACHE_REGION", "us-east-1")
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", "https://s3.us-east-1.amazonaws.com")
	t.Setenv("AWS_ACCESS_KEY_ID", "from-aws-vars")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "also-from-aws-vars")

	got, _, err := senro.RemoteCacheFromEnv()
	if err != nil {
		t.Fatalf("RemoteCacheFromEnv: %v", err)
	}
	if got.AccessKeyID != "from-aws-vars" || got.SecretAccessKey != "also-from-aws-vars" {
		t.Errorf("the standard AWS credential variables were not read: %+v", got)
	}
}

func TestRemoteCacheFromEnvReadsReadOnlyAndPathStyle(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "s3://b/p")
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", "http://minio.internal:9000")
	t.Setenv("SENRO_REMOTE_CACHE_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	t.Setenv("SENRO_REMOTE_CACHE_READ_ONLY", "1")
	t.Setenv("SENRO_REMOTE_CACHE_PATH_STYLE", "0")

	got, _, err := senro.RemoteCacheFromEnv()
	if err != nil {
		t.Fatalf("RemoteCacheFromEnv: %v", err)
	}
	if !got.ReadOnly {
		t.Error("SENRO_REMOTE_CACHE_READ_ONLY=1 did not produce a read-only cache")
	}
	if got.PathStyle == nil || *got.PathStyle {
		t.Errorf("SENRO_REMOTE_CACHE_PATH_STYLE=0 did not turn path style off: %v", got.PathStyle)
	}
}

// TestRemoteCacheFromEnvRefusesAHalfConfiguration. A cache that is quietly
// not there is the failure mode this whole feature has to avoid: somebody
// sets three of the four variables, sees no error, and spends a week
// wondering why CI never hits.
func TestRemoteCacheFromEnvRefusesAHalfConfiguration(t *testing.T) {
	base := map[string]string{
		"SENRO_REMOTE_CACHE":          "s3://b/p",
		"SENRO_REMOTE_CACHE_ENDPOINT": "https://s3.us-east-1.amazonaws.com",
		"SENRO_REMOTE_CACHE_REGION":   "us-east-1",
		"AWS_ACCESS_KEY_ID":           "k",
		"AWS_SECRET_ACCESS_KEY":       "s",
	}
	for missing := range base {
		if missing == "SENRO_REMOTE_CACHE" {
			continue // absent means "no remote cache", which is not an error
		}
		t.Run("without "+missing, func(t *testing.T) {
			for _, name := range remoteCacheEnvNames() {
				t.Setenv(name, "")
			}
			for k, v := range base {
				if k != missing {
					t.Setenv(k, v)
				}
			}
			_, _, err := senro.RemoteCacheFromEnv()
			if err == nil {
				t.Fatalf("a configuration with no %s was accepted; the cache would silently "+
					"never be used", missing)
			}
			if !strings.Contains(err.Error(), missing) {
				t.Errorf("the error does not name the variable that is missing: %v", err)
			}
		})
	}
}

func TestRemoteCacheFromEnvRefusesAnUnusableURL(t *testing.T) {
	for name, value := range map[string]string{
		"not a URL at all":       "team-cache",
		"an unknown scheme":      "gs://team-cache",
		"no bucket":              "s3://",
		"credentials in the URL": "s3://key:secret@team-cache/p",
	} {
		t.Run(name, func(t *testing.T) {
			for _, n := range remoteCacheEnvNames() {
				t.Setenv(n, "")
			}
			t.Setenv("SENRO_REMOTE_CACHE", value)
			t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", "https://s3.us-east-1.amazonaws.com")
			t.Setenv("SENRO_REMOTE_CACHE_REGION", "us-east-1")
			t.Setenv("AWS_ACCESS_KEY_ID", "k")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
			if _, _, err := senro.RemoteCacheFromEnv(); err == nil {
				t.Errorf("SENRO_REMOTE_CACHE=%q was accepted", value)
			}
		})
	}
}

// TestARemoteCacheThatCannotBeOpenedFailsTheRunUpFront. This is the one
// remote-cache problem that SHOULD stop a run: the operator wrote something
// that cannot work, and telling them "your cache is down" would send them
// looking in the wrong place.
func TestARemoteCacheThatCannotBeOpenedFailsTheRunUpFront(t *testing.T) {
	pipe := senro.New("remote-cache-bad-config")
	pipe.Workflow("main").Step("noop", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	err = senro.RunPlan(t.Context(), p,
		senro.WithDir(t.TempDir()),
		senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			// No bucket: unusable, and no amount of network would fix it.
			Endpoint: "https://s3.us-east-1.amazonaws.com", Region: "us-east-1",
			AccessKeyID: "k", SecretAccessKey: "s",
		}),
	)
	if err == nil {
		t.Fatal("a remote cache config that cannot possibly work was accepted")
	}
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Errorf("a configuration mistake was reported as a run failure rather than a "+
			"refusal before the run: %v", err)
	}
}

// TestARunWhoseRemoteCacheIsDownStillSucceeds is the headline rule, end to
// end through the public API: nothing is listening, and the pipeline runs.
func TestARunWhoseRemoteCacheIsDownStillSucceeds(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("remote-cache-down")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("build", exec.Command("sh", "-c", "wc -c main.go > out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	pathStyle := true
	dir := t.TempDir()
	err = senro.RunPlan(t.Context(), p,
		senro.WithDir(dir),
		senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "team-cache",
			AccessKeyID: "k", SecretAccessKey: "s",
			PathStyle: &pathStyle, Timeout: 2 * time.Second,
		}),
	)
	if err != nil {
		t.Fatalf("a run whose remote cache is unreachable failed: %v", err)
	}

	// And it said so, in the ledger, exactly once. readLedger is
	// notify_e2e_test.go's helper: an event is only real once it is on disk,
	// which is precisely the claim being made here.
	if n := len(eventsOfType(readLedger(t, dir), api.CacheDegraded)); n != 1 {
		t.Errorf("the run recorded %d cache.degraded events, want exactly 1", n)
	}
}

func remoteCacheEnvNames() []string {
	return []string{
		"SENRO_REMOTE_CACHE",
		"SENRO_REMOTE_CACHE_ENDPOINT",
		"SENRO_REMOTE_CACHE_REGION",
		"SENRO_REMOTE_CACHE_TIMEOUT",
		"SENRO_REMOTE_CACHE_READ_ONLY",
		"SENRO_REMOTE_CACHE_PATH_STYLE",
		"SENRO_REMOTE_CACHE_USERNAME",
		"SENRO_REMOTE_CACHE_PASSWORD",
		"SENRO_REMOTE_CACHE_PLAIN_HTTP",
		"SENRO_REMOTE_SCRATCH",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
	}
}

// TestTheEnvironmentNameListIsTheOneThePackageDeclares. The list above is what
// every test in this file clears, and a variable that is read but not on it
// would leave a developer's own exported configuration leaking into the suite.
func TestTheEnvironmentNameListIsTheOneThePackageDeclares(t *testing.T) {
	declared := remotecache.EnvNames()
	if len(declared) != len(remoteCacheEnvNames()) {
		t.Fatalf("remotecache.EnvNames() has %d names and this file clears %d:\n  %v\n  %v",
			len(declared), len(remoteCacheEnvNames()), declared, remoteCacheEnvNames())
	}
	have := make(map[string]bool, len(remoteCacheEnvNames()))
	for _, n := range remoteCacheEnvNames() {
		have[n] = true
	}
	for _, n := range declared {
		if !have[n] {
			t.Errorf("remotecache.EnvNames() declares %s and this file does not clear it", n)
		}
	}
}

// --- a registry target -------------------------------------------------------

// TestRemoteCacheFromEnvReadsARegistryTarget is the configuration change the
// registry backend exists to make possible: one variable, not a code change.
func TestRemoteCacheFromEnvReadsARegistryTarget(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "oci://ghcr.io/acme/senro-cache")
	t.Setenv("SENRO_REMOTE_CACHE_USERNAME", "x-access-token")
	t.Setenv("SENRO_REMOTE_CACHE_PASSWORD", "a-personal-access-token")
	t.Setenv("SENRO_REMOTE_CACHE_TIMEOUT", "45s")
	t.Setenv("SENRO_REMOTE_CACHE_READ_ONLY", "1")

	got, ok, err := senro.RemoteCacheFromEnv()
	if err != nil {
		t.Fatalf("RemoteCacheFromEnv: %v", err)
	}
	if !ok {
		t.Fatal("RemoteCacheFromEnv found nothing with a registry target set")
	}
	want := senro.RemoteCache{
		Registry: senro.RegistryCache{
			Host: "ghcr.io", Repository: "acme/senro-cache",
			Username: "x-access-token", Password: "a-personal-access-token",
		},
		Timeout: 45 * time.Second, ReadOnly: true,
	}
	if got != want {
		t.Errorf("RemoteCacheFromEnv =\n  %+v\nwant\n  %+v", got, want)
	}
}

// TestARegistryTargetKeepsItsPort. A self-hosted registry is named
// host:port, and dropping the port would send every request to 443.
func TestARegistryTargetKeepsItsPort(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "oci://registry.internal:5000/acme/senro-cache")
	t.Setenv("SENRO_REMOTE_CACHE_PLAIN_HTTP", "1")

	got, ok, err := senro.RemoteCacheFromEnv()
	if err != nil || !ok {
		t.Fatalf("RemoteCacheFromEnv = (%v, %v)", ok, err)
	}
	if got.Registry.Host != "registry.internal:5000" {
		t.Errorf("the registry host is %q, want registry.internal:5000", got.Registry.Host)
	}
	if got.Registry.Repository != "acme/senro-cache" {
		t.Errorf("the repository is %q, want acme/senro-cache", got.Registry.Repository)
	}
	if !got.Registry.PlainHTTP {
		t.Error("SENRO_REMOTE_CACHE_PLAIN_HTTP=1 did not turn plain http on")
	}
}

// TestRemoteCacheFromEnvRefusesAnUnusableRegistryTarget. Same rule as the
// bucket: a target that cannot work is a mistake somebody typed, and it is
// reported as one rather than degraded into a cache that is mysteriously
// always cold.
func TestRemoteCacheFromEnvRefusesAnUnusableRegistryTarget(t *testing.T) {
	for name, value := range map[string]string{
		"no repository":          "oci://ghcr.io",
		"a trailing slash only":  "oci://ghcr.io/",
		"no registry":            "oci:///acme/senro-cache",
		"credentials in the URL": "oci://user:password@ghcr.io/acme/senro-cache",
	} {
		t.Run(name, func(t *testing.T) {
			for _, n := range remoteCacheEnvNames() {
				t.Setenv(n, "")
			}
			t.Setenv("SENRO_REMOTE_CACHE", value)
			if _, _, err := senro.RemoteCacheFromEnv(); err == nil {
				t.Errorf("SENRO_REMOTE_CACHE=%q was accepted", value)
			}
		})
	}
}

// TestTheTwoTargetsDoNotShareTheirVariables. A pipeline that moved from a
// bucket to a registry leaves the bucket's variables behind, and a leftover
// that is silently ignored is how somebody spends an afternoon wondering
// which endpoint their cache is really using.
func TestTheTwoTargetsDoNotShareTheirVariables(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"an endpoint with a registry target": {
			"SENRO_REMOTE_CACHE":          "oci://ghcr.io/acme/senro-cache",
			"SENRO_REMOTE_CACHE_ENDPOINT": "https://s3.us-east-1.amazonaws.com",
		},
		"a region with a registry target": {
			"SENRO_REMOTE_CACHE":        "oci://ghcr.io/acme/senro-cache",
			"SENRO_REMOTE_CACHE_REGION": "us-east-1",
		},
		"an addressing style with a registry target": {
			"SENRO_REMOTE_CACHE":            "oci://ghcr.io/acme/senro-cache",
			"SENRO_REMOTE_CACHE_PATH_STYLE": "1",
		},
		"a registry password with a bucket target": {
			"SENRO_REMOTE_CACHE":          "s3://team-cache",
			"SENRO_REMOTE_CACHE_ENDPOINT": "https://s3.us-east-1.amazonaws.com",
			"SENRO_REMOTE_CACHE_REGION":   "us-east-1",
			"AWS_ACCESS_KEY_ID":           "k",
			"AWS_SECRET_ACCESS_KEY":       "s",
			"SENRO_REMOTE_CACHE_PASSWORD": "a-personal-access-token",
		},
		"plain http with a bucket target": {
			"SENRO_REMOTE_CACHE":            "s3://team-cache",
			"SENRO_REMOTE_CACHE_ENDPOINT":   "https://s3.us-east-1.amazonaws.com",
			"SENRO_REMOTE_CACHE_REGION":     "us-east-1",
			"AWS_ACCESS_KEY_ID":             "k",
			"AWS_SECRET_ACCESS_KEY":         "s",
			"SENRO_REMOTE_CACHE_PLAIN_HTTP": "1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, n := range remoteCacheEnvNames() {
				t.Setenv(n, "")
			}
			for k, v := range env {
				t.Setenv(k, v)
			}
			_, _, err := senro.RemoteCacheFromEnv()
			if err == nil {
				t.Fatalf("%s was accepted, so the variable is set and does nothing", name)
			}
			t.Logf("%s: %v", name, err)
		})
	}
}

// TestARegistryCacheThatCannotBeOpenedFailsTheRunUpFront. Whether a name is
// WELL FORMED is settled when the remote is opened, not when the environment
// is read, exactly as it is for a bucket: RemoteCacheFromEnv answers "is
// anything missing" and the client answers "can this work at all". Either way
// it stops the run before the first step, which is where a mistake somebody
// typed should surface.
func TestARegistryCacheThatCannotBeOpenedFailsTheRunUpFront(t *testing.T) {
	pipe := senro.New("registry-cache-bad-config")
	pipe.Workflow("main").Step("noop", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for name, reg := range map[string]senro.RegistryCache{
		"an uppercase repository": {Host: "ghcr.io", Repository: "Acme/Senro-Cache"},
		"a registry that is a URL": {
			Host: "https://ghcr.io", Repository: "acme/senro-cache",
		},
		"a repository that walks out of itself": {
			Host: "ghcr.io", Repository: "acme/../../v2/_catalog",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := senro.RunPlan(t.Context(), p,
				senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()),
				senro.WithRemoteCache(senro.RemoteCache{Registry: reg}))
			if err == nil {
				t.Fatalf("%s was accepted", name)
			}
			var runErr *senro.RunError
			if errors.As(err, &runErr) {
				t.Errorf("a configuration mistake was reported as a run failure rather than a "+
					"refusal before the run: %v", err)
			}
		})
	}
}

// TestARemoteCacheThatIsBothABucketAndARegistryIsRefused covers the Go API,
// where nothing forces the choice the way a single URL does.
func TestARemoteCacheThatIsBothABucketAndARegistryIsRefused(t *testing.T) {
	pipe := senro.New("remote-cache-both")
	pipe.Workflow("main").Step("noop", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = senro.RunPlan(t.Context(), p,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			Endpoint: "https://s3.us-east-1.amazonaws.com", Region: "us-east-1",
			Bucket: "team-cache", AccessKeyID: "k", SecretAccessKey: "s",
			Registry: senro.RegistryCache{Host: "ghcr.io", Repository: "acme/senro-cache"},
		}))
	if err == nil {
		t.Fatal("a remote cache that is both a bucket and a registry was accepted")
	}
	if !strings.Contains(err.Error(), "registry") || !strings.Contains(err.Error(), "bucket") {
		t.Errorf("the refusal does not say what the conflict is: %v", err)
	}
}

// TestRunReadsTheEnvironmentWhenNoRemoteCacheWasPassed. A CI job sets
// variables; it does not edit the pipeline's Go source. This is the same
// arrangement SENRO_CACHE_DIR already has.
func TestRunReadsTheEnvironmentWhenNoRemoteCacheWasPassed(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "s3://team-cache")
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("SENRO_REMOTE_CACHE_REGION", "us-east-1")
	t.Setenv("SENRO_REMOTE_CACHE_TIMEOUT", "2s")
	t.Setenv("SENRO_REMOTE_CACHE_PATH_STYLE", "1")
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")

	dir := t.TempDir()
	if err := senro.RunPlan(t.Context(), pureBuildPlan(t, "env-remote-cache"),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir())); err != nil {
		t.Fatalf("a run whose environment names an unreachable cache failed: %v", err)
	}
	if n := len(eventsOfType(readLedger(t, dir), api.CacheDegraded)); n != 1 {
		t.Errorf("the run recorded %d cache.degraded events, want exactly 1: the environment "+
			"was not read", n)
	}
}

// TestAHalfConfiguredEnvironmentRefusesTheRun: the same asymmetry
// RemoteCacheFromEnv has, reached through Run, because that is where somebody
// actually meets it.
func TestAHalfConfiguredEnvironmentRefusesTheRun(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("SENRO_REMOTE_CACHE", "s3://team-cache")
	// No endpoint, no region, no credentials.

	err := senro.RunPlan(t.Context(), pureBuildPlan(t, "half-configured"),
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()))
	if err == nil {
		t.Fatal("a half-configured shared cache was accepted, so it would have been " +
			"configured and never used")
	}
	if !strings.Contains(err.Error(), "SENRO_REMOTE_CACHE_ENDPOINT") {
		t.Errorf("the error does not name what is missing: %v", err)
	}
}

// TestAnExplicitRemoteCacheBeatsTheEnvironment. Code that says what it wants
// is not overridden by an ambient variable.
func TestAnExplicitRemoteCacheBeatsTheEnvironment(t *testing.T) {
	for _, name := range remoteCacheEnvNames() {
		t.Setenv(name, "")
	}
	// A configuration that would refuse the run if it were read at all.
	t.Setenv("SENRO_REMOTE_CACHE", "s3://from-the-environment")

	dir := t.TempDir()
	pathStyle := true
	err := senro.RunPlan(t.Context(), pureBuildPlan(t, "explicit-beats-env"),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "from-the-call",
			AccessKeyID: "k", SecretAccessKey: "s",
			PathStyle: &pathStyle, Timeout: 2 * time.Second,
		}))
	if err != nil {
		t.Fatalf("an explicitly configured cache did not override the environment: %v", err)
	}
	for _, e := range eventsOfType(readLedger(t, dir), api.CacheDegraded) {
		var b api.CacheDegradedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if !strings.Contains(b.Store, "from-the-call") {
			t.Errorf("the run used %q, not the cache the call named", b.Store)
		}
	}
}

// pureBuildPlan is a two-step pipeline whose second step is cacheable, which
// is the minimum needed to make a run touch its object store at all.
func pureBuildPlan(t *testing.T, name string) *senro.Plan {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New(name)
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("build", exec.Command("sh", "-c", "wc -c main.go > out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}
