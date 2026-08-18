package senro

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/remotecache"
)

// RemoteCache points a run at a shared cache, so machines can reuse each
// other's work: a fresh CI runner starts with an empty disk, and a shared
// cache is already warm on its first build.
//
// The cache lives in an S3-compatible bucket (the fields below) or in an OCI
// registry repository (Registry). One or the other, never both: two places
// to keep one cache, not two caches. It is a second tier behind the local
// cache, never a replacement: a run reads its own disk first, falls back to
// the shared store, and writes what it fetches through to disk.
//
// Two properties are guaranteed rather than best effort. Nothing is served
// without being verified: every object is checked against the digest it was
// asked for, and every cache entry against its key, so a truncated, stale or
// foreign body is a cache MISS, never a wrong build. And a cache that is
// down never fails a run: unreachable, unauthenticated, refusing writes,
// slow or corrupt all mean "no shared cache", reported once on standard
// error and once as an api.CacheDegraded event, after which the run stops
// trying so it does not pay a timeout per lookup.
//
// The zero value configures nothing. See RemoteCacheFromEnv for the form CI
// usually wants.
type RemoteCache struct {
	// Endpoint is the object store's URL: "https://s3.eu-west-1.amazonaws.com",
	// "https://<account>.r2.cloudflarestorage.com", "http://minio.internal:9000".
	// Scheme and host; a path on it is used as a prefix, for a store behind a
	// reverse proxy.
	//
	// It must not carry a username or password. Credentials go in the fields
	// below, because an endpoint is named in error messages and events and a
	// credential in one would travel into every log that saw it.
	Endpoint string

	// Region scopes the request signature. Required even for a store that has
	// no regions of its own, because it is signed over and both ends have to
	// expect the same one. "us-east-1" is the conventional answer for a store
	// that does not care.
	Region string

	// Bucket holds the cache.
	Bucket string

	// Prefix is the key prefix inside the bucket, so one bucket can hold
	// senro's cache alongside other things. Empty means "senro".
	Prefix string

	// The credentials. In CI these usually come from an assumed role, in which
	// case all three are set and they expire; senro reads them once, at the
	// start of a run, and a run outlasting its credentials degrades to no
	// cache exactly as any other authentication failure does.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// PathStyle chooses bucket-in-path (endpoint/bucket/key) over
	// bucket-in-host (bucket.endpoint/key) addressing.
	//
	// Nil means "work it out from the endpoint", which is right almost always:
	// bucket-in-host for Amazon, which requires it for buckets created since
	// 2020, and bucket-in-path for everything else, which either requires it
	// or accepts it and rarely has the wildcard DNS the other style needs.
	// Set it only when a store disagrees with that.
	PathStyle *bool

	// Timeout bounds one request to the store, including reading its body.
	// Zero means five minutes, which is generous because the largest object a
	// cache moves is a workspace snapshot and a cold runner's uplink is not
	// always fast.
	Timeout time.Duration

	// ReadOnly reads the shared cache and never writes to it.
	//
	// This is what a pull-request build, especially one from a fork, should
	// use: it gets the speed of the cache the trunk builds fill, and it cannot
	// put anything into a cache that other people's builds will trust. Set it
	// alongside a credential that also cannot write, rather than instead of
	// one: this is a courtesy, and the store's policy is the control.
	ReadOnly bool

	// Registry holds the cache in an OCI registry repository instead of a
	// bucket. Setting Host on it selects that backend, and the bucket fields
	// above must then be left alone.
	Registry RegistryCache
}

// RegistryCache is the half of a RemoteCache that names an OCI registry.
//
// A nested struct rather than five more fields on RemoteCache: a bucket and
// a registry agree on almost nothing, and flattened together half the fields
// would always be wrong with no way to tell which half was live. Timeout and
// ReadOnly stay on RemoteCache because they mean the same thing on either.
type RegistryCache struct {
	// Host is the registry's host and optional port: "ghcr.io",
	// "registry.internal:5000". A host, not a URL, and it must not carry a
	// username or password, for the reason Endpoint must not: a host is named
	// in error messages and events.
	Host string

	// Repository is the path inside the registry that holds the cache, such as
	// "acme/senro-cache". Lowercase, as the distribution specification
	// requires. Most registries create it on first push.
	Repository string

	// Username and Password are the credential presented to the registry's
	// token endpoint, which is the one authentication flow senro implements and
	// the one every hosted registry serves. Both empty means anonymous, which
	// works against a registry that demands nothing.
	//
	// senro runs no credential helper, reads no ~/.docker/config.json and
	// contacts no metadata service. For a registry whose credential is issued
	// by another service, resolve it first and pass the result: "AWS" and
	// `aws ecr get-login-password` for Elastic Container Registry,
	// "oauth2accesstoken" and an access token for Artifact Registry.
	Username string
	Password string

	// PlainHTTP talks to the registry over http rather than https, for a
	// registry on a trusted network that serves no certificate. Off by default,
	// because a credential sent in clear text to a host on the internet is a
	// leaked credential.
	PlainHTTP bool
}

// WithRemoteCache points this run at a shared cache. See RemoteCache.
//
// A configuration that cannot possibly work (no bucket, a malformed
// endpoint, credentials in the endpoint, both a bucket and a registry) fails
// the run before it starts, the only remote-cache problem that does: it is a
// mistake in what somebody wrote, not a condition of the network, and
// degrading it to "your cache is down" would send them looking in the wrong
// place. Everything afterwards, including an unreachable store, degrades
// instead.
//
// The zero RemoteCache configures nothing at all, so
//
//	rc, ok, err := senro.RemoteCacheFromEnv()
//	...
//	senro.Run(ctx, p, senro.WithRemoteCache(rc))
//
// is correct whether or not ok was true.
func WithRemoteCache(rc RemoteCache) Option {
	return func(c *runConfig) { c.remoteCache = rc }
}

// Environment variables RemoteCacheFromEnv reads. A bucket's credentials come
// from the standard AWS names as well: AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN.
const (
	// EnvRemoteCache turns the shared cache on and says where it is, which is
	// also what chooses the backend: "s3://<bucket>" or "s3://<bucket>/<prefix>"
	// for a bucket, "oci://<registry>/<repository>" for a registry. Unset means
	// no shared cache, which is the default and is not an error.
	EnvRemoteCache = remotecache.EnvTarget
	// EnvRemoteCacheEndpoint is the bucket store's URL.
	EnvRemoteCacheEndpoint = remotecache.EnvEndpoint
	// EnvRemoteCacheRegion scopes the bucket request signature.
	EnvRemoteCacheRegion = remotecache.EnvRegion
	// EnvRemoteCachePathStyle overrides the bucket addressing style; see
	// RemoteCache.PathStyle. Unset means "work it out from the endpoint".
	EnvRemoteCachePathStyle = remotecache.EnvPathStyle
	// EnvRemoteCacheUsername and EnvRemoteCachePassword are the credential a
	// registry target presents to the registry's token endpoint. Unset means
	// anonymous, which works against a registry that demands nothing.
	EnvRemoteCacheUsername = remotecache.EnvUsername
	EnvRemoteCachePassword = remotecache.EnvPassword
	// EnvRemoteCachePlainHTTP talks to the registry over http rather than
	// https.
	EnvRemoteCachePlainHTTP = remotecache.EnvPlainHTTP
	// EnvRemoteCacheTimeout bounds one request, as a Go duration ("45s").
	EnvRemoteCacheTimeout = remotecache.EnvTimeout
	// EnvRemoteCacheReadOnly makes the run read the cache and never write it.
	EnvRemoteCacheReadOnly = remotecache.EnvReadOnly
)

// RemoteCacheFromEnv reads a shared-cache configuration from the environment.
//
// It returns ok=false, and no error, when SENRO_REMOTE_CACHE is unset: no
// shared cache is the ordinary state of a machine. When it IS set, anything
// else missing is an error rather than a silently disabled cache, because a
// cache that is quietly not there looks exactly like a cold cache and nobody
// investigates one of those.
//
// A bucket's credentials come from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
// and AWS_SESSION_TOKEN, which CI already sets when a job assumes a role. A
// registry's come from SENRO_REMOTE_CACHE_USERNAME and
// SENRO_REMOTE_CACHE_PASSWORD, senro's own names because no standard pair
// exists to borrow.
//
// The scheme of SENRO_REMOTE_CACHE chooses the backend, and variables
// belonging to the OTHER backend are refused rather than ignored: a leftover
// that is silently ignored is how somebody spends an afternoon wondering
// which endpoint their cache is really using.
//
// senro reads no credential file and contacts no metadata service: the
// credentials are whatever the process was given, resolved once. A caller
// who wants a fuller resolution chain fills in a RemoteCache directly.
func RemoteCacheFromEnv() (RemoteCache, bool, error) {
	target := strings.TrimSpace(os.Getenv(EnvRemoteCache))
	if target == "" {
		return RemoteCache{}, false, nil
	}
	u, err := url.Parse(target)
	if err != nil {
		return RemoteCache{}, false, fmt.Errorf("senro: %s=%q is not a URL: %w",
			EnvRemoteCache, target, err)
	}

	var rc RemoteCache
	switch u.Scheme {
	case "s3":
		rc, err = bucketFromEnv(target, u)
	case "oci":
		rc, err = registryFromEnv(target, u)
	default:
		err = fmt.Errorf(
			"senro: %s=%q: the scheme says which kind of shared cache this is, and there are "+
				"two: \"s3://<bucket>\" or \"s3://<bucket>/<prefix>\" for an S3-compatible "+
				"bucket, \"oci://<registry>/<repository>\" for an OCI registry",
			EnvRemoteCache, target)
	}
	if err != nil {
		return RemoteCache{}, false, err
	}

	if v := strings.TrimSpace(os.Getenv(EnvRemoteCacheTimeout)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return RemoteCache{}, false, fmt.Errorf("senro: %s=%q is not a duration: %w",
				EnvRemoteCacheTimeout, v, err)
		}
		rc.Timeout = d
	}
	readOnly, set, err := envBool(EnvRemoteCacheReadOnly)
	if err != nil {
		return RemoteCache{}, false, err
	}
	rc.ReadOnly = set && readOnly
	return rc, true, nil
}

// bucketFromEnv reads the configuration an "s3://bucket/prefix" target needs.
func bucketFromEnv(target string, u *url.URL) (RemoteCache, error) {
	if u.User != nil {
		return RemoteCache{}, fmt.Errorf(
			"senro: %s must not carry a username or password; credentials come from "+
				"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY", EnvRemoteCache)
	}
	if u.Host == "" {
		return RemoteCache{}, fmt.Errorf("senro: %s=%q names no bucket", EnvRemoteCache, target)
	}
	if err := noneOf(target, "a bucket",
		EnvRemoteCacheUsername, EnvRemoteCachePassword, EnvRemoteCachePlainHTTP); err != nil {
		return RemoteCache{}, err
	}

	rc := RemoteCache{
		Bucket:          u.Host,
		Prefix:          strings.Trim(u.Path, "/"),
		Endpoint:        strings.TrimSpace(os.Getenv(EnvRemoteCacheEndpoint)),
		Region:          strings.TrimSpace(os.Getenv(EnvRemoteCacheRegion)),
		AccessKeyID:     os.Getenv(remotecache.EnvAccessKeyID),
		SecretAccessKey: os.Getenv(remotecache.EnvSecretAccessKey),
		SessionToken:    os.Getenv(remotecache.EnvSessionToken),
	}
	for _, missing := range []struct{ name, value string }{
		{EnvRemoteCacheEndpoint, rc.Endpoint},
		{EnvRemoteCacheRegion, rc.Region},
		{remotecache.EnvAccessKeyID, rc.AccessKeyID},
		{remotecache.EnvSecretAccessKey, rc.SecretAccessKey},
	} {
		if missing.value == "" {
			return RemoteCache{}, fmt.Errorf(
				"senro: %s is set, so %s must be too, or the shared cache would be configured "+
					"and never used", EnvRemoteCache, missing.name)
		}
	}

	pathStyle, set, err := envBool(EnvRemoteCachePathStyle)
	if err != nil {
		return RemoteCache{}, err
	}
	if set {
		rc.PathStyle = &pathStyle
	}
	return rc, nil
}

// registryFromEnv reads the configuration an "oci://registry/repository"
// target needs. Nothing beyond the target itself is required: unlike a
// bucket, the target already carries everything needed to address the
// repository, anonymous access is a real deployment, and a registry that
// does want a credential says so on the first request and degrades loudly.
func registryFromEnv(target string, u *url.URL) (RemoteCache, error) {
	if u.User != nil {
		return RemoteCache{}, fmt.Errorf(
			"senro: %s must not carry a username or password; credentials come from %s and %s",
			EnvRemoteCache, EnvRemoteCacheUsername, EnvRemoteCachePassword)
	}
	if u.Host == "" {
		return RemoteCache{}, fmt.Errorf(
			"senro: %s=%q names no registry; it goes between the scheme and the repository, "+
				"as in \"oci://ghcr.io/acme/senro-cache\"", EnvRemoteCache, target)
	}
	repository := strings.Trim(u.Path, "/")
	if repository == "" {
		return RemoteCache{}, fmt.Errorf(
			"senro: %s=%q names a registry and no repository inside it; a shared cache lives in "+
				"one repository, as in \"oci://%s/acme/senro-cache\"",
			EnvRemoteCache, target, u.Host)
	}
	if err := noneOf(target, "a registry",
		EnvRemoteCacheEndpoint, EnvRemoteCacheRegion, EnvRemoteCachePathStyle); err != nil {
		return RemoteCache{}, err
	}

	plainHTTP, _, err := envBool(EnvRemoteCachePlainHTTP)
	if err != nil {
		return RemoteCache{}, err
	}
	return RemoteCache{
		Registry: RegistryCache{
			Host:       u.Host,
			Repository: repository,
			Username:   os.Getenv(EnvRemoteCacheUsername),
			Password:   os.Getenv(EnvRemoteCachePassword),
			PlainHTTP:  plainHTTP,
		},
	}, nil
}

// noneOf refuses a variable that belongs to the other kind of shared cache.
//
// Refused rather than ignored, for the reason RemoteCacheFromEnv states: a
// setting that is present and does nothing is worse than one that is absent,
// because somebody believes it.
func noneOf(target, kind string, names ...string) error {
	for _, name := range names {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			continue
		}
		return fmt.Errorf(
			"senro: %s=%q is %s, and %s configures the other kind, so it would be read by "+
				"nothing; unset it, or point %s at what it belongs to",
			EnvRemoteCache, target, kind, name, EnvRemoteCache)
	}
	return nil
}

// envBool reads a boolean environment variable, reporting whether it was set
// at all so that "unset" and "explicitly false" stay distinguishable: that
// difference is what PathStyle's nil means.
func envBool(name string) (value, set bool, err error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return false, false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, false, fmt.Errorf(
			"senro: %s=%q is not a boolean; use 1 or 0, true or false", name, v)
	}
	return b, true, nil
}

// configured reports whether this RemoteCache asks for anything at all. The
// zero value does not, which is what lets a caller pass the result of
// RemoteCacheFromEnv straight through without branching on it.
func (rc RemoteCache) configured() bool { return rc != RemoteCache{} }

// open turns the public configuration into an opened remote.
//
// Both backends produce the same *remotecache.Remote, so nothing downstream of
// here, in storage or in the engine, has a branch or a second field for which
// kind of shared cache a run was given.
func (rc RemoteCache) open() (*remotecache.Remote, error) {
	if !rc.configured() {
		return nil, nil
	}
	if rc.Registry.Host != "" {
		return rc.openRegistry()
	}
	r, err := remotecache.Open(remotecache.Config{
		Endpoint:        rc.Endpoint,
		Region:          rc.Region,
		Bucket:          rc.Bucket,
		Prefix:          rc.Prefix,
		AccessKeyID:     rc.AccessKeyID,
		SecretAccessKey: rc.SecretAccessKey,
		SessionToken:    rc.SessionToken,
		PathStyle:       rc.PathStyle,
		Timeout:         rc.Timeout,
		ReadOnly:        rc.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("senro: %w", err)
	}
	return r, nil
}

// openRegistry opens the registry half, having first refused a configuration
// that names both kinds. The environment cannot express that (one URL, one
// scheme) but a struct literal can, and the two would otherwise be resolved
// by a precedence rule nobody wrote down.
func (rc RemoteCache) openRegistry() (*remotecache.Remote, error) {
	for _, both := range []struct{ field, value string }{
		{"Bucket", rc.Bucket},
		{"Endpoint", rc.Endpoint},
		{"Region", rc.Region},
		{"AccessKeyID", rc.AccessKeyID},
	} {
		if both.value != "" {
			return nil, fmt.Errorf(
				"senro: this RemoteCache names both a registry (Registry.Host %q) and a bucket "+
					"(%s %q), and a shared cache lives in one place; clear whichever is not "+
					"meant", rc.Registry.Host, both.field, both.value)
		}
	}
	r, err := remotecache.OpenOCI(remotecache.OCIConfig{
		Registry:   rc.Registry.Host,
		Repository: rc.Registry.Repository,
		Username:   rc.Registry.Username,
		Password:   rc.Registry.Password,
		PlainHTTP:  rc.Registry.PlainHTTP,
		Timeout:    rc.Timeout,
		ReadOnly:   rc.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("senro: %w", err)
	}
	return r, nil
}
