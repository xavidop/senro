package remotecache

import "os"

// The environment variables a shared cache is configured from. They live
// here, not in the root package that parses them, so EnvNames can be one
// list; the root package re-exports each as public API.
const (
	// EnvTarget turns the shared cache on and says where it is, which is also
	// what chooses the backend: "s3://<bucket>[/<prefix>]" for a bucket, and
	// "oci://<registry>/<repository>" for a registry.
	EnvTarget = "SENRO_REMOTE_CACHE"

	// The bucket's own variables.

	// EnvEndpoint is the object store's URL.
	EnvEndpoint = "SENRO_REMOTE_CACHE_ENDPOINT"
	// EnvRegion scopes the request signature.
	EnvRegion = "SENRO_REMOTE_CACHE_REGION"
	// EnvPathStyle overrides the bucket addressing style.
	EnvPathStyle = "SENRO_REMOTE_CACHE_PATH_STYLE"

	// The registry's own variables.

	// EnvUsername and EnvPassword are the credential presented to the
	// registry's token endpoint. senro's own names, unlike the AWS ones
	// below: no standard pair exists for a registry (Docker keeps its
	// credential in a config file senro deliberately does not read, and
	// every forge spells its own differently).
	EnvUsername = "SENRO_REMOTE_CACHE_USERNAME"
	EnvPassword = "SENRO_REMOTE_CACHE_PASSWORD"
	// EnvPlainHTTP talks to the registry over http rather than https, for one
	// on a trusted network that serves no certificate.
	EnvPlainHTTP = "SENRO_REMOTE_CACHE_PLAIN_HTTP"

	// Shared by both backends.

	// EnvTimeout bounds one request, as a Go duration.
	EnvTimeout = "SENRO_REMOTE_CACHE_TIMEOUT"
	// EnvReadOnly makes a run read the shared cache and never write it.
	EnvReadOnly = "SENRO_REMOTE_CACHE_READ_ONLY"

	// The bucket's credential variables, deliberately the standard AWS names:
	// CI already sets these when a job assumes a role, and a senro-specific
	// spelling would mean every pipeline copying them across for no reason.
	EnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	EnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	EnvSessionToken    = "AWS_SESSION_TOKEN"
)

// EnvNames is every variable above, in one list.
func EnvNames() []string {
	return []string{
		EnvTarget, EnvEndpoint, EnvRegion, EnvPathStyle,
		EnvUsername, EnvPassword, EnvPlainHTTP,
		EnvTimeout, EnvReadOnly,
		EnvAccessKeyID, EnvSecretAccessKey, EnvSessionToken,
	}
}

// ClearEnv removes every one of them from this process's environment. For
// test binaries: a run with no explicit configuration reads these
// variables, so a developer exporting SENRO_REMOTE_CACHE would have the
// test suite writing into their team's bucket, and WithCacheDir isolates
// only the LOCAL cache root. Non-test code never calls this.
func ClearEnv() {
	for _, name := range EnvNames() {
		_ = os.Unsetenv(name)
	}
}
