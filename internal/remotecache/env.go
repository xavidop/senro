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

	// EnvScratch shares scratch caches through the bucket as well, which is
	// OFF by default and deliberately its own variable rather than something
	// EnvTarget turns on with everything else.
	//
	// Three reasons it is opt-in. A scratch entry is one whole-tree tarball,
	// often gigabytes, whose key churns on every lock-file edit, so turning
	// it on silently would put a multi-gigabyte upload on every dependency
	// bump to save a download the toolchain already does incrementally. It
	// needs s3:ListBucket, which nothing else senro does requires, so a
	// credential scoped to reading and writing objects would start failing.
	// And a scratch tree is not platform-tagged: the key says nothing about
	// the machine that filled it, so sharing one between a darwin
	// coordinator and a linux pod is the operator's judgement to make, not
	// senro's to make for them.
	//
	// Registry-backed remotes ignore it: prefix fallback is a listing, and
	// the OCI backend cannot list by prefix (see internal/oci).
	EnvScratch = "SENRO_REMOTE_SCRATCH"

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
		EnvTimeout, EnvReadOnly, EnvScratch,
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
