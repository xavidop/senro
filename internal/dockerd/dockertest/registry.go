package dockertest

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
)

// RegistryImage is the OCI registry every registry-cache test in this
// repository runs against.
//
// The reference implementation, not a fake: it verifies upload digests,
// rejects manifests whose blobs it does not hold, issues its own
// WWW-Authenticate challenge, and validates tokens against a certificate,
// so a request senro forms incorrectly is refused exactly as a hosted
// registry would refuse it. Pinned to a release for the reason MinIOImage
// is: an oracle that changes underneath the suite is not an oracle.
const RegistryImage = "registry:3.0.0"

const (
	// registryPort is the port the registry serves on inside the container.
	registryPort = 5000
	// registryService is the name the registry calls itself in its challenge,
	// and the audience every token has to name.
	registryService = "senro-test-registry"
	// registryIssuer is the issuer the registry requires of a token.
	registryIssuer = "senro-test-issuer"
	// registryConfigDir is where the generated configuration and the token
	// CA certificate are written INSIDE the container.
	registryConfigDir = "/senro"
)

// Registry is one test's handle on the shared registry: its own repository,
// and the credentials to reach it.
type Registry struct {
	// Host is "127.0.0.1:<port>", the registry's authority. Plain HTTP: this
	// is loopback to a container on the same machine, and a self-signed
	// certificate would only be testing Go's TLS stack.
	Host string
	// Repository is this test's own repository. No other test in this binary
	// is given the same one, and a registry creates one on first push, so
	// nothing has to be set up in advance.
	Repository string
	// Username and Password can pull and push.
	Username string
	Password string
	// ReadOnlyUsername and ReadOnlyPassword can pull and nothing else, so a
	// test can watch a push be refused for the reason a real deployment
	// produces rather than by taking the server away.
	ReadOnlyUsername string
	ReadOnlyPassword string
}

// RequireRegistry returns a live OCI registry, or skips the test with a
// reason.
//
// Started once per test binary and shared; each caller gets its own
// repository. Configured for token authentication, the flow every hosted
// registry uses and the only one senro implements; anonymous requests are
// refused, so a test cannot pass by accident. Skipping follows Require's
// rule (SENRO_REQUIRE_DOCKER=1 turns skips into failures), and a caller
// MUST route its TestMain through RunMain.
func RequireRegistry(t *testing.T) Registry {
	t.Helper()
	// Gate on the daemon first, so a machine with no Docker gets Require's
	// own carefully worded skip rather than a failure from further in.
	_ = Require(t)

	srv, err := sharedRegistry()
	if err != nil {
		t.Fatalf("starting %s: %v. This test needs a live registry and did not run.",
			RegistryImage, err)
	}
	n := registryNextRepo.Add(1) - 1
	return Registry{
		Host:             srv.addr,
		Repository:       fmt.Sprintf("senro/cache-%02d", n),
		Username:         RegistryUser,
		Password:         RegistryPassword,
		ReadOnlyUsername: RegistryReadOnlyUser,
		ReadOnlyPassword: RegistryReadOnlyPassword,
	}
}

// StopSharedRegistry removes the container this package started, if it started
// one, and stops the token server with it. It is what RunMain calls, exported
// for the one package that cannot use RunMain because it already has a
// TestMain of its own doing unrelated work.
//
// Safe to call when nothing was ever started, and safe to call twice.
func StopSharedRegistry() { stopSharedRegistry() }

var (
	registryOnce     sync.Once
	registrySrv      *registryServer
	registryStartErr error
	registryNextRepo atomic.Int64
)

type registryServer struct {
	client *dockerd.Client
	id     string
	addr   string
	issuer *tokenIssuer
}

func sharedRegistry() (*registryServer, error) {
	registryOnce.Do(func() { registrySrv, registryStartErr = startRegistry() })
	return registrySrv, registryStartErr
}

func stopSharedRegistry() {
	if registrySrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Removing the container removes its filesystem, and with it every blob
	// the tests pushed: nothing is meant to outlive the run.
	_ = registrySrv.client.ContainerRemove(ctx, registrySrv.id)
	_ = registrySrv.client.Close()
	registrySrv.issuer.close()
	registrySrv = nil
}

// startRegistry creates, starts and waits for the registry.
//
// It opens its own daemon client rather than borrowing a test's: the client a
// test received from Require is closed when that test finishes, and this
// container has to outlive it.
func startRegistry() (*registryServer, error) {
	// The token server first, because the registry has to be told the realm
	// to publish in its challenge before it starts. Nothing inside the
	// container ever calls it; only the client does, from this same process.
	issuer, err := newTokenIssuer(registryIssuer, registryService)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			issuer.close()
		}
	}()

	client, err := dockerd.Open()
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			_ = client.Close()
		}
	}()

	// Before starting one, clear away any left by a test process that is no
	// longer running. Same reasoning, and the same mechanism, as the object
	// store's: see reapAbandonedMinIOs.
	reapCtx, reapCancel := context.WithTimeout(context.Background(), 15*time.Second)
	ReapAbandoned(reapCtx, client, "registry")
	reapCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if _, found, err := client.ImageInspect(ctx, RegistryImage); err != nil || !found {
		if err := client.ImagePull(ctx, RegistryImage, nil); err != nil {
			return nil, err
		}
	}

	// Configuration is written by the container's own first command, not
	// bind-mounted: a test run should leave nothing on the developer's
	// disk, and `registry serve <path>` has stayed stable across major
	// versions while the image's own config file has moved.
	id, err := client.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image:      RegistryImage,
		Entrypoint: []string{"sh", "-c"},
		Cmd: []string{fmt.Sprintf(
			"mkdir -p %[1]s && printf '%%s' \"$SENRO_REGISTRY_CONFIG\" > %[1]s/config.yml && "+
				"printf '%%s' \"$SENRO_TOKEN_CA\" > %[1]s/token-ca.crt && "+
				"exec registry serve %[1]s/config.yml", registryConfigDir)},
		Env: []string{
			"SENRO_REGISTRY_CONFIG=" + registryConfig(issuer.realm()),
			"SENRO_TOKEN_CA=" + issuer.caPEM,
		},
		Ports:  []dockerd.Port{{Container: registryPort}},
		Labels: OwnerLabels("registry"),
	})
	if err != nil {
		return nil, err
	}
	srv := &registryServer{client: client, id: id, issuer: issuer}
	defer func() {
		if !ok {
			stop, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = client.ContainerRemove(stop, id)
		}
	}()

	if err := client.ContainerStart(ctx, id); err != nil {
		return nil, err
	}
	addr, err := waitForPort(ctx, client, id, registryPort)
	if err != nil {
		return nil, err
	}
	srv.addr = addr
	if err := waitForChallenge(ctx, "http://"+addr+"/v2/"); err != nil {
		return nil, err
	}
	ok = true
	return srv, nil
}

// registryConfig is the whole configuration the registry runs with, written
// out rather than assembled from REGISTRY_* variables so every setting is
// visible in one place and the auth section cannot be half-applied: a
// registry that quietly started with no auth would let every test pass
// without exercising the token flow.
func registryConfig(realm string) string {
	return fmt.Sprintf(`version: 0.1
log:
  level: warn
storage:
  filesystem:
    rootdirectory: /var/lib/registry
  delete:
    enabled: true
http:
  addr: :%d
auth:
  token:
    realm: %s
    service: %s
    issuer: %s
    rootcertbundle: %s/token-ca.crt
`, registryPort, realm, registryService, registryIssuer, registryConfigDir)
}

// waitForChallenge polls until the registry answers its version endpoint.
//
// A 401 is the expected answer and counts as up: this registry demands a
// token for everything, including the ping. Anything else that is not a
// connection failure means the server is listening and has an opinion, which
// is all this is waiting for.
func waitForChallenge(ctx context.Context, url string) error {
	hc := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(90 * time.Second)
	var last error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := hc.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				if challenge := resp.Header.Get("Www-Authenticate"); challenge != "" {
					return nil
				}
				last = fmt.Errorf("%s answered 401 with no WWW-Authenticate challenge, "+
					"so it is not running with token authentication", url)
			} else {
				last = fmt.Errorf("%s answered %s, want 401 with a token challenge", url, resp.Status)
			}
		} else {
			last = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s never became ready within 90s: %w", url, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
