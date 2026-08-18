package kindtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/kubeapi"
)

// ClusterName is the kind cluster THIS TEST BINARY creates. Senro-specific
// so it cannot collide with a developer's own cluster, and derived rather
// than random so Require can delete a leftover from a SIGKILLed run before
// creating its own. Per BINARY, not per repository: `go test ./...` runs
// harness-using binaries concurrently and Require's first act is a delete,
// so one fixed name would have each binary destroying the other's cluster.
func ClusterName() string { return "senro-" + binaryTag() }

// binaryTag is the running test binary's package name, reduced to something
// kind will accept as a cluster name. `go test` builds "<package>.test", so
// this is "k8sexec" or "kubeapi": stable across runs of the same package,
// distinct between packages.
func binaryTag() string {
	base := filepath.Base(os.Args[0])
	base = strings.TrimSuffix(base, ".exe")
	base = strings.TrimSuffix(base, ".test")
	b := make([]byte, 0, len(base))
	for i := 0; i < len(base) && len(b) < 32; i++ {
		c := base[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b = append(b, c)
		case c >= 'A' && c <= 'Z':
			b = append(b, c+('a'-'A'))
		default:
			b = append(b, '-')
		}
	}
	if len(b) == 0 {
		return "executor"
	}
	return string(b)
}

// Namespace is where every test in this repository creates pods. Not
// "default": the executor refuses to guess a namespace, and a test that used
// the cluster's default would be testing a path production cannot take.
const Namespace = "senro-exec"

// Image is the image every k8s executor test runs: busybox 1.36 (shared
// with the container executor's tests), pinned to its multi-arch manifest
// list digest because the executor refuses anything unpinned and the node
// resolves the list to its own architecture.
//
// The node pulls this; a slow registry shows up as pods sitting in
// ContainerCreating until awaitStart's budget expires, which reads like a
// senro bug and is not one. Preloading does not work: `kind load` cannot
// round-trip a manifest list through ctr ("content digest ... not found"),
// and every alternative (a single-arch digest, crictl pre-pull, a local
// registry) is a real tradeoff rather than a tidy-up, so the node pulls.
const Image = "busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"

// Env names the escape hatches, both of which exist for the developer
// iterating on this executor rather than for CI.
const (
	// EnvRequire turns a skip into a failure, exactly as
	// SENRO_REQUIRE_DOCKER=1 does for the container executor. CI sets it, so
	// a suite that stopped running is a red build rather than a quietly green
	// one.
	EnvRequire = "SENRO_REQUIRE_KIND"
	// EnvKeep leaves the cluster in place after the tests, so an edit-run
	// cycle does not pay a fresh cluster creation each time. Unset, which is
	// the default and what CI gets, the cluster is deleted.
	EnvKeep = "SENRO_KIND_KEEP"
)

// Cluster is one guarded kind cluster and the client bound to it.
type Cluster struct {
	Conn Conn
	// Kubeconfig is the path to THIS test run's own kubeconfig file, inside
	// the test's temp directory. Every kubectl invocation in this package
	// passes it explicitly. The ambient one is never read, never written, and
	// never consulted for a current context.
	Kubeconfig string
	Client     *kubeapi.Client
}

var (
	once     sync.Once
	setup    *Cluster
	setupErr error
)

// Require returns a client to a guarded kind cluster, or skips the test when
// kind or Docker is not installed.
//
// The lifecycle, once per test binary:
//
//  1. Check kind and docker are on PATH and that Docker answers. Absent, skip
//     (or fail under SENRO_REQUIRE_KIND=1), exactly as dockertest.Require
//     does. This is the ONLY thing that skips.
//  2. Delete any existing cluster of this name, so a leftover from a killed
//     run cannot be silently reused with unknown state.
//  3. Create the cluster, and export its kubeconfig into a file in the test's
//     own temp directory. --kubeconfig is passed to both kind and kubectl on
//     every single invocation, so the developer's own kubeconfig is neither
//     read nor written at any point.
//  4. Run guard over that file. A failure here is a FAILURE, never a skip:
//     it means the harness is pointed at something it should not be.
//  5. Create the namespace, and register cleanup.
//
// Everything but step 5 is shared across the binary's tests through a
// sync.Once, so one cluster serves the whole package.
func Require(t *testing.T) *Cluster {
	t.Helper()
	once.Do(func() { setup, setupErr = provision() })

	var skip *skipError
	if errors.As(setupErr, &skip) {
		if os.Getenv(EnvRequire) == "1" {
			t.Fatalf("%s=1 is set, but %v. These tests were not run.", EnvRequire, skip.err)
		}
		t.Skipf("%v. Kubernetes executor tests were not run. Set %s=1 to make that a failure "+
			"instead of a skip.", skip.err, EnvRequire)
	}
	if setupErr != nil {
		t.Fatalf("preparing the kind cluster: %v", setupErr)
	}
	return setup
}

// skipError marks the one condition that skips rather than fails: the tools
// are not installed. Everything else, including every guard refusal, is a
// failure.
type skipError struct{ err error }

func (e *skipError) Error() string { return e.err.Error() }

func provision() (*Cluster, error) {
	if _, err := exec.LookPath("kind"); err != nil {
		return nil, &skipError{fmt.Errorf("kind is not installed: %w", err)}
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		return nil, &skipError{fmt.Errorf("kubectl is not installed: %w", err)}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, &skipError{fmt.Errorf("docker is not installed: %w", err)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := run(ctx, "", "docker", "info", "--format", "{{.ServerVersion}}"); err != nil {
		return nil, &skipError{fmt.Errorf("the Docker daemon did not answer: %w", err)}
	}

	// A directory of this process's own, not t.TempDir(): the cluster outlives
	// any single test, and so must the kubeconfig every command is pinned to.
	dir, err := os.MkdirTemp("", "senro-kindtest-")
	if err != nil {
		return nil, err
	}
	kubeconfigPath := filepath.Join(dir, "kubeconfig")

	// Delete first, unconditionally. A previous run that was killed leaves a
	// cluster behind, and reusing one whose state nothing here established is
	// how a test starts passing for the wrong reason.
	del, cancelDel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelDel()
	_, _ = run(del, kubeconfigPath, "kind", "delete", "cluster", "--name", ClusterName())

	create, cancelCreate := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancelCreate()
	if _, err := run(create, kubeconfigPath,
		"kind", "create", "cluster", "--name", ClusterName(),
		"--kubeconfig", kubeconfigPath, "--wait", "120s",
	); err != nil {
		return nil, fmt.Errorf("creating the kind cluster: %w", err)
	}

	c, err := connect(kubeconfigPath)
	if err != nil {
		// The cluster exists and the guard refused it, or the connection did
		// not come up. Either way it is this function's cluster to remove.
		_, _ = run(del, kubeconfigPath, "kind", "delete", "cluster", "--name", ClusterName())
		return nil, err
	}
	return c, nil
}

// connect reads the exported kubeconfig, guards it, and builds a client.
func connect(kubeconfigPath string) (*Cluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// `kubectl config view -o json`, with --kubeconfig pinned to the file kind
	// just wrote. JSON rather than the file's own YAML because the root module
	// has no YAML parser and this is not the place to acquire one.
	out, err := run(ctx, kubeconfigPath, "kubectl", "config", "view", "--raw", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("reading the exported kubeconfig: %w", err)
	}
	var cfg kubeconfig
	if err := json.Unmarshal([]byte(out), &cfg); err != nil {
		return nil, fmt.Errorf("parsing the exported kubeconfig: %w", err)
	}

	conn, err := guard(&cfg, ClusterName())
	if err != nil {
		return nil, err
	}

	cli, err := kubeapi.New(kubeapi.Config{
		Server: conn.Server, CAData: conn.CAData,
		CertData: conn.CertData, KeyData: conn.KeyData,
	})
	if err != nil {
		return nil, err
	}
	if _, err := cli.Ping(ctx); err != nil {
		return nil, fmt.Errorf("the guarded cluster at %s did not answer: %w", conn.Server, err)
	}

	// The namespace, created through the same pinned kubeconfig. Created only
	// when absent, so a second run against a kept cluster (SENRO_KIND_KEEP=1)
	// is not an error.
	if _, err := run(ctx, kubeconfigPath, "kubectl", "get", "namespace", Namespace); err != nil {
		// AlreadyExists is success: the read above can fail for unrelated
		// reasons on an apiserver that came up seconds ago, and one unlucky
		// read must not fail the run.
		if _, err := run(ctx, kubeconfigPath, "kubectl", "create", "namespace", Namespace); err != nil &&
			!strings.Contains(err.Error(), "AlreadyExists") {
			return nil, fmt.Errorf("creating namespace %s: %w", Namespace, err)
		}
	}

	return &Cluster{Conn: conn, Kubeconfig: kubeconfigPath, Client: cli}, nil
}

// Cleanup deletes the cluster. Registered by TestMain in the packages that use
// this, rather than by Require, because Require runs per test and the cluster
// is per binary.
//
// Idempotent: deleting a cluster that is not there succeeds, which is what
// makes it safe to call from a TestMain that may not have created one.
func Cleanup() {
	if os.Getenv(EnvKeep) == "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	path := ""
	if setup != nil {
		path = setup.Kubeconfig
	}
	_, _ = run(ctx, path, "kind", "delete", "cluster", "--name", ClusterName())
	if setup != nil {
		_ = os.RemoveAll(filepath.Dir(setup.Kubeconfig))
	}
}

// run executes one command with KUBECONFIG pinned to the file this run owns.
//
// The environment is built explicitly rather than inherited with one variable
// overwritten, so nothing the developer's shell exported can reach a kubectl
// or a kind invocation: not KUBECONFIG, and not the KUBE_* variables some
// setups use. PATH and HOME are passed through because kind needs to find
// docker and docker needs its context; a kubeconfig in HOME is irrelevant
// because KUBECONFIG below wins over it and is what kubectl reads.
func run(ctx context.Context, kubeconfigPath string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...) // #nosec G204 -- fixed test tooling
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	if kubeconfigPath != "" {
		env = append(env, "KUBECONFIG="+kubeconfigPath)
	} else {
		// No file at all rather than the ambient one. A command reaching here
		// with no kubeconfig is one that must not touch a cluster, and
		// pointing it at a path that does not exist is what makes that true
		// rather than merely intended.
		env = append(env, "KUBECONFIG="+filepath.Join(os.TempDir(), "senro-kindtest-no-kubeconfig"))
	}
	if v := os.Getenv("DOCKER_HOST"); v != "" {
		env = append(env, "DOCKER_HOST="+v)
	}
	cmd.Env = env
	var out strings.Builder
	cmd.Stdout = &out
	var errOut strings.Builder
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s %s: %w: %s",
			name, strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// Kubectl runs kubectl against the guarded cluster. It exists so no test
// ever writes its own exec.Command("kubectl", ...), which would inherit the
// ambient KUBECONFIG and could reach a production context.
func (c *Cluster) Kubectl(ctx context.Context, args ...string) (string, error) {
	return run(ctx, c.Kubeconfig, "kubectl", append([]string{"-n", Namespace}, args...)...)
}
