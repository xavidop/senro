// Package kindtest gates the tests that need a real Kubernetes cluster, and
// pins that cluster to one this test run created with kind.
//
// A developer machine's kubeconfig is not a test fixture: it commonly holds
// dozens of contexts, all of them customers' remote production clusters. A
// test that respected it (or shelled out to kubectl without --kubeconfig)
// would create pods in somebody's live cloud on the first run. So nothing
// here reads $KUBECONFIG, ~/.kube/config, or the ambient current-context:
// Require creates its own cluster, exports a kubeconfig to its own temp
// file, and runs guard over it before a single request is made.
//
// Fail closed: Conn is the only way to reach a cluster from this package,
// so a failed check cannot be walked past. An unexpected cluster is a
// FAILURE, never a skip; only the absence of kind or Docker skips, as
// dockertest.Require does.
package kindtest

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// contextPrefix is what kind names the context it exports: a cluster created
// as `kind create cluster --name foo` is reachable as context "kind-foo".
const contextPrefix = "kind-"

// Conn is a verified connection to a kind cluster: everything kubeapi.New
// needs, and nothing a caller could have assembled without passing guard.
//
// The credential fields are the DECODED bytes rather than the kubeconfig's
// base64, so a caller has no decoding step of its own in which to accept a
// value guard rejected.
type Conn struct {
	// Context is the kubeconfig context name that was verified, kept so a
	// failure message can name the cluster the request actually went to.
	Context  string
	Server   string
	CAData   []byte
	CertData []byte
	KeyData  []byte
}

// kubeconfig is the subset of a kubeconfig document this package reads.
// Parsed from JSON (`kubectl config view --raw -o json`), not YAML: the
// root module has no YAML parser. The production client never parses a
// kubeconfig at all, so these types stay in test-only code where they
// cannot become a path to an ambient cluster.
type kubeconfig struct {
	CurrentContext string         `json:"current-context"`
	Clusters       []namedCluster `json:"clusters"`
	Contexts       []namedContext `json:"contexts"`
	Users          []namedUser    `json:"users"`
}

type namedCluster struct {
	Name    string       `json:"name"`
	Cluster clusterEntry `json:"cluster"`
}

type clusterEntry struct {
	Server                   string `json:"server"`
	CertificateAuthorityData string `json:"certificate-authority-data"`
}

type namedContext struct {
	Name    string       `json:"name"`
	Context contextEntry `json:"context"`
}

type contextEntry struct {
	Cluster string `json:"cluster"`
	User    string `json:"user"`
}

type namedUser struct {
	Name string    `json:"name"`
	User userEntry `json:"user"`
}

type userEntry struct {
	ClientCertificateData string `json:"client-certificate-data"`
	ClientKeyData         string `json:"client-key-data"`
}

// guard verifies that cfg describes exactly the kind cluster named want,
// and nothing else. Six checks, each closing a way the previous alone could
// be satisfied by a cluster nobody meant to touch:
//
//  1. The current context is "kind-<want>".
//  2. Every context, and (3) every cluster, in the file is that one: extra
//     entries mean somebody's real kubeconfig arrived, each one --context
//     flag away.
//  4. The context's cluster resolves and is the kind one, tying (1) to (5).
//  5. The server is on loopback: kind publishes on 127.0.0.1, so a remote
//     address under a kind- name is a contradiction. This survives a
//     context deliberately named to look local.
//  6. The credentials are complete: a Conn missing its key would dial
//     anonymously.
func guard(cfg *kubeconfig, want string) (Conn, error) {
	wantCtx := contextPrefix + want

	if cfg.CurrentContext != wantCtx {
		return Conn{}, fmt.Errorf(
			"kindtest: refusing to use kubeconfig context %q: this suite only ever talks to the "+
				"kind cluster it created itself, which is context %q. Nothing here may reach a "+
				"cluster senro did not create",
			cfg.CurrentContext, wantCtx)
	}
	for _, c := range cfg.Contexts {
		if c.Name != wantCtx {
			return Conn{}, fmt.Errorf(
				"kindtest: kubeconfig also carries context %q alongside %q; this looks like an "+
					"ambient kubeconfig rather than the one this test exported, and every extra "+
					"context in it is a cluster one --context flag away",
				c.Name, wantCtx)
		}
	}
	for _, c := range cfg.Clusters {
		if c.Name != wantCtx {
			return Conn{}, fmt.Errorf(
				"kindtest: kubeconfig also carries cluster %q alongside %q; this looks like an "+
					"ambient kubeconfig rather than the one this test exported",
				c.Name, wantCtx)
		}
	}

	ctx, ok := findContext(cfg, wantCtx)
	if !ok {
		return Conn{}, fmt.Errorf(
			"kindtest: kubeconfig names %q as its current context but defines no such context",
			wantCtx)
	}
	if ctx.Context.Cluster != wantCtx {
		return Conn{}, fmt.Errorf(
			"kindtest: context %q points at cluster %q, and kind names a cluster after its own "+
				"context; this kubeconfig was not produced by `kind export kubeconfig`",
			wantCtx, ctx.Context.Cluster)
	}

	cluster, ok := findCluster(cfg, ctx.Context.Cluster)
	if !ok {
		return Conn{}, fmt.Errorf("kindtest: kubeconfig defines no cluster %q", ctx.Context.Cluster)
	}
	if err := checkLoopback(cluster.Cluster.Server); err != nil {
		return Conn{}, err
	}

	user, ok := findUser(cfg, ctx.Context.User)
	if !ok {
		return Conn{}, fmt.Errorf("kindtest: kubeconfig defines no user %q", ctx.Context.User)
	}

	ca, err := decode("certificate-authority-data", cluster.Cluster.CertificateAuthorityData)
	if err != nil {
		return Conn{}, err
	}
	cert, err := decode("client-certificate-data", user.User.ClientCertificateData)
	if err != nil {
		return Conn{}, err
	}
	key, err := decode("client-key-data", user.User.ClientKeyData)
	if err != nil {
		return Conn{}, err
	}

	return Conn{
		Context: wantCtx, Server: cluster.Cluster.Server,
		CAData: ca, CertData: cert, KeyData: key,
	}, nil
}

// checkLoopback refuses any apiserver that is not on this machine. A
// hostname other than "localhost" is refused outright rather than resolved:
// a name resolving to 127.0.0.1 today can resolve elsewhere tomorrow, and
// kind publishes a literal address, so refusing names removes the resolver
// from the trust chain at no cost.
func checkLoopback(server string) error {
	if server == "" {
		return fmt.Errorf("kindtest: kubeconfig cluster has no server address")
	}
	u, err := url.Parse(server)
	if err != nil {
		return fmt.Errorf("kindtest: cannot parse apiserver address %q: %w", server, err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf(
			"kindtest: apiserver %q is not https; kind always publishes https", server)
	}
	host := u.Hostname()
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf(
			"kindtest: refusing apiserver %q: it is not on the loopback interface, so it is not a "+
				"kind cluster on this machine whatever its context is called. This suite must "+
				"never reach a remote cluster",
			server)
	}
	return nil
}

func findContext(cfg *kubeconfig, name string) (namedContext, bool) {
	for _, c := range cfg.Contexts {
		if c.Name == name {
			return c, true
		}
	}
	return namedContext{}, false
}

func findCluster(cfg *kubeconfig, name string) (namedCluster, bool) {
	for _, c := range cfg.Clusters {
		if c.Name == name {
			return c, true
		}
	}
	return namedCluster{}, false
}

func findUser(cfg *kubeconfig, name string) (namedUser, bool) {
	for _, u := range cfg.Users {
		if u.Name == name {
			return u, true
		}
	}
	return namedUser{}, false
}

// decode turns one base64 kubeconfig field into bytes, refusing an empty one
// by name so the failure says which credential is missing.
func decode(field, v string) ([]byte, error) {
	if strings.TrimSpace(v) == "" {
		return nil, fmt.Errorf(
			"kindtest: kubeconfig has no %s; a connection without it would be anonymous", field)
	}
	b, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return nil, fmt.Errorf("kindtest: %s is not valid base64: %w", field, err)
	}
	return b, nil
}
