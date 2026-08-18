package kindtest

import (
	"strings"
	"testing"
)

// kindLike is the kubeconfig `kind export kubeconfig` writes for a cluster
// named "senro-executor": one context, one cluster, one user, a loopback
// server, and client-certificate credentials.
func kindLike() *kubeconfig {
	return &kubeconfig{
		CurrentContext: "kind-senro-executor",
		Clusters: []namedCluster{{
			Name: "kind-senro-executor",
			Cluster: clusterEntry{
				Server:                   "https://127.0.0.1:52413",
				CertificateAuthorityData: "LS0tLUNB",
			},
		}},
		Contexts: []namedContext{{
			Name:    "kind-senro-executor",
			Context: contextEntry{Cluster: "kind-senro-executor", User: "kind-senro-executor"},
		}},
		Users: []namedUser{{
			Name: "kind-senro-executor",
			User: userEntry{ClientCertificateData: "LS0tLUNFUlQ=", ClientKeyData: "LS0tLUtFWQ=="},
		}},
	}
}

// remoteLike is shaped on the contexts this development machine actually
// carries: twenty-four EKS clusters and two GKE ones, every single one a
// customer's live cloud. Nothing in this repository may ever create a pod in
// one of them, and this is the fixture that says so in code.
func remoteLike() *kubeconfig {
	return &kubeconfig{
		CurrentContext: "cm4-vodafone-0-p3",
		Clusters: []namedCluster{{
			Name: "cm4-vodafone-0-p3",
			Cluster: clusterEntry{
				Server:                   "https://A1B2C3D4E5F6.gr7.eu-west-1.eks.amazonaws.com",
				CertificateAuthorityData: "LS0tLUNB",
			},
		}},
		Contexts: []namedContext{{
			Name:    "cm4-vodafone-0-p3",
			Context: contextEntry{Cluster: "cm4-vodafone-0-p3", User: "cm4-vodafone-0-p3"},
		}},
		Users: []namedUser{{
			Name: "cm4-vodafone-0-p3",
			User: userEntry{ClientCertificateData: "LS0tLUNFUlQ=", ClientKeyData: "LS0tLUtFWQ=="},
		}},
	}
}

// TestTheGuardRefusesARemoteContext is the test this whole package exists
// for. A kubeconfig naming a customer's production EKS cluster must not
// yield a connection, and the refusal must be a refusal: no Conn, an error,
// and no partially-populated server address a caller could still dial.
func TestTheGuardRefusesARemoteContext(t *testing.T) {
	conn, err := guard(remoteLike(), "senro-executor")
	if err == nil {
		t.Fatal("guard accepted a remote EKS context; it must refuse every cluster kind did not create")
	}
	if conn.Server != "" || conn.Context != "" || len(conn.CAData)+len(conn.CertData)+len(conn.KeyData) != 0 {
		t.Fatalf("guard refused but still returned a connection: %+v", conn)
	}
	for _, want := range []string{"cm4-vodafone-0-p3", "kind-senro-executor"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q, so a reader cannot tell what was found or expected",
				err, want)
		}
	}
}

// TestTheGuardAcceptsTheClusterItAskedFor is the other half: a refusal that
// refuses everything is not a guard, it is a broken build.
func TestTheGuardAcceptsTheClusterItAskedFor(t *testing.T) {
	conn, err := guard(kindLike(), "senro-executor")
	if err != nil {
		t.Fatalf("guard refused the kind cluster it was pointed at: %v", err)
	}
	if conn.Server != "https://127.0.0.1:52413" {
		t.Errorf("Server = %q", conn.Server)
	}
	if len(conn.CAData) == 0 || len(conn.CertData) == 0 || len(conn.KeyData) == 0 {
		t.Errorf("guard passed but returned incomplete credentials: %+v", conn)
	}
}

// TestTheGuardRefusesAKindClusterWithTheWrongName stops one senro test run
// from tearing down or writing into a kind cluster somebody else on the same
// machine is using. "It is local" is not the property being checked; "it is
// the cluster this run created" is.
func TestTheGuardRefusesAKindClusterWithTheWrongName(t *testing.T) {
	cfg := kindLike()
	if _, err := guard(cfg, "somebody-elses-cluster"); err == nil {
		t.Fatal("guard accepted kind-senro-executor while asking for kind-somebody-elses-cluster")
	}
}

// TestTheGuardRefusesANonLoopbackServer covers the case a name check alone
// misses: a context called kind-senro-executor whose server is not on this
// machine at all. The context name is attacker-controlled in the sense that
// nothing stops a kubeconfig from using it, so the address is checked too.
func TestTheGuardRefusesANonLoopbackServer(t *testing.T) {
	cfg := kindLike()
	cfg.Clusters[0].Cluster.Server = "https://A1B2C3D4E5F6.gr7.eu-west-1.eks.amazonaws.com"
	_, err := guard(cfg, "senro-executor")
	if err == nil {
		t.Fatal("guard accepted a kind-named context pointing at a remote apiserver")
	}
	if !strings.Contains(err.Error(), "eks.amazonaws.com") {
		t.Errorf("error %q does not name the server it refused", err)
	}
}

// TestTheGuardRefusesAKubeconfigCarryingAnyOtherCluster is what makes the
// ambient kubeconfig unusable here even by accident. This machine's own file
// has twenty-six entries; a guard that only inspected current-context would
// pass it the moment somebody ran `kubectl config use-context kind-...`, and
// every other cluster would still be one `--context` flag away.
func TestTheGuardRefusesAKubeconfigCarryingAnyOtherCluster(t *testing.T) {
	cfg := kindLike()
	cfg.Clusters = append(cfg.Clusters, remoteLike().Clusters...)
	cfg.Contexts = append(cfg.Contexts, remoteLike().Contexts...)
	_, err := guard(cfg, "senro-executor")
	if err == nil {
		t.Fatal("guard accepted a kubeconfig that also carries a production cluster")
	}
	if !strings.Contains(err.Error(), "cm4-vodafone-0-p3") {
		t.Errorf("error %q does not name the extra cluster it refused", err)
	}
}

// TestTheGuardRefusesAnEmptyKubeconfig covers the failure mode where an
// export silently produced nothing: no contexts, no current-context, and a
// guard that reads "no mismatch found" as "safe to proceed".
func TestTheGuardRefusesAnEmptyKubeconfig(t *testing.T) {
	if _, err := guard(&kubeconfig{}, "senro-executor"); err == nil {
		t.Fatal("guard accepted an empty kubeconfig")
	}
}

// TestTheGuardRefusesCredentialsItCannotUse keeps the guard from handing back
// a Conn that dials anonymously. An apiserver that answers an unauthenticated
// request is not a cluster this suite should be talking to either.
func TestTheGuardRefusesCredentialsItCannotUse(t *testing.T) {
	cfg := kindLike()
	cfg.Users[0].User.ClientKeyData = ""
	if _, err := guard(cfg, "senro-executor"); err == nil {
		t.Fatal("guard accepted a kubeconfig with no client key")
	}
}
