package kubeapi_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/kubeapi"
)

// ambient writes a kubeconfig shaped like a real developer machine's (a
// customer's production EKS cluster as the current context), points
// $KUBECONFIG and $HOME at it, and returns the path. If any production code
// path grows a kubeconfig fallback, this is the file the tests below would
// notice it reading.
func ambient(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	const doc = `apiVersion: v1
kind: Config
current-context: cm4-vodafone-0-p3
clusters:
- name: cm4-vodafone-0-p3
  cluster:
    server: https://A1B2C3D4E5F6.gr7.eu-west-1.eks.amazonaws.com
contexts:
- name: cm4-vodafone-0-p3
  context: {cluster: cm4-vodafone-0-p3, user: cm4-vodafone-0-p3}
users:
- name: cm4-vodafone-0-p3
  user: {token: a-real-production-token}
`
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".kube"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".kube", "config"), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", path)
	t.Setenv("HOME", dir)
	return path
}

// TestFromEnvIgnoresAnAmbientKubeconfig is the production-side counterpart of
// the kind guard. The guard stops the TEST harness reaching a cluster it did
// not create; this stops the EXECUTOR doing it.
//
// A kubeconfig naming a customer's production cluster is present, is the
// current context, and has a working credential in it. FromEnv must still
// fail, because nothing senro ships may pick a cluster the operator did not
// name.
func TestFromEnvIgnoresAnAmbientKubeconfig(t *testing.T) {
	path := ambient(t)
	t.Setenv(kubeapi.EnvServer, "")

	_, err := kubeapi.FromEnv()
	if err == nil {
		t.Fatal("FromEnv found a cluster with no SENRO_K8S_* variable set, which can only mean " +
			"it read the ambient kubeconfig")
	}
	for _, forbidden := range []string{path, "eks.amazonaws.com", "cm4-vodafone-0-p3"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Errorf("the error mentions %q, so something read the ambient kubeconfig: %v",
				forbidden, err)
		}
	}
	// It must also tell the operator what to do, since "not configured" is
	// the state every first-time user is in.
	if !strings.Contains(err.Error(), kubeapi.EnvServer) {
		t.Errorf("the error does not name the variable to set: %v", err)
	}
	if !strings.Contains(err.Error(), "KUBECONFIG") {
		t.Errorf("the error does not say senro deliberately ignores KUBECONFIG, so a user who "+
			"has one configured will assume this is a bug: %v", err)
	}
}

// TestFromEnvRefusesAServerWithNoCA: verification is not optional, and there
// is no flag here to turn it off.
func TestFromEnvRefusesAServerWithNoCA(t *testing.T) {
	ambient(t)
	t.Setenv(kubeapi.EnvServer, "https://10.0.0.1:6443")
	t.Setenv(kubeapi.EnvCAFile, "")
	if _, err := kubeapi.FromEnv(); err == nil {
		t.Fatal("FromEnv accepted a server with no CA bundle")
	}
}

// TestFromEnvRefusesAnAnonymousConnection. An apiserver that answers an
// unauthenticated request is not one to run a pipeline in, and finding that
// out at the first pod create is worse than finding it out at run start.
func TestFromEnvRefusesAnAnonymousConnection(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(kubeapi.EnvServer, "https://10.0.0.1:6443")
	t.Setenv(kubeapi.EnvCAFile, ca)
	t.Setenv(kubeapi.EnvToken, "")
	t.Setenv(kubeapi.EnvTokenFile, "")
	t.Setenv(kubeapi.EnvClientCert, "")
	t.Setenv(kubeapi.EnvClientKey, "")

	_, err := kubeapi.FromEnv()
	if err == nil {
		t.Fatal("FromEnv accepted a configuration with no credentials at all")
	}
	if !strings.Contains(err.Error(), "credential") {
		t.Errorf("error does not say what is missing: %v", err)
	}
}

// TestFromEnvRefusesHalfAClientCertificate: a certificate with no key
// authenticates nothing, and sending it would fail at the TLS handshake with
// a message about neither.
func TestFromEnvRefusesHalfAClientCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(kubeapi.EnvServer, "https://10.0.0.1:6443")
	t.Setenv(kubeapi.EnvCAFile, ca)
	t.Setenv(kubeapi.EnvClientCert, filepath.Join(dir, "cert.pem"))
	t.Setenv(kubeapi.EnvClientKey, "")
	if _, err := kubeapi.FromEnv(); err == nil {
		t.Fatal("FromEnv accepted a client certificate with no key")
	}
}

// TestNewRefusesACABundleThatIsNotOne closes the path where a configuration
// is present, complete, and points at a file that is not a certificate: an
// empty pool would verify nothing while looking configured.
func TestNewRefusesACABundleThatIsNotOne(t *testing.T) {
	_, err := kubeapi.New(kubeapi.Config{
		Server: "https://10.0.0.1:6443",
		CAData: []byte("this is not a certificate"),
		Token:  "t",
	})
	if err == nil {
		t.Fatal("New accepted a CA bundle containing no certificate")
	}
	if !strings.Contains(err.Error(), "PEM") {
		t.Errorf("error does not say what was wrong with the bundle: %v", err)
	}
}
