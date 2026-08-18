package kubeapi

import (
	"fmt"
	"os"
	"strings"
)

// Environment variables FromEnv reads. Nothing else is consulted, and in
// particular KUBECONFIG and ~/.kube/config are not: see this package's doc
// for why a Kubernetes executor must never inherit an ambient context.
const (
	EnvServer     = "SENRO_K8S_SERVER"
	EnvCAFile     = "SENRO_K8S_CA_FILE"
	EnvTokenFile  = "SENRO_K8S_TOKEN_FILE"
	EnvToken      = "SENRO_K8S_TOKEN"
	EnvClientCert = "SENRO_K8S_CLIENT_CERT_FILE"
	EnvClientKey  = "SENRO_K8S_CLIENT_KEY_FILE"
)

// Config is one apiserver connection, in full. Every field is supplied by the
// caller: there is no discovery step and no fallback.
type Config struct {
	// Server is the apiserver's base URL, "https://10.0.0.1:6443". Required.
	Server string
	// CAData is the PEM bundle the apiserver's certificate is verified
	// against. Required: an empty bundle would mean either trusting the
	// host's root store for a certificate almost no cluster has, or skipping
	// verification, and this package offers no way to skip verification.
	CAData []byte
	// Token is a bearer token, and CertData/KeyData are a client certificate
	// and key. Exactly one of the two forms must be present. Kubernetes
	// accepts both; kind issues a client certificate, a ServiceAccount holds
	// a token.
	Token    string
	CertData []byte
	KeyData  []byte
}

// FromEnv builds a Config from the SENRO_K8S_* variables.
//
// The credential files are read here rather than remembered as paths, so a
// token file that is unreadable fails at run start with the path named,
// rather than at the first pod create with a 401 the operator has to work
// backwards from.
//
// Inline SENRO_K8S_TOKEN exists because there are environments where a file
// is genuinely awkward, but SENRO_K8S_TOKEN_FILE is the better one: an
// environment variable is visible in /proc/<pid>/environ, is inherited by
// every child process the coordinator spawns, and lands in a crash dump.
func FromEnv() (Config, error) {
	var cfg Config
	cfg.Server = strings.TrimSpace(os.Getenv(EnvServer))
	if cfg.Server == "" {
		return Config{}, fmt.Errorf(
			"kubeapi: %s is not set, so there is no cluster to talk to. senro never reads "+
				"KUBECONFIG or ~/.kube/config for this: a pipeline that deploys into whichever "+
				"cluster your shell last selected is how work lands in the wrong company's cloud. "+
				"Set %s, %s, and either %s or both %s and %s",
			EnvServer, EnvServer, EnvCAFile, EnvTokenFile, EnvClientCert, EnvClientKey)
	}

	caFile := strings.TrimSpace(os.Getenv(EnvCAFile))
	if caFile == "" {
		return Config{}, fmt.Errorf(
			"kubeapi: %s is not set. The apiserver's certificate has to be verified against the "+
				"cluster's own CA, and senro will not fall back to skipping verification",
			EnvCAFile)
	}
	ca, err := os.ReadFile(caFile) // #nosec G304 -- the operator names this file on purpose
	if err != nil {
		return Config{}, fmt.Errorf("kubeapi: reading %s=%s: %w", EnvCAFile, caFile, err)
	}
	cfg.CAData = ca

	if f := strings.TrimSpace(os.Getenv(EnvTokenFile)); f != "" {
		b, err := os.ReadFile(f) // #nosec G304 -- the operator names this file on purpose
		if err != nil {
			return Config{}, fmt.Errorf("kubeapi: reading %s=%s: %w", EnvTokenFile, f, err)
		}
		cfg.Token = strings.TrimSpace(string(b))
	} else if t := strings.TrimSpace(os.Getenv(EnvToken)); t != "" {
		cfg.Token = t
	}

	certFile := strings.TrimSpace(os.Getenv(EnvClientCert))
	keyFile := strings.TrimSpace(os.Getenv(EnvClientKey))
	if (certFile == "") != (keyFile == "") {
		return Config{}, fmt.Errorf(
			"kubeapi: %s and %s must be set together; a client certificate without its key "+
				"authenticates nothing", EnvClientCert, EnvClientKey)
	}
	if certFile != "" {
		cert, err := os.ReadFile(certFile) // #nosec G304 -- the operator names this file on purpose
		if err != nil {
			return Config{}, fmt.Errorf("kubeapi: reading %s=%s: %w", EnvClientCert, certFile, err)
		}
		key, err := os.ReadFile(keyFile) // #nosec G304 -- the operator names this file on purpose
		if err != nil {
			return Config{}, fmt.Errorf("kubeapi: reading %s=%s: %w", EnvClientKey, keyFile, err)
		}
		cfg.CertData, cfg.KeyData = cert, key
	}

	return cfg, cfg.check()
}

// check refuses a Config that could not authenticate. It is called by both
// FromEnv and New, so a caller that built a Config by hand (the kind test
// harness does) gets the same refusals as one that read the environment.
func (c Config) check() error {
	if strings.TrimSpace(c.Server) == "" {
		return fmt.Errorf("kubeapi: no apiserver address")
	}
	if len(c.CAData) == 0 {
		return fmt.Errorf("kubeapi: no CA bundle for %s", c.Server)
	}
	hasCert := len(c.CertData) > 0 && len(c.KeyData) > 0
	if c.Token == "" && !hasCert {
		return fmt.Errorf(
			"kubeapi: no credentials for %s: set %s (or %s), or both %s and %s. An anonymous "+
				"connection is refused here rather than at the apiserver, because a cluster that "+
				"answers anonymous requests is not one to run a pipeline in",
			c.Server, EnvTokenFile, EnvToken, EnvClientCert, EnvClientKey)
	}
	return nil
}
