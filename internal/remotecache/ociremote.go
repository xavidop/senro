package remotecache

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/xavidop/senro/internal/oci"
)

// OCIConfig is everything needed to open a shared object store held in an
// OCI registry. It exists beside Config rather than inside it because the
// two backends agree on nothing that could be shared: one struct holding
// both would be a struct where half the fields are always wrong.
type OCIConfig struct {
	// Registry is the host and optional port: "ghcr.io",
	// "registry.internal:5000".
	Registry string
	// Repository is the path inside it that holds the cache, such as
	// "acme/senro-cache".
	Repository string

	// Username and Password are the credential presented to the registry's
	// token endpoint. See oci.Config for what senro does and does not do to
	// resolve one.
	Username string
	Password string

	// PlainHTTP talks to the registry over http rather than https, for a
	// registry on a trusted network that serves no certificate.
	PlainHTTP bool

	// Timeout bounds one request. Zero means oci.DefaultTimeout.
	Timeout time.Duration

	// ReadOnly reads the shared cache and never writes to it; see
	// Config.ReadOnly.
	ReadOnly bool

	// Report receives every degradation. Optional: the report also goes to
	// ReportWriter regardless, so a caller that wires nothing still finds out.
	Report func(Degradation)

	// ReportWriter is where the human-readable degradation line goes. Zero
	// means os.Stderr, for the same reason Config.ReportWriter does.
	ReportWriter io.Writer

	// Transport is the HTTP round tripper requests go through. Zero means the
	// client's own. It exists for the tests, and specifically for failures a
	// real registry will not produce on demand.
	Transport http.RoundTripper
}

// OpenOCI validates the config and prepares a shared cache held in a
// registry repository. No I/O: reachability is discovered on first use and
// answered by degrading.
//
// It returns the same *Remote that Open does, and that is the point: a
// registry is a place to keep the cache, not a different cache, so a run
// configured for one behaves identically to one configured for a bucket,
// including when it is down. A configuration that cannot possibly work is
// an error here, at startup, for the reason Open gives.
func OpenOCI(cfg OCIConfig) (*Remote, error) {
	client, err := oci.New(oci.Config{
		Registry:   cfg.Registry,
		Repository: cfg.Repository,
		Username:   cfg.Username,
		Password:   cfg.Password,
		PlainHTTP:  cfg.PlainHTTP,
		Timeout:    cfg.Timeout,
		Transport:  cfg.Transport,
	})
	if err != nil {
		return nil, fmt.Errorf("remote cache: %w", err)
	}
	w := cfg.ReportWriter
	if w == nil {
		w = os.Stderr
	}
	deg := &degrader{store: client.String(), report: cfg.Report, w: w}

	// One config blob for both stores: they push the identical two bytes, and
	// a second copy of the flag would mean a second needless request.
	config := &ociConfigBlob{}
	objects := &OCIObjects{client: client, readOnly: cfg.ReadOnly, config: config}
	docs := &ociDocs{client: client, config: config}
	return &Remote{
		name:    client.String(),
		deg:     deg,
		objects: objects,
		entries: &Entries{docs: docs, readOnly: cfg.ReadOnly, deg: deg},
		runLogs: &RunLogs{objects: objects, docs: docs, readOnly: cfg.ReadOnly, deg: deg},
	}, nil
}
