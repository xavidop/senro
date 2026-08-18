package k8sexec_test

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/xavidop/senro/internal/executor/k8sexec"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/plan"
)

// A cluster that was briefly unreachable must not be unreachable for the
// rest of the run: resolve is memoized once per executor, but memoizing the
// FAILURE too (what a sync.Once does) would let one dropped packet
// permanently fail every step, with retry.OnInfra receiving the cached
// error without a request being made. sshexec draws the same line.
//
// The seam is a real apiserver-shaped TLS server that fails once and then
// answers, so the retry is a genuine second request rather than an
// assertion about a struct field.
func TestAClusterThatWasBrieflyUnreachableIsNotUnreachableForTheRun(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/version" {
			// Everything after /version is the platform probe; keep it happy
			// so the only variable in this test is the first call.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[{"status":{"nodeInfo":{"operatingSystem":"linux","architecture":"amd64"}}}]}`))
			return
		}
		if calls.Add(1) == 1 {
			// The dropped packet. Not a refusal, not a permanent condition:
			// exactly the transient this test exists for.
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"gitVersion":"v1.34.0"}`))
	}))
	t.Cleanup(srv.Close)

	cli, err := kubeapi.New(kubeapi.Config{
		Server: srv.URL,
		CAData: pemOf(t, srv),
		Token:  "test-token",
	})
	if err != nil {
		t.Fatalf("kubeapi.New: %v", err)
	}

	ex, err := k8sexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorK8s, Namespace: "senro-test", Image: "busybox@sha256:0000000000000000000000000000000000000000000000000000000000000001"},
		nil, k8sexec.WithClient(cli))
	if err != nil {
		t.Fatalf("k8sexec.New: %v", err)
	}

	if _, err := ex.DeclaredPlatform(context.Background()); err == nil {
		t.Fatal("the first probe succeeded against a server that answered 502")
	}
	if _, err := ex.DeclaredPlatform(context.Background()); err != nil {
		t.Fatalf("the cluster answered on the second probe and the executor still refused it: %v\n"+
			"one transient failure has poisoned this executor for the whole run, and retry.OnInfra "+
			"cannot reach it because no request is made at all", err)
	}
	if got := calls.Load(); got < 2 {
		t.Errorf("/version was requested %d time(s): the second attempt never reached the apiserver, "+
			"so the failure was served from memory", got)
	}
}

// pemOf renders the test server's own certificate as a PEM bundle, which is
// what kubeapi.Config.CAData wants and what makes this a real TLS client
// against a real TLS server rather than verification turned off. kubeapi
// offers no way to skip verification, deliberately.
func pemOf(t *testing.T, srv *httptest.Server) []byte {
	t.Helper()
	cert := srv.Certificate()
	if cert == nil {
		t.Fatal("httptest server has no certificate")
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}
