package dockertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/s3"
)

// MinIOImage is the S3-compatible object store every remote-cache test in
// this repository runs against.
//
// A real server, not an in-process fake: a fake written alongside the
// client agrees with the client, including where the client is wrong.
// MinIO implements Signature Version 4 independently, so a request signed
// incorrectly is rejected here exactly as Amazon would reject it.
//
// Pinned to a dated release: an oracle that changes underneath the suite is
// not an oracle.
const MinIOImage = "minio/minio:RELEASE.2025-09-07T16-13-09Z"

// minioRoot are the credentials the server is started with. They are a test
// fixture and nothing else: they never leave this machine, and the point of
// having them at all is that a wrong signature has something real to fail
// against.
const (
	minioRootUser     = "senrotestroot"
	minioRootPassword = "senrotestrootsecret"
	// minioRegion is what the server is told to call itself. Every signature
	// is scoped to a region, so the tests and the server have to agree on one.
	minioRegion = "us-east-1"
	// minioPort is the port MinIO serves the S3 API on inside the container.
	minioPort = 9000
	// minioBuckets is how many buckets are created before the server
	// starts. Handing each test its own out of a fixed pool keeps tests
	// from seeing each other's objects with nothing created at runtime.
	minioBuckets = 64
	// minioDataDir is where the server keeps its objects, INSIDE the
	// container. Deliberately not a bind mount: MinIO refuses writes below
	// a free-space threshold, and a bind mount would make that the
	// developer's own nearly-full laptop disk. Nothing needs the objects to
	// outlive the container.
	minioDataDir = "/data"
)

// MinIO is one test's handle on the shared object store: its own bucket, and
// the endpoint and credentials to reach it.
type MinIO struct {
	// Endpoint is the base URL, "http://127.0.0.1:<port>". Plain HTTP: this
	// is loopback to a container on the same machine, and a self-signed
	// certificate would only be testing Go's TLS stack.
	Endpoint string
	Region   string
	// Bucket is this test's own bucket. No other test in this binary is given
	// the same one.
	Bucket    string
	AccessKey string
	SecretKey string
}

// RequireMinIO returns a live S3-compatible object store, or skips the test
// with a reason.
//
// The server is started once per test binary and shared; each caller gets
// its own bucket. Skipping follows Require's rule: SENRO_REQUIRE_DOCKER=1
// turns every skip into a failure. A test binary calling this MUST route
// its TestMain through RunMain, which stops the container outliving the
// run.
func RequireMinIO(t *testing.T) MinIO {
	t.Helper()
	// Gate on the daemon first, so a machine with no Docker gets Require's
	// own carefully worded skip rather than a failure from further in.
	_ = Require(t)

	srv, err := sharedMinIO()
	if err != nil {
		t.Fatalf("starting %s: %v. This test needs a live object store and did not run.", MinIOImage, err)
	}
	// A store that is up but refusing writes (MinIO's free-space threshold)
	// would turn every test into a confusing 507; skip with a reason
	// instead, same ruling as Require's missing daemon.
	if err := srv.writable(); err != nil {
		if os.Getenv("SENRO_REQUIRE_DOCKER") == "1" {
			t.Fatalf("SENRO_REQUIRE_DOCKER=1 is set, but the object store cannot accept writes: %v. "+
				"This test was not run.", err)
		}
		t.Skipf("the object store cannot accept writes: %v. Tests needing one were not run. "+
			"On a nearly full disk, free some space. Set SENRO_REQUIRE_DOCKER=1 to make this a "+
			"failure instead of a skip.", err)
	}

	n := minioNextBucket.Add(1) - 1
	if n >= minioBuckets {
		t.Fatalf("this test binary asked for more than %d buckets; raise minioBuckets in %s",
			minioBuckets, "internal/dockerd/dockertest/minio.go")
	}
	return MinIO{
		Endpoint:  "http://" + srv.addr,
		Region:    minioRegion,
		Bucket:    bucketName(int(n)),
		AccessKey: minioRootUser,
		SecretKey: minioRootPassword,
	}
}

// RunMain runs a package's tests and then stops whatever container they
// started:
//
//	func TestMain(m *testing.M) { os.Exit(dockertest.RunMain(m)) }
//
// Every shared server is stopped whether or not the binary asked for one:
// stopping the unstarted is a no-op, and a caller-maintained list is a leak
// waiting to happen. No t.Cleanup can do this: the first test to finish
// would take the server away from the rest.
func RunMain(m *testing.M) int {
	code := m.Run()
	StopSharedMinIO()
	StopSharedRegistry()
	return code
}

// StopSharedMinIO removes the container this package started, if any.
// Called by RunMain; exported for the one package whose TestMain does
// unrelated work. Safe when nothing was started, and safe twice. A package
// that never calls this leaks a container per test binary, which has
// exhausted the daemon's capacity before (kind could not create a cluster).
func StopSharedMinIO() { stopSharedMinIO() }

func bucketName(n int) string { return fmt.Sprintf("senro-cache-%02d", n) }

// probeBucket is reserved for writable's check and handed to no test, so a
// failed probe cannot leave a stray object in a bucket a test is asserting
// about.
const probeBucket = "senro-cache-probe"

// probeGrace is how long writable keeps trying before believing a refusal.
//
// MinIO has a state between "listening" and "working": the liveness
// endpoint answers 200 while drives are still coming up, and a write in
// that window gets 503 XMinioServerNotInitialized. The client's own retry
// budget is sized for a blip, not a cold container, and the sync.Once below
// makes the FIRST probe's verdict the verdict for the whole test binary, so
// one unlucky quarter-second would fail every object-store test.
const probeGrace = 60 * time.Second

// writable reports whether the store will actually accept an object,
// checked once per test binary by writing one: every proxy is worse (host
// free space says nothing about MinIO's threshold). The probe uses this
// repository's own client, acceptable because a broken client fails the
// probe loudly rather than passing wrongly. Retried until probeGrace runs
// out, reporting the LAST error: a genuinely refusing store refuses for
// sixty seconds as readily as for one, so the grace costs nothing in the
// case that matters.
func (s *minioServer) writable() error {
	s.probeOnce.Do(func() {
		c, err := s3.New(s3.Config{
			Endpoint: "http://" + s.addr, Region: minioRegion, Bucket: probeBucket,
			AccessKeyID: minioRootUser, SecretAccessKey: minioRootPassword,
			PathStyle: true, Timeout: 30 * time.Second,
		})
		if err != nil {
			s.probeErr = err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), probeGrace)
		defer cancel()
		deadline := time.Now().Add(probeGrace)
		for {
			s.probeErr = c.PutBytes(ctx, "probe", []byte("senro dockertest write probe"))
			if s.probeErr == nil || time.Now().After(deadline) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	})
	return s.probeErr
}

var (
	minioOnce       sync.Once
	minioSrv        *minioServer
	minioStartErr   error
	minioNextBucket atomic.Int64
)

type minioServer struct {
	client *dockerd.Client
	id     string
	addr   string

	probeOnce sync.Once
	probeErr  error
}

// reapAbandonedMinIOs removes servers left behind by a test process that is
// no longer running: TestMain covers every ordinary exit but a SIGKILL runs
// no cleanup, and enough abandoned servers can leave the daemon unable to
// start another container.
//
// Ownership is by pid, not age: suites run concurrently and an age cutoff
// would remove a server a sibling package is using; a gone pid is
// unambiguous. Best-effort throughout: a failure here must never fail a
// test.
func reapAbandonedMinIOs(ctx context.Context, client *dockerd.Client) {
	ReapAbandoned(ctx, client, "minio")
}

// ReapAbandoned removes containers labelled senro.test=<kind> whose
// creating test process is gone. Exported because more than one harness
// starts a long-lived container (MinIO here, sshdtest's sshd); two copies
// of this fix would drift. A caller must label its container with what
// OwnerLabels builds.
func ReapAbandoned(ctx context.Context, client *dockerd.Client, kind string) {
	ids, err := client.ContainerList(ctx, map[string]string{"senro.test": kind})
	if err != nil {
		return
	}
	for _, id := range ids {
		raw, err := client.ContainerInspectRaw(ctx, id)
		if err != nil {
			continue
		}
		var doc struct {
			Config struct {
				Labels map[string]string `json:"Labels"`
			} `json:"Config"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			continue
		}
		pid, err := strconv.Atoi(doc.Config.Labels["senro.test.pid"])
		if err != nil || pid == os.Getpid() || pidAlive(pid) {
			continue
		}
		_ = client.ContainerRemove(ctx, id)
	}
}

// OwnerLabels are the labels a reapable test container must carry: what it
// is, and which test process owns it. One constructor so the harnesses
// cannot spell the keys differently and leave a container invisible to the
// reaper.
func OwnerLabels(kind string) map[string]string {
	return map[string]string{
		"senro.test":     kind,
		"senro.test.pid": strconv.Itoa(os.Getpid()),
	}
}

// pidAlive mirrors internal/attachsrv's own, and treats an unclassifiable
// error as alive: wrongly keeping an abandoned container costs a little disk,
// wrongly removing a live one breaks somebody else's test run.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	switch {
	case err == nil:
		return true
	case errors.Is(err, syscall.ESRCH):
		return false
	default:
		return true
	}
}

func sharedMinIO() (*minioServer, error) {
	minioOnce.Do(func() { minioSrv, minioStartErr = startMinIO() })
	return minioSrv, minioStartErr
}

func stopSharedMinIO() {
	if minioSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Removing the container removes its filesystem, and with it every object
	// the tests wrote: nothing is meant to outlive the run.
	_ = minioSrv.client.ContainerRemove(ctx, minioSrv.id)
	_ = minioSrv.client.Close()
	minioSrv = nil
}

// startMinIO creates, starts and waits for the server.
//
// It opens its own daemon client rather than borrowing a test's: the client a
// test received from Require is closed when that test finishes, and this
// container has to outlive it.
func startMinIO() (*minioServer, error) {
	client, err := dockerd.Open()
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = client.Close()
		}
	}()

	// Reap abandoned servers here rather than on a timer: a suite about to
	// start a container has already decided to talk to the daemon.
	reapCtx, reapCancel := context.WithTimeout(context.Background(), 15*time.Second)
	reapAbandonedMinIOs(reapCtx, client)
	reapCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if _, found, err := client.ImageInspect(ctx, MinIOImage); err != nil || !found {
		if err := client.ImagePull(ctx, MinIOImage, nil); err != nil {
			return nil, err
		}
	}

	// Buckets are directories under the data directory, created before the
	// server starts so no test needs a working client to set up the check
	// of that same client. Made by the container's own first command
	// because the data directory is not on the host (see minioDataDir).
	buckets := make([]string, 0, minioBuckets+1)
	for i := range minioBuckets {
		buckets = append(buckets, minioDataDir+"/"+bucketName(i))
	}
	buckets = append(buckets, minioDataDir+"/"+probeBucket)

	id, err := client.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image:      MinIOImage,
		Entrypoint: []string{"sh", "-c"},
		Cmd: []string{fmt.Sprintf("mkdir -p %s && exec minio server %s --address :%d",
			strings.Join(buckets, " "), minioDataDir, minioPort)},
		Env: []string{
			"MINIO_ROOT_USER=" + minioRootUser,
			"MINIO_ROOT_PASSWORD=" + minioRootPassword,
			"MINIO_REGION=" + minioRegion,
			// Off, loudly: an update check is a network call this suite has no
			// use for and that fails noisily on a machine with no route out.
			"MINIO_UPDATE=off",
		},
		Ports:  []dockerd.Port{{Container: minioPort}},
		Labels: OwnerLabels("minio"),
	})
	if err != nil {
		return nil, err
	}
	srv := &minioServer{client: client, id: id}
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
	addr, err := waitForPort(ctx, client, id, minioPort)
	if err != nil {
		return nil, err
	}
	srv.addr = addr
	if err := waitForHealth(ctx, "http://"+addr+"/minio/health/live"); err != nil {
		return nil, err
	}
	ok = true
	return srv, nil
}

// waitForPort polls until the daemon reports the published host port. There
// is a window after start in which the container exists and the binding does
// not yet, and it is not an error.
func waitForPort(ctx context.Context, client *dockerd.Client, id string, port int) (string, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		addr, ok, err := client.ContainerHostAddress(ctx, id, port)
		if err != nil {
			return "", err
		}
		if ok {
			return addr, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("container %s published no host port for %d within 60s", id[:12], port)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// waitForHealth polls MinIO's own liveness endpoint. A published port answers
// before the server behind it does, so connecting is not enough.
func waitForHealth(ctx context.Context, url string) error {
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
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Errorf("%s answered %s", url, resp.Status)
		} else {
			last = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s never became healthy within 90s: %w", url, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
