package remotecache_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/s3"
)

func TestMain(m *testing.M) { os.Exit(dockertest.RunMain(m)) }

// reports collects what a degraded remote said, so a test can assert that it
// said it once and said something useful.
type reports struct {
	mu   sync.Mutex
	seen []remotecache.Degradation
}

func (r *reports) add(d remotecache.Degradation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, d)
}

func (r *reports) all() []remotecache.Degradation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]remotecache.Degradation(nil), r.seen...)
}

func (r *reports) disabled() int {
	n := 0
	for _, d := range r.all() {
		if d.Disabled {
			n++
		}
	}
	return n
}

// bucketConfig is a config for a live MinIO bucket, with no report wired.
func bucketConfig(m dockertest.MinIO) remotecache.Config {
	pathStyle := true
	return remotecache.Config{
		Endpoint:        m.Endpoint,
		Region:          m.Region,
		Bucket:          m.Bucket,
		AccessKeyID:     m.AccessKey,
		SecretAccessKey: m.SecretKey,
		PathStyle:       &pathStyle,
		Timeout:         30 * time.Second,
	}
}

// openLive opens a remote against a live bucket, capturing every degradation.
func openLive(t *testing.T, m dockertest.MinIO) (*remotecache.Remote, *reports) {
	t.Helper()
	return openWith(t, bucketConfig(m))
}

func openWith(t *testing.T, cfg remotecache.Config) (*remotecache.Remote, *reports) {
	t.Helper()
	rep := &reports{}
	cfg.Report = rep.add
	// Nothing may reach the real standard error from a test: a degradation
	// report is deliberately loud, and a suite that printed one per test
	// would be unreadable.
	cfg.ReportWriter = io.Discard
	r, err := remotecache.Open(cfg)
	if err != nil {
		t.Fatalf("remotecache.Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, rep
}

// localDir is a fresh local CAS, standing in for one machine's disk.
func localDir(t *testing.T) *cas.Dir {
	t.Helper()
	d, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	return d
}

// rawClient is a direct handle on the bucket, for planting the bytes a
// well-behaved writer would never produce.
func rawClient(t *testing.T, m dockertest.MinIO) *s3.Client {
	t.Helper()
	c, err := s3.New(s3.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
		PathStyle: true, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	return c
}

func readAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading an object: %v", err)
	}
	return b
}

// --- the remote store on its own -------------------------------------------

func TestObjectsRoundTripThroughARealBucket(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)
	ctx := t.Context()

	want := bytes.Repeat([]byte("a workspace tarball, more or less\n"), 500)
	d, err := r.Objects().Put(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if d != cas.FromBytes(want) {
		t.Fatalf("Put returned %s, want the digest of the plaintext %s", d, cas.FromBytes(want))
	}

	ok, err := r.Objects().Has(ctx, d)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatal("Has says an object that was just stored is not there")
	}

	rc, err := r.Objects().Get(ctx, d)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, want) {
		t.Errorf("Get returned %d bytes, want %d, and they differ", len(got), len(want))
	}

	if n := len(rep.all()); n != 0 {
		t.Errorf("a healthy remote reported %d degradations: %v", n, rep.all())
	}
}

func TestAnObjectThatWasNeverStoredIsAMiss(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)

	d := cas.FromBytes([]byte("never uploaded"))
	if _, err := r.Objects().Get(t.Context(), d); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get of an absent object = %v, want ErrNotFound", err)
	}
	ok, err := r.Objects().Has(t.Context(), d)
	if err != nil || ok {
		t.Errorf("Has of an absent object = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestATruncatedObjectIsRefused. A remote body cut short is an ordinary
// occurrence: a dropped connection, a proxy timeout, an upload that died.
func TestATruncatedObjectIsRefused(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)
	raw := rawClient(t, m)
	ctx := t.Context()

	plain := bytes.Repeat([]byte("the real content of this object\n"), 400)
	d, err := r.Objects().Put(ctx, bytes.NewReader(plain))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Read what is actually in the bucket and put back a prefix of it.
	stored, err := raw.Get(ctx, r.Objects().Name(d))
	if err != nil {
		t.Fatalf("reading the stored object: %v", err)
	}
	full := readAll(t, stored)
	if len(full) < 8 {
		t.Fatalf("the stored object is only %d bytes; this test needs something to truncate", len(full))
	}
	if err := raw.PutBytes(ctx, r.Objects().Name(d), full[:len(full)/2]); err != nil {
		t.Fatalf("planting a truncated object: %v", err)
	}

	rc, err := r.Objects().Get(ctx, d)
	if err != nil {
		if !errors.Is(err, cas.ErrCorrupt) {
			t.Fatalf("Get of a truncated object = %v, want ErrCorrupt", err)
		}
		return
	}
	defer func() { _ = rc.Close() }()
	if _, err := io.ReadAll(rc); !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading a truncated object = %v, want ErrCorrupt", err)
	}
}

// TestAnObjectWhoseBytesAreNotItsDigestIsRefused is the poisoning case: the
// object decodes perfectly and is simply not what was asked for.
func TestAnObjectWhoseBytesAreNotItsDigestIsRefused(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)
	raw := rawClient(t, m)
	ctx := t.Context()

	wanted := []byte("the content this build expects")
	other := []byte("the content an attacker or a bug substituted")

	d := cas.FromBytes(wanted)
	dOther, err := r.Objects().Put(ctx, bytes.NewReader(other))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Copy the other object's stored bytes, verbatim and perfectly valid, to
	// the key the wanted digest addresses.
	body := readAll(t, mustGet(t, raw, r.Objects().Name(dOther)))
	if err := raw.PutBytes(ctx, r.Objects().Name(d), body); err != nil {
		t.Fatalf("planting a swapped object: %v", err)
	}

	rc, err := r.Objects().Get(ctx, d)
	if err != nil {
		if !errors.Is(err, cas.ErrCorrupt) {
			t.Fatalf("Get of a swapped object = %v, want ErrCorrupt", err)
		}
		return
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading a swapped object = %v, want ErrCorrupt", err)
	}
	if bytes.Equal(got, other) && err == nil {
		t.Fatal("the wrong content was handed over intact, which is a poisoned build")
	}
}

func mustGet(t *testing.T, c *s3.Client, key string) io.ReadCloser {
	t.Helper()
	rc, err := c.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	return rc
}

// --- the tiered store: two machines, one bucket ----------------------------

// TestASecondMachineGetsWhatTheFirstOneStored is the entire point of the
// feature. Two separate local caches, one bucket.
func TestASecondMachineGetsWhatTheFirstOneStored(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	remoteA, _ := openLive(t, m)
	remoteB, _ := openLive(t, m)
	diskA, diskB := localDir(t), localDir(t)
	a := remoteA.TierObjects(diskA)
	b := remoteB.TierObjects(diskB)

	want := bytes.Repeat([]byte("what the first machine built\n"), 1000)
	d, err := a.Put(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("machine A Put: %v", err)
	}

	// B has never seen it locally.
	if ok, _ := diskB.Has(ctx, d); ok {
		t.Fatal("machine B already had the object locally; the test proves nothing")
	}
	ok, err := b.Has(ctx, d)
	if err != nil {
		t.Fatalf("machine B Has: %v", err)
	}
	if !ok {
		t.Fatal("machine B cannot see an object machine A stored, which is the whole feature")
	}

	rc, err := b.Get(ctx, d)
	if err != nil {
		t.Fatalf("machine B Get: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, want) {
		t.Errorf("machine B read %d bytes, want %d, and they differ", len(got), len(want))
	}

	// And the fetch warmed B's own disk, so the next run there is local.
	if ok, err := diskB.Has(ctx, d); err != nil || !ok {
		t.Errorf("after a remote fetch, machine B's local store has the object = (%v, %v), want true",
			ok, err)
	}
}

// TestACorruptRemoteObjectNeverReachesTheLocalStore. A poisoned bucket must
// not become a poisoned disk that outlives the run that fetched it.
func TestACorruptRemoteObjectNeverReachesTheLocalStore(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	writer, _ := openLive(t, m)
	raw := rawClient(t, m)

	wanted := []byte("what this build is supposed to receive")
	d := cas.FromBytes(wanted)
	dOther, err := writer.Objects().Put(ctx, bytes.NewReader([]byte("something else")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	body := readAll(t, mustGet(t, raw, writer.Objects().Name(dOther)))
	if err := raw.PutBytes(ctx, writer.Objects().Name(d), body); err != nil {
		t.Fatalf("planting a swapped object: %v", err)
	}

	reader, rep := openLive(t, m)
	disk := localDir(t)
	tier := reader.TierObjects(disk)

	rc, err := tier.Get(ctx, d)
	if err == nil {
		_, err = io.ReadAll(rc)
		_ = rc.Close()
	}
	if err == nil {
		t.Fatal("a swapped remote object was served to the caller")
	}

	if ok, herr := disk.Has(ctx, d); herr != nil || ok {
		t.Errorf("the corrupt object was written into the local store under the digest it "+
			"was requested by = (%v, %v), want false", ok, herr)
	}
	// Corruption of one object is not a reason to stop using the store: the
	// rest of the bucket is very likely fine.
	if rep.disabled() != 0 {
		t.Errorf("one corrupt object disabled the whole remote: %v", rep.all())
	}
	if len(rep.all()) == 0 {
		t.Error("a corrupt remote object was handled silently; nobody would ever fix it")
	}
}

// TestConcurrentUploadsOfTheSameDigestAreSafe: two machines finishing the
// same step at the same moment is the ordinary case, not the exotic one.
func TestConcurrentUploadsOfTheSameDigestAreSafe(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	const machines = 8
	content := bytes.Repeat([]byte("the identical output of one identical step\n"), 2000)

	tiers := make([]cas.Store, machines)
	reps := make([]*reports, machines)
	for i := range tiers {
		r, rep := openLive(t, m)
		tiers[i] = r.TierObjects(localDir(t))
		reps[i] = rep
	}

	var start sync.WaitGroup
	start.Add(1)
	digests := make([]cas.Digest, machines)
	errs := make([]error, machines)
	var wg sync.WaitGroup
	for i := range machines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			digests[i], errs[i] = tiers[i].Put(ctx, bytes.NewReader(content))
		}()
	}
	start.Done()
	wg.Wait()

	want := cas.FromBytes(content)
	for i := range machines {
		if errs[i] != nil {
			t.Errorf("machine %d: Put: %v", i, errs[i])
		}
		if digests[i] != want {
			t.Errorf("machine %d: Put returned %s, want %s", i, digests[i], want)
		}
		if n := reps[i].disabled(); n != 0 {
			t.Errorf("machine %d disabled its remote during a race it is supposed to tolerate: %v",
				i, reps[i].all())
		}
	}

	// A ninth machine, which took no part in the race, must be able to read
	// exactly the right bytes back.
	ninth, _ := openLive(t, m)
	rc, err := ninth.TierObjects(localDir(t)).Get(ctx, want)
	if err != nil {
		t.Fatalf("a machine that did not race cannot read the object: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, content) {
		t.Errorf("after %d concurrent uploads the object reads back as %d bytes, want %d",
			machines, len(got), len(content))
	}
}

// --- degradation -----------------------------------------------------------

// unreachableConfig points at a port nothing is listening on.
func unreachableConfig() remotecache.Config {
	pathStyle := true
	return remotecache.Config{
		Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "team-cache",
		AccessKeyID: "AKIAEXAMPLE", SecretAccessKey: "example-secret",
		PathStyle: &pathStyle, Timeout: 3 * time.Second,
	}
}

// TestAnUnreachableRemoteDegradesToNoCacheAndTheWorkGoesOn is the rule that
// matters most: a cache that is down must not be able to stop a build.
func TestAnUnreachableRemoteDegradesToNoCacheAndTheWorkGoesOn(t *testing.T) {
	t.Parallel()
	r, rep := openWith(t, unreachableConfig())
	disk := localDir(t)
	tier := r.TierObjects(disk)
	ctx := t.Context()

	content := []byte("what the step produced anyway")

	// A store still succeeds: the local half of it worked.
	d, err := tier.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed because the remote is down, which fails the build: %v", err)
	}
	if d != cas.FromBytes(content) {
		t.Errorf("Put returned %s, want %s", d, cas.FromBytes(content))
	}
	if ok, err := disk.Has(ctx, d); err != nil || !ok {
		t.Errorf("the object did not reach the local store = (%v, %v)", ok, err)
	}

	// A lookup for something nobody has is a miss, not an error.
	absent := cas.FromBytes([]byte("nobody has this"))
	if ok, err := tier.Has(ctx, absent); err != nil || ok {
		t.Errorf("Has against a down remote = (%v, %v), want (false, nil)", ok, err)
	}
	if _, err := tier.Get(ctx, absent); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get against a down remote = %v, want ErrNotFound", err)
	}

	// And what is on the local disk is still served.
	rc, err := tier.Get(ctx, d)
	if err != nil {
		t.Fatalf("a down remote broke a local hit: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, content) {
		t.Error("a local hit returned the wrong bytes while the remote was down")
	}

	// Loudly: exactly once, naming the store and the reason.
	if n := rep.disabled(); n != 1 {
		t.Fatalf("a down remote produced %d disable reports, want exactly 1: %v", n, rep.all())
	}
	var d0 remotecache.Degradation
	for _, x := range rep.all() {
		if x.Disabled {
			d0 = x
		}
	}
	if !strings.Contains(d0.Store, "team-cache") {
		t.Errorf("the report does not name the store: %+v", d0)
	}
	if d0.Err == nil {
		t.Errorf("the report carries no reason: %+v", d0)
	}
	if d0.Op == "" {
		t.Errorf("the report does not say which operation failed: %+v", d0)
	}
	if r.Live() {
		t.Error("the remote still reports itself as live after being disabled")
	}
}

// TestADownRemoteIsNotRetriedForEveryObject: a build must not pay a
// connection timeout per object for a store that is already known to be down.
func TestADownRemoteIsNotRetriedForEveryObject(t *testing.T) {
	t.Parallel()
	r, _ := openWith(t, unreachableConfig())
	tier := r.TierObjects(localDir(t))
	ctx := t.Context()

	// The first operation pays the failure and switches the remote off.
	_, _ = tier.Get(ctx, cas.FromBytes([]byte("first")))

	start := time.Now()
	for i := range 50 {
		_, _ = tier.Has(ctx, cas.FromBytes([]byte{byte(i)}))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("50 lookups against a remote already known to be down took %v", d)
	}
}

// TestAPermissionErrorDegradesToNoCache. A wrong credential is the single
// most common way a remote cache is misconfigured, and it must not be able to
// fail a run either.
func TestAPermissionErrorDegradesToNoCache(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	cfg := bucketConfig(m)
	cfg.SecretAccessKey = "not-the-right-secret-at-all"
	r, rep := openWith(t, cfg)
	tier := r.TierObjects(localDir(t))
	ctx := t.Context()

	content := []byte("a step's output, cached locally only")
	if _, err := tier.Put(ctx, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put failed because the credentials are wrong, which fails the build: %v", err)
	}
	if _, err := tier.Get(ctx, cas.FromBytes([]byte("absent"))); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get with wrong credentials = %v, want ErrNotFound", err)
	}
	if n := rep.disabled(); n != 1 {
		t.Fatalf("wrong credentials produced %d disable reports, want exactly 1: %v", n, rep.all())
	}
	if !errors.Is(rep.all()[0].Err, s3.ErrDenied) {
		t.Errorf("the report does not say the problem was a permission failure: %v", rep.all()[0].Err)
	}
}

// TestNothingReportedEverCarriesACredential. These reports go to standard
// error, into an event stream and quite possibly into a public CI log.
func TestNothingReportedEverCarriesACredential(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	const secret = "an-extremely-recognisable-secret"
	const token = "an-extremely-recognisable-session-token"

	cfg := bucketConfig(m)
	cfg.SecretAccessKey = secret
	cfg.SessionToken = token

	var stderr bytes.Buffer
	rep := &reports{}
	cfg.Report = rep.add
	cfg.ReportWriter = &stderr
	r, err := remotecache.Open(cfg)
	if err != nil {
		t.Fatalf("remotecache.Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	tier := r.TierObjects(localDir(t))
	if _, err := tier.Put(t.Context(), bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rep.disabled() == 0 {
		t.Fatal("wrong credentials did not degrade the remote, so this test checked nothing")
	}

	haystacks := map[string]string{"the standard error line": stderr.String(), "String()": r.String()}
	for _, d := range rep.all() {
		haystacks["the report's store name"] = d.Store
		if d.Err != nil {
			haystacks["the report's error"] = d.Err.Error()
		}
	}
	for where, text := range haystacks {
		for what, needle := range map[string]string{"secret key": secret, "session token": token} {
			if strings.Contains(text, needle) {
				t.Errorf("%s carries the %s: %s", where, what, text)
			}
		}
	}
	if stderr.Len() == 0 {
		t.Error("nothing was written to standard error, so a run with no attached client " +
			"would never learn its cache had gone away")
	}
}

// TestReadOnlyNeverWritesToTheBucket covers the pull-request build: it should
// read a cache the trunk builds fill, and must not be able to fill it.
func TestReadOnlyNeverWritesToTheBucket(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	cfg := bucketConfig(m)
	cfg.ReadOnly = true
	r, rep := openWith(t, cfg)
	tier := r.TierObjects(localDir(t))

	content := []byte("a fork's build output, which nobody else should trust")
	d, err := tier.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	writable, _ := openLive(t, m)
	if ok, err := writable.Objects().Has(ctx, d); err != nil || ok {
		t.Errorf("a read-only remote uploaded an object = (%v, %v), want false", ok, err)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a read-only remote reported %d degradations: %v", n, rep.all())
	}

	// It still reads.
	seeded, _ := openLive(t, m)
	if _, err := seeded.Objects().Put(ctx, bytes.NewReader([]byte("seeded elsewhere"))); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	sd := cas.FromBytes([]byte("seeded elsewhere"))
	rc, err := tier.Get(ctx, sd)
	if err != nil {
		t.Fatalf("a read-only remote cannot read: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, []byte("seeded elsewhere")) {
		t.Error("a read-only remote read the wrong bytes")
	}
}

// --- the action cache ------------------------------------------------------

func testKey(command string) cache.Key {
	return cache.Key{
		Command:       command,
		ExecutorClass: "local/linux/amd64",
		Platform:      "linux/amd64",
		Version:       cache.KeyVersion,
	}
}

func testResult(runID string) *cache.Result {
	return &cache.Result{
		ExitCode: 0, RunID: runID, Hermeticity: cache.HermeticityTrusted,
		SavedAt: time.Now().UTC().Truncate(time.Second), Bytes: 42,
	}
}

// TestASecondMachineHitsAnEntryTheFirstOneSaved. Without this, a shared CAS
// is useless: nothing on the second machine would ever look an object up.
func TestASecondMachineHitsAnEntryTheFirstOneSaved(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	remoteA, _ := openLive(t, m)
	remoteB, _ := openLive(t, m)
	entriesA, entriesB := localEntries(t), localEntries(t)
	a := remoteA.TierEntries(entriesA)
	b := remoteB.TierEntries(entriesB)

	k := testKey("exec go build ./...")
	if err := a.Save(ctx, "build", k, testResult("run-a")); err != nil {
		t.Fatalf("machine A Save: %v", err)
	}

	if _, ok, _ := entriesB.Lookup(ctx, "build", k); ok {
		t.Fatal("machine B already had the entry locally; the test proves nothing")
	}
	res, ok, err := b.Lookup(ctx, "build", k)
	if err != nil {
		t.Fatalf("machine B Lookup: %v", err)
	}
	if !ok {
		t.Fatal("machine B misses an entry machine A saved, so the shared cache never hits")
	}
	if res.RunID != "run-a" {
		t.Errorf("machine B got a result from run %q, want run-a", res.RunID)
	}

	// The hit warmed B's own store, so the next run there needs no network.
	if _, ok, _ := entriesB.Lookup(ctx, "build", k); !ok {
		t.Error("a remote hit was not written through to the local action cache")
	}

	prev, ok, err := b.Previous(ctx, "build")
	if err != nil {
		t.Fatalf("Previous: %v", err)
	}
	if !ok || prev.Key.Digest() != k.Digest() {
		t.Errorf("Previous on machine B = (%v, %v), want the entry machine A saved", ok, prev)
	}
}

// TestAnEntryFiledUnderTheWrongKeyIsAMissRatherThanAWrongHit is the action
// cache's own version of the digest check. A store that returns the wrong
// object under a key must never produce a hit, because a hit here SKIPS THE
// STEP: it is the one place a wrong answer is silently wrong forever.
func TestAnEntryFiledUnderTheWrongKeyIsAMissRatherThanAWrongHit(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)
	raw := rawClient(t, m)
	ctx := t.Context()

	asked := testKey("exec go test ./...")
	other := testKey("exec rm -rf /")

	entry, err := json.Marshal(cache.Entry{Key: other, Result: *testResult("run-x")})
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	if err := raw.PutBytes(ctx, r.Entries().EntryKey(asked.Digest()), entry); err != nil {
		t.Fatalf("planting a mismatched entry: %v", err)
	}

	res, ok, err := r.Entries().Lookup(ctx, "test", asked)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if ok {
		t.Fatalf("an entry for a DIFFERENT key was served as a hit, which skips the step: %+v", res)
	}
	if len(rep.all()) == 0 {
		t.Error("a mismatched entry was ignored silently; a poisoned bucket would never be noticed")
	}
	if rep.disabled() != 0 {
		t.Errorf("one bad entry disabled the whole remote: %v", rep.all())
	}
}

func TestAnUnreadableEntryIsAMiss(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)
	raw := rawClient(t, m)
	ctx := t.Context()

	k := testKey("exec make")
	if err := raw.PutBytes(ctx, r.Entries().EntryKey(k.Digest()), []byte("{not json at all")); err != nil {
		t.Fatalf("planting: %v", err)
	}
	if _, ok, err := r.Entries().Lookup(ctx, "make", k); ok || err != nil {
		t.Errorf("Lookup of an unreadable entry = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestAnActionLookupAgainstADownRemoteIsAMiss: same rule as the object store.
func TestAnActionLookupAgainstADownRemoteIsAMiss(t *testing.T) {
	t.Parallel()
	r, rep := openWith(t, unreachableConfig())
	tier := r.TierEntries(localEntries(t))
	ctx := t.Context()

	k := testKey("exec go build ./...")
	if _, ok, err := tier.Lookup(ctx, "build", k); err != nil || ok {
		t.Errorf("Lookup against a down remote = (%v, %v), want (false, nil)", ok, err)
	}
	if err := tier.Save(ctx, "build", k, testResult("run-local")); err != nil {
		t.Fatalf("Save failed because the remote is down, which fails the build: %v", err)
	}
	// The local half still holds it.
	if _, ok, err := tier.Lookup(ctx, "build", k); err != nil || !ok {
		t.Errorf("a locally saved entry is not found while the remote is down = (%v, %v)", ok, err)
	}
	if rep.disabled() != 1 {
		t.Errorf("a down remote produced %d disable reports, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
}

// TestForgetOnlyForgetsLocally. Forget runs when a hit turns out to reference
// content a sweep collected, which is a statement about THIS machine's disk.
// Deleting the shared entry on that evidence would let one machine with a
// pruned cache repeatedly wipe an entry every other machine can still use.
func TestForgetOnlyForgetsLocally(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	r, _ := openLive(t, m)
	local := localEntries(t)
	tier := r.TierEntries(local)

	k := testKey("exec go vet ./...")
	if err := tier.Save(ctx, "vet", k, testResult("run-a")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := tier.Forget(ctx, k); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, ok, _ := local.Lookup(ctx, "vet", k); ok {
		t.Error("Forget left the entry in the local cache")
	}
	if _, ok, err := r.Entries().Lookup(ctx, "vet", k); err != nil || !ok {
		t.Errorf("Forget removed the shared entry = (%v, %v), want it still there", ok, err)
	}
}

func localEntries(t *testing.T) *cache.Dir {
	t.Helper()
	d, err := cache.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	return d
}

// TestObjectsAndEntriesShareOneDegradedState. A bucket that stopped answering
// stopped answering for both; saving an action entry that points at objects
// which failed to upload would publish a hit nobody can reproduce.
func TestObjectsAndEntriesShareOneDegradedState(t *testing.T) {
	t.Parallel()
	r, rep := openWith(t, unreachableConfig())
	ctx := t.Context()

	objects := r.TierObjects(localDir(t))
	entries := r.TierEntries(localEntries(t))

	if _, err := objects.Put(ctx, bytes.NewReader([]byte("output"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rep.disabled() != 1 {
		t.Fatalf("the object store did not disable the remote: %v", rep.all())
	}
	if err := entries.Save(ctx, "s", testKey("exec x"), testResult("r")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if n := rep.disabled(); n != 1 {
		t.Errorf("the action cache reported the same outage again: %d reports, want 1: %v",
			n, rep.all())
	}
}
