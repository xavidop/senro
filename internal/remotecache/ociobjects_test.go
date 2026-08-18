package remotecache_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/oci"
	"github.com/xavidop/senro/internal/remotecache"
)

// registryConfig is a config for a live registry repository, with no report
// wired.
func registryConfig(r dockertest.Registry) remotecache.OCIConfig {
	return remotecache.OCIConfig{
		Registry:   r.Host,
		Repository: r.Repository,
		Username:   r.Username,
		Password:   r.Password,
		PlainHTTP:  true,
		Timeout:    30 * time.Second,
	}
}

// openLiveRegistry opens a registry-backed remote, capturing every
// degradation.
func openLiveRegistry(t *testing.T, r dockertest.Registry) (*remotecache.Remote, *reports) {
	t.Helper()
	return openRegistryWith(t, registryConfig(r))
}

func openRegistryWith(t *testing.T, cfg remotecache.OCIConfig) (*remotecache.Remote, *reports) {
	t.Helper()
	rep := &reports{}
	cfg.Report = rep.add
	// Nothing may reach the real standard error from a test: a degradation
	// report is deliberately loud, and a suite that printed one per test
	// would be unreadable.
	cfg.ReportWriter = io.Discard
	remote, err := remotecache.OpenOCI(cfg)
	if err != nil {
		t.Fatalf("remotecache.OpenOCI: %v", err)
	}
	t.Cleanup(func() { _ = remote.Close() })
	return remote, rep
}

// unreachableRegistryConfig points at a port nothing is listening on, so the
// connection is refused immediately rather than timing out.
func unreachableRegistryConfig() remotecache.OCIConfig {
	return remotecache.OCIConfig{
		Registry: "127.0.0.1:1", Repository: "acme/senro-cache",
		Username: "senro", Password: "example-registry-password",
		PlainHTTP: true, Timeout: 3 * time.Second,
	}
}

// rawRegistry is a direct handle on the repository, for planting the bytes a
// well-behaved writer would never produce.
func rawRegistry(t *testing.T, r dockertest.Registry) *oci.Client {
	t.Helper()
	c, err := oci.New(oci.Config{
		Registry: r.Host, Repository: r.Repository,
		Username: r.Username, Password: r.Password,
		PlainHTTP: true, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("oci.New: %v", err)
	}
	return c
}

func sha256Of(b []byte) [sha256.Size]byte { return sha256.Sum256(b) }

// encoded is the bytes the local backend would have written to disk for this
// content, which is exactly what the registry backend stores as a blob.
func encoded(t *testing.T, plaintext []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := cas.NewEncoder(&buf)
	if err != nil {
		t.Fatalf("cas.NewEncoder: %v", err)
	}
	if _, err := enc.Write(plaintext); err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("closing the encoder: %v", err)
	}
	return buf.Bytes()
}

// plant pushes blob bytes and files them under want's tag, whatever they
// actually are. It is how a test produces a poisoned cache: a registry
// verifies that a blob is its own digest, so the only way an object can be
// wrong is for the manifest naming it to be wrong, which is precisely the
// case a cache has to survive.
func plant(t *testing.T, r dockertest.Registry, want cas.Digest, blob []byte) {
	t.Helper()
	c := rawRegistry(t, r)
	ctx := t.Context()

	blobDigest := fmt.Sprintf("sha256:%x", sha256Of(blob))
	if err := c.PutBlob(ctx, blobDigest, bytes.NewReader(blob), int64(len(blob))); err != nil {
		t.Fatalf("planting a blob: %v", err)
	}
	empty := []byte("{}")
	emptyDigest := fmt.Sprintf("sha256:%x", sha256Of(empty))
	if err := c.PutBlob(ctx, emptyDigest, bytes.NewReader(empty), int64(len(empty))); err != nil {
		t.Fatalf("planting the empty config: %v", err)
	}
	manifest := fmt.Appendf(nil, `{"schemaVersion":2,`+
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"artifactType":"application/vnd.senro.cache.object.v1",`+
		`"config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":%q,"size":2},`+
		`"layers":[{"mediaType":"application/vnd.senro.cache.object.v1+zstd","digest":%q,"size":%d}],`+
		`"annotations":{"dev.senro.object.digest":%q}}`,
		emptyDigest, blobDigest, len(blob), string(want))
	if err := c.PutManifest(ctx, remotecache.OCITag(want), oci.MediaTypeImageManifest, manifest); err != nil {
		t.Fatalf("planting a manifest: %v", err)
	}
}

// --- the registry store on its own -----------------------------------------

func TestOCIObjectsRoundTripThroughARealRegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, rep := openLiveRegistry(t, reg)
	ctx := t.Context()

	want := bytes.Repeat([]byte("a workspace tarball, more or less\n"), 500)
	d, err := remote.Objects().Put(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if d != cas.FromBytes(want) {
		t.Fatalf("Put returned %s, want the digest of the plaintext %s", d, cas.FromBytes(want))
	}

	ok, err := remote.Objects().Has(ctx, d)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatal("Has says an object that was just stored is not there")
	}

	rc, err := remote.Objects().Get(ctx, d)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, want) {
		t.Errorf("Get returned %d bytes, want %d, and they differ", len(got), len(want))
	}

	if n := len(rep.all()); n != 0 {
		t.Errorf("a healthy registry reported %d degradations: %v", n, rep.all())
	}
}

// TestOCIObjectsStoresTheCompressedForm pins the reason the layout has a
// manifest in it at all: what is pushed is the encoding the local backend
// writes to disk, so an object can be uploaded from disk verbatim rather than
// decoded and re-encoded, and the digest that names it stays the digest of
// the PLAINTEXT.
func TestOCIObjectsStoresTheCompressedForm(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, _ := openLiveRegistry(t, reg)
	ctx := t.Context()

	// Highly compressible, so a store that pushed the plaintext would be
	// obvious in the blob's size.
	want := bytes.Repeat([]byte("compress me\n"), 20000)
	d, err := remote.Objects().Put(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	raw := rawRegistry(t, reg)
	manifest, err := raw.GetManifest(ctx, remotecache.OCITag(d))
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if !strings.Contains(string(manifest), string(d)) {
		t.Errorf("the manifest does not record the object's digest %s:\n%s", d, manifest)
	}

	blobDigest := fmt.Sprintf("sha256:%x", sha256Of(encoded(t, want)))
	size, ok, err := raw.HasBlob(ctx, blobDigest)
	if err != nil || !ok {
		t.Fatalf("HasBlob for the encoded form = %v, %v; the stored blob is not the encoding "+
			"the local backend writes", ok, err)
	}
	if size >= int64(len(want)) {
		t.Errorf("the stored blob is %d bytes for %d bytes of plaintext, so nothing was compressed",
			size, len(want))
	}
}

func TestOCIObjectsHasSaysNoForSomethingNobodyStored(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, rep := openLiveRegistry(t, reg)
	ctx := t.Context()

	absent := cas.FromBytes([]byte("nothing has ever stored this"))
	ok, err := remote.Objects().Has(ctx, absent)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if ok {
		t.Error("Has says an object nobody stored is there")
	}
	if _, err := remote.Objects().Get(ctx, absent); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get of an absent object = %v, want cas.ErrNotFound", err)
	}
	// A miss is an answer, not a degradation.
	if n := len(rep.all()); n != 0 {
		t.Errorf("a miss reported %d degradations: %v", n, rep.all())
	}
}

// TestOCIObjectsRefusesBytesThatAreNotWhatTheDigestPromised is the rule the
// whole feature rests on. The registry cannot stop this: it verifies that a
// blob is its own digest, and this object's blob genuinely is. What is wrong
// is the name it is filed under, which is exactly what a poisoned cache looks
// like, and only the reader can catch it.
func TestOCIObjectsRefusesBytesThatAreNotWhatTheDigestPromised(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, rep := openLiveRegistry(t, reg)
	ctx := t.Context()

	promised := cas.FromBytes([]byte("what the cache was asked for"))
	plant(t, reg, promised, encoded(t, []byte("something else entirely")))

	rc, err := remote.Objects().Get(ctx, promised)
	if err == nil {
		_, err = io.ReadAll(rc)
		_ = rc.Close()
	}
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading an object stored under the wrong digest = %v, want cas.ErrCorrupt", err)
	}
	if len(rep.all()) != 0 {
		t.Errorf("the store reported %v, but nothing above it has been told the object is "+
			"bad yet: the reader is what refuses it", rep.all())
	}
}

func TestOCIObjectsRefusesATruncatedObject(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, _ := openLiveRegistry(t, reg)
	ctx := t.Context()

	content := bytes.Repeat([]byte("a long object that will be cut short\n"), 500)
	promised := cas.FromBytes(content)
	whole := encoded(t, content)
	plant(t, reg, promised, whole[:len(whole)/2])

	rc, err := remote.Objects().Get(ctx, promised)
	if err == nil {
		_, err = io.ReadAll(rc)
		_ = rc.Close()
	}
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading a truncated object = %v, want cas.ErrCorrupt", err)
	}
}

// TestOCIObjectsRefusesAManifestFiledUnderAnotherDigest catches the wrong
// object one round trip earlier than the reader would, before a workspace
// tarball is pulled to find out.
func TestOCIObjectsRefusesAManifestFiledUnderAnotherDigest(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, _ := openLiveRegistry(t, reg)
	ctx := t.Context()

	asked := cas.FromBytes([]byte("the object that was asked for"))
	other := cas.FromBytes([]byte("an object with a different digest"))
	raw := rawRegistry(t, reg)
	blob := encoded(t, []byte("an object with a different digest"))
	blobDigest := fmt.Sprintf("sha256:%x", sha256Of(blob))
	if err := raw.PutBlob(ctx, blobDigest, bytes.NewReader(blob), int64(len(blob))); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	empty := []byte("{}")
	emptyDigest := fmt.Sprintf("sha256:%x", sha256Of(empty))
	if err := raw.PutBlob(ctx, emptyDigest, bytes.NewReader(empty), int64(len(empty))); err != nil {
		t.Fatalf("PutBlob (config): %v", err)
	}
	// The manifest says it holds `other` but it is filed under `asked`.
	manifest := fmt.Appendf(nil, `{"schemaVersion":2,`+
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"artifactType":"application/vnd.senro.cache.object.v1",`+
		`"config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":%q,"size":2},`+
		`"layers":[{"mediaType":"application/vnd.senro.cache.object.v1+zstd","digest":%q,"size":%d}],`+
		`"annotations":{"dev.senro.object.digest":%q}}`,
		emptyDigest, blobDigest, len(blob), string(other))
	if err := raw.PutManifest(ctx, remotecache.OCITag(asked), oci.MediaTypeImageManifest, manifest); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}

	// Refused by Get itself, before a byte of the layer is fetched. That is
	// the whole point of recording the digest inside the manifest: the reader
	// would catch this too, but only after pulling what might be a
	// multi-gigabyte workspace to find out.
	rc, err := remote.Objects().Get(ctx, asked)
	if err == nil {
		_ = rc.Close()
		t.Fatal("a manifest filed under another object's digest was accepted, and the " +
			"mismatch was left for the reader to find after downloading the whole layer")
	}
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("Get of an object whose manifest names another digest = %v, want cas.ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), other.Short()) {
		t.Errorf("the refusal does not say what the manifest actually held: %v", err)
	}
}

// TestOCIObjectsConcurrentPutsOfOneObjectAllSucceed is the case two runners
// finishing the same step at the same moment produce.
func TestOCIObjectsConcurrentPutsOfOneObjectAllSucceed(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, rep := openLiveRegistry(t, reg)
	ctx := t.Context()

	content := bytes.Repeat([]byte("two runners, one step, one object\n"), 30000)
	want := cas.FromBytes(content)

	const racers = 8
	digests := make([]cas.Digest, racers)
	errs := make([]error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			digests[i], errs[i] = remote.Objects().Put(ctx, bytes.NewReader(content))
		}()
	}
	close(start)
	wg.Wait()

	for i := range racers {
		if errs[i] != nil {
			t.Errorf("concurrent Put %d: %v", i, errs[i])
		}
		if digests[i] != want {
			t.Errorf("concurrent Put %d returned %s, want %s", i, digests[i], want)
		}
	}
	rc, err := remote.Objects().Get(ctx, want)
	if err != nil {
		t.Fatalf("Get after the race: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, content) {
		t.Errorf("after %d concurrent stores the object is %d bytes, want %d",
			racers, len(got), len(content))
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a race that is supposed to be safe reported %d degradations: %v", n, rep.all())
	}
}

// TestOCIObjectsReadOnlyStoresNothing covers the credential a pull-request
// build should be given: read the cache, never write it.
func TestOCIObjectsReadOnlyStoresNothing(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	cfg := registryConfig(reg)
	cfg.ReadOnly = true
	remote, rep := openRegistryWith(t, cfg)
	ctx := t.Context()

	content := []byte("what a fork's build produced")
	d, err := remote.Objects().Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put in read-only mode: %v", err)
	}
	if d != cas.FromBytes(content) {
		t.Errorf("Put returned %s, want %s: a read-only store still has to say what the "+
			"digest would have been", d, cas.FromBytes(content))
	}
	ok, err := remote.Objects().Has(ctx, d)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if ok {
		t.Error("a read-only store wrote to the registry")
	}
	// Read-only is a deliberate setting, not a failure.
	if n := len(rep.all()); n != 0 {
		t.Errorf("read-only reported %d degradations: %v", n, rep.all())
	}
}

// --- the tier ---------------------------------------------------------------

func TestOCITierFillsTheLocalStoreFromTheRegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	seeder, _ := openLiveRegistry(t, reg)
	ctx := t.Context()

	content := bytes.Repeat([]byte("what another machine already built\n"), 200)
	d, err := seeder.Objects().Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("seeding the registry: %v", err)
	}

	// A different machine: its own empty disk, the same registry.
	remote, rep := openLiveRegistry(t, reg)
	disk := localDir(t)
	tier := remote.TierObjects(disk)

	if ok, err := disk.Has(ctx, d); err != nil || ok {
		t.Fatalf("the local store starts empty: Has = %v, %v", ok, err)
	}
	rc, err := tier.Get(ctx, d)
	if err != nil {
		t.Fatalf("Get through the tier: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, content) {
		t.Errorf("the tier returned %d bytes, want %d, and they differ", len(got), len(content))
	}
	// The write-through is the point: the next run on this machine needs no
	// network for it.
	if ok, err := disk.Has(ctx, d); err != nil || !ok {
		t.Errorf("after a remote hit the local store has it = %v, %v; the fetch was not "+
			"written through", ok, err)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a healthy fill reported %d degradations: %v", n, rep.all())
	}
}

// TestOCITierNeverWritesACorruptObjectToDisk is the second half of the
// verification rule: bad bytes off the network must not reach this machine's
// cache either, where the next run would find them and trust them.
func TestOCITierNeverWritesACorruptObjectToDisk(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	remote, rep := openLiveRegistry(t, reg)
	disk := localDir(t)
	tier := remote.TierObjects(disk)
	ctx := t.Context()

	promised := cas.FromBytes([]byte("what the cache was asked for"))
	plant(t, reg, promised, encoded(t, []byte("bytes from somebody else's build")))

	if _, err := tier.Get(ctx, promised); !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("a corrupt remote object produced %v, want a plain miss (cas.ErrNotFound)", err)
	}
	if ok, err := disk.Has(ctx, promised); err != nil || ok {
		t.Errorf("the corrupt object was written to local disk: Has = %v, %v", ok, err)
	}
	if len(rep.all()) == 0 {
		t.Error("a corrupt object was refused silently; nobody would ever find out")
	}
	// One bad object says nothing about the rest of the registry, so the
	// store stays in use.
	if n := rep.disabled(); n != 0 {
		t.Errorf("one corrupt object switched the whole registry off (%d reports)", n)
	}
	if !remote.Live() {
		t.Error("one corrupt object switched the registry off for the rest of the run")
	}
}

// TestAnUnreachableRegistryDegradesToNoCacheAndTheWorkGoesOn is the rule that
// matters most: a cache that is down must not be able to stop a build.
func TestAnUnreachableRegistryDegradesToNoCacheAndTheWorkGoesOn(t *testing.T) {
	t.Parallel()
	remote, rep := openRegistryWith(t, unreachableRegistryConfig())
	disk := localDir(t)
	tier := remote.TierObjects(disk)
	ctx := t.Context()

	content := []byte("what the step produced anyway")

	// A store still succeeds: the local half of it worked.
	d, err := tier.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put against a registry that is down: %v; a cache that is down must never "+
			"fail a run", err)
	}
	if d != cas.FromBytes(content) {
		t.Errorf("Put returned %s, want %s", d, cas.FromBytes(content))
	}
	// And it is readable, from this machine's own disk.
	rc, err := tier.Get(ctx, d)
	if err != nil {
		t.Fatalf("Get against a registry that is down: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, content) {
		t.Errorf("Get returned %q, want %q", got, content)
	}
	// An object nobody has is a miss, not an error.
	absent := cas.FromBytes([]byte("nobody has this"))
	if _, err := tier.Get(ctx, absent); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get of an absent object against a registry that is down = %v, want cas.ErrNotFound", err)
	}
	if ok, err := tier.Has(ctx, absent); err != nil || ok {
		t.Errorf("Has against a registry that is down = %v, %v, want false and no error", ok, err)
	}

	// It said so, once, and it stopped trying.
	// Fatal rather than an error, because everything below reads the report
	// that this says is there.
	if rep.disabled() != 1 {
		t.Fatalf("a registry that is down was reported as disabled %d times, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
	if remote.Live() {
		t.Error("the registry is still in use after it failed")
	}
	first := rep.all()[0]
	if !strings.Contains(first.String(), "127.0.0.1:1") {
		t.Errorf("the report does not say which registry failed: %s", first)
	}
	if strings.Contains(first.String(), "example-registry-password") {
		t.Errorf("the report leaked the password: %s", first)
	}
}

// TestEveryRegistryOperationOnItsOwnDegradesRatherThanFailing gives each entry
// point its OWN unreachable remote, so each one is the FIRST failure that
// remote ever sees. After the first failure trips the breaker every later
// operation returns early and never reaches its own error handling, so a
// suite that only ran them in sequence would prove much less than it looks.
func TestEveryRegistryOperationOnItsOwnDegradesRatherThanFailing(t *testing.T) {
	t.Parallel()
	absent := cas.FromBytes([]byte("nothing has this"))

	for name, op := range map[string]func(t *testing.T, r *remotecache.Remote) error{
		"put": func(t *testing.T, r *remotecache.Remote) error {
			_, err := r.TierObjects(localDir(t)).Put(t.Context(), bytes.NewReader([]byte("out")))
			return err
		},
		"has": func(t *testing.T, r *remotecache.Remote) error {
			_, err := r.TierObjects(localDir(t)).Has(t.Context(), absent)
			return err
		},
		"get": func(t *testing.T, r *remotecache.Remote) error {
			// A miss is the right answer here and is not a failure; anything
			// else is the remote's problem reaching the caller.
			_, err := r.TierObjects(localDir(t)).Get(t.Context(), absent)
			if errors.Is(err, cas.ErrNotFound) {
				return nil
			}
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			remote, rep := openRegistryWith(t, unreachableRegistryConfig())
			if err := op(t, remote); err != nil {
				t.Fatalf("%s against a registry that is down returned %v; a cache that is down "+
					"must never fail a run", name, err)
			}
			if len(rep.all()) == 0 {
				t.Errorf("%s silently did nothing; nobody would learn the cache was down", name)
			}
		})
	}
}

// TestARegistryThatRefusesTheCredentialsDegradesWithoutFailing is the case a
// rotated password, an expired token or a mistyped secret produces.
func TestARegistryThatRefusesTheCredentialsDegradesWithoutFailing(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	cfg := registryConfig(reg)
	cfg.Password = "the password this was yesterday"
	remote, rep := openRegistryWith(t, cfg)
	disk := localDir(t)
	tier := remote.TierObjects(disk)
	ctx := t.Context()

	content := []byte("a step's output, on a machine whose credentials expired")
	d, err := tier.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put with a refused credential: %v; a cache that will not have us must "+
			"never fail a run", err)
	}
	if ok, err := disk.Has(ctx, d); err != nil || !ok {
		t.Errorf("the object did not reach local disk: Has = %v, %v", ok, err)
	}
	if rep.disabled() != 1 {
		t.Fatalf("a refused credential was reported as disabled %d times, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
	if got := rep.all()[0].String(); !strings.Contains(got, "denied") {
		t.Errorf("the report does not say the credentials were refused: %s", got)
	}
	if got := rep.all()[0].String(); strings.Contains(got, "the password this was yesterday") {
		t.Errorf("the report leaked the password: %s", got)
	}
}

// TestOpenOCIRefusesAConfigurationThatCannotWork keeps a mistake somebody
// typed separate from a condition of the network: this one fails the run,
// where it will be read as a mistake, rather than degrading into a cache that
// is mysteriously always cold.
func TestOpenOCIRefusesAConfigurationThatCannotWork(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]remotecache.OCIConfig{
		"no registry":   {Repository: "acme/senro-cache"},
		"no repository": {Registry: "registry.example.com"},
		"a URL":         {Registry: "https://registry.example.com", Repository: "acme/senro-cache"},
		"credentials in the registry": {
			Registry: "user:password@registry.example.com", Repository: "acme/senro-cache",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := remotecache.OpenOCI(cfg); err == nil {
				t.Errorf("OpenOCI(%+v) was accepted", cfg)
			}
		})
	}
}
