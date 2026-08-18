package remotecache_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/oci"
	"github.com/xavidop/senro/internal/remotecache"
)

// plantDocument files raw bytes under the document called name: how a test
// poisons the MUTABLE half of a shared cache. A registry verifies a blob
// is its own digest, so the only way a document can be wrong is a wrong
// manifest under its tag, and a tag is a name anybody with push access can
// write.
func plantDocument(t *testing.T, r dockertest.Registry, name string, body []byte) {
	t.Helper()
	c := rawRegistry(t, r)
	ctx := t.Context()

	blobDigest := fmt.Sprintf("sha256:%x", sha256Of(body))
	if err := c.PutBlob(ctx, blobDigest, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("planting a document blob: %v", err)
	}
	empty := []byte("{}")
	emptyDigest := fmt.Sprintf("sha256:%x", sha256Of(empty))
	if err := c.PutBlob(ctx, emptyDigest, bytes.NewReader(empty), int64(len(empty))); err != nil {
		t.Fatalf("planting the empty config: %v", err)
	}
	manifest := fmt.Appendf(nil, `{"schemaVersion":2,`+
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"artifactType":"application/vnd.senro.cache.document.v1",`+
		`"config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":%q,"size":2},`+
		`"layers":[{"mediaType":"application/octet-stream","digest":%q,"size":%d}],`+
		`"annotations":{"dev.senro.document.name":%q}}`,
		emptyDigest, blobDigest, len(body), name)
	if err := c.PutManifest(ctx, name, oci.MediaTypeImageManifest, manifest); err != nil {
		t.Fatalf("planting a document manifest: %v", err)
	}
}

// entryJSON is what a well-behaved writer stores for one entry: exactly what
// the local backend writes to disk, indentation and all.
func entryJSON(t *testing.T, k cache.Key, r *cache.Result) []byte {
	t.Helper()
	b, err := json.MarshalIndent(cache.Entry{Key: k, Result: *r}, "", "  ")
	if err != nil {
		t.Fatalf("marshalling an entry: %v", err)
	}
	return b
}

// TestTheEntryTagIsTheKeyDigestItself pins the mapping the whole registry
// action cache rests on, because it is the half that is NOT a hash of
// something else: an entry lives at the key digest the local backend files it
// under, with the one character a tag cannot hold rewritten.
func TestTheEntryTagIsTheKeyDigestItself(t *testing.T) {
	t.Parallel()
	remote, _ := openRegistryWith(t, remotecache.OCIConfig{
		Registry: "registry.example.com", Repository: "acme/senro-cache",
	})
	k := testKey("exec go build ./...")
	got := remote.Entries().EntryKey(k.Digest())
	want := "senro-v1-action-sha256-" + k.Digest().Hex()
	if got != want {
		t.Errorf("the entry tag is %q, want %q", got, want)
	}
	if len(got) > 128 {
		t.Errorf("the tag is %d characters, and a registry takes at most 128", len(got))
	}
}

// TestARegistryEntrySavedOnOneMachineHitsOnAnother is the point of the whole
// exercise. Objects alone are nearly useless: nothing on the second machine
// would ever look one up, because it is an action-cache hit that says which
// object to want.
func TestARegistryEntrySavedOnOneMachineHitsOnAnother(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remoteA, _ := openLiveRegistry(t, reg)
	remoteB, repB := openLiveRegistry(t, reg)
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
		t.Fatal("machine B misses an entry machine A saved, so the registry cache never hits")
	}
	if res.RunID != "run-a" {
		t.Errorf("machine B got a result from run %q, want run-a", res.RunID)
	}

	// The hit warmed B's own store, so the next run there needs no network.
	if _, ok, _ := entriesB.Lookup(ctx, "build", k); !ok {
		t.Error("a remote hit was not written through to the local action cache")
	}
	if n := len(repB.all()); n != 0 {
		t.Errorf("a healthy hit reported %d degradations: %v", n, repB.all())
	}
}

// TestAnEntrySavedTwiceForOneKeyReturnsTheNewerResult is the one thing a
// content-addressed store cannot do and an action cache must: the same name,
// written again, with different bytes behind it. A tag is the only mutable
// name a registry has, and this is what it is for.
func TestAnEntrySavedTwiceForOneKeyReturnsTheNewerResult(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remote, rep := openLiveRegistry(t, reg)
	entries := remote.Entries()
	k := testKey("exec go test ./...")

	if err := entries.Save(ctx, "test", k, testResult("run-first")); err != nil {
		t.Fatalf("the first Save: %v", err)
	}
	if err := entries.Save(ctx, "test", k, testResult("run-second")); err != nil {
		t.Fatalf("the second Save: %v", err)
	}

	res, ok, err := entries.Lookup(ctx, "test", k)
	if err != nil || !ok {
		t.Fatalf("Lookup after two saves = (%v, %v), want a hit", ok, err)
	}
	if res.RunID != "run-second" {
		t.Errorf("the entry came back from run %q, want run-second: a re-run's result did not "+
			"replace the one before it, so the cache is stuck on the first answer it ever got",
			res.RunID)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("writing one key twice reported %d degradations: %v", n, rep.all())
	}
}

// TestARegistryEntryFiledUnderAnotherKeyIsAMissRatherThanAWrongHit is the
// most safety-critical check in the package: a hit SKIPS THE STEP, and an
// entry served under the wrong key produces a build that quietly did not
// do what it was told, on every machine sharing the repository.
func TestARegistryEntryFiledUnderAnotherKeyIsAMissRatherThanAWrongHit(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remote, rep := openLiveRegistry(t, reg)
	asked := testKey("exec go build ./...")
	other := testKey("exec rm -rf /")

	// A genuine entry for `other`, filed under the tag `asked` resolves to.
	plantDocument(t, reg, remote.Entries().EntryKey(asked.Digest()),
		entryJSON(t, other, testResult("somebody-elses-build")))

	res, ok, err := remote.Entries().Lookup(ctx, "build", asked)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if ok {
		t.Fatalf("an entry holding another key's result was served as a hit: %+v", res)
	}
	if len(rep.all()) == 0 {
		t.Error("a mis-filed entry was refused silently; nobody would ever find out")
	}
	if !remote.Live() {
		t.Error("one bad entry switched the whole registry off")
	}
}

// TestARegistryEntryThatDoesNotParseIsAMiss. Somebody else writing to the
// repository, a half-finished push, a tag reused by a different tool: a cache
// meets all of them, and none of them is a reason to fail a build.
func TestARegistryEntryThatDoesNotParseIsAMiss(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remote, rep := openLiveRegistry(t, reg)
	k := testKey("exec make")
	plantDocument(t, reg, remote.Entries().EntryKey(k.Digest()), []byte("{not json at all"))

	if _, ok, err := remote.Entries().Lookup(ctx, "make", k); ok || err != nil {
		t.Errorf("an unparseable entry = (%v, %v), want a plain miss", ok, err)
	}
	if len(rep.all()) == 0 {
		t.Error("an unreadable entry was refused silently")
	}
}

// TestADocumentManifestFiledUnderAnotherNameIsRefused is the registry's
// mis-filing check, one layer below the entry's: a manifest copied from
// one tag to another would still hold a valid entry, just not this one's,
// so the manifest must say for itself which document it is.
func TestADocumentManifestFiledUnderAnotherNameIsRefused(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remote, rep := openLiveRegistry(t, reg)
	asked := testKey("exec go build ./...")
	name := remote.Entries().EntryKey(asked.Digest())

	// A genuine entry for the key that was asked for, and an annotation naming
	// a different document, filed under the right tag.
	body := entryJSON(t, asked, testResult("run-a"))
	c := rawRegistry(t, reg)
	blobDigest := fmt.Sprintf("sha256:%x", sha256Of(body))
	if err := c.PutBlob(ctx, blobDigest, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("planting a blob: %v", err)
	}
	empty := []byte("{}")
	emptyDigest := fmt.Sprintf("sha256:%x", sha256Of(empty))
	if err := c.PutBlob(ctx, emptyDigest, bytes.NewReader(empty), int64(len(empty))); err != nil {
		t.Fatalf("planting the empty config: %v", err)
	}
	manifest := fmt.Appendf(nil, `{"schemaVersion":2,`+
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"artifactType":"application/vnd.senro.cache.document.v1",`+
		`"config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":%q,"size":2},`+
		`"layers":[{"mediaType":"application/octet-stream","digest":%q,"size":%d}],`+
		`"annotations":{"dev.senro.document.name":"senro-v1-action-sha256-%s"}}`,
		emptyDigest, blobDigest, len(body),
		"0000000000000000000000000000000000000000000000000000000000000000")
	if err := c.PutManifest(ctx, name, oci.MediaTypeImageManifest, manifest); err != nil {
		t.Fatalf("planting a manifest: %v", err)
	}

	if _, ok, err := remote.Entries().Lookup(ctx, "build", asked); ok || err != nil {
		t.Fatalf("a manifest that says it is another document was served = (%v, %v), "+
			"want a plain miss", ok, err)
	}
	if len(rep.all()) == 0 {
		t.Error("a mis-filed manifest was refused silently")
	}
}

// corruptedExitCode is what a served entry's exit code is rewritten to on its
// way back, and it is chosen to be the difference between a build that passed
// and a build that did not.
const corruptedExitCode = 9

// corruptBlobs rewrites a served entry's exit code and changes nothing
// else: deliberately the most dangerous alteration, not the easiest.
// Flipping a random byte would break the JSON and be refused as
// unparseable, proving nothing about the digest check. This substitution
// leaves valid JSON claiming exactly the key it was filed under, so only
// hashing the bytes against the manifest's digest stands between it and a
// served result no build ever produced.
//
// A conformant registry cannot produce this; a proxy or storage backend
// can. Manifest requests still go to the live registry underneath.
type corruptBlobs struct{ under http.RoundTripper }

func (c corruptBlobs) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.under.RoundTrip(req)
	if err != nil || req.Method != http.MethodGet ||
		!strings.Contains(req.URL.Path, "/blobs/") || resp.StatusCode != http.StatusOK {
		return resp, err
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	body = bytes.Replace(body,
		[]byte(`"exit_code": 0`),
		fmt.Appendf(nil, `"exit_code": %d`, corruptedExitCode), 1)
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	return resp, nil
}

// TestARegistryEntryWhoseBytesAreNotItsDigestIsRefused: what comes back is
// a well-formed entry, filed under the right key, claiming a succeeded
// step exited 9. Undetectable by parsing or key checks; refused only
// because the bytes are hashed against what the manifest named.
func TestARegistryEntryWhoseBytesAreNotItsDigestIsRefused(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	writer, _ := openLiveRegistry(t, reg)
	k := testKey("exec go vet ./...")
	saved := testResult("run-a")
	if saved.ExitCode != 0 {
		t.Fatalf("this test rewrites a zero exit code and the fixture has %d", saved.ExitCode)
	}
	if err := writer.Entries().Save(ctx, "vet", k, saved); err != nil {
		t.Fatalf("seeding an entry: %v", err)
	}

	cfg := registryConfig(reg)
	cfg.Transport = corruptBlobs{under: http.DefaultTransport}
	reader, rep := openRegistryWith(t, cfg)

	res, ok, err := reader.Entries().Lookup(ctx, "vet", k)
	if ok {
		t.Fatalf("an entry whose bytes are not what the manifest named was served as a hit, "+
			"reporting exit code %d for a step that exited %d", res.ExitCode, saved.ExitCode)
	}
	if err != nil {
		t.Fatalf("a corrupt entry produced an error rather than a miss: %v", err)
	}
	if len(rep.all()) == 0 {
		t.Error("bytes that did not match their digest were refused silently")
	}
}

// TestTheCorruptingTransportReallyAltersTheEntry keeps the test above
// honest: if the substitution stopped matching, the entry would arrive
// intact and the lookup would be a clean miss for another reason. This
// reads through the transport with verification bypassed and requires the
// blob to come back altered.
func TestTheCorruptingTransportReallyAltersTheEntry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	writer, _ := openLiveRegistry(t, reg)
	k := testKey("exec go vet ./... -tags canary")
	if err := writer.Entries().Save(ctx, "vet", k, testResult("run-a")); err != nil {
		t.Fatalf("seeding an entry: %v", err)
	}

	// Read the blob straight through the corrupting transport, with no senro
	// verification in the way at all.
	raw, err := oci.New(oci.Config{
		Registry: reg.Host, Repository: reg.Repository,
		Username: reg.Username, Password: reg.Password,
		PlainHTTP: true, Timeout: 30 * time.Second,
		Transport: corruptBlobs{under: http.DefaultTransport},
	})
	if err != nil {
		t.Fatalf("oci.New: %v", err)
	}
	manifest, err := raw.GetManifest(ctx, writer.Entries().EntryKey(k.Digest()))
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	var m struct {
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(manifest, &m); err != nil || len(m.Layers) != 1 {
		t.Fatalf("reading the manifest: %v, %d layers", err, len(m.Layers))
	}
	body, err := raw.GetBlob(ctx, m.Layers[0].Digest)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	got := readAll(t, body)

	want := fmt.Sprintf(`"exit_code": %d`, corruptedExitCode)
	if !strings.Contains(string(got), want) {
		t.Fatalf("the corrupting transport did not alter the entry, so the test above proves "+
			"nothing: no %q in\n%s", want, got)
	}
	var entry cache.Entry
	if err := json.Unmarshal(got, &entry); err != nil {
		t.Fatalf("the altered entry no longer parses, so the test above would pass for the "+
			"wrong reason: %v", err)
	}
	if entry.Key.Digest() != k.Digest() {
		t.Fatalf("the alteration changed the key, so the test above would pass for the wrong "+
			"reason: %s, want %s", entry.Key.Digest(), k.Digest())
	}
}

// TestPreviousComesBackForAStepIDNoTagCouldHold is the other half of the
// mapping, and the half that costs something: a step id is arbitrary text, a
// tag is 128 characters of a restricted alphabet, so the pointer's name is a
// hash of the id and the repository's tag list no longer says which steps it
// holds.
func TestPreviousComesBackForAStepIDNoTagCouldHold(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remote, rep := openLiveRegistry(t, reg)
	const step = "build/test[os=linux, arch=amd64]/" + "verylongsuffix-" +
		"0123456789012345678901234567890123456789012345678901234567890123456789"

	k := testKey("exec go test ./... -race")
	if err := remote.Entries().Save(ctx, step, k, testResult("run-a")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	prev, ok, err := remote.Entries().Previous(ctx, step)
	if err != nil {
		t.Fatalf("Previous: %v", err)
	}
	if !ok {
		t.Fatal("Previous found nothing for a step whose id a tag cannot hold verbatim")
	}
	if prev.Key.Digest() != k.Digest() {
		t.Errorf("Previous returned the entry for %s, want %s", prev.Key.Digest(), k.Digest())
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a healthy Previous reported %d degradations: %v", n, rep.all())
	}
}

// TestAnActionLookupAgainstADownRegistryIsAMiss. The rule the whole feature
// rests on, at the layer where breaking it would be worst: a lookup that
// returned an error would let a registry outage fail a build.
func TestAnActionLookupAgainstADownRegistryIsAMiss(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	remote, rep := openRegistryWith(t, unreachableRegistryConfig())
	local := localEntries(t)
	tier := remote.TierEntries(local)

	k := testKey("exec go build ./...")
	if _, ok, err := tier.Lookup(ctx, "build", k); ok || err != nil {
		t.Fatalf("a lookup against a registry that is down = (%v, %v), want a plain miss", ok, err)
	}
	if err := tier.Save(ctx, "build", k, testResult("run-a")); err != nil {
		t.Fatalf("Save against a registry that is down: %v; a cache that is down must never "+
			"fail a run", err)
	}
	if _, ok, err := local.Lookup(ctx, "build", k); err != nil || !ok {
		t.Errorf("the entry did not reach the local cache = (%v, %v)", ok, err)
	}
	if _, ok, err := tier.Previous(ctx, "build"); err != nil || !ok {
		t.Errorf("Previous against a registry that is down = (%v, %v), want the local answer", ok, err)
	}
	if rep.disabled() != 1 {
		t.Errorf("a registry that is down was reported as disabled %d times, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
}

// TestARegistryThatRefusesAWriteDegradesWithoutFailingTheRun uses the
// harness's read-only credential, so the refusal is the one a real deployment
// produces rather than one a fake transport invented: the registry is
// reachable, the reads work, and only the push comes back denied.
func TestARegistryThatRefusesAWriteDegradesWithoutFailingTheRun(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	// Something genuine to find, written by a credential that may write.
	writer, _ := openLiveRegistry(t, reg)
	seeded := testKey("exec go build ./...")
	if err := writer.Entries().Save(ctx, "build", seeded, testResult("trunk")); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	cfg := registryConfig(reg)
	cfg.Username, cfg.Password = reg.ReadOnlyUsername, reg.ReadOnlyPassword
	puller, rep := openRegistryWith(t, cfg)

	// The read half works, which is what makes the write assertion below mean
	// what it says.
	if _, ok, err := puller.Entries().Lookup(ctx, "build", seeded); err != nil || !ok {
		t.Fatalf("a pull-only credential could not read the cache = (%v, %v)", ok, err)
	}
	if n := rep.disabled(); n != 0 {
		t.Fatalf("a successful read disabled the registry: %v", rep.all())
	}

	local := localEntries(t)
	tier := puller.TierEntries(local)
	k := testKey("exec go test ./... -run TestFork")
	if err := tier.Save(ctx, "test", k, testResult("fork-build")); err != nil {
		t.Fatalf("a refused entry save failed the run: %v", err)
	}
	if _, ok, err := local.Lookup(ctx, "test", k); err != nil || !ok {
		t.Errorf("the entry did not reach the local cache = (%v, %v)", ok, err)
	}
	if rep.disabled() != 1 {
		t.Fatalf("a refused save produced %d disable reports, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
	// The refusal keeps its identity all the way up, which is what lets a
	// caller say "check your credentials" rather than "something went wrong".
	// Asserted on the error rather than on the words, because a registry
	// answers a refused push with 401 as readily as with 403 and the sentence
	// is the registry's to choose.
	if err := rep.all()[0].Err; !errors.Is(err, oci.ErrDenied) {
		t.Errorf("the report does not carry a refusal: %v", err)
	}
	if got := rep.all()[0].String(); strings.Contains(got, reg.ReadOnlyPassword) {
		t.Errorf("the report leaked the password: %s", got)
	}
}

// TestTwoMachinesSavingOneKeyAtOnceBothSucceed is the concurrent-writer case a
// mutable name has and a content-addressed one does not: the racers are not
// writing the same bytes, because each carries its own run id. Last writer
// wins, every writer succeeds, and whichever result survives is a result some
// build genuinely produced for this key.
func TestTwoMachinesSavingOneKeyAtOnceBothSucceed(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	k := testKey("exec go build ./...")
	const racers = 8
	errs := make([]error, racers)
	ids := make(map[string]bool, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		runID := fmt.Sprintf("run-%02d", i)
		ids[runID] = true
		remote, _ := openLiveRegistry(t, reg)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = remote.Entries().Save(ctx, "build", k, testResult(runID))
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Save %d: %v", i, err)
		}
	}

	remote, rep := openLiveRegistry(t, reg)
	res, ok, err := remote.Entries().Lookup(ctx, "build", k)
	if err != nil || !ok {
		t.Fatalf("after %d concurrent saves the key does not resolve = (%v, %v)", racers, ok, err)
	}
	if !ids[res.RunID] {
		t.Errorf("the surviving entry names run %q, which is not one of the racers", res.RunID)
	}
	prev, ok, err := remote.Entries().Previous(ctx, "build")
	if err != nil || !ok {
		t.Fatalf("Previous after the race = (%v, %v)", ok, err)
	}
	if prev.Key.Digest() != k.Digest() {
		t.Errorf("Previous points at %s, want %s", prev.Key.Digest(), k.Digest())
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a race that is supposed to be safe reported %d degradations: %v", n, rep.all())
	}
}

// TestForgetLeavesTheRegistryAlone. Forget runs when a hit turns out to
// reference content a sweep collected, which is a statement about THIS
// machine's disk, and it is also what lets the cache be used with a credential
// that cannot delete, which is the policy a sensible team writes.
func TestForgetLeavesTheRegistryAlone(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	remote, _ := openLiveRegistry(t, reg)
	local := localEntries(t)
	tier := remote.TierEntries(local)

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
	if _, ok, err := remote.Entries().Lookup(ctx, "vet", k); err != nil || !ok {
		t.Errorf("Forget removed the shared entry = (%v, %v), want it still there", ok, err)
	}
}

// TestARegistryReadOnlyRemoteSavesNoEntry covers the credential a
// pull-request build should be given, from the senro side of it: the setting
// is deliberate, so it is not a degradation.
func TestARegistryReadOnlyRemoteSavesNoEntry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	ctx := t.Context()

	cfg := registryConfig(reg)
	cfg.ReadOnly = true
	remote, rep := openRegistryWith(t, cfg)

	k := testKey("exec go build ./... -tags fork")
	if err := remote.Entries().Save(ctx, "build", k, testResult("fork")); err != nil {
		t.Fatalf("Save in read-only mode: %v", err)
	}
	if _, ok, err := remote.Entries().Lookup(ctx, "build", k); ok || err != nil {
		t.Errorf("a read-only remote wrote an entry = (%v, %v)", ok, err)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("read-only reported %d degradations: %v", n, rep.all())
	}
}

// The registry action cache is the interface the engine already takes, not a
// parallel one: whatever backs it, a run consults one type.
var _ cache.ActionCache = (*remotecache.Entries)(nil)
