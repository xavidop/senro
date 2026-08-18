package remotecache_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
)

// TestEveryOperationOnItsOwnDegradesRatherThanFailing gives each entry
// point its OWN unreachable remote, so each is the FIRST failure that
// remote sees: exercised in sequence, everything after the first trips the
// "already off" check and never reaches its own error handling, so a
// regression that made a first failure propagate would sail through.
func TestEveryOperationOnItsOwnDegradesRatherThanFailing(t *testing.T) {
	t.Parallel()
	key := testKey("exec go build ./...")
	absent := cas.FromBytes([]byte("nothing has this"))

	for name, op := range map[string]func(t *testing.T, r *remotecache.Remote) error{
		"objects put": func(t *testing.T, r *remotecache.Remote) error {
			_, err := r.TierObjects(localDir(t)).Put(t.Context(), bytes.NewReader([]byte("out")))
			return err
		},
		"objects has": func(t *testing.T, r *remotecache.Remote) error {
			_, err := r.TierObjects(localDir(t)).Has(t.Context(), absent)
			return err
		},
		"objects get": func(t *testing.T, r *remotecache.Remote) error {
			// A miss is the right answer here and is not a failure; anything
			// else is the remote's problem reaching the caller.
			_, err := r.TierObjects(localDir(t)).Get(t.Context(), absent)
			if errors.Is(err, cas.ErrNotFound) {
				return nil
			}
			return err
		},
		"entries lookup": func(t *testing.T, r *remotecache.Remote) error {
			_, _, err := r.TierEntries(localEntries(t)).Lookup(t.Context(), "build", key)
			return err
		},
		"entries save": func(t *testing.T, r *remotecache.Remote) error {
			return r.TierEntries(localEntries(t)).Save(t.Context(), "build", key, testResult("r"))
		},
		"entries previous": func(t *testing.T, r *remotecache.Remote) error {
			_, _, err := r.TierEntries(localEntries(t)).Previous(t.Context(), "build")
			return err
		},
		"entries forget": func(t *testing.T, r *remotecache.Remote) error {
			return r.TierEntries(localEntries(t)).Forget(t.Context(), key)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, rep := openWith(t, unreachableConfig())
			if err := op(t, r); err != nil {
				t.Fatalf("%s against a remote that is down returned %v; a cache that is down "+
					"must never fail a run", name, err)
			}
			// Forget touches only the local half by design, so it neither
			// fails nor reports. Everything else went to the network and has
			// to have said something about what it found there.
			if name != "entries forget" && len(rep.all()) == 0 {
				t.Errorf("%s silently did nothing; nobody would learn the cache was down", name)
			}
		})
	}
}

// refuseWrites lets reads through to the real store and refuses every
// write: the one failure in this suite no real server produces on request
// (a read-only credential is a policy MinIO's root user does not have).
// Reads still go to the live store, and the test below checks that they
// do.
type refuseWrites struct{ under http.RoundTripper }

func (r refuseWrites) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPut && req.Method != http.MethodPost {
		return r.under.RoundTrip(req)
	}
	const body = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code>` +
		`<Message>Access Denied.</Message></Error>`
	return &http.Response{
		StatusCode:    http.StatusForbidden,
		Status:        "403 Forbidden",
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"application/xml"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

// TestAnUploadRefusedWhileReadsWorkDegradesWithoutFailing covers the
// credential a careful team actually issues to a pull-request build: read the
// cache, do not write it. The store is reachable and the reads succeed; only
// the writes come back refused.
func TestAnUploadRefusedWhileReadsWorkDegradesWithoutFailing(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	// Seed a real object through a fully permitted client, so the read half
	// has something genuine to find.
	seeder, _ := openLive(t, m)
	seeded := []byte("something the trunk build published")
	if _, err := seeder.Objects().Put(ctx, bytes.NewReader(seeded)); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	cfg := bucketConfig(m)
	cfg.Transport = refuseWrites{under: http.DefaultTransport}
	r, rep := openWith(t, cfg)

	if _, err := r.TierObjects(localDir(t)).Put(
		ctx, bytes.NewReader([]byte("what this build produced"))); err != nil {
		t.Fatalf("a refused upload failed the run: %v", err)
	}
	if rep.disabled() != 1 {
		t.Fatalf("a refused upload produced %d disable reports, want exactly 1: %v",
			rep.disabled(), rep.all())
	}

	// The same transport, a fresh remote, reading: this is what shows the
	// read half really works through it, so the assertion above is about the
	// write being refused rather than about nothing working at all.
	cfg2 := bucketConfig(m)
	cfg2.Transport = refuseWrites{under: http.DefaultTransport}
	r2, rep2 := openWith(t, cfg2)
	rc, err := r2.TierObjects(localDir(t)).Get(ctx, cas.FromBytes(seeded))
	if err != nil {
		t.Fatalf("reads do not work through this transport, so the check above proved little: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, seeded) {
		t.Error("the read returned the wrong bytes")
	}
	if rep2.disabled() != 0 {
		t.Errorf("a successful read disabled the remote: %v", rep2.all())
	}
}

// TestAnEntrySaveRefusedWhileLookupsWorkDegradesWithoutFailing is the action
// cache's half of the same story.
func TestAnEntrySaveRefusedWhileLookupsWorkDegradesWithoutFailing(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	cfg := bucketConfig(m)
	cfg.Transport = refuseWrites{under: http.DefaultTransport}
	r, rep := openWith(t, cfg)

	local := localEntries(t)
	tier := r.TierEntries(local)
	k := testKey("exec go test ./...")

	if err := tier.Save(ctx, "test", k, testResult("run-pr")); err != nil {
		t.Fatalf("a refused entry save failed the run: %v", err)
	}
	if rep.disabled() != 1 {
		t.Fatalf("a refused save produced %d disable reports, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
	// The local half kept it, which is what makes this run itself still
	// benefit from the work it just did.
	if _, ok, err := local.Lookup(ctx, "test", k); err != nil || !ok {
		t.Errorf("the entry did not reach the local cache = (%v, %v)", ok, err)
	}
}

// TestPerObjectComplaintsAreBounded. A bucket full of rubbish must not be
// able to bury a build's real output under one line per lookup.
func TestPerObjectComplaintsAreBounded(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)
	raw := rawClient(t, m)
	ctx := t.Context()

	for i := range 30 {
		k := testKey(string(rune('a'+i%26)) + "-command-" + string(rune('0'+i/26)))
		if err := raw.PutBytes(ctx, r.Entries().EntryKey(k.Digest()), []byte("not json")); err != nil {
			t.Fatalf("planting: %v", err)
		}
		if _, ok, err := r.Entries().Lookup(ctx, "step", k); ok || err != nil {
			t.Fatalf("Lookup of rubbish = (%v, %v), want a miss", ok, err)
		}
	}
	all := rep.all()
	if len(all) == 0 {
		t.Error("30 unusable entries produced no report at all")
	}
	if len(all) > 12 {
		t.Errorf("30 unusable entries produced %d reports; a bad bucket can bury the build log",
			len(all))
	}
	if rep.disabled() != 0 {
		t.Errorf("unusable entries disabled the store: %v", all)
	}
}

// TestDegradationReadsAsAnInstruction. This line is the only thing a person
// sees when their cache stops working, so it has to say what happened, to
// what, and what follows from it.
func TestDegradationReadsAsAnInstruction(t *testing.T) {
	t.Parallel()
	got := remotecache.Degradation{
		Store:    "s3 bucket team-cache at s3.eu-west-1.amazonaws.com",
		Op:       "get",
		Err:      errors.New("dial tcp 10.0.0.1:443: connect: connection refused"),
		Disabled: true,
	}.String()

	for _, want := range []string{
		"team-cache",                        // which store
		"get",                               // which operation
		"not used for the rest of this run", // what follows
		"connection refused",                // why
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the degradation line does not mention %q: %s", want, got)
		}
	}

	perObject := remotecache.Degradation{Store: "s", Op: "get", Err: errors.New("bad")}.String()
	if strings.Contains(perObject, "rest of this run") {
		t.Errorf("a single bad object reads as if the whole store were switched off: %s", perObject)
	}
}
