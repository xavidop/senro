package cas_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
)

func mustOpen(t *testing.T) *cas.Dir {
	t.Helper()
	s, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestPutIsContentAddressedAndIdempotent(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()

	d1, err := s.Put(ctx, strings.NewReader("same bytes"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	d2, err := s.Put(ctx, strings.NewReader("same bytes"))
	if err != nil {
		t.Fatalf("Put again: %v", err)
	}
	if d1 != d2 {
		t.Errorf("the same content stored twice gave %q then %q", d1, d2)
	}
	if d1 != cas.FromBytes([]byte("same bytes")) {
		t.Errorf("Put digest %q does not match FromBytes", d1)
	}

	d3, err := s.Put(ctx, strings.NewReader("different bytes"))
	if err != nil {
		t.Fatalf("Put different: %v", err)
	}
	if d3 == d1 {
		t.Error("different content produced the same digest")
	}
}

// The digest is taken over the PLAINTEXT, and compression is an on-disk
// encoding this store owns. If the digest were taken over the compressed
// bytes, a zstd version bump would change every workspace digest in the
// same silent way an unnormalized mtime does.
func TestDigestIsIndependentOfTheOnDiskEncoding(t *testing.T) {
	s := mustOpen(t)
	plain := bytes.Repeat([]byte("a"), 4096)

	d, err := s.Put(context.Background(), bytes.NewReader(plain))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if d != cas.FromBytes(plain) {
		t.Errorf("stored digest %q is not sha256 of the plaintext", d)
	}
	onDisk, err := os.ReadFile(s.Path(d))
	if err != nil {
		t.Fatalf("read stored object: %v", err)
	}
	if bytes.Equal(onDisk, plain) {
		t.Error("the object was stored uncompressed, so this test proves nothing about the encoding")
	}
	if len(onDisk) >= len(plain) {
		t.Errorf("4096 identical bytes stored as %d bytes, expected compression", len(onDisk))
	}
}

func TestGetOnAMissingObjectIsErrNotFound(t *testing.T) {
	s := mustOpen(t)
	_, err := s.Get(context.Background(), cas.FromBytes([]byte("never stored")))
	if !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get of a missing object = %v, want ErrNotFound", err)
	}
}

func TestGetOnAMalformedDigestIsErrNotFoundNotAPathTraversal(t *testing.T) {
	s := mustOpen(t)
	_, err := s.Get(context.Background(), cas.Digest("sha256:../../../etc/passwd"))
	if !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get of a malformed digest = %v, want ErrNotFound", err)
	}
}

// The negative half of "the CAS returns what it stored". A store that only
// ever gets tested with objects it just wrote cannot tell you whether it
// would notice a bit rot, a truncated write, or a half-copied backup.
func TestGetDetectsACorruptedObject(t *testing.T) {
	ctx := context.Background()

	t.Run("undecodable body", func(t *testing.T) {
		s := mustOpen(t)
		d, err := s.Put(ctx, strings.NewReader("hello"))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := os.WriteFile(s.Path(d), []byte("not a zstd frame at all"), 0o644); err != nil {
			t.Fatalf("scribble: %v", err)
		}
		rc, err := s.Get(ctx, d)
		if err == nil {
			_, err = io.ReadAll(rc)
			_ = rc.Close()
		}
		if !errors.Is(err, cas.ErrCorrupt) {
			t.Errorf("reading a scribbled object = %v, want ErrCorrupt", err)
		}
	})

	t.Run("decodable body with the wrong content", func(t *testing.T) {
		s := mustOpen(t)
		dA, err := s.Put(ctx, strings.NewReader("content A"))
		if err != nil {
			t.Fatalf("Put A: %v", err)
		}
		dB, err := s.Put(ctx, strings.NewReader("content B"))
		if err != nil {
			t.Fatalf("Put B: %v", err)
		}
		b, err := os.ReadFile(s.Path(dB))
		if err != nil {
			t.Fatalf("read B: %v", err)
		}
		// A perfectly valid zstd frame sitting at A's address. Nothing but a
		// digest check over the decoded stream can catch this.
		if err := os.WriteFile(s.Path(dA), b, 0o644); err != nil {
			t.Fatalf("swap: %v", err)
		}
		rc, err := s.Get(ctx, dA)
		if err == nil {
			_, err = io.ReadAll(rc)
			_ = rc.Close()
		}
		if !errors.Is(err, cas.ErrCorrupt) {
			t.Errorf("reading a swapped object = %v, want ErrCorrupt", err)
		}
	})
}

func TestVerifyAgreesWithGet(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	d, err := s.Put(ctx, strings.NewReader("good"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Verify(ctx, d); err != nil {
		t.Errorf("Verify on a good object: %v", err)
	}
	if err := os.WriteFile(s.Path(d), []byte("junk"), 0o644); err != nil {
		t.Fatalf("scribble: %v", err)
	}
	if err := s.Verify(ctx, d); !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Verify on a corrupt object = %v, want ErrCorrupt", err)
	}
}

func TestHasReportsPresenceWithoutDecoding(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	d, err := s.Put(ctx, strings.NewReader("present"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ok, err := s.Has(ctx, d)
	if err != nil || !ok {
		t.Errorf("Has(stored) = %v, %v; want true, nil", ok, err)
	}
	ok, err = s.Has(ctx, cas.FromBytes([]byte("absent")))
	if err != nil || ok {
		t.Errorf("Has(absent) = %v, %v; want false, nil", ok, err)
	}
}

// A partly-written object must never become readable under its final name.
// The temp-file-plus-rename is what guarantees that, and this asserts the
// temp file does not leak into the object namespace when Put fails.
func TestAFailedPutLeavesNoObjectAndNoTempFile(t *testing.T) {
	s := mustOpen(t)
	_, err := s.Put(context.Background(), iotest{err: errors.New("source exploded")})
	if err == nil {
		t.Fatal("Put over a failing reader returned no error")
	}
	var objects int
	if walkErr := s.Walk(func(cas.Object) error { objects++; return nil }); walkErr != nil {
		t.Fatalf("Walk: %v", walkErr)
	}
	if objects != 0 {
		t.Errorf("a failed Put left %d objects behind", objects)
	}
	entries, _ := os.ReadDir(filepath.Join(s.Root(), "tmp"))
	if len(entries) != 0 {
		t.Errorf("a failed Put left %d temp files behind", len(entries))
	}
}

// TestSweepTmpRemovesAStaleLeakedTempFile: a killed Put never reaches its
// deferred cleanup (unlike the in-process failure above). Simulated
// directly: a leaked temp file backdated past TmpStaleAge.
func TestSweepTmpRemovesAStaleLeakedTempFile(t *testing.T) {
	s := mustOpen(t)
	leaked := filepath.Join(s.Root(), "tmp", "put-crashed")
	if err := os.WriteFile(leaked, []byte("half-written object"), 0o644); err != nil {
		t.Fatalf("simulate leaked temp file: %v", err)
	}
	old := time.Now().Add(-cas.TmpStaleAge)
	if err := os.Chtimes(leaked, old, old); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	n, err := s.SweepTmp(time.Now())
	if err != nil {
		t.Fatalf("SweepTmp: %v", err)
	}
	if n != 1 {
		t.Errorf("SweepTmp removed %d files, want 1", n)
	}
	if _, err := os.Stat(leaked); !os.IsNotExist(err) {
		t.Errorf("the leaked temp file is still present: err=%v", err)
	}
}

// TestSweepTmpDoesNotRemoveARecentTempFile is the negative half: a temp
// file younger than TmpStaleAge is plausibly a Put still genuinely writing
// a large object, and must not be swept out from under it.
func TestSweepTmpDoesNotRemoveARecentTempFile(t *testing.T) {
	s := mustOpen(t)
	active := filepath.Join(s.Root(), "tmp", "put-inprogress")
	if err := os.WriteFile(active, []byte("still being written"), 0o644); err != nil {
		t.Fatalf("simulate an active Put: %v", err)
	}

	n, err := s.SweepTmp(time.Now())
	if err != nil {
		t.Fatalf("SweepTmp: %v", err)
	}
	if n != 0 {
		t.Errorf("SweepTmp removed %d files, want 0: a recent temp file was swept", n)
	}
	if _, err := os.Stat(active); err != nil {
		t.Errorf("the active temp file was disturbed: %v", err)
	}
}

// TestSweepTmpOverAnEmptyStoreIsNotAnError pins the ordinary case: nothing
// leaked, nothing to do.
func TestSweepTmpOverAnEmptyStoreIsNotAnError(t *testing.T) {
	s := mustOpen(t)
	n, err := s.SweepTmp(time.Now())
	if err != nil {
		t.Fatalf("SweepTmp: %v", err)
	}
	if n != 0 {
		t.Errorf("SweepTmp = %d over an empty store, want 0", n)
	}
}

type iotest struct{ err error }

func (r iotest) Read([]byte) (int, error) { return 0, r.err }

// Walk feeds the GC. The access clock is mtime, not atime: immutable
// content leaves mtime free, and atime is unreliable under relatime and
// noatime mounts.
func TestWalkReportsEveryObjectAndGetUpdatesTheAccessClock(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()

	da, err := s.Put(ctx, strings.NewReader("a"))
	if err != nil {
		t.Fatalf("Put a: %v", err)
	}
	db, err := s.Put(ctx, strings.NewReader("b"))
	if err != nil {
		t.Fatalf("Put b: %v", err)
	}

	old := time.Now().Add(-72 * time.Hour)
	if err := os.Chtimes(s.Path(da), old, old); err != nil {
		t.Fatalf("age a: %v", err)
	}

	seen := map[cas.Digest]time.Time{}
	if err := s.Walk(func(o cas.Object) error {
		if o.Bytes <= 0 {
			t.Errorf("object %s reported %d bytes", o.Digest, o.Bytes)
		}
		seen[o.Digest] = o.Accessed
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(seen) != 2 || seen[da].IsZero() || seen[db].IsZero() {
		t.Fatalf("Walk saw %d objects: %v", len(seen), seen)
	}
	if !seen[da].Before(seen[db]) {
		t.Errorf("the aged object reported %v, not older than %v", seen[da], seen[db])
	}

	rc, err := s.Get(ctx, da)
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	_, _ = io.ReadAll(rc)
	_ = rc.Close()

	after := map[cas.Digest]time.Time{}
	_ = s.Walk(func(o cas.Object) error { after[o.Digest] = o.Accessed; return nil })
	if !after[da].After(seen[da]) {
		t.Errorf("Get did not advance the access clock: %v then %v", seen[da], after[da])
	}
}

func TestDeleteRemovesAnObject(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	d, err := s.Put(ctx, strings.NewReader("doomed"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(d); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := s.Has(ctx, d); ok {
		t.Error("Has still reports a deleted object")
	}
	if err := s.Delete(d); err != nil {
		t.Errorf("Delete is not idempotent: %v", err)
	}
}
