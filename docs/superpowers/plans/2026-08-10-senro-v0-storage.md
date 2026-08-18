# senro v0 Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A step's result can be skipped entirely because nothing it depends on changed, and the filesystem a step produced can be named by a digest, restored anywhere, and fed to the next step as an input to its key. Two caches, deliberately separate: an action cache that is correctness-critical and opt-in via `Pure()`, and a scratch cache that is best-effort and never enters a key.

**Architecture:** Everything is content-addressed through one local-directory CAS. A workspace is not a mount, it is a **normalized tar plus a separate file index**, both stored in the CAS, and the digest of that tar is the workspace's identity. An action cache key is not an opaque hash but a struct of **named, ordered components** stored alongside the entry, which is what makes `senro cache explain` a formatter over recorded facts rather than a re-planning exercise. The engine computes a key just before a step runs, looks it up, and either replays the stored logs and restores the stored workspaces, or runs the step and saves what it produced.

**Tech Stack:** Go 1.26, darwin and linux. One new root-module dependency: `github.com/klauspost/compress/zstd`, for the CAS's on-disk encoding. `api` stays standard-library only. Nothing here binds a socket.

**Spec:** `docs/superpowers/specs/2026-08-07-senro-v0-design.md` §4.3, and the source design's §3, §4, §10 and §11 items 2 and 3.

---

## The one thing this plan is really about

`docs/design.md` §11 item 3, in the source's own words:

> **Critical detail: normalize mtime, uid, gid and the file order inside the tar.** tar records mtimes, `go build` touches files, and an unnormalized tar produces a different digest on every run, which silently destroys every cache key downstream of a workspace. Fixed epoch, uid/gid 0, lexicographic order, no extended attributes unless explicitly enabled. **This is the single most likely way to ship a cache that appears to work and never hits.**

This is not a detail inside Task 2. It is the spine, and the plan is arranged around proving it:

- **Task 2** writes the normalization and the tests that pin it at the byte level: the same tree snapshotted twice through two separate operations must produce byte-identical digests; touching a file's mtime must not change the digest; and a reader of the produced tar must find every header carrying uid 0, gid 0, the fixed epoch, and names in sorted order. That last one is the check that would actually catch a regression, because a digest-equality test alone still passes if someone replaces the tar with a constant.
- **Task 13** proves the same property where it is observable: two runs of a pipeline whose first step rewrites a file with identical content and a fresh mtime, asserting the second run's downstream pure step reports `cache.hit`. That is the `go build` scenario the design names, driven through `senro.Run`, the real entry point.

If Task 2's tests pass and Task 13's does not, the bug is in the wiring. If Task 2's tests do not pass, nothing after it is worth building.

---

## Global Constraints

These bind every task in this plan.

- **`api/go.mod` must keep zero `require` entries, stdlib only.** The root module may take dependencies. Adding one to `api` is never the fix.
- **The event log is the single source of truth.** `Seq` is monotonic: a gap is survivable, a regression or duplicate is not, and `api.RunState.Apply` rejects a regression.
- **`Sink.Emit` must never block and never fail.**
- **Secret values must never appear in cache keys, events, or logs.** This one bites hardest in this plan, since a cache key is derived from inputs and a step's environment may hold secrets. Plan for it explicitly rather than leaving it to a later task.
- **No TCP binding anywhere in v0, unix sockets only.**
- **Go 1.26, targeting darwin and linux. Windows is a documented exclusion.**
- **`golangci-lint run ./...` must stay clean in both modules, and `make all` includes a `GOWORK=off` module check.**
- **No em dashes in any prose, comment, or commit message.** Restructure the sentence instead.

And four rules this plan adds, each one the scar of a defect the first four plans shipped:

- **Nothing ships unwired.** Four times this project shipped code with no production caller: a peer-credential check, a discovery registry, a public `Run` that did not exist, and a terminal marker made unreachable by close ordering. Every task below either wires its capability to a production caller in the same task, with a test that exercises it through the real entry point, or names in its own text the exact task and file that does. Task 1 is the only task that uses the second form, and it names Task 3.
- **Every behaviour gets its negative case planned, not just its positive one.** A cache miss as well as a hit. A corrupted CAS entry as well as a good one. A restore that fails as well as one that succeeds. A key component that changes as well as one that does not.
- **Fix the class, not the instance.** Three times a fix in this project addressed the reported case and left the general defect. Where a task below closes a category of problem rather than one occurrence, its text says so under the heading **Class, not instance**.
- **Plan the check that can see the failure.** `go.work` masked a missing `require`, `golangci-lint` defaults hid two thirds of a backlog, and a nodeps test was blind to test-only imports. Where a task adds a guarantee, its text names the check that would fail if the guarantee were violated, under the heading **The check that catches it**.

### Two design decisions this plan makes, written down because they are load-bearing

**1. A workspace digest is the digest of the normalized, uncompressed tar. Compression is a storage detail inside the CAS and never enters any digest.**

§11.3 says "zstd for the body". If the digest were taken over the compressed bytes, a zstd version bump, an encoder-level change, or a concurrency setting would change every workspace digest in exactly the same silent way an unnormalized mtime does, and the mitigation in §11.3 would be necessary but not sufficient. So `cas.Dir` computes sha256 over the plaintext it is handed, stores the zstd-compressed bytes, and decompresses on `Get`. The compressor can be swapped without invalidating one cache key, and Task 1 has a test that says so.

**2. A step's environment enters the cache key as declared names paired with value digests, never as values.**

§3.3's key component is `sorted(env ∩ envAllowlist)`. This plan narrows it further: only names the step declared with `CacheEnv` enter the key, and each enters as `NAME=<first 8 hex of sha256(value)>`. Two consequences, both wanted. A secret that reaches a step's environment by mistake cannot reach a cache entry, which persists across runs and outlives the run directory. And an undeclared environment variable cannot silently invalidate every key, which is the "cache that never hits" failure in its other clothes. Secrets are delivered by `Sandbox.PutSecret` and never through `Node.Env`, and that is the phase-9 contract this plan does not weaken. Task 13 is the task that proves the containment.

---

## What already exists

`internal/plan` holds `Node` and `Plan` with `Digest()` and `Validate()`. `internal/engine` runs a plan: `runCore.emit` appends to the ledger and then hands the stamped event to the sink under one lock, `runStep` owns the retry loop, `runAttempt` owns one attempt's sandbox and log writers, `finishStep` emits the single `step.finished` through `outcomes.settle`. `internal/executor` defines `Executor`, `Sandbox`, `SandboxSpec` and `Mount`, and `internal/executor/localexec` implements them, with `Sandbox.Snapshot` currently returning `"localexec: snapshot not implemented in this phase"`. `internal/eventlog` owns the ledger and the per-step, per-attempt log files. `api` already declares `cache.hit`, `cache.miss`, `cache.saved`, `ws.snapshot`, `ws.restored`, the payload bodies for all five, `StateCached`, and `StepFinishedBody.Cached`.

**Nothing emits any of them.** There is no CAS, no cache, no workspace, and `senro.Run` opens no storage of any kind.

---

## File Structure

```
internal/cas/cas.go             Digest, Store, ErrNotFound, ErrCorrupt, PutBytes/GetBytes
internal/cas/dir.go             local-directory backend: Put, Get, Has, Verify, Walk, Delete
internal/storage/storage.go     the storage root: CAS + action cache + scratch + snapshotter

internal/workspace/index.go     Entry, Index, canonical marshalling
internal/workspace/exclude.go   Excluder, DefaultExcludes, .senroignore, ** glob matching
internal/workspace/tar.go       WriteTar / ReadTar and the normalization (the spine)
internal/workspace/snapshot.go  Snapshotter: Snapshot, Restore, LoadIndex

internal/cache/key.go           Key, Component, Digest, Diff, Explain
internal/cache/action.go        Result, Entry, ActionCache, the local-directory backend
internal/cache/record.go        per-run key records and the `cache explain` formatter
internal/cache/gc.go            pins, retention, the LRU sweep

internal/scratch/scratch.go     restore by key then restore-key prefix, immutable save
internal/scratch/template.go    the {{ hashFiles }} key template

internal/engine/workspaces.go   the run's workspace manager: realize, snapshot, restore
internal/engine/cache.go        the lookup / hit / miss / save path around one step

artifact/artifact.go            Glob and File selectors
senro.go                        Workspace, ScratchCache, Mount, Pure, Inputs, Outputs, CacheEnv
cmd/senro/cmd_cache.go          senro cache explain, senro cache gc
cmd/senro/cmd_ws.go             senro ws ls
```

`internal/cas` imports nothing from this repo. `internal/workspace` imports `internal/cas`. `internal/cache` and `internal/scratch` import both. `internal/storage` imports all four and is the only thing `internal/engine` and `senro.Run` need to know about. There are no cycles, and `internal/cas` in particular must never learn what a step is.

---

### Task 1: The CAS, and the storage root wired into `senro.Run`

**Files:**
- Create `internal/cas/cas.go`, `internal/cas/dir.go`, `internal/cas/cas_test.go`, `internal/cas/dir_test.go`
- Create `internal/storage/storage.go`, `internal/storage/storage_test.go`
- Modify `internal/engine/engine.go` (add `Options.Storage`, around line 29 to line 69)
- Modify `run.go` (add `WithCacheDir`, open storage, around line 33 to line 158)
- Modify `go.mod`, `go.sum`
- Test `run_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks. This is the base of the plan.
- Produces:
  ```go
  package cas
  const Prefix = "sha256:"
  type Digest string
  func FromBytes(b []byte) Digest
  func (d Digest) Valid() bool
  func (d Digest) Hex() string
  func (d Digest) Short() string
  var ErrNotFound = errors.New("cas: object not found")
  var ErrCorrupt  = errors.New("cas: object does not match its digest")
  type Store interface {
      Put(ctx context.Context, r io.Reader) (Digest, error)
      Get(ctx context.Context, d Digest) (io.ReadCloser, error)
      Has(ctx context.Context, d Digest) (bool, error)
  }
  func PutBytes(ctx context.Context, s Store, b []byte) (Digest, error)
  func GetBytes(ctx context.Context, s Store, d Digest) ([]byte, error)
  type Object struct { Digest Digest; Bytes int64; Accessed time.Time }
  type Dir struct { /* unexported */ }
  func Open(root string) (*Dir, error)
  func (s *Dir) Root() string
  func (s *Dir) Path(d Digest) string
  func (s *Dir) Put(ctx context.Context, r io.Reader) (Digest, error)
  func (s *Dir) Get(ctx context.Context, d Digest) (io.ReadCloser, error)
  func (s *Dir) Has(ctx context.Context, d Digest) (bool, error)
  func (s *Dir) Verify(ctx context.Context, d Digest) error
  func (s *Dir) Delete(d Digest) error
  func (s *Dir) Walk(fn func(Object) error) error

  package storage
  type Storage struct { Root string; CAS *cas.Dir }
  func DefaultRoot() (string, error)
  func Open(root string) (*Storage, error)
  func (s *Storage) Close() error

  package engine
  // Options gains: Storage *storage.Storage

  package senro
  func WithCacheDir(dir string) Option
  ```

**Wiring.** `senro.Run` resolves a cache root and opens a `*storage.Storage`, and hands it to `engine.Options.Storage`. Task 1's wiring is deliberately thin: the store is opened, its directory tree is created, and its handle is threaded into the engine. **Task 3 is the first task that writes to it, in `internal/workspace/snapshot.go`'s `Snapshotter.Snapshot`, and Task 9 the first that reads, in `internal/engine/cache.go`'s `serveFromCache`.** Both are in this plan. If this plan is ever abandoned partway, `internal/cas` and `internal/storage` must be reverted rather than left in the tree with no reader, because a store nothing stores into is exactly the defect this project has shipped four times.

- [ ] **Step 1: Add the zstd dependency**

```bash
go get github.com/klauspost/compress@latest
go mod tidy
```

Then confirm the module graph outside the workspace still resolves, which is the gate that caught a missing `require` once already:

```bash
make modcheck
```

- [ ] **Step 2: Write the failing test for `cas.Digest`**

Create `internal/cas/cas_test.go`:

```go
package cas_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
)

func TestFromBytesIsPlainSHA256(t *testing.T) {
	b := []byte("the quick brown fox")
	sum := sha256.Sum256(b)
	want := cas.Digest("sha256:" + hex.EncodeToString(sum[:]))
	if got := cas.FromBytes(b); got != want {
		t.Errorf("FromBytes = %q, want %q", got, want)
	}
}

// Valid is the guard in front of every path that turns a digest into a
// filename. A digest that arrives from an event log, a plan, or a CLI
// argument is untrusted input, and "sha256:../../etc/passwd" must not
// become a path.
func TestValidRejectsAnythingThatIsNotADigest(t *testing.T) {
	sum := sha256.Sum256(nil)
	good := cas.Digest("sha256:" + hex.EncodeToString(sum[:]))
	if !good.Valid() {
		t.Errorf("%q should be valid", good)
	}
	for _, bad := range []cas.Digest{
		"",
		"sha256:",
		"sha256:../../etc/passwd",
		"sha256:" + strings.Repeat("z", 64),
		"sha256:" + strings.Repeat("a", 63),
		"sha256:" + strings.Repeat("a", 65),
		"sha1:" + strings.Repeat("a", 40),
		cas.Digest(strings.ToUpper("sha256:" + hex.EncodeToString(sum[:]))),
	} {
		if bad.Valid() {
			t.Errorf("%q should not be valid", bad)
		}
	}
}

func TestShortIsEightHexDigits(t *testing.T) {
	d := cas.FromBytes([]byte("x"))
	if got := d.Short(); len(got) != 8 || !strings.HasPrefix(d.Hex(), got) {
		t.Errorf("Short() = %q, want the first 8 hex digits of %q", got, d.Hex())
	}
}

func TestPutBytesAndGetBytesRoundTrip(t *testing.T) {
	s, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	want := []byte("payload")
	d, err := cas.PutBytes(ctx, s, want)
	if err != nil {
		t.Fatalf("PutBytes: %v", err)
	}
	got, err := cas.GetBytes(ctx, s, d)
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round trip = %q, want %q", got, want)
	}
}
```

- [ ] **Step 3: Write the failing test for the directory backend**

Create `internal/cas/dir_test.go`:

```go
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
// same silent way an unnormalized mtime does. See the plan's Global
// Constraints, decision 1.
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

type iotest struct{ err error }

func (r iotest) Read([]byte) (int, error) { return 0, r.err }

// Walk feeds the GC in Task 11. The access clock is mtime, not atime: CAS
// content is immutable so mtime carries no other meaning, while atime needs
// a build-tagged syscall on darwin and linux and is unreliable under
// relatime and noatime mounts.
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
```

- [ ] **Step 4: Run to verify it fails** with `go test ./internal/cas/...`. Expect a build failure: package `internal/cas` does not exist.

- [ ] **Step 5: Implement `internal/cas/cas.go`**

```go
// Package cas is senro's content-addressed store: bytes in, a digest out,
// and the same bytes back for that digest anywhere the store is reachable.
//
// A digest is taken over the PLAINTEXT a caller hands in. How a backend
// encodes those bytes on its own storage, compressed or not, is the
// backend's business and never enters the digest. That is not a detail: a
// workspace digest feeds the next step's cache key (design.md §3.3), so if
// an encoder version could change a digest, upgrading a compression library
// would silently invalidate every cache key in the fleet, which is the same
// failure design.md §11.3 warns about for tar mtimes and just as quiet.
package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// Prefix is the algorithm marker every digest carries. One algorithm in v0;
// the prefix exists so a second one can be added without re-parsing anything
// already written to disk or to an event log.
const Prefix = "sha256:"

// Digest is a content address: Prefix followed by 64 lowercase hex digits.
type Digest string

var (
	// ErrNotFound means the store has no object at that address, or the
	// address was not a well-formed digest at all. The two are deliberately
	// one error: a caller asking for a malformed digest is asking for
	// something the store does not have, and distinguishing them would tempt
	// a caller into building a path from an address it has not validated.
	ErrNotFound = errors.New("cas: object not found")

	// ErrCorrupt means the object could not be produced as promised: the
	// stored bytes did not decode, or they decoded to content whose digest is
	// not the one requested. Both are reported as corruption rather than as
	// an I/O error, because from a caller's side the store failed to keep the
	// only promise it makes.
	ErrCorrupt = errors.New("cas: object does not match its digest")
)

// FromBytes is the digest of b.
func FromBytes(b []byte) Digest {
	sum := sha256.Sum256(b)
	return Digest(Prefix + hex.EncodeToString(sum[:]))
}

// Valid reports whether d is a well-formed digest. Every function that turns
// a digest into a filesystem path calls this first: a digest reaches this
// package from event logs, plans and CLI arguments, all of which are
// untrusted input, and "sha256:../../etc/passwd" must never become a path.
func (d Digest) Valid() bool {
	s := string(d)
	if len(s) != len(Prefix)+64 || s[:len(Prefix)] != Prefix {
		return false
	}
	for _, c := range s[len(Prefix):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Hex is the digest without its algorithm prefix.
func (d Digest) Hex() string {
	if len(d) <= len(Prefix) {
		return ""
	}
	return string(d[len(Prefix):])
}

// Short is the first eight hex digits, for error messages, `cache explain`
// output, and the secret identity form design.md §1.6 specifies. Never use
// it as an address.
func (d Digest) Short() string {
	h := d.Hex()
	if len(h) < 8 {
		return h
	}
	return h[:8]
}

// Store is the content-addressed store interface from design.md §3.6. v0
// ships one implementation, Dir. S3 and OCI registry backends are v1.
type Store interface {
	Put(ctx context.Context, r io.Reader) (Digest, error)
	Get(ctx context.Context, d Digest) (io.ReadCloser, error)
	Has(ctx context.Context, d Digest) (bool, error)
}

// PutBytes stores b. For the small JSON objects this repo stores (indexes,
// cache entries) streaming buys nothing and a byte slice reads better.
func PutBytes(ctx context.Context, s Store, b []byte) (Digest, error) {
	return s.Put(ctx, bytesReader(b))
}

// GetBytes reads an object whole. Only for objects a caller already knows
// are small: a workspace tarball goes through Get and stays streamed.
func GetBytes(ctx context.Context, s Store, d Digest) ([]byte, error) {
	rc, err := s.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("cas: read %s: %w", d.Short(), err)
	}
	return b, nil
}

func bytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
```

- [ ] **Step 6: Implement `internal/cas/dir.go`**

```go
package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Dir is the local-directory backend: <root>/sha256/<aa>/<bb>/<hex>, with
// <root>/tmp for in-flight writes. Two levels of fanout keep any one
// directory from holding the whole store, which matters on filesystems whose
// directory lookup degrades with entry count.
type Dir struct {
	root string
	tmp  string
}

var _ Store = (*Dir)(nil)

// Open prepares root as a store, creating it if it is not there.
func Open(root string) (*Dir, error) {
	tmp := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("cas: open %s: %w", root, err)
	}
	return &Dir{root: root, tmp: tmp}, nil
}

// Root is the directory this store lives in.
func (s *Dir) Root() string { return s.root }

// Path is where d is stored. It returns "" for a digest that is not
// well-formed, and every caller checks Valid before using the result.
func (s *Dir) Path(d Digest) string {
	if !d.Valid() {
		return ""
	}
	h := d.Hex()
	return filepath.Join(s.root, "sha256", h[0:2], h[2:4], h)
}

// Put stores everything r yields and returns its digest.
//
// The digest is computed over the plaintext while the compressed form is
// written to a temp file, and the temp file is renamed into place only once
// the whole object is on disk. A reader can therefore never observe a
// partial object under its final name, which is the property that lets a
// concurrent run share this store safely.
func (s *Dir) Put(ctx context.Context, r io.Reader) (Digest, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(s.tmp, "put-")
	if err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	// Both are no-ops on the success path: Close has already run and Remove
	// finds nothing at the old name after the rename. On every failure path
	// they are what stops a partial object leaking into the temp directory.
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	h := sha256.New()
	enc, err := zstd.NewWriter(tmp, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if _, err := io.Copy(io.MultiWriter(enc, h), r); err != nil {
		_ = enc.Close()
		return "", fmt.Errorf("cas: %w", err)
	}
	if err := enc.Close(); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}

	d := Digest(Prefix + hex.EncodeToString(h.Sum(nil)))
	p := s.Path(d)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	if _, err := os.Stat(p); err == nil {
		// Already stored. Content is immutable, so the object already there
		// is these same bytes; touch it so the GC's clock sees the write as
		// an access rather than ageing out an object still in use.
		_ = s.touch(p)
		return d, nil
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return "", fmt.Errorf("cas: %w", err)
	}
	return d, nil
}

// Get returns d's content. The returned reader verifies as it goes and fails
// with ErrCorrupt at EOF if what it decoded does not hash to d, so a caller
// that reads to completion cannot be handed the wrong bytes. A caller that
// stops early gets no such guarantee, which is why Verify exists separately.
func (s *Dir) Get(ctx context.Context, d Digest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := s.Path(d)
	if p == "" {
		return nil, fmt.Errorf("cas: %w: %q is not a digest", ErrNotFound, string(d))
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("cas: %w: %s", ErrNotFound, d.Short())
		}
		return nil, fmt.Errorf("cas: %w", err)
	}
	_ = s.touch(p)
	dec, err := zstd.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cas: %w: %s: %v", ErrCorrupt, d.Short(), err)
	}
	return &verifyReader{
		body: dec.IOReadCloser(),
		dec:  dec,
		file: f,
		want: d,
		h:    sha256.New(),
	}, nil
}

// Has reports whether d is stored. It stats and does not decode, so it is
// cheap and it cannot detect corruption: a Has that says yes followed by a
// Get that says ErrCorrupt is a legitimate sequence, and every caller of Has
// must be able to survive it. Verify is the deep check.
func (s *Dir) Has(_ context.Context, d Digest) (bool, error) {
	p := s.Path(d)
	if p == "" {
		return false, nil
	}
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("cas: %w", err)
}

// Verify reads d in full and checks it. This is what a fsck or a GC uses.
func (s *Dir) Verify(ctx context.Context, d Digest) error {
	rc, err := s.Get(ctx, d)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return err
	}
	return nil
}

// Delete removes an object. Missing is not an error: a GC that races another
// GC must not fail, and a caller deleting what it already deleted is asking
// for a state this achieves either way.
func (s *Dir) Delete(d Digest) error {
	p := s.Path(d)
	if p == "" {
		return nil
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cas: %w", err)
	}
	return nil
}

// Object is one stored object as the GC sees it.
type Object struct {
	Digest Digest
	// Bytes is the size on disk, compressed. The GC works against a disk
	// budget, so the on-disk figure is the one that matters to it.
	Bytes int64
	// Accessed is this store's access clock. It is the file's mtime, not its
	// atime: CAS content is immutable so mtime carries no other meaning,
	// while atime needs a build-tagged syscall on darwin and linux and is
	// unreliable under relatime and noatime mounts. Put and Get both advance
	// it; see touch.
	Accessed time.Time
}

// Walk calls fn for every stored object. Files under tmp are skipped: they
// are in-flight writes with no address yet.
func (s *Dir) Walk(fn func(Object) error) error {
	base := filepath.Join(s.root, "sha256")
	err := filepath.WalkDir(base, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && p == base {
				return fs.SkipAll
			}
			return err
		}
		if e.IsDir() {
			return nil
		}
		d := Digest(Prefix + e.Name())
		if !d.Valid() {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // deleted underneath us by a concurrent GC
			}
			return err
		}
		return fn(Object{Digest: d, Bytes: info.Size(), Accessed: info.ModTime()})
	})
	if err != nil {
		return fmt.Errorf("cas: walk: %w", err)
	}
	return nil
}

// touch advances the access clock. See Object.Accessed.
func (s *Dir) touch(p string) error {
	now := time.Now()
	return os.Chtimes(p, now, now)
}

// verifyReader decodes an object and checks its digest at EOF.
type verifyReader struct {
	body io.ReadCloser
	dec  *zstd.Decoder
	file *os.File
	want Digest
	h    hash.Hash
	done bool
}

func (r *verifyReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if n > 0 {
		_, _ = r.h.Write(p[:n])
	}
	switch {
	case errors.Is(err, io.EOF):
		if r.done {
			return n, io.EOF
		}
		r.done = true
		got := Digest(Prefix + hex.EncodeToString(r.h.Sum(nil)))
		if got != r.want {
			return n, fmt.Errorf("cas: %w: %s decoded to %s", ErrCorrupt, r.want.Short(), got.Short())
		}
		return n, io.EOF
	case err != nil:
		// A decode failure or a read failure on a stored object is reported
		// as corruption rather than as an I/O error, because from the
		// caller's side the store failed to produce what it promised, and
		// distinguishing the two only tempts a caller into retrying.
		return n, fmt.Errorf("cas: %w: %s: %v", ErrCorrupt, r.want.Short(), err)
	}
	return n, nil
}

func (r *verifyReader) Close() error {
	r.dec.Close()
	return r.file.Close()
}
```

- [ ] **Step 7: Run to verify it passes** with `go test ./internal/cas/... -race`.

- [ ] **Step 8: Prove the corruption check can fail.** Temporarily make `verifyReader.Read` return plain `io.EOF` without comparing digests. Confirm `TestGetDetectsACorruptedObject/decodable_body_with_the_wrong_content` fails. Restore, and record the result in the commit body.

- [ ] **Step 9: Write the failing test for `internal/storage`**

Create `internal/storage/storage_test.go`:

```go
package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/storage"
)

func TestOpenCreatesTheStoreLayout(t *testing.T) {
	root := filepath.Join(t.TempDir(), "senro-cache")
	s, err := storage.Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.CAS == nil {
		t.Fatal("Open returned a Storage with no CAS")
	}
	for _, sub := range []string{"cas", "action", "scratch", "pins"} {
		if fi, err := os.Stat(filepath.Join(root, sub)); err != nil || !fi.IsDir() {
			t.Errorf("Open did not create %s: %v", sub, err)
		}
	}
}

func TestOpenIsIdempotentOverAnExistingRoot(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 2; i++ {
		s, err := storage.Open(root)
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		_ = s.Close()
	}
}

func TestDefaultRootPrefersTheEnvironmentOverride(t *testing.T) {
	want := t.TempDir()
	t.Setenv("SENRO_CACHE_DIR", want)
	got, err := storage.DefaultRoot()
	if err != nil {
		t.Fatalf("DefaultRoot: %v", err)
	}
	if got != want {
		t.Errorf("DefaultRoot = %q, want %q", got, want)
	}
}

func TestDefaultRootFallsBackToTheUserCacheDir(t *testing.T) {
	t.Setenv("SENRO_CACHE_DIR", "")
	got, err := storage.DefaultRoot()
	if err != nil {
		t.Fatalf("DefaultRoot: %v", err)
	}
	base, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no user cache dir on this host: %v", err)
	}
	if want := filepath.Join(base, "senro"); got != want {
		t.Errorf("DefaultRoot = %q, want %q", got, want)
	}
}
```

- [ ] **Step 10: Run to verify it fails** with `go test ./internal/storage/...`. Expect a build failure: package does not exist.

- [ ] **Step 11: Implement `internal/storage/storage.go`**

```go
// Package storage is the one handle the engine holds on everything
// content-addressed: the CAS, the action cache, the scratch cache and the
// workspace snapshotter, all rooted in one directory.
//
// It exists so engine.Options grows one field rather than four, and so the
// question "where does this run's cache live" has exactly one answer.
package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/senro/internal/cas"
)

// Storage is an opened storage root.
type Storage struct {
	// Root is the directory everything below lives in.
	Root string
	// CAS holds every content-addressed object: workspace tarballs, file
	// indexes, cached logs, cached output artifacts.
	CAS *cas.Dir
}

// DefaultRoot is where a cache lives when nobody says otherwise:
// $SENRO_CACHE_DIR when it is set, and os.UserCacheDir()/senro otherwise.
// One environment variable rather than a flag on every command, because a
// developer switching cache roots wants to switch it for everything at once,
// and CI wants to point it at a workspace-local path in one place.
func DefaultRoot() (string, error) {
	if v := os.Getenv("SENRO_CACHE_DIR"); v != "" {
		return v, nil
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("storage: no cache directory available: %w", err)
	}
	return filepath.Join(base, "senro"), nil
}

// Open prepares root, creating whatever is missing.
//
// Every subdirectory is created up front, even the ones a given run will
// never write to, so that a directory listing of a cache root reads the same
// on every machine and a missing directory is a real anomaly rather than a
// run that happened not to need it yet.
func Open(root string) (*Storage, error) {
	for _, sub := range []string{"action", "scratch", "pins"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o755); err != nil {
			return nil, fmt.Errorf("storage: open %s: %w", root, err)
		}
	}
	store, err := cas.Open(filepath.Join(root, "cas"))
	if err != nil {
		return nil, err
	}
	return &Storage{Root: root, CAS: store}, nil
}

// Close releases whatever the store holds. Nothing does today; the method
// exists so callers write the defer now rather than discovering they need
// one after a backend starts holding a file handle or a connection.
func (s *Storage) Close() error { return nil }
```

- [ ] **Step 12: Run to verify it passes** with `go test ./internal/storage/... -race`.

- [ ] **Step 13: Write the failing test for the production wiring**

Append to `run_test.go`:

```go
// The store is opened by the real entry point, not by a test helper. This
// project has shipped four separate capabilities with no production caller;
// this assertion is what stops internal/cas becoming the fifth.
func TestRunOpensTheStorageRoot(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	runDir := filepath.Join(t.TempDir(), "run")

	pipe := senro.New("storage")
	line := pipe.Workflow("main")
	line.Step("noop", exec.Command("true"))
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(cacheDir, "cas")); err != nil || !fi.IsDir() {
		t.Errorf("Run did not open a CAS under the cache dir: %v", err)
	}
}

// A cache root that cannot be created is an engine failure, not a silent
// downgrade to "no caching". A run that quietly stops caching is the exact
// shape of the bug design.md §11.3 warns about, arriving through a
// different door.
func TestRunFailsWhenTheCacheRootCannotBeOpened(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("file, not a directory"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	pipe := senro.New("storage")
	line := pipe.Workflow("main")
	line.Step("noop", exec.Command("true"))
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")), senro.WithCacheDir(blocked))
	if err == nil {
		t.Fatal("Run over an unopenable cache root returned nil")
	}
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Errorf("an unopenable cache root was reported as a run outcome (%s), want an engine error", runErr.Status)
	}
}
```

- [ ] **Step 14: Run to verify it fails** with `go test . -run TestRunOpens`. Expect `undefined: senro.WithCacheDir`.

- [ ] **Step 15: Add `Options.Storage` to the engine.** In `internal/engine/engine.go`, add the field to `Options` with this doc:

```go
	// Storage is the run's content-addressed store, action cache and
	// workspace snapshotter (design.md §3.6, §4.2). A nil Storage is legal
	// and means this run has no cache and no workspaces: every step runs,
	// nothing is snapshotted, and a plan that declares a workspace or a
	// Pure() step is rejected by Run rather than executed with the
	// declaration silently ignored. senro.Run always supplies one.
	Storage *storage.Storage
```

- [ ] **Step 16: Add `WithCacheDir` and open the store in `run.go`**

```go
// WithCacheDir overrides where the content-addressed store, the action
// cache and the scratch cache live. Unset, Run uses storage.DefaultRoot:
// $SENRO_CACHE_DIR when set, and os.UserCacheDir()/senro otherwise.
//
// Unlike WithDir, this is deliberately NOT per-run. A run directory is one
// run's record; a cache root is shared by every run on the machine, and
// that sharing is the entire point of a cache.
func WithCacheDir(dir string) Option {
	return func(c *runConfig) { c.cacheDir = dir }
}
```

and in `Run`, after the run directory is resolved and before `engine.Run`:

```go
	cacheDir := cfg.cacheDir
	if cacheDir == "" {
		cacheDir, err = storage.DefaultRoot()
		if err != nil {
			return fmt.Errorf("senro: %w", err)
		}
	}
	store, err := storage.Open(cacheDir)
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}
	defer func() { _ = store.Close() }()
```

with `err` declared alongside, and `Storage: store` added to the `engine.Options` literal.

- [ ] **Step 17: Run to verify it passes** with `go test . -race` and `go test ./... -race`.

- [ ] **Step 18: Run the gates**

```bash
make all
golangci-lint run ./... && (cd api && golangci-lint run ./...)
```

- [ ] **Step 19: Commit**

```bash
git add go.mod go.sum internal/cas internal/storage internal/engine/engine.go run.go run_test.go
git commit -m "feat(cas): content-addressed store, opened by senro.Run

The digest is taken over the plaintext and compression is an on-disk
encoding the backend owns, so swapping the compressor cannot invalidate a
cache key. Get verifies as it reads and reports ErrCorrupt for a body that
does not decode as well as for one that decodes to the wrong content."
```

---

### Task 2: The normalized tar and the file index

**This is the spine.** Everything downstream of a workspace inherits its digest, so a digest that moves for a reason that is not a content change destroys the cache silently. Read `docs/design.md` §11 item 3 before starting.

**Files:**
- Create `internal/workspace/index.go`, `internal/workspace/exclude.go`, `internal/workspace/tar.go`
- Create `internal/workspace/index_test.go`, `internal/workspace/exclude_test.go`, `internal/workspace/tar_test.go`

**Interfaces:**
- Consumes: Task 1's `cas.Digest`, `cas.FromBytes`.
- Produces:
  ```go
  package workspace
  var Epoch = time.Unix(0, 0).UTC()
  const IndexVersion = 1
  type Entry struct {
      Path   string     `json:"path"`
      Mode   uint32     `json:"mode"`
      Size   int64      `json:"size,omitempty"`
      Digest cas.Digest `json:"digest,omitempty"`
      Link   string     `json:"link,omitempty"`
  }
  type Index struct {
      Version int     `json:"version"`
      Entries []Entry `json:"entries"`
  }
  func (ix Index) Marshal() ([]byte, error)
  func UnmarshalIndex(b []byte) (Index, error)
  func (ix Index) Bytes() int64
  type Excluder struct { /* unexported */ }
  var DefaultExcludes = []string{".git/", "node_modules/"}
  func NewExcluder(patterns ...string) *Excluder
  func (e *Excluder) Match(rel string, isDir bool) bool
  func LoadIgnoreFile(root string) ([]string, error)
  const IgnoreFile = ".senroignore"
  func WriteTar(w io.Writer, root string, ex *Excluder) (Index, error)
  func ReadTar(r io.Reader, dest string) (Index, error)
  var ErrUnsafePath = errors.New("workspace: entry escapes the destination")
  ```

**The check that catches it.** A digest-equality test alone still passes if someone replaces the tar body with a constant, and a "content changed so the digest changed" test alone still passes if mtimes leak in. Three checks together are what pin this: byte-identical digests across two independent snapshots, an unchanged digest after `os.Chtimes`, and a reader over the produced tar asserting every header carries uid 0, gid 0, the fixed epoch, no atime or ctime record, and names in sorted order. The third is the one that fails the instant somebody drops the normalization and keeps the walk.

**Class, not instance.** `ReadTar` rejects any entry whose cleaned destination escapes the target directory, and any symlink whose target escapes it, rather than filtering the particular shapes seen in a bug report. A workspace tarball is content from a previous run and, in v1, from a shared cache backend, so it is untrusted input by construction.

- [ ] **Step 1: Write the failing test for the index**

Create `internal/workspace/index_test.go`:

```go
package workspace_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// The index is stored in the CAS, so its bytes are its address. Marshalling
// must therefore be canonical: the same index must produce the same bytes on
// every machine and every Go release, or ws ls and the digest that names it
// come apart.
func TestIndexMarshalIsCanonicalAndSorted(t *testing.T) {
	ix := workspace.Index{
		Version: workspace.IndexVersion,
		Entries: []workspace.Entry{
			{Path: "b.txt", Mode: 0o644, Size: 1, Digest: cas.FromBytes([]byte("b"))},
			{Path: "a.txt", Mode: 0o644, Size: 1, Digest: cas.FromBytes([]byte("a"))},
			{Path: "a", Mode: 0o755},
		},
	}
	b1, err := ix.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	b2, err := ix.Marshal()
	if err != nil {
		t.Fatalf("Marshal again: %v", err)
	}
	if string(b1) != string(b2) {
		t.Error("two Marshal calls over the same index produced different bytes")
	}

	got := string(b1)
	ia, ib := strings.Index(got, `"a.txt"`), strings.Index(got, `"b.txt"`)
	if ia < 0 || ib < 0 || ia > ib {
		t.Errorf("Marshal did not sort entries by path:\n%s", got)
	}
	if strings.Contains(got, `&`) || strings.Contains(got, `<`) {
		t.Errorf("Marshal HTML-escaped a path, which makes the bytes depend on content:\n%s", got)
	}
}

func TestIndexRoundTrips(t *testing.T) {
	want := workspace.Index{
		Version: workspace.IndexVersion,
		Entries: []workspace.Entry{
			{Path: "dir", Mode: 0o755},
			{Path: "dir/file", Mode: 0o644, Size: 3, Digest: cas.FromBytes([]byte("abc"))},
			{Path: "link", Mode: 0o777, Link: "dir/file"},
		},
	}
	b, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := workspace.UnmarshalIndex(b)
	if err != nil {
		t.Fatalf("UnmarshalIndex: %v", err)
	}
	if len(got.Entries) != len(want.Entries) || got.Version != want.Version {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	for i := range want.Entries {
		if got.Entries[i] != want.Entries[i] {
			t.Errorf("entry %d = %+v, want %+v", i, got.Entries[i], want.Entries[i])
		}
	}
}

func TestUnmarshalIndexRejectsAFutureVersion(t *testing.T) {
	_, err := workspace.UnmarshalIndex([]byte(`{"version":999,"entries":[]}`))
	if err == nil {
		t.Error("an index from a future version was accepted; a reader that guesses at an unknown layout is worse than one that refuses")
	}
}
```

- [ ] **Step 2: Write the failing test for the excluder**

Create `internal/workspace/exclude_test.go`:

```go
package workspace_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/workspace"
)

func TestExcluderMatchesGlobsIncludingDoubleStar(t *testing.T) {
	ex := workspace.NewExcluder("**/*.tmp", "build/", "top.log", "a/?/c")
	for _, tc := range []struct {
		path  string
		isDir bool
		want  bool
	}{
		{"x.tmp", false, true},
		{"deep/nested/x.tmp", false, true},
		{"x.tmpx", false, false},
		{"build", true, true},
		{"build/out", false, true},
		{"rebuild", true, false},
		{"top.log", false, true},
		{"sub/top.log", false, false},
		{"a/b/c", false, true},
		{"a/bb/c", false, false},
		{"src/main.go", false, false},
	} {
		if got := ex.Match(tc.path, tc.isDir); got != tc.want {
			t.Errorf("Match(%q, isDir=%v) = %v, want %v", tc.path, tc.isDir, got, tc.want)
		}
	}
}

// The negative case that matters most: an excluder with no patterns must
// exclude nothing at all. An excluder that quietly matched everything would
// produce an empty, stable, wrong workspace digest, which is the failure
// mode this whole task exists to prevent, only louder.
func TestAnEmptyExcluderMatchesNothing(t *testing.T) {
	ex := workspace.NewExcluder()
	for _, p := range []string{"a", "a/b", ".git", "anything at all"} {
		if ex.Match(p, false) {
			t.Errorf("an empty excluder matched %q", p)
		}
	}
}

func TestDefaultExcludesCoverTheDirectoriesTheDesignNames(t *testing.T) {
	ex := workspace.NewExcluder(workspace.DefaultExcludes...)
	for _, tc := range []struct {
		path  string
		isDir bool
	}{
		{".git", true},
		{".git/config", false},
		{"node_modules", true},
		{"node_modules/left-pad/index.js", false},
		{"pkg/node_modules", true},
	} {
		if !ex.Match(tc.path, tc.isDir) {
			t.Errorf("DefaultExcludes did not match %q", tc.path)
		}
	}
	if ex.Match("src/main.go", false) {
		t.Error("DefaultExcludes matched an ordinary source file")
	}
}

func TestLoadIgnoreFileReadsPatternsAndSkipsCommentsAndBlanks(t *testing.T) {
	root := t.TempDir()
	body := "# a comment\n\n  *.tmp  \nbuild/\n"
	if err := os.WriteFile(filepath.Join(root, workspace.IgnoreFile), []byte(body), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := workspace.LoadIgnoreFile(root)
	if err != nil {
		t.Fatalf("LoadIgnoreFile: %v", err)
	}
	want := []string{"*.tmp", "build/"}
	if len(got) != len(want) {
		t.Fatalf("LoadIgnoreFile = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pattern %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLoadIgnoreFileIsNotAnErrorWhenThereIsNoFile(t *testing.T) {
	got, err := workspace.LoadIgnoreFile(t.TempDir())
	if err != nil {
		t.Fatalf("LoadIgnoreFile on a workspace with no ignore file: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("LoadIgnoreFile invented %d patterns", len(got))
	}
}

// A negation pattern is the one piece of .gitignore syntax that changes what
// an earlier pattern means, and half-supporting it is how a file silently
// enters or leaves a snapshot. v0 refuses it by name rather than ignoring
// the "!" and matching the rest of the line as a literal.
func TestLoadIgnoreFileRejectsNegationRatherThanMisreadingIt(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, workspace.IgnoreFile), []byte("!keep.txt\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := workspace.LoadIgnoreFile(root); err == nil {
		t.Error("a negation pattern was accepted; v0 does not implement negation and must say so")
	}
}
```

- [ ] **Step 3: Write the failing test for the tar. This is the plan's centre.**

Create `internal/workspace/tar_test.go`:

```go
package workspace_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// tree writes files into a fresh directory. Paths use forward slashes; a
// value ending in "/" is a directory, and a value of the form "->target" is
// a symlink.
func tree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		full := filepath.Join(root, filepath.FromSlash(p))
		body := files[p]
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", p, err)
		}
		switch {
		case body == "/":
			if err := os.MkdirAll(full, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", p, err)
			}
		case len(body) > 2 && body[:2] == "->":
			if err := os.Symlink(body[2:], full); err != nil {
				t.Fatalf("symlink %s: %v", p, err)
			}
		default:
			if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
				t.Fatalf("write %s: %v", p, err)
			}
		}
	}
	return root
}

func tarOf(t *testing.T, root string, ex *workspace.Excluder) ([]byte, workspace.Index) {
	t.Helper()
	var buf bytes.Buffer
	ix, err := workspace.WriteTar(&buf, root, ex)
	if err != nil {
		t.Fatalf("WriteTar: %v", err)
	}
	return buf.Bytes(), ix
}

// THE test. design.md §11 item 3: an unnormalized tar produces a different
// digest on every run, which silently destroys every cache key downstream of
// a workspace. Two separate WriteTar operations over the same tree must
// produce byte-identical output, and therefore byte-identical digests.
func TestSnapshotDigestIsIdenticalAcrossTwoSeparateOperations(t *testing.T) {
	root := tree(t, map[string]string{
		"go.sum":            "h1:abc\n",
		"cmd/app/main.go":   "package main\n",
		"internal/lib.go":   "package internal\n",
		"internal/nested/":  "/",
		"link":              "->go.sum",
	})

	first, ixA := tarOf(t, root, workspace.NewExcluder())
	second, ixB := tarOf(t, root, workspace.NewExcluder())

	if !bytes.Equal(first, second) {
		t.Fatalf("two WriteTar operations over the same tree produced different bytes (%d vs %d)",
			len(first), len(second))
	}
	if cas.FromBytes(first) != cas.FromBytes(second) {
		t.Fatal("byte-equal tars produced different digests, which is impossible and means the comparison above is wrong")
	}
	if len(ixA.Entries) != len(ixB.Entries) {
		t.Errorf("the two indexes disagree on entry count: %d vs %d", len(ixA.Entries), len(ixB.Entries))
	}
}

// The other half of THE test, and the one that names the actual mechanism.
// `go build` rewrites files it did not change. If mtime reached the tar, this
// is the assertion that would fail, and every cache key downstream of a
// workspace would silently stop hitting.
func TestTouchingAnMtimeDoesNotChangeTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"go.sum":          "h1:abc\n",
		"cmd/app/main.go": "package main\n",
	})

	before, _ := tarOf(t, root, workspace.NewExcluder())

	future := time.Now().Add(48 * time.Hour)
	for _, p := range []string{"go.sum", "cmd/app/main.go", "cmd/app", "cmd"} {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(p)), future, future); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}

	after, _ := tarOf(t, root, workspace.NewExcluder())

	if !bytes.Equal(before, after) {
		t.Fatalf("touching mtimes changed the tar: %s then %s.\n"+
			"design.md §11 item 3: this is the single most likely way to ship a cache that appears to work and never hits",
			cas.FromBytes(before), cas.FromBytes(after))
	}
}

// The negative half. Without this, a WriteTar that emitted a constant would
// pass both tests above.
func TestChangingContentDoesChangeTheDigest(t *testing.T) {
	root := tree(t, map[string]string{"a.txt": "one\n"})
	before, _ := tarOf(t, root, workspace.NewExcluder())

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("two\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	after, _ := tarOf(t, root, workspace.NewExcluder())

	if bytes.Equal(before, after) {
		t.Fatal("changing a file's content did not change the tar, so the digest is not a content address")
	}
}

func TestChangingAnExecutableBitDoesChangeTheDigest(t *testing.T) {
	root := tree(t, map[string]string{"run.sh": "#!/bin/sh\n"})
	before, _ := tarOf(t, root, workspace.NewExcluder())

	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	after, _ := tarOf(t, root, workspace.NewExcluder())

	if bytes.Equal(before, after) {
		t.Fatal("making a file executable did not change the digest, so a restored workspace could lose the bit undetected")
	}
}

// The check that would actually catch a regression. The two digest tests
// above still pass if somebody drops the normalization and the tree happens
// to be written fast enough that every mtime lands in the same second. This
// one reads the tar and asserts the normalization directly.
func TestEveryTarHeaderIsNormalizedAndNamesAreSorted(t *testing.T) {
	root := tree(t, map[string]string{
		"z.txt":       "z\n",
		"a.txt":       "a\n",
		"m/":          "/",
		"m/inner.txt": "inner\n",
		"run.sh":      "#!/bin/sh\n",
		"link":        "->a.txt",
	})
	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	body, _ := tarOf(t, root, workspace.NewExcluder())

	var names []string
	tr := tar.NewReader(bytes.NewReader(body))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		names = append(names, hdr.Name)

		if !hdr.ModTime.Equal(workspace.Epoch) {
			t.Errorf("%s: ModTime = %v, want the fixed epoch %v", hdr.Name, hdr.ModTime, workspace.Epoch)
		}
		if !hdr.AccessTime.IsZero() {
			t.Errorf("%s: AccessTime = %v, want the zero time so no PAX atime record is written",
				hdr.Name, hdr.AccessTime)
		}
		if !hdr.ChangeTime.IsZero() {
			t.Errorf("%s: ChangeTime = %v, want the zero time so no PAX ctime record is written",
				hdr.Name, hdr.ChangeTime)
		}
		if hdr.Uid != 0 || hdr.Gid != 0 {
			t.Errorf("%s: uid/gid = %d/%d, want 0/0", hdr.Name, hdr.Uid, hdr.Gid)
		}
		if hdr.Uname != "" || hdr.Gname != "" {
			t.Errorf("%s: uname/gname = %q/%q, want empty", hdr.Name, hdr.Uname, hdr.Gname)
		}
		if len(hdr.PAXRecords) != 0 {
			t.Errorf("%s: PAX records %v, want none", hdr.Name, hdr.PAXRecords)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if hdr.Mode != 0o755 {
				t.Errorf("%s: dir mode %o, want 755", hdr.Name, hdr.Mode)
			}
		case tar.TypeSymlink:
			// mode is not meaningful for a symlink in tar; nothing to assert.
		default:
			if hdr.Mode != 0o644 && hdr.Mode != 0o755 {
				t.Errorf("%s: file mode %o, want 644 or 755", hdr.Name, hdr.Mode)
			}
		}
	}

	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for i := range names {
		if names[i] != sorted[i] {
			t.Fatalf("tar entries are not in lexicographic order:\n got %v\nwant %v", names, sorted)
		}
	}
}

func TestWriteTarBuildsAnIndexWithPerFileDigests(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt": "hello",
		"d/":    "/",
		"l":     "->a.txt",
	})
	_, ix := tarOf(t, root, workspace.NewExcluder())

	byPath := map[string]workspace.Entry{}
	for _, e := range ix.Entries {
		byPath[e.Path] = e
	}
	if e := byPath["a.txt"]; e.Digest != cas.FromBytes([]byte("hello")) || e.Size != 5 {
		t.Errorf("a.txt entry = %+v, want size 5 and the sha256 of its content", e)
	}
	if e := byPath["d"]; e.Digest != "" || e.Mode != 0o755 {
		t.Errorf("directory entry = %+v, want no digest and mode 755", e)
	}
	if e := byPath["l"]; e.Link != "a.txt" || e.Digest != "" {
		t.Errorf("symlink entry = %+v, want Link=a.txt and no digest", e)
	}
	if ix.Version != workspace.IndexVersion {
		t.Errorf("index version = %d, want %d", ix.Version, workspace.IndexVersion)
	}
}

func TestWriteTarHonoursTheExcluder(t *testing.T) {
	root := tree(t, map[string]string{
		"keep.go":            "package a\n",
		"skip.tmp":           "junk\n",
		".git/config":        "[core]\n",
		"node_modules/x/i.js": "1\n",
	})
	ex := workspace.NewExcluder(append([]string{"**/*.tmp"}, workspace.DefaultExcludes...)...)
	_, ix := tarOf(t, root, ex)

	for _, e := range ix.Entries {
		switch e.Path {
		case "skip.tmp", ".git", ".git/config", "node_modules", "node_modules/x", "node_modules/x/i.js":
			t.Errorf("excluded path %q reached the index", e.Path)
		}
	}
	var found bool
	for _, e := range ix.Entries {
		if e.Path == "keep.go" {
			found = true
		}
	}
	if !found {
		t.Error("the excluder dropped a file it was not asked to drop")
	}
}

func TestWriteTarSkipsIrregularFilesRatherThanFailing(t *testing.T) {
	root := tree(t, map[string]string{"ok.txt": "x\n"})
	fifo := filepath.Join(root, "pipe")
	if err := mkfifo(fifo); err != nil {
		t.Skipf("cannot create a fifo here: %v", err)
	}
	_, ix := tarOf(t, root, workspace.NewExcluder())
	for _, e := range ix.Entries {
		if e.Path == "pipe" {
			t.Error("a fifo reached the index; only regular files, directories and symlinks are portable")
		}
	}
}

// Restore is the other half. Restoring a snapshot and snapshotting the result
// must reproduce the digest exactly, which is the only end-to-end statement
// that both halves agree on the same normalization.
func TestReadTarThenWriteTarReproducesTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt":    "a\n",
		"d/b.txt":  "b\n",
		"run.sh":   "#!/bin/sh\n",
		"l":        "->a.txt",
	})
	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	body, _ := tarOf(t, root, workspace.NewExcluder())

	dest := t.TempDir()
	if _, err := workspace.ReadTar(bytes.NewReader(body), dest); err != nil {
		t.Fatalf("ReadTar: %v", err)
	}
	again, _ := tarOf(t, dest, workspace.NewExcluder())

	if !bytes.Equal(body, again) {
		t.Fatalf("snapshot(restore(snapshot(tree))) changed the digest: %s then %s",
			cas.FromBytes(body), cas.FromBytes(again))
	}
}

// Untrusted input. A workspace tarball comes from a previous run and, in v1,
// from a shared cache backend, so a path that escapes the destination is a
// remote write primitive.
func TestReadTarRejectsPathsThatEscapeTheDestination(t *testing.T) {
	for _, tc := range []struct {
		name string
		hdr  tar.Header
	}{
		{"parent traversal", tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644}},
		{"absolute path", tar.Header{Name: "/etc/escape", Typeflag: tar.TypeReg, Mode: 0o644}},
		{"nested traversal", tar.Header{Name: "a/../../escape", Typeflag: tar.TypeReg, Mode: 0o644}},
		{"symlink out", tar.Header{Name: "l", Typeflag: tar.TypeSymlink, Linkname: "../../etc/passwd"}},
		{"absolute symlink", tar.Header{Name: "l", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			hdr := tc.hdr
			hdr.ModTime = workspace.Epoch
			if err := tw.WriteHeader(&hdr); err != nil {
				t.Fatalf("WriteHeader: %v", err)
			}
			if err := tw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			dest := t.TempDir()
			_, err := workspace.ReadTar(bytes.NewReader(buf.Bytes()), dest)
			if !errors.Is(err, workspace.ErrUnsafePath) {
				t.Errorf("ReadTar over %s = %v, want ErrUnsafePath", tc.name, err)
			}
		})
	}
}
```

Add `internal/workspace/mkfifo_unix.go` alongside, so the fifo test compiles on darwin and linux and nowhere else, which matches the module's documented target set:

```go
//go:build unix

package workspace_test

import "golang.org/x/sys/unix"

func mkfifo(path string) error { return unix.Mkfifo(path, 0o600) }
```

`golang.org/x/sys` is already an indirect dependency of the root module; `go mod tidy` will promote it to direct.

- [ ] **Step 4: Run to verify it fails** with `go test ./internal/workspace/...`. Expect a build failure: package does not exist.

- [ ] **Step 5: Implement `internal/workspace/index.go`**

```go
// Package workspace turns a directory into a digest and a digest back into a
// directory.
//
// A workspace is a named, versioned directory with a content digest, not a
// mount (design.md §4.1). A snapshot is a normalized tar plus a separate file
// index (design.md §11 item 3): the tar is the body, the index carries path,
// mode, size, digest and symlink target so `ws ls` never has to download the
// body.
//
// # Why normalization is the whole point
//
// tar records mtime, uid, gid and whatever order the walk produced. Every one
// of those is a fact about the machine that took the snapshot rather than
// about the files in it. `go build` rewrites files it did not change, so an
// unnormalized tar digests differently on every run, and because a workspace
// digest is an input to the next step's cache key (design.md §3.3), the cache
// stops hitting and nothing anywhere reports an error. design.md §11 item 3
// calls this "the single most likely way to ship a cache that appears to work
// and never hits". Normalization is therefore not a tidiness measure, it is
// the correctness condition for everything downstream.
package workspace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/xavidop/senro/internal/cas"
)

// IndexVersion is the index layout version. A reader that meets a version it
// does not know refuses rather than guessing: an index is how `ws ls` reports
// what is in a snapshot, and a wrong answer there sends someone debugging the
// wrong build.
const IndexVersion = 1

// Entry is one path in a snapshot.
type Entry struct {
	// Path is relative to the workspace root and always uses forward
	// slashes, on every platform, so an index taken on one host reads
	// identically on another.
	Path string `json:"path"`
	// Mode is the normalized permission bits: 0644 or 0755 for a regular
	// file, 0755 for a directory, 0777 for a symlink. Nothing else survives,
	// because nothing else is portable between the executors design.md §4.3
	// lists.
	Mode uint32 `json:"mode"`
	// Size and Digest are set for regular files only.
	Size   int64      `json:"size,omitempty"`
	Digest cas.Digest `json:"digest,omitempty"`
	// Link is a symlink's target, verbatim.
	Link string `json:"link,omitempty"`
}

// Index is the file list of one snapshot.
type Index struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

// Marshal encodes the index canonically: entries sorted by path, no HTML
// escaping, and a trailing newline. The bytes are the index's address in the
// CAS, so two indexes describing the same tree must encode identically or the
// digest naming one of them is wrong.
func (ix Index) Marshal() ([]byte, error) {
	out := Index{Version: ix.Version, Entries: append([]Entry(nil), ix.Entries...)}
	sort.Slice(out.Entries, func(i, j int) bool { return out.Entries[i].Path < out.Entries[j].Path })

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Without this, encoding/json rewrites <, > and & as < and friends,
	// so a path containing one of them would encode differently from the same
	// path assembled another way.
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return nil, fmt.Errorf("workspace: marshal index: %w", err)
	}
	return buf.Bytes(), nil
}

// UnmarshalIndex decodes what Marshal produced.
func UnmarshalIndex(b []byte) (Index, error) {
	var ix Index
	if err := json.Unmarshal(b, &ix); err != nil {
		return Index{}, fmt.Errorf("workspace: unmarshal index: %w", err)
	}
	if ix.Version != IndexVersion {
		return Index{}, fmt.Errorf(
			"workspace: index version %d, this build understands %d: upgrade senro rather than reading a layout it does not know",
			ix.Version, IndexVersion)
	}
	return ix, nil
}

// Bytes is the total size of the regular files in the index. It is what
// ws.snapshot reports and what the size warning in design.md §4.2 measures.
func (ix Index) Bytes() int64 {
	var n int64
	for _, e := range ix.Entries {
		n += e.Size
	}
	return n
}
```

- [ ] **Step 6: Implement `internal/workspace/exclude.go`**

```go
package workspace

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// IgnoreFile is the per-workspace exclusion list, read from the workspace
// root. One glob per line; "#" starts a comment; blank lines are ignored.
const IgnoreFile = ".senroignore"

// DefaultExcludes are the directories excluded from every workspace unless a
// caller builds an Excluder without them (design.md §4.2 names them as
// mandatory mitigations). The list is deliberately two entries rather than
// "and friends": every addition is a directory somebody's pipeline can no
// longer snapshot, and silently dropping a path is the worst failure this
// package has.
var DefaultExcludes = []string{".git/", "node_modules/"}

// Excluder decides which paths stay out of a snapshot.
//
// Pattern syntax, and only this syntax: "*" matches within one path segment,
// "?" matches one character within a segment, "**" matches any number of
// segments, and a trailing "/" matches a directory and everything under it. A
// pattern with no "/" in it matches against the last segment only, so "*.tmp"
// matches "a/b/c.tmp"; a pattern containing "/" matches against the whole
// relative path.
//
// Negation ("!pattern") is NOT supported. .gitignore's negation changes what
// an earlier pattern means, and a half-implementation is how a file silently
// enters or leaves a snapshot and moves a digest for a reason nobody can see.
// LoadIgnoreFile refuses a negation by name rather than dropping the "!".
type Excluder struct{ pats []string }

// NewExcluder compiles patterns. An Excluder with no patterns matches
// nothing at all: that is what an unfiltered snapshot needs, and an excluder
// that quietly matched everything would produce a stable, empty, wrong digest.
func NewExcluder(patterns ...string) *Excluder {
	pats := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p = strings.TrimSpace(p); p != "" {
			pats = append(pats, p)
		}
	}
	return &Excluder{pats: pats}
}

// Match reports whether rel is excluded. rel is a forward-slash relative
// path; isDir says whether it is a directory, which is what a trailing "/"
// in a pattern keys off.
func (e *Excluder) Match(rel string, isDir bool) bool {
	if e == nil {
		return false
	}
	base := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		base = rel[i+1:]
	}
	for _, p := range e.pats {
		dirOnly := strings.HasSuffix(p, "/")
		pat := strings.TrimSuffix(p, "/")
		if dirOnly {
			// A directory pattern matches the directory and everything under
			// it, wherever it appears in the tree. That is what makes
			// "node_modules/" mean what everyone expects it to mean.
			if matchSegments(pat, base) && isDir {
				return true
			}
			for _, seg := range strings.Split(rel, "/") {
				if matchSegments(pat, seg) {
					return true
				}
			}
			continue
		}
		if strings.Contains(pat, "/") {
			if matchPath(pat, rel) {
				return true
			}
			continue
		}
		if matchSegments(pat, base) {
			return true
		}
	}
	return false
}

// matchSegments matches a pattern with no "/" against a single segment.
func matchSegments(pat, seg string) bool {
	ok, err := filepath.Match(pat, seg)
	return err == nil && ok
}

// matchPath matches a pattern containing "/" against a whole relative path,
// with "**" spanning any number of segments. filepath.Match cannot do "**",
// so the path is split and matched segment by segment.
func matchPath(pat, rel string) bool {
	return matchParts(strings.Split(pat, "/"), strings.Split(rel, "/"))
}

func matchParts(pat, in []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			// "**" matches zero or more segments: try every split point.
			for i := 0; i <= len(in); i++ {
				if matchParts(pat[1:], in[i:]) {
					return true
				}
			}
			return false
		}
		if len(in) == 0 {
			return false
		}
		if !matchSegments(pat[0], in[0]) {
			return false
		}
		pat, in = pat[1:], in[1:]
	}
	return len(in) == 0
}

// LoadIgnoreFile reads root/.senroignore. A workspace without one is the
// ordinary case, not an error.
func LoadIgnoreFile(root string) ([]string, error) {
	f, err := os.Open(filepath.Join(root, IgnoreFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("workspace: %w", err)
	}
	defer func() { _ = f.Close() }()

	var out []string
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		if strings.HasPrefix(s, "!") {
			return nil, fmt.Errorf(
				"workspace: %s line %d: %q is a negation pattern, which this build does not implement: "+
					"remove the pattern the negation was cancelling instead", IgnoreFile, line, s)
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("workspace: read %s: %w", IgnoreFile, err)
	}
	return out, nil
}
```

- [ ] **Step 7: Implement `internal/workspace/tar.go`**

```go
package workspace

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/cas"
)

// Epoch is the mtime every header in every snapshot carries. The value is
// arbitrary; that it is FIXED is not. See the package doc.
var Epoch = time.Unix(0, 0).UTC()

// ErrUnsafePath marks a tar entry whose destination, or whose symlink
// target, leaves the directory being restored into.
var ErrUnsafePath = errors.New("workspace: entry escapes the destination")

// WriteTar writes a normalized tar of root to w and returns its index.
//
// Normalization, in full, and every item is load-bearing:
//
//   - Entries are emitted in lexicographic order of their relative path. The
//     order is produced by an explicit sort, not by whatever order the walk
//     happened to yield, so it cannot drift with a filesystem or a Go release.
//   - ModTime is Epoch on every entry. AccessTime and ChangeTime are the ZERO
//     time, not Epoch: archive/tar writes a PAX record for either one when it
//     is non-zero, and that record would carry the producing machine's clock
//     straight back into the digest.
//   - Uid and Gid are 0 and Uname and Gname are empty. archive/tar's unix
//     stat hook fills all four from the file, so leaving them alone makes the
//     digest depend on which account ran the build.
//   - Mode keeps only the executable bit for regular files (0644 or 0755),
//     and is fixed for directories and symlinks, so a different umask cannot
//     move a digest.
//   - PAXRecords are cleared, so no extended attribute reaches the body.
//
// Only regular files, directories and symlinks are emitted. Devices, sockets
// and fifos are skipped: none of them survives a restore onto a different
// executor, and carrying them would make a digest depend on a thing the
// restore could not reproduce.
func WriteTar(w io.Writer, root string, ex *Excluder) (Index, error) {
	rels, err := collect(root, ex)
	if err != nil {
		return Index{}, err
	}

	tw := tar.NewWriter(w)
	ix := Index{Version: IndexVersion}
	for _, rel := range rels {
		e, err := writeEntry(tw, root, rel)
		if err != nil {
			return Index{}, err
		}
		ix.Entries = append(ix.Entries, e)
	}
	if err := tw.Close(); err != nil {
		return Index{}, fmt.Errorf("workspace: close tar for %s: %w", root, err)
	}
	return ix, nil
}

// collect returns every non-excluded relative path under root, sorted.
func collect(root string, ex *Excluder) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		relOS, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(relOS)
		if ex.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() && !d.Type().IsRegular() && d.Type()&fs.ModeSymlink == 0 {
			// Device, socket or fifo: not portable across executors, so not
			// part of a workspace's identity either.
			return nil
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("workspace: walk %s: %w", root, err)
	}
	// Explicit, so ordering is this function's guarantee rather than
	// WalkDir's incidental one.
	sort.Strings(out)
	return out, nil
}

func writeEntry(tw *tar.Writer, root, rel string) (Entry, error) {
	full := filepath.Join(root, filepath.FromSlash(rel))
	fi, err := os.Lstat(full)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: stat %s: %w", rel, err)
	}

	var link string
	if fi.Mode()&fs.ModeSymlink != 0 {
		if link, err = os.Readlink(full); err != nil {
			return Entry{}, fmt.Errorf("workspace: readlink %s: %w", rel, err)
		}
	}
	hdr, err := tar.FileInfoHeader(fi, link)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: header %s: %w", rel, err)
	}
	normalize(hdr, rel, fi)

	if err := tw.WriteHeader(hdr); err != nil {
		return Entry{}, fmt.Errorf("workspace: write header %s: %w", rel, err)
	}

	e := Entry{Path: rel, Mode: uint32(hdr.Mode), Link: link}
	if hdr.Typeflag != tar.TypeReg {
		return e, nil
	}

	f, err := os.Open(full)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: open %s: %w", rel, err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tw, h), f)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: copy %s: %w", rel, err)
	}
	if n != hdr.Size {
		// The file changed under the snapshot. Every byte after this point
		// would be misaligned in the tar, so this is fatal rather than a
		// warning.
		return Entry{}, fmt.Errorf(
			"workspace: %s changed size during the snapshot (%d bytes declared, %d copied)", rel, hdr.Size, n)
	}
	e.Size = n
	e.Digest = cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil)))
	return e, nil
}

// normalize strips every field that records something about the machine that
// produced the file rather than the file itself. See WriteTar's doc for the
// full list and design.md §11 item 3 for why this function is the reason the
// cache can hit at all.
func normalize(hdr *tar.Header, rel string, fi fs.FileInfo) {
	hdr.Name = rel
	hdr.Format = tar.FormatPAX
	hdr.ModTime = Epoch
	hdr.AccessTime = time.Time{}
	hdr.ChangeTime = time.Time{}
	hdr.Uid, hdr.Gid = 0, 0
	hdr.Uname, hdr.Gname = "", ""
	hdr.PAXRecords = nil
	hdr.Devmajor, hdr.Devminor = 0, 0

	switch {
	case fi.IsDir():
		hdr.Typeflag = tar.TypeDir
		hdr.Name = rel + "/"
		hdr.Mode = 0o755
		hdr.Size = 0
	case fi.Mode()&fs.ModeSymlink != 0:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Mode = 0o777
		hdr.Size = 0
	default:
		hdr.Typeflag = tar.TypeReg
		if fi.Mode().Perm()&0o111 != 0 {
			hdr.Mode = 0o755
		} else {
			hdr.Mode = 0o644
		}
	}
}

// ReadTar materializes a tar into dest, which must already exist, and
// returns the index of what it wrote.
//
// Restored mtimes are Epoch, matching the tar, so snapshotting a restored
// workspace reproduces the digest it came from. A build tool that keys off
// mtime rather than content sees every file as old, which is the safe
// direction: it rebuilds rather than skipping work it should have done.
func ReadTar(r io.Reader, dest string) (Index, error) {
	tr := tar.NewReader(r)
	ix := Index{Version: IndexVersion}
	var dirs []string

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Index{}, fmt.Errorf("workspace: read tar: %w", err)
		}
		rel := strings.TrimSuffix(hdr.Name, "/")
		target, err := safeJoin(dest, rel)
		if err != nil {
			return Index{}, err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return Index{}, fmt.Errorf("workspace: mkdir %s: %w", rel, err)
			}
			dirs = append(dirs, target)
			ix.Entries = append(ix.Entries, Entry{Path: rel, Mode: 0o755})

		case tar.TypeSymlink:
			if err := checkLinkTarget(rel, hdr.Linkname); err != nil {
				return Index{}, err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return Index{}, fmt.Errorf("workspace: mkdir for %s: %w", rel, err)
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return Index{}, fmt.Errorf("workspace: symlink %s: %w", rel, err)
			}
			ix.Entries = append(ix.Entries, Entry{Path: rel, Mode: 0o777, Link: hdr.Linkname})

		case tar.TypeReg:
			e, err := restoreFile(tr, hdr, target, rel)
			if err != nil {
				return Index{}, err
			}
			ix.Entries = append(ix.Entries, e)

		default:
			// WriteTar emits nothing else, so this is a tarball from
			// somewhere unexpected. Skipping is right: refusing would make a
			// future additive entry type break every older reader.
			continue
		}
	}

	// Directory mtimes are set last, because writing a child updates its
	// parent. Deepest first, so a parent set now is not disturbed by a child
	// written after it.
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, d := range dirs {
		if err := os.Chtimes(d, Epoch, Epoch); err != nil {
			return Index{}, fmt.Errorf("workspace: set times on %s: %w", d, err)
		}
	}
	return ix, nil
}

func restoreFile(tr io.Reader, hdr *tar.Header, target, rel string) (Entry, error) {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return Entry{}, fmt.Errorf("workspace: mkdir for %s: %w", rel, err)
	}
	mode := fs.FileMode(hdr.Mode).Perm()
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: create %s: %w", rel, err)
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), tr)
	closeErr := f.Close()
	if copyErr != nil {
		return Entry{}, fmt.Errorf("workspace: write %s: %w", rel, copyErr)
	}
	if closeErr != nil {
		return Entry{}, fmt.Errorf("workspace: close %s: %w", rel, closeErr)
	}
	// OpenFile's mode is masked by umask, so it is not enough on its own.
	if err := os.Chmod(target, mode); err != nil {
		return Entry{}, fmt.Errorf("workspace: chmod %s: %w", rel, err)
	}
	if err := os.Chtimes(target, Epoch, Epoch); err != nil {
		return Entry{}, fmt.Errorf("workspace: set times on %s: %w", rel, err)
	}
	return Entry{
		Path:   rel,
		Mode:   uint32(mode),
		Size:   n,
		Digest: cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil))),
	}, nil
}

// safeJoin resolves rel under dest and refuses anything that leaves it. A
// workspace tarball is content from a previous run and, in v1, from a shared
// cache backend, so this is untrusted input by construction.
func safeJoin(dest, rel string) (string, error) {
	clean := path.Clean("/" + rel)
	if clean == "/" {
		return "", fmt.Errorf("%w: empty entry name", ErrUnsafePath)
	}
	if strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
		// Checked on the raw name as well as the cleaned one, so an entry
		// that cleans to something harmless but was written to look like a
		// traversal is still refused rather than silently rewritten.
		return "", fmt.Errorf("%w: %q", ErrUnsafePath, rel)
	}
	return filepath.Join(dest, filepath.FromSlash(strings.TrimPrefix(clean, "/"))), nil
}

// checkLinkTarget refuses a symlink pointing outside the workspace. A
// relative target is resolved against the link's own directory before it is
// judged, so "a/../b" inside the workspace is fine and "../../etc" is not.
func checkLinkTarget(rel, link string) error {
	if link == "" {
		return fmt.Errorf("%w: %q has an empty symlink target", ErrUnsafePath, rel)
	}
	if path.IsAbs(link) {
		return fmt.Errorf("%w: %q points at the absolute path %q", ErrUnsafePath, rel, link)
	}
	resolved := path.Clean(path.Join(path.Dir(rel), link))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("%w: %q points at %q, outside the workspace", ErrUnsafePath, rel, link)
	}
	return nil
}
```

- [ ] **Step 8: Run to verify it passes** with `go test ./internal/workspace/... -race -v`.

- [ ] **Step 9: Prove the spine tests can fail.** Do all three, one at a time, restoring after each, and record every result in the commit body. This is the most important step in the plan.

  1. Delete `hdr.ModTime = Epoch` from `normalize`. Confirm `TestTouchingAnMtimeDoesNotChangeTheDigest` and `TestEveryTarHeaderIsNormalizedAndNamesAreSorted` both fail.
  2. Delete `hdr.Uid, hdr.Gid = 0, 0` and the `Uname`/`Gname` line. Confirm `TestEveryTarHeaderIsNormalizedAndNamesAreSorted` fails. Note whether the two digest tests still pass, because that is exactly why the header test exists.
  3. Replace `sort.Strings(out)` in `collect` with a reversal of the slice. Confirm `TestEveryTarHeaderIsNormalizedAndNamesAreSorted` fails on ordering and `TestSnapshotDigestIsIdenticalAcrossTwoSeparateOperations` still passes, because a deterministic wrong order is still deterministic. That asymmetry is the whole argument for keeping all three tests.

- [ ] **Step 10: Run the gates**

```bash
go mod tidy && make all
golangci-lint run ./...
```

- [ ] **Step 11: Commit**

```bash
git add go.mod go.sum internal/workspace
git commit -m "feat(workspace): the normalized tar and the file index

Fixed epoch mtime, uid and gid zero, zero atime and ctime so no PAX record
carries a clock, mode reduced to the executable bit, entries sorted by an
explicit sort. design.md §11 item 3: an unnormalized tar digests differently
on every run and silently destroys every cache key downstream of a
workspace. Three tests pin it: byte-identical digests across two separate
operations, an unchanged digest after touching mtimes, and a reader over the
produced tar asserting the headers directly. Mutation results for all three
are in the plan's Task 2 step 9."
```

---

### Task 3: Snapshot and restore through the CAS

**Files:**
- Create `internal/workspace/snapshot.go`, `internal/workspace/snapshot_test.go`
- Modify `internal/storage/storage.go` (add `Snapshotter`), `internal/storage/storage_test.go`

**Interfaces:**
- Consumes: Task 1's `cas.Store`, `cas.Dir`, `cas.PutBytes`, `cas.GetBytes`, `cas.ErrNotFound`, `cas.ErrCorrupt`; Task 2's `WriteTar`, `ReadTar`, `Index`, `Excluder`.
- Produces:
  ```go
  package workspace
  type Snapshot struct {
      Digest cas.Digest `json:"digest"`
      Index  cas.Digest `json:"index"`
      Bytes  int64      `json:"bytes"`
      Files  int        `json:"files"`
  }
  type Snapshotter struct { /* unexported */ }
  func NewSnapshotter(store cas.Store) *Snapshotter
  func (s *Snapshotter) Snapshot(ctx context.Context, root string, ex *Excluder) (Snapshot, error)
  func (s *Snapshotter) Restore(ctx context.Context, d cas.Digest, dest string) error
  func (s *Snapshotter) LoadIndex(ctx context.Context, index cas.Digest) (Index, error)

  package storage
  // Storage gains: Snapshotter *workspace.Snapshotter
  ```

**Wiring.** `storage.Open` constructs the `Snapshotter` over its own CAS, so the handle Task 1 threaded into `engine.Options` now carries the thing Task 6 calls. This is the task Task 1 named as its reader.

**Two digests, and why.** `Snapshot.Digest` is the workspace's identity: the CAS address of the normalized, uncompressed tar. `Snapshot.Index` is a separate CAS address holding the canonical index JSON, so `ws ls` reads a file list without pulling the body (design.md §11 item 3). Both travel in the `ws.snapshot` event, which is why `api.WSSnapshotBody` gains an `Index` field in this task. Restore takes the workspace digest alone, because that is the only value a cache entry, an event log or a CLI argument ever carries.

**Class, not instance.** `Restore` replaces the destination rather than merging into it. A merge leaves whatever the previous step wrote sitting alongside the restored content, so the restored directory would not hash to the digest that named it, and every downstream key computed from it would be wrong in a way no single test case would reveal. Replacement makes `Snapshot(Restore(d)) == d` an invariant, and there is a test for exactly that.

- [ ] **Step 1: Write the failing test**

Create `internal/workspace/snapshot_test.go`:

```go
package workspace_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

func snapshotter(t *testing.T) (*workspace.Snapshotter, *cas.Dir) {
	t.Helper()
	store, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	return workspace.NewSnapshotter(store), store
}

// The spine again, this time through the CAS, and through two Snapshotters
// over two separate stores so nothing shared can be memoizing the answer.
func TestTwoSnapshottersOverTheSameTreeAgreeOnTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"go.sum":          "h1:abc\n",
		"cmd/app/main.go": "package main\n",
		"empty/":          "/",
	})
	ctx := context.Background()

	a, _ := snapshotter(t)
	b, _ := snapshotter(t)

	sa, err := a.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot a: %v", err)
	}
	sb, err := b.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot b: %v", err)
	}
	if sa.Digest != sb.Digest {
		t.Errorf("two independent snapshotters disagreed: %s vs %s", sa.Digest, sb.Digest)
	}
	if sa.Index != sb.Index {
		t.Errorf("two independent snapshotters produced different indexes: %s vs %s", sa.Index, sb.Index)
	}
	if sa.Files != 4 {
		t.Errorf("Files = %d, want 4 (two dirs, two files)", sa.Files)
	}
	if sa.Bytes <= 0 {
		t.Errorf("Bytes = %d, want the total size of the regular files", sa.Bytes)
	}
}

func TestTouchingAnMtimeDoesNotChangeTheSnapshotDigest(t *testing.T) {
	root := tree(t, map[string]string{"a.go": "package a\n", "b/c.go": "package c\n"})
	ctx := context.Background()
	s, _ := snapshotter(t)

	before, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	future := time.Now().Add(72 * time.Hour)
	for _, p := range []string{"a.go", "b/c.go", "b"} {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(p)), future, future); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}
	after, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot again: %v", err)
	}
	if before.Digest != after.Digest {
		t.Fatalf("touching mtimes moved the workspace digest: %s then %s", before.Digest, after.Digest)
	}
}

func TestSnapshotOfARestoreReproducesTheDigest(t *testing.T) {
	root := tree(t, map[string]string{
		"a.txt":   "a\n",
		"d/b.txt": "b\n",
		"run.sh":  "#!/bin/sh\n",
		"l":       "->a.txt",
	})
	if err := os.Chmod(filepath.Join(root, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	ctx := context.Background()
	s, _ := snapshotter(t)

	orig, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err := s.Restore(ctx, orig.Digest, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	again, err := s.Snapshot(ctx, dest, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot of the restore: %v", err)
	}
	if again.Digest != orig.Digest {
		t.Errorf("snapshot(restore(d)) = %s, want %s", again.Digest, orig.Digest)
	}
}

// Restore replaces, it does not merge. A leftover file from a previous step
// would make the directory hash to something other than the digest that
// named it, and every key computed from it downstream would be wrong.
func TestRestoreReplacesWhateverWasThere(t *testing.T) {
	root := tree(t, map[string]string{"wanted.txt": "wanted\n"})
	ctx := context.Background()
	s, _ := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if err := os.MkdirAll(filepath.Join(dest, "stale"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "stale", "junk.txt"), []byte("junk\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Restore(ctx, snap.Digest, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "stale")); err == nil {
		t.Error("Restore left a directory from before it behind, so the destination no longer matches its digest")
	}
	if _, err := os.Stat(filepath.Join(dest, "wanted.txt")); err != nil {
		t.Errorf("Restore did not write the snapshot's own content: %v", err)
	}
}

// The negative cases. A restore that fails must say why and must not leave a
// half-populated directory that a later snapshot would happily digest.
func TestRestoreFailsLoudlyOnAMissingDigest(t *testing.T) {
	s, _ := snapshotter(t)
	dest := filepath.Join(t.TempDir(), "dest")
	err := s.Restore(context.Background(), cas.FromBytes([]byte("never stored")), dest)
	if !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Restore of an unknown digest = %v, want ErrNotFound", err)
	}
}

func TestRestoreFailsLoudlyOnACorruptedBody(t *testing.T) {
	root := tree(t, map[string]string{"a.txt": "a\n"})
	ctx := context.Background()
	s, store := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := os.WriteFile(store.Path(snap.Digest), []byte("not a zstd frame"), 0o644); err != nil {
		t.Fatalf("scribble: %v", err)
	}
	err = s.Restore(ctx, snap.Digest, filepath.Join(t.TempDir(), "dest"))
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Restore of a corrupted body = %v, want ErrCorrupt", err)
	}
}

func TestRestoreOfAMalformedDigestIsRefusedBeforeAnythingIsDeleted(t *testing.T) {
	s, _ := snapshotter(t)
	dest := t.TempDir()
	keep := filepath.Join(dest, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Restore(context.Background(), cas.Digest("not-a-digest"), dest); err == nil {
		t.Fatal("Restore accepted a malformed digest")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("Restore deleted the destination before discovering it could not fill it")
	}
}

func TestLoadIndexReadsTheFileListWithoutTheBody(t *testing.T) {
	root := tree(t, map[string]string{"a.txt": "hello", "d/": "/"})
	ctx := context.Background()
	s, _ := snapshotter(t)

	snap, err := s.Snapshot(ctx, root, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	ix, err := s.LoadIndex(ctx, snap.Index)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	if len(ix.Entries) != 2 {
		t.Fatalf("index has %d entries, want 2", len(ix.Entries))
	}
	for _, e := range ix.Entries {
		if e.Path == "a.txt" && e.Digest != cas.FromBytes([]byte("hello")) {
			t.Errorf("a.txt digest = %s, want the sha256 of its content", e.Digest)
		}
	}
}

// A snapshot of a tree that cannot be walked must not report success with an
// empty digest, which would be a perfectly stable content address for
// "nothing" and would poison every key downstream.
func TestSnapshotOfAMissingRootIsAnError(t *testing.T) {
	s, _ := snapshotter(t)
	_, err := s.Snapshot(context.Background(), filepath.Join(t.TempDir(), "absent"), workspace.NewExcluder())
	if err == nil {
		t.Fatal("Snapshot of a missing directory returned no error")
	}
}
```

- [ ] **Step 2: Run to verify it fails** with `go test ./internal/workspace/ -run Snapshot`. Expect `undefined: workspace.NewSnapshotter`.

- [ ] **Step 3: Implement `internal/workspace/snapshot.go`**

```go
package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/xavidop/senro/internal/cas"
)

// Snapshot is one workspace captured at one moment.
type Snapshot struct {
	// Digest is the workspace's identity: the CAS address of the normalized,
	// uncompressed tar. This is the value that enters a cache key, travels in
	// ws.snapshot, and is the only argument Restore needs.
	Digest cas.Digest `json:"digest"`
	// Index is the CAS address of the canonical file index. Separate from the
	// body so ws ls can list a snapshot without downloading it (design.md
	// §11 item 3).
	Index cas.Digest `json:"index"`
	// Bytes is the total size of the regular files, uncompressed.
	Bytes int64 `json:"bytes"`
	// Files is the number of entries, directories and symlinks included.
	Files int `json:"files"`
}

// Snapshotter turns directories into snapshots and back.
type Snapshotter struct{ store cas.Store }

// NewSnapshotter returns a Snapshotter backed by store.
func NewSnapshotter(store cas.Store) *Snapshotter { return &Snapshotter{store: store} }

// Snapshot captures root and stores both the body and the index.
//
// The tar is streamed into the store through a pipe rather than buffered or
// spooled to a temp file: a workspace is exactly the thing that can be
// gigabytes, and design.md §4.2 warns above 2 GiB rather than forbidding it.
func (s *Snapshotter) Snapshot(ctx context.Context, root string, ex *Excluder) (Snapshot, error) {
	if fi, err := os.Stat(root); err != nil {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: %w", root, err)
	} else if !fi.IsDir() {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: not a directory", root)
	}

	pr, pw := io.Pipe()
	type written struct {
		ix  Index
		err error
	}
	done := make(chan written, 1)
	go func() {
		ix, err := WriteTar(pw, root, ex)
		// CloseWithError(nil) is Close, so the reader sees EOF only once
		// WriteTar has finished, and sees the writer's error otherwise.
		_ = pw.CloseWithError(err)
		done <- written{ix: ix, err: err}
	}()

	d, putErr := s.store.Put(ctx, pr)
	// Unblocks the writer if Put gave up early. On the ordinary path the
	// pipe is already at EOF and this is a no-op.
	_ = pr.Close()
	w := <-done

	if w.err != nil {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: %w", root, w.err)
	}
	if putErr != nil {
		return Snapshot{}, fmt.Errorf("workspace: snapshot %s: %w", root, putErr)
	}

	b, err := w.ix.Marshal()
	if err != nil {
		return Snapshot{}, err
	}
	id, err := cas.PutBytes(ctx, s.store, b)
	if err != nil {
		return Snapshot{}, fmt.Errorf("workspace: store index for %s: %w", root, err)
	}
	return Snapshot{Digest: d, Index: id, Bytes: w.ix.Bytes(), Files: len(w.ix.Entries)}, nil
}

// Restore materializes d into dest, REPLACING whatever is there.
//
// Replacement, not merge. A file left over from a previous step would make
// dest hash to something other than d, so every cache key computed from it
// afterwards would be wrong, and no single test case would show it. The
// destination is only removed once the object has been opened, so a restore
// that cannot find or decode its body leaves the previous content alone
// rather than emptying a directory it then cannot fill.
func (s *Snapshotter) Restore(ctx context.Context, d cas.Digest, dest string) error {
	if !d.Valid() {
		return fmt.Errorf("workspace: restore into %s: %q is not a digest", dest, string(d))
	}
	rc, err := s.store.Get(ctx, d)
	if err != nil {
		return fmt.Errorf("workspace: restore %s: %w", d.Short(), err)
	}
	defer func() { _ = rc.Close() }()

	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("workspace: clear %s: %w", dest, err)
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("workspace: create %s: %w", dest, err)
	}
	if _, err := ReadTar(rc, dest); err != nil {
		return fmt.Errorf("workspace: restore %s into %s: %w", d.Short(), dest, err)
	}
	// The body is only verified once it has been read to EOF (see
	// cas.Dir.Get), and ReadTar stops at the tar's end-of-archive marker,
	// which can precede the end of the object. Draining is what makes a
	// corrupted tail an error rather than a silently short restore.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return fmt.Errorf("workspace: restore %s into %s: %w", d.Short(), dest, err)
	}
	return nil
}

// LoadIndex reads a snapshot's file list. The argument is Snapshot.Index,
// not Snapshot.Digest: the two are separate objects on purpose.
func (s *Snapshotter) LoadIndex(ctx context.Context, index cas.Digest) (Index, error) {
	b, err := cas.GetBytes(ctx, s.store, index)
	if err != nil {
		return Index{}, fmt.Errorf("workspace: load index %s: %w", index.Short(), err)
	}
	return UnmarshalIndex(b)
}
```

- [ ] **Step 4: Run to verify it passes** with `go test ./internal/workspace/... -race`.

- [ ] **Step 5: Wire the Snapshotter into `internal/storage`.** Add the field and construct it in `Open`:

```go
	// Snapshotter turns a run's workspace directories into digests and back
	// (design.md §4.2). It shares this Storage's CAS, so a workspace
	// snapshotted by one run is restorable by the next.
	Snapshotter *workspace.Snapshotter
```

```go
	return &Storage{Root: root, CAS: store, Snapshotter: workspace.NewSnapshotter(store)}, nil
```

and extend `TestOpenCreatesTheStoreLayout` in `internal/storage/storage_test.go`:

```go
	if s.Snapshotter == nil {
		t.Error("Open returned a Storage with no Snapshotter, so the engine has nothing to snapshot with")
	}
```

- [ ] **Step 6: Add the `Index` field to `api.WSSnapshotBody`.** In `api/payload_aux.go`:

```go
// WSSnapshotBody is the payload of a ws.snapshot event.
//
// Two digests, and they address different objects. Digest is the workspace's
// identity, the content address of the normalized tar, and it is what enters
// the next step's cache key and what `senro ws` commands take as an argument.
// Index addresses the file list, stored separately so a client can show what
// is in a snapshot without downloading the body (design.md §11 item 3).
type WSSnapshotBody struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
	Index  string `json:"index,omitempty"`
	Bytes  int64  `json:"bytes"`
	Files  int    `json:"files"`
}
```

This is additive within the major version, which is the only kind of change §11 item 5 allows, and it adds no dependency to `api`.

- [ ] **Step 7: Run the gates**

```bash
go test ./... -race && (cd api && go test ./... -race)
make all
golangci-lint run ./... && (cd api && golangci-lint run ./...)
```

- [ ] **Step 8: Commit**

```bash
git add internal/workspace internal/storage api/payload_aux.go
git commit -m "feat(workspace): snapshot and restore through the CAS

Snapshot streams the normalized tar into the store and stores the index as a
separate object, so ws ls never pulls a body. Restore replaces the
destination rather than merging into it, which is what makes
snapshot(restore(d)) == d an invariant, and it clears the destination only
after the object has opened so a missing or corrupt body leaves the previous
content alone. ws.snapshot gains an additive index field."
```

---

### Task 4: The declaration surface, plan serialization and the plan-time rules

**Files:**
- Create `artifact/artifact.go`, `artifact/artifact_test.go`
- Modify `senro.go` (add workspace, scratch and cache builders, extend `StepBuilder` around lines 238 to 366, extend `toNode` around lines 157 to 208, extend `Build` around lines 88 to 112)
- Modify `internal/plan/plan.go` (extend `Node` around lines 18 to 40, extend `Plan` around lines 56 to 60, extend `Digest` around lines 98 to 115)
- Modify `internal/plan/validate.go` (extend `Validate` around lines 15 to 43, add rules)
- Test `senro_test.go`, `internal/plan/plan_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks. This task is pure declaration and validation, and deliberately touches no storage.
- Produces:
  ```go
  package artifact
  type Selector struct { /* unexported */ }
  func Glob(pattern string) Selector
  func File(p string) Selector
  func Parse(s string) (Selector, error)
  func (s Selector) Serial() string
  func (s Selector) Kind() string     // "glob" or "file"
  func (s Selector) Pattern() string

  package senro
  type ScopeKind string
  const ScopeStep ScopeKind = "step"
  const ScopeRun ScopeKind = "run"
  const ScopePersistent ScopeKind = "persistent"
  type MountMode string
  const RO MountMode = "ro"
  const RW MountMode = "rw"
  type WorkspaceOption func(*workspaceConfig)
  func Scope(k ScopeKind) WorkspaceOption
  func Exclude(patterns ...string) WorkspaceOption
  type WorkspaceRef struct { /* unexported */ }
  func Workspace(name string, opts ...WorkspaceOption) *WorkspaceRef
  func (w *WorkspaceRef) At(at string, mode MountMode) Mount
  type ScratchOption func(*scratchConfig)
  func Key(template string) ScratchOption
  func RestoreKeys(prefixes ...string) ScratchOption
  type ScratchRef struct { /* unexported */ }
  func ScratchCache(name string, opts ...ScratchOption) *ScratchRef
  func (c *ScratchRef) At(at string) Mount
  type Mount struct { /* unexported */ }
  func (s *StepBuilder) Mount(m ...Mount) *StepBuilder
  func (s *StepBuilder) Pure() *StepBuilder
  func (s *StepBuilder) Inputs(sel ...artifact.Selector) *StepBuilder
  func (s *StepBuilder) Outputs(sel ...artifact.Selector) *StepBuilder
  func (s *StepBuilder) CacheEnv(names ...string) *StepBuilder
  func (s *StepBuilder) NoSnapshot() *StepBuilder

  package plan
  type WorkspaceSpec struct {
      Name    string   `json:"name"`
      Scope   string   `json:"scope"`
      Exclude []string `json:"exclude,omitempty"`
  }
  type ScratchSpec struct {
      Name        string   `json:"name"`
      Key         string   `json:"key"`
      RestoreKeys []string `json:"restore_keys,omitempty"`
  }
  type MountSpec struct {
      Workspace string `json:"workspace,omitempty"`
      Scratch   string `json:"scratch,omitempty"`
      At        string `json:"at"`
      Mode      string `json:"mode,omitempty"`
  }
  // Node gains: Mounts []MountSpec, Pure bool, Inputs []string,
  //             Outputs []string, CacheEnv []string, NoSnapshot bool
  // Plan gains: Workspaces []WorkspaceSpec, Scratch []ScratchSpec
  ```

**Wiring.** The production caller is `senro.Build`, and the test that exercises it is a build of a real pipeline whose `plan.json` round-trips. Nothing in this task reaches storage, which is why it can land before Tasks 5 and 6 without either being half-built.

**Class, not instance.** Every cache-only declaration on a step that is not `Pure()` is a plan-time error: `Inputs`, `Outputs` and `CacheEnv` alike. The rule is written once and covers all three, rather than rejecting whichever one somebody reports first. The reason is the failure this project keeps shipping: a declaration that is silently ignored looks exactly like one that works, and the difference only surfaces as a cache that never hits.

**The check that catches it.** Every new `Node` and `Plan` field carries `omitempty`, so a pipeline that declares none of them must serialize byte-identically to what this build already produces, and `plan.Digest()` must be unchanged. The check is the existing golden suite in `internal/engine/golden_test.go`, which pins `plan_digest` in `run.started` and is explicitly documented as the mutation detector for `Digest`. If it stays green, no existing pipeline's identity moved.

- [ ] **Step 1: Write the failing test for `artifact`**

Create `artifact/artifact_test.go`:

```go
package artifact_test

import (
	"testing"

	"github.com/xavidop/senro/artifact"
)

func TestSelectorsSerializeAndParseBack(t *testing.T) {
	for _, s := range []artifact.Selector{
		artifact.Glob("**/*.go"),
		artifact.File("go.sum"),
		artifact.Glob("cmd/*/main.go"),
	} {
		back, err := artifact.Parse(s.Serial())
		if err != nil {
			t.Fatalf("Parse(%q): %v", s.Serial(), err)
		}
		if back.Serial() != s.Serial() {
			t.Errorf("round trip %q gave %q", s.Serial(), back.Serial())
		}
		if back.Kind() != s.Kind() || back.Pattern() != s.Pattern() {
			t.Errorf("round trip lost fields: %s/%s vs %s/%s", back.Kind(), back.Pattern(), s.Kind(), s.Pattern())
		}
	}
}

func TestGlobAndFileAreDistinguishable(t *testing.T) {
	if artifact.Glob("go.sum").Serial() == artifact.File("go.sum").Serial() {
		t.Error("a glob and a file with the same text serialize identically, so a plan cannot tell them apart")
	}
	if artifact.Glob("x").Kind() != "glob" || artifact.File("x").Kind() != "file" {
		t.Error("Kind does not report what the constructor said")
	}
}

func TestParseRejectsWhatItCannotRepresent(t *testing.T) {
	for _, bad := range []string{"", "go.sum", "regex:.*", "glob:", "file:"} {
		if _, err := artifact.Parse(bad); err == nil {
			t.Errorf("Parse(%q) returned no error", bad)
		}
	}
}
```

- [ ] **Step 2: Implement `artifact/artifact.go`**

```go
// Package artifact selects the files a step reads and the files it produces.
//
// You cannot hash what you have not declared (design.md §3.4). v0 declares
// inputs explicitly with globs; the Observed mode that learns a read set by
// watching a run is deliberately later, because an implicit input set that
// changes per run is a debugging nightmare.
package artifact

import (
	"fmt"
	"strings"
)

// Selector names files. It carries its own serialized form for the same
// reason retry.Predicate does: a plan is JSON and cannot carry a closure or
// an interface value across the process boundary the engine executes it in.
type Selector struct{ serial string }

// Glob selects every file matching pattern. "*" and "?" match within a path
// segment and "**" spans segments, matching the workspace excluder's syntax
// exactly, so a pattern reads the same wherever it appears in a pipeline.
func Glob(pattern string) Selector { return Selector{serial: "glob:" + pattern} }

// File selects one path.
func File(p string) Selector { return Selector{serial: "file:" + p} }

// Serial is the form a plan records.
func (s Selector) Serial() string { return s.serial }

// Kind is "glob" or "file", and "" for a zero Selector.
func (s Selector) Kind() string {
	k, _, ok := strings.Cut(s.serial, ":")
	if !ok {
		return ""
	}
	return k
}

// Pattern is the selector's text without its kind.
func (s Selector) Pattern() string {
	_, p, _ := strings.Cut(s.serial, ":")
	return p
}

// Parse reads back what Serial wrote. It refuses anything else rather than
// treating an unknown kind as a literal path, which would silently select
// nothing and make a Pure() step's input set empty without saying so.
func Parse(s string) (Selector, error) {
	kind, pattern, ok := strings.Cut(s, ":")
	if !ok {
		return Selector{}, fmt.Errorf("artifact: %q has no kind prefix, want \"glob:\" or \"file:\"", s)
	}
	if pattern == "" {
		return Selector{}, fmt.Errorf("artifact: %q has an empty pattern", s)
	}
	switch kind {
	case "glob", "file":
		return Selector{serial: s}, nil
	default:
		return Selector{}, fmt.Errorf("artifact: unknown selector kind %q in %q, want \"glob\" or \"file\"", kind, s)
	}
}
```

- [ ] **Step 3: Run to verify the artifact tests pass** with `go test ./artifact/...`.

- [ ] **Step 4: Write the failing test for the builder and the plan**

Append to `senro_test.go`:

```go
func TestWorkspaceMountsReachThePlan(t *testing.T) {
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	build := senro.Workspace("build", senro.Scope(senro.ScopeRun), senro.Exclude("**/*.tmp"))
	gomod := senro.ScratchCache("gomod",
		senro.Key(`gomod-{{ hashFiles "go.sum" }}`), senro.RestoreKeys("gomod-"))

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("compile", exec.Command("go", "build", "-o", "out/app", "./cmd/app")).
		Mount(src.At("/src", senro.RO), build.At("/src/out", senro.RW), gomod.At("/root/go/pkg/mod")).
		Pure().
		Inputs(artifact.Glob("**/*.go"), artifact.File("go.sum")).
		Outputs(artifact.File("out/app")).
		CacheEnv("CGO_ENABLED")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Workspaces) != 2 {
		t.Fatalf("plan declares %d workspaces, want 2", len(p.Workspaces))
	}
	if len(p.Scratch) != 1 || p.Scratch[0].Name != "gomod" {
		t.Fatalf("plan scratch = %+v, want one named gomod", p.Scratch)
	}
	n, ok := p.Node("compile")
	if !ok {
		t.Fatal("compile is missing from the plan")
	}
	if !n.Pure {
		t.Error("Pure() did not reach the plan")
	}
	if len(n.Mounts) != 3 {
		t.Errorf("compile has %d mounts, want 3", len(n.Mounts))
	}
	if len(n.Inputs) != 2 || n.Inputs[0] != "glob:**/*.go" {
		t.Errorf("inputs = %v", n.Inputs)
	}
	if len(n.Outputs) != 1 || n.Outputs[0] != "file:out/app" {
		t.Errorf("outputs = %v", n.Outputs)
	}
	if len(n.CacheEnv) != 1 || n.CacheEnv[0] != "CGO_ENABLED" {
		t.Errorf("cache env = %v", n.CacheEnv)
	}
}

func TestAWorkspaceDeclaredOnceIsRecordedOnceHoweverManyStepsMountIt(t *testing.T) {
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Mount(src.At("/src", senro.RW))
	l.Step("b", exec.Command("true")).Mount(src.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Workspaces) != 1 {
		t.Errorf("plan declares %d workspaces, want 1", len(p.Workspaces))
	}
}

// The whole class in one test. A declaration that is silently ignored looks
// exactly like one that works.
func TestCacheOnlyDeclarationsOnAnImpureStepAreRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*senro.StepBuilder)
	}{
		{"Inputs", func(s *senro.StepBuilder) { s.Inputs(artifact.Glob("**/*.go")) }},
		{"Outputs", func(s *senro.StepBuilder) { s.Outputs(artifact.File("out")) }},
		{"CacheEnv", func(s *senro.StepBuilder) { s.CacheEnv("CGO_ENABLED") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe := senro.New("ci")
			l := pipe.Workflow("main")
			sb := l.Step("s", exec.Command("true"))
			tc.mut(sb)
			_, err := pipe.Build()
			if err == nil {
				t.Fatalf("%s on a step that is not Pure() built without complaint", tc.name)
			}
			if !strings.Contains(err.Error(), "Pure()") {
				t.Errorf("error does not point at the fix: %v", err)
			}
		})
	}
}

func TestAPureStepWithNoInputsIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Pure()
	_, err := pipe.Build()
	if err == nil {
		t.Fatal("a Pure() step with no declared inputs built without complaint")
	}
	if !strings.Contains(err.Error(), "Inputs") {
		t.Errorf("error does not name the missing declaration: %v", err)
	}
}

func TestOnlyScopeRunIsSupported(t *testing.T) {
	for _, k := range []senro.ScopeKind{senro.ScopeStep, senro.ScopePersistent} {
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		ws := senro.Workspace("w", senro.Scope(k))
		l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.RW))
		_, err := pipe.Build()
		if err == nil {
			t.Errorf("scope %q was accepted; v0 supports ScopeRun only", k)
		}
	}
}

func TestTwoMountsAtTheSamePathAreRejected(t *testing.T) {
	a := senro.Workspace("a", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("b", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(a.At("/x", senro.RW), b.At("/x", senro.RW))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("two mounts at the same path were accepted, so which one the step sees is undefined")
	}
}

func TestAHandlerMustNotDeclareItsOwnMounts(t *testing.T) {
	ws := senro.Workspace("w", senro.Scope(senro.ScopeRun))
	h := senro.Handler("cleanup", exec.Command("true")).Mount(ws.At("/w", senro.RW))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.RW)).Always(h)
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a handler declaring its own mounts was accepted; a handler inherits its parent's workspaces")
	}
}

func TestAPureStepWithInputsAndAmbiguousWorkspacesIsRejected(t *testing.T) {
	a := senro.Workspace("a", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("b", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).
		Mount(a.At("/a", senro.RO), b.At("/b", senro.RO)).
		Pure().
		Inputs(artifact.Glob("**/*.go"))
	_, err := pipe.Build()
	if err == nil {
		t.Fatal("a Pure() step with two workspaces and no mount at its WorkDir was accepted, so the input root is ambiguous")
	}
	if !strings.Contains(err.Error(), "WorkDir") {
		t.Errorf("error does not say how to resolve the ambiguity: %v", err)
	}
}

func TestAPureStepWithTwoWorkspacesIsFineWhenOneIsAtTheWorkDir(t *testing.T) {
	a := senro.Workspace("a", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("b", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).
		WorkDir("/a").
		Mount(a.At("/a", senro.RO), b.At("/b", senro.RW)).
		Pure().
		Inputs(artifact.Glob("**/*.go"))
	if _, err := pipe.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

func TestAMountNamingNothingIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(senro.Mount{})
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a zero Mount was accepted")
	}
}

func TestAScratchCacheWithNoKeyIsRejected(t *testing.T) {
	c := senro.ScratchCache("gomod")
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(c.At("/m"))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a scratch cache with no key was accepted; there is nothing to look it up by")
	}
}
```

Append to `internal/plan/plan_test.go`:

```go
// The check that catches an accidental identity change. Every new field is
// omitempty, so a plan that declares none of them must marshal exactly as it
// did before this task, and its digest must not move.
func TestNewFieldsDoNotChangeAnExistingPlansDigest(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a"}},
	}}
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, field := range []string{"mounts", "pure", "inputs", "outputs", "cache_env", "no_snapshot", "workspaces", "scratch"} {
		if strings.Contains(string(b), `"`+field+`"`) {
			t.Errorf("a plan declaring nothing still serialized %q, so every existing plan's digest just moved", field)
		}
	}
}

func TestDigestSortsTheUnorderedNewFields(t *testing.T) {
	mk := func(mounts []plan.MountSpec, inputs []string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"}, Pure: true,
			Mounts: mounts, Inputs: inputs,
		}}, Workspaces: []plan.WorkspaceSpec{
			{Name: "x", Scope: "run"}, {Name: "y", Scope: "run"},
		}}
	}
	one := mk(
		[]plan.MountSpec{{Workspace: "x", At: "/x", Mode: "ro"}, {Workspace: "y", At: "/y", Mode: "rw"}},
		[]string{"glob:**/*.go", "file:go.sum"})
	two := mk(
		[]plan.MountSpec{{Workspace: "y", At: "/y", Mode: "rw"}, {Workspace: "x", At: "/x", Mode: "ro"}},
		[]string{"file:go.sum", "glob:**/*.go"})

	if one.Digest() != two.Digest() {
		t.Error("reordering a mount set or an input set changed the plan digest, so declaration order became part of the pipeline's identity")
	}
}
```

- [ ] **Step 5: Run to verify it fails** with `go test . ./internal/plan/...`. Expect `undefined: senro.Workspace`.

- [ ] **Step 6: Extend `internal/plan/plan.go`.** Add the three spec types with their docs:

```go
// WorkspaceSpec is one workspace a plan declares. A workspace is a named,
// versioned directory with a content digest, not a mount: Mounts are how a
// given step and executor realize it (design.md §4.1).
type WorkspaceSpec struct {
	Name string `json:"name"`
	// Scope is "run" in v0. "step" and "persistent" are declared in the
	// builder so that a pipeline naming one gets a clear refusal rather than
	// a silent reinterpretation, and Validate is what refuses them.
	Scope   string   `json:"scope"`
	Exclude []string `json:"exclude,omitempty"`
}

// ScratchSpec is one scratch cache a plan declares. Distinct from a
// workspace because the semantics are best-effort: a miss is not an error, a
// stale hit only costs time, and it is NEVER an input to an action cache key
// (design.md §4.4).
type ScratchSpec struct {
	Name string `json:"name"`
	// Key is a template resolved once per run. The only function available
	// is hashFiles.
	Key         string   `json:"key"`
	RestoreKeys []string `json:"restore_keys,omitempty"`
}

// MountSpec is one workspace or scratch cache realized into one step.
// Exactly one of Workspace and Scratch is set.
type MountSpec struct {
	Workspace string `json:"workspace,omitempty"`
	Scratch   string `json:"scratch,omitempty"`
	At        string `json:"at"`
	// Mode is "ro" or "rw", and empty means "rw". A scratch cache is always
	// writable and never carries a mode.
	Mode string `json:"mode,omitempty"`
}
```

Add to `Node`, after `TimeoutMS`:

```go
	Mounts []MountSpec `json:"mounts,omitempty"`
	// Pure marks a step eligible for the action cache (design.md §3.2).
	// Steps are impure by DEFAULT: senro can ssh into production and restart
	// a service, so an unmarked step is never cached, never skipped, and
	// re-executed on every run. Marking one is a visible, reviewable act.
	Pure bool `json:"pure,omitempty"`
	// Inputs and Outputs are artifact.Selector serial forms. Inputs are
	// hashed into the cache key; Outputs are stored on a save and restored
	// on a hit. Both are resolved against the step's input root, which
	// Validate makes unambiguous.
	Inputs  []string `json:"inputs,omitempty"`
	Outputs []string `json:"outputs,omitempty"`
	// CacheEnv names the environment variables that enter the cache key. Only
	// names, and only ones declared here: the VALUE never enters a key, only
	// a digest of it, so a secret that reached a step's environment by
	// mistake cannot reach a cache entry that outlives the run.
	CacheEnv []string `json:"cache_env,omitempty"`
	// NoSnapshot suppresses the post-step workspace snapshot for a step whose
	// output nobody consumes (design.md §4.2's escape hatch).
	NoSnapshot bool `json:"no_snapshot,omitempty"`
```

Add to `Plan`:

```go
	Workspaces []WorkspaceSpec `json:"workspaces,omitempty"`
	Scratch    []ScratchSpec   `json:"scratch,omitempty"`
```

Extend `Digest`, inside the per-node copy loop, before the node is stored:

```go
		// Sorted for the same reason Needs is: a mount set, an input set, an
		// output set and an env allowlist are unordered, and reordering one
		// changes nothing semantic. Cmd and Env stay in their given order,
		// because those genuinely are ordered.
		n.Mounts = append([]MountSpec(nil), n.Mounts...)
		sort.Slice(n.Mounts, func(a, b int) bool { return n.Mounts[a].At < n.Mounts[b].At })
		n.Inputs = sortedCopy(n.Inputs)
		n.Outputs = sortedCopy(n.Outputs)
		n.CacheEnv = sortedCopy(n.CacheEnv)
```

and after the node loop, before marshalling:

```go
	c.Workspaces = append([]WorkspaceSpec(nil), p.Workspaces...)
	sort.Slice(c.Workspaces, func(i, j int) bool { return c.Workspaces[i].Name < c.Workspaces[j].Name })
	c.Scratch = append([]ScratchSpec(nil), p.Scratch...)
	sort.Slice(c.Scratch, func(i, j int) bool { return c.Scratch[i].Name < c.Scratch[j].Name })
```

with the helper:

```go
// sortedCopy sorts a copy, so Digest never mutates the caller's plan.
func sortedCopy(in []string) []string {
	if in == nil {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
```

- [ ] **Step 7: Extend `internal/plan/validate.go`.** Add one function and call it from `Validate` after the acyclic check:

```go
// validateStorage checks everything the workspace, scratch and cache
// declarations can be wrong about without running a step.
//
// Every rule here refuses a declaration rather than ignoring it. A
// declaration that is silently ignored looks exactly like one that works,
// and the only symptom is a cache that never hits, which is the failure
// design.md §11 item 3 warns about arriving through a different door.
func (p *Plan) validateStorage() error {
	ws := make(map[string]WorkspaceSpec, len(p.Workspaces))
	for _, w := range p.Workspaces {
		if w.Name == "" {
			return fmt.Errorf("plan: a workspace has an empty name")
		}
		if _, dup := ws[w.Name]; dup {
			return fmt.Errorf("plan: duplicate workspace %q", w.Name)
		}
		if w.Scope != "run" {
			return fmt.Errorf(
				"plan: workspace %q has scope %q; this build supports senro.ScopeRun only "+
					"(ScopeStep and ScopePersistent are later, and ScopePersistent needs MaxAge and MaxSize first)",
				w.Name, w.Scope)
		}
		ws[w.Name] = w
	}

	sc := make(map[string]ScratchSpec, len(p.Scratch))
	for _, c := range p.Scratch {
		if c.Name == "" {
			return fmt.Errorf("plan: a scratch cache has an empty name")
		}
		if _, dup := sc[c.Name]; dup {
			return fmt.Errorf("plan: duplicate scratch cache %q", c.Name)
		}
		if c.Key == "" {
			return fmt.Errorf("plan: scratch cache %q has no key, so there is nothing to look it up by", c.Name)
		}
		sc[c.Name] = c
	}

	for i := range p.Nodes {
		n := &p.Nodes[i]
		if err := validateNodeStorage(n, ws, sc); err != nil {
			return err
		}
		for _, list := range [][]Node{n.OnFailure, n.Always} {
			for _, h := range list {
				if len(h.Mounts) > 0 {
					return fmt.Errorf(
						"plan: handler %q of step %q declares its own mounts; a handler inherits its parent's "+
							"workspaces so it can collect evidence from the environment that broke", h.ID, n.ID)
				}
				if h.Pure || len(h.Inputs) > 0 || len(h.Outputs) > 0 || len(h.CacheEnv) > 0 {
					return fmt.Errorf(
						"plan: handler %q of step %q declares cache settings; a handler runs because its parent "+
							"settled, so caching it would mean skipping the cleanup", h.ID, n.ID)
				}
			}
		}
	}
	return nil
}

func validateNodeStorage(n *Node, ws map[string]WorkspaceSpec, sc map[string]ScratchSpec) error {
	at := make(map[string]bool, len(n.Mounts))
	var workspaceMounts int
	for _, m := range n.Mounts {
		switch {
		case m.Workspace == "" && m.Scratch == "":
			return fmt.Errorf("plan: step %q has a mount naming neither a workspace nor a scratch cache", n.ID)
		case m.Workspace != "" && m.Scratch != "":
			return fmt.Errorf("plan: step %q mounts %q and %q at once", n.ID, m.Workspace, m.Scratch)
		}
		if m.At == "" {
			return fmt.Errorf("plan: step %q has a mount with no path", n.ID)
		}
		if at[m.At] {
			return fmt.Errorf("plan: step %q mounts two things at %q, so which one it sees is undefined", n.ID, m.At)
		}
		at[m.At] = true

		if m.Workspace != "" {
			if _, ok := ws[m.Workspace]; !ok {
				return fmt.Errorf("plan: step %q mounts workspace %q, which the plan does not declare", n.ID, m.Workspace)
			}
			workspaceMounts++
			if m.Mode != "" && m.Mode != "ro" && m.Mode != "rw" {
				return fmt.Errorf("plan: step %q mounts %q with mode %q, want \"ro\" or \"rw\"", n.ID, m.Workspace, m.Mode)
			}
			continue
		}
		if _, ok := sc[m.Scratch]; !ok {
			return fmt.Errorf("plan: step %q mounts scratch cache %q, which the plan does not declare", n.ID, m.Scratch)
		}
		if m.Mode != "" && m.Mode != "rw" {
			return fmt.Errorf("plan: step %q mounts scratch cache %q read-only; a scratch cache is always writable", n.ID, m.Scratch)
		}
	}

	if !n.Pure {
		// One rule, three declarations. Rejecting whichever one a report
		// happens to name would leave the other two silently ignored.
		for name, declared := range map[string]bool{
			"Inputs": len(n.Inputs) > 0, "Outputs": len(n.Outputs) > 0, "CacheEnv": len(n.CacheEnv) > 0,
		} {
			if declared {
				return fmt.Errorf(
					"plan: step %q declares %s but is not Pure(), so nothing would ever read it: "+
						"add Pure() or remove the declaration", n.ID, name)
			}
		}
		return nil
	}

	if len(n.Inputs) == 0 {
		return fmt.Errorf(
			"plan: step %q is Pure() with no Inputs, so its cache key would not change when its sources do: "+
				"declare them with Inputs(artifact.Glob(...))", n.ID)
	}
	if workspaceMounts > 1 && !mountsAtWorkDir(n) {
		return fmt.Errorf(
			"plan: step %q is Pure() with %d workspaces and no mount at its WorkDir, so which one its Inputs "+
				"and Outputs are relative to is ambiguous: set WorkDir to one of the mount paths", n.ID, workspaceMounts)
	}
	return nil
}

// mountsAtWorkDir reports whether the node mounts a workspace exactly where
// the step will run. That mount is the step's input root; see the engine's
// wsManager.InputRoot, which resolves it the same way.
func mountsAtWorkDir(n *Node) bool {
	for _, m := range n.Mounts {
		if m.Workspace == "" {
			continue
		}
		if m.At == n.WorkDir {
			return true
		}
		if n.WorkDir == "" && (m.At == "." || m.At == "/" || m.At == "") {
			return true
		}
	}
	return false
}
```

Wire it into `Validate`, replacing the final `return p.checkAcyclic(byID)`:

```go
	if err := p.checkAcyclic(byID); err != nil {
		return err
	}
	return p.validateStorage()
```

- [ ] **Step 8: Extend `senro.go`.** Add the declaration types before `StepBuilder`:

```go
// ScopeKind is a workspace's lifetime.
type ScopeKind string

const (
	// ScopeStep is ephemeral and discarded. Declared so a pipeline asking
	// for it gets a clear refusal rather than a silent promotion to
	// ScopeRun; Build rejects it in this version.
	ScopeStep ScopeKind = "step"
	// ScopeRun is shared across the steps of one run. The common case, and
	// the only scope this build supports.
	ScopeRun ScopeKind = "run"
	// ScopePersistent survives runs. Declared, and rejected by Build: it
	// needs an explicit MaxAge and MaxSize first, or it becomes the mutable
	// global state that makes a pipeline irreproducible.
	ScopePersistent ScopeKind = "persistent"
)

// MountMode is whether a step may write through a mount.
type MountMode string

const (
	RO MountMode = "ro"
	RW MountMode = "rw"
)

type workspaceConfig struct {
	scope   ScopeKind
	exclude []string
}

// WorkspaceOption configures a workspace.
type WorkspaceOption func(*workspaceConfig)

// Scope sets a workspace's lifetime.
func Scope(k ScopeKind) WorkspaceOption { return func(c *workspaceConfig) { c.scope = k } }

// Exclude keeps paths out of the workspace's snapshots. Patterns use the
// same syntax everywhere in senro: "*" and "?" within a segment, "**" across
// segments, and a trailing "/" for a directory and everything under it.
func Exclude(patterns ...string) WorkspaceOption {
	return func(c *workspaceConfig) { c.exclude = append(c.exclude, patterns...) }
}

// WorkspaceRef names a workspace. It is a declaration, not a directory: what
// a directory it becomes is the executor's business (design.md §4.1).
type WorkspaceRef struct {
	name    string
	scope   ScopeKind
	exclude []string
}

// Workspace declares a named, versioned directory with a content digest.
func Workspace(name string, opts ...WorkspaceOption) *WorkspaceRef {
	cfg := workspaceConfig{scope: ScopeRun}
	for _, o := range opts {
		o(&cfg)
	}
	return &WorkspaceRef{name: name, scope: cfg.scope, exclude: append([]string(nil), cfg.exclude...)}
}

// At mounts the workspace into a step at a path.
func (w *WorkspaceRef) At(at string, mode MountMode) Mount {
	return Mount{ws: w, at: at, mode: mode}
}

type scratchConfig struct {
	key         string
	restoreKeys []string
}

// ScratchOption configures a scratch cache.
type ScratchOption func(*scratchConfig)

// Key sets the scratch cache's lookup key. The value is a template evaluated
// once per run, with one function available: hashFiles, which takes globs
// relative to the pipeline process's working directory.
func Key(template string) ScratchOption { return func(c *scratchConfig) { c.key = template } }

// RestoreKeys are prefixes tried, in order, when the exact key misses. The
// newest entry under the first matching prefix wins.
func RestoreKeys(prefixes ...string) ScratchOption {
	return func(c *scratchConfig) { c.restoreKeys = append(c.restoreKeys, prefixes...) }
}

// ScratchRef names a scratch cache: a mutable directory restored
// best-effort, such as a module cache. Distinct from a workspace because a
// miss is not an error and a stale hit only costs time, and because it is
// NEVER an input to an action cache key (design.md §4.4).
type ScratchRef struct {
	name        string
	key         string
	restoreKeys []string
}

// ScratchCache declares one.
func ScratchCache(name string, opts ...ScratchOption) *ScratchRef {
	var cfg scratchConfig
	for _, o := range opts {
		o(&cfg)
	}
	return &ScratchRef{name: name, key: cfg.key, restoreKeys: append([]string(nil), cfg.restoreKeys...)}
}

// At mounts the scratch cache into a step. There is no mode: a scratch cache
// is always writable, since the point is that the step fills it.
func (c *ScratchRef) At(at string) Mount { return Mount{scratch: c, at: at} }

// Mount is one workspace or scratch cache realized into one step.
type Mount struct {
	ws      *WorkspaceRef
	scratch *ScratchRef
	at      string
	mode    MountMode
}
```

Add the fields to `StepBuilder`:

```go
	mounts     []Mount
	pure       bool
	inputs     []artifact.Selector
	outputs    []artifact.Selector
	cacheEnv   []string
	noSnapshot bool
```

Add the methods:

```go
// Mount realizes workspaces and scratch caches into this step.
func (s *StepBuilder) Mount(m ...Mount) *StepBuilder {
	s.mounts = append(s.mounts, m...)
	return s
}

// Pure declares this step eligible for the action cache.
//
// Steps are impure by default and that is the correct default for a tool
// that can ssh into production and restart a service (design.md §3.2). An
// impure step is never cached, never skipped, and re-executed every run.
// Pure() is trusted rather than enforced: nothing sandboxes the network in
// this build, so declaring it is a claim the pipeline author makes and a
// reviewer can see.
//
// A Pure() step must declare Inputs. Build refuses one that does not,
// because a key that cannot change when the sources change is worse than no
// cache at all.
func (s *StepBuilder) Pure() *StepBuilder {
	s.pure = true
	return s
}

// Inputs declares the files this step reads. They are hashed into its cache
// key: you cannot hash what you have not declared (design.md §3.4).
func (s *StepBuilder) Inputs(sel ...artifact.Selector) *StepBuilder {
	s.inputs = append(s.inputs, sel...)
	return s
}

// Outputs declares the files this step produces. They are stored when the
// step's result is saved and restored when it is served from cache, so a
// cached step still leaves behind what an uncached one would have.
func (s *StepBuilder) Outputs(sel ...artifact.Selector) *StepBuilder {
	s.outputs = append(s.outputs, sel...)
	return s
}

// CacheEnv names environment variables that belong in this step's cache key.
//
// Only names, and only these. The value never enters the key: what enters is
// a digest of it, so a credential that reached the step's environment by
// mistake cannot reach a cache entry, which persists across runs and outlives
// the run directory. Secrets are delivered as files by the secrets subsystem
// and never through Env.
//
// Nothing is allowlisted by default, on purpose. An environment that entered
// keys wholesale would put every variable that differs between two machines
// into every key, and the cache would never hit for a reason nobody could see.
func (s *StepBuilder) CacheEnv(names ...string) *StepBuilder {
	s.cacheEnv = append(s.cacheEnv, names...)
	return s
}

// NoSnapshot suppresses the workspace snapshot this step would otherwise
// take when it settles. For a step whose filesystem output nobody consumes
// (design.md §4.2).
func (s *StepBuilder) NoSnapshot() *StepBuilder {
	s.noSnapshot = true
	return s
}
```

- [ ] **Step 9: Extend `toNode` and `Build` in `senro.go`.** In `toNode`, after the retry and timeout blocks:

```go
	n.Pure = sb.pure
	n.NoSnapshot = sb.noSnapshot
	n.CacheEnv = append([]string(nil), sb.cacheEnv...)
	for _, sel := range sb.inputs {
		n.Inputs = append(n.Inputs, sel.Serial())
	}
	for _, sel := range sb.outputs {
		n.Outputs = append(n.Outputs, sel.Serial())
	}
	for _, m := range sb.mounts {
		spec, err := toMountSpec(sb.id, m)
		if err != nil {
			return plan.Node{}, err
		}
		n.Mounts = append(n.Mounts, spec)
	}
```

and the converter, which is also where a zero Mount is caught before it can reach the plan as an empty spec:

```go
// toMountSpec converts one Mount. A zero Mount names nothing, which is what
// a caller writing senro.Mount{} by hand produces; catching it here means the
// error names the step rather than surfacing later as an unresolvable name.
func toMountSpec(stepID string, m Mount) (plan.MountSpec, error) {
	switch {
	case m.ws != nil:
		mode := string(m.mode)
		if mode == "" {
			mode = string(RW)
		}
		return plan.MountSpec{Workspace: m.ws.name, At: m.at, Mode: mode}, nil
	case m.scratch != nil:
		return plan.MountSpec{Scratch: m.scratch.name, At: m.at}, nil
	default:
		return plan.MountSpec{}, fmt.Errorf(
			"senro: step %q has a mount that names neither a workspace nor a scratch cache; "+
				"build one with Workspace(...).At(...) or ScratchCache(...).At(...)", stepID)
	}
}
```

In `Build`, collect the declarations before validating. Collection walks the builders rather than the nodes, because a `*WorkspaceRef` carries its scope and excludes and a `MountSpec` does not:

```go
	if err := collectDeclarations(p, l.steps); err != nil {
		return nil, err
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
```

```go
// collectDeclarations records every workspace and scratch cache any step
// mounts, once each, in a deterministic order.
//
// Two steps mounting the same workspace declare it once, which is what makes
// a workspace a shared, named thing rather than a per-step directory. Two
// DIFFERENT declarations under one name are refused: a workspace whose
// excludes depend on which step happened to be built first would snapshot
// differently from run to run, which is the digest instability this whole
// slice exists to prevent.
func collectDeclarations(p *plan.Plan, steps []*StepBuilder) error {
	seenWS := make(map[string]*WorkspaceRef)
	seenSC := make(map[string]*ScratchRef)
	for _, sb := range steps {
		for _, m := range sb.mounts {
			switch {
			case m.ws != nil:
				prev, ok := seenWS[m.ws.name]
				if !ok {
					seenWS[m.ws.name] = m.ws
					p.Workspaces = append(p.Workspaces, plan.WorkspaceSpec{
						Name:    m.ws.name,
						Scope:   string(m.ws.scope),
						Exclude: append([]string(nil), m.ws.exclude...),
					})
					continue
				}
				if prev.scope != m.ws.scope || !equalStrings(prev.exclude, m.ws.exclude) {
					return fmt.Errorf(
						"senro: workspace %q is declared twice with different options; "+
							"declare it once and share the value", m.ws.name)
				}
			case m.scratch != nil:
				prev, ok := seenSC[m.scratch.name]
				if !ok {
					seenSC[m.scratch.name] = m.scratch
					p.Scratch = append(p.Scratch, plan.ScratchSpec{
						Name:        m.scratch.name,
						Key:         m.scratch.key,
						RestoreKeys: append([]string(nil), m.scratch.restoreKeys...),
					})
					continue
				}
				if prev.key != m.scratch.key || !equalStrings(prev.restoreKeys, m.scratch.restoreKeys) {
					return fmt.Errorf(
						"senro: scratch cache %q is declared twice with different options; "+
							"declare it once and share the value", m.scratch.name)
				}
			}
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 10: Run to verify it passes** with `go test . ./artifact/... ./internal/plan/... -race`.

- [ ] **Step 11: Run the whole suite and confirm no existing plan's identity moved.**

```bash
go test ./... -race
```

`internal/engine/golden_test.go` pins `plan_digest` in every golden. If a golden fails, an `omitempty` is missing and every existing pipeline just changed identity. Fix the tag rather than regenerating the golden.

- [ ] **Step 12: Run the gates**

```bash
make all
golangci-lint run ./...
```

- [ ] **Step 13: Commit**

```bash
git add artifact senro.go senro_test.go internal/plan
git commit -m "feat(senro): workspace, scratch cache and Pure() declarations

Every new plan field is omitempty, so a pipeline declaring none of them
serializes exactly as before and its digest does not move; the golden suite
is the check for that. Plan-time rules refuse rather than ignore: a
cache-only declaration on a step that is not Pure(), a Pure() step with no
Inputs, a scope this build does not support, two mounts at one path, a
handler with its own mounts, and a Pure() step whose input root is
ambiguous."
```

---

### Task 5: Mount realization in the local executor

**Files:**
- Modify `internal/executor/executor.go` (extend `Mount` around lines 36 to 44, add `Snapshot`, change `Sandbox.Snapshot` around lines 74 to 96)
- Modify `internal/executor/localexec/localexec.go` (`New` around line 80, `Sandbox` around lines 90 to 96, `sandbox` around lines 98 to 111, `Run`'s directory resolution around lines 136 to 145)
- Modify `internal/executor/executor_test.go`, `internal/executor/localexec/localexec_test.go`
- Modify `run.go` (the `localexec.New` call, around line 145)

**Interfaces:**
- Consumes: Task 3's `*workspace.Snapshotter`, `workspace.NewExcluder`, `workspace.DefaultExcludes`; Task 1's `cas.Digest`.
- Produces:
  ```go
  package executor
  // Mount gains: Path string, Exclude []string
  type Snapshot struct {
      Digest string
      Index  string
      Bytes  int64
      Files  int
  }
  // Sandbox.Snapshot changes to:
  //   Snapshot(ctx context.Context, name string) (Snapshot, error)

  package localexec
  func New(root string, snap *workspace.Snapshotter) senroexec.Executor
  ```

**Why the executor snapshots rather than the engine.** `Sandbox.Snapshot` is already on the interface and already returns "not implemented in this phase". Implementing it, and having the engine call it, is what keeps one snapshot path rather than two. It is also the seam a v1 k8s executor needs: there the coordinator cannot see the filesystem at all, and a sidecar or an exit wrapper is what produces the digest. `executor.Snapshot` is a plain struct rather than `workspace.Snapshot` so `internal/executor` stays free of the tar code, which is what lets a future executor report a digest an init container computed.

**Local realization, precisely.** A `Mount.At` is interpreted relative to the sandbox directory, and a leading separator is stripped, so `src.At("/src", RO)` lands at `<sandbox>/src`. A container executor binds the same host directory at the same absolute path, which is why the declaration is written the way it is. There are two cases:

- The mount is at the step's working directory. The sandbox's working directory becomes the workspace directory itself. This is what makes a step's declared `Inputs` resolvable by an ordinary directory walk, with no symlink to follow.
- Any other mount. A symlink is created at `<sandbox>/<rel>` pointing at the workspace directory.

A symlink is the honest local realization: the coordinator cannot bind-mount without privileges, and copying would make the workspace two directories that disagree. Hardlinking from the CAS is deliberately not done, per the v0 spec's correction 8: a step that writes through a hardlink corrupts the CAS silently and for every future run.

- [ ] **Step 1: Write the failing test**

Append to `internal/executor/localexec/localexec_test.go`:

```go
func newLocal(t *testing.T, root string) senroexec.Executor {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	return localexec.New(root, workspace.NewSnapshotter(store))
}

func TestAMountAtTheWorkDirBecomesTheWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws-src")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "marker.txt"), []byte("here\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1, WorkDir: "/src",
		Mounts: []senroexec.Mount{{Name: "src", At: "/src", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{Args: []string{"cat", "marker.txt"}}, &out, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	if out.String() != "here\n" {
		t.Errorf("the step did not run inside the mounted workspace: %q", out.String())
	}
}

func TestAMountElsewhereIsReachableFromTheSandbox(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws-cache")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "hit.txt"), []byte("cached\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "cache", At: "/deps", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{Args: []string{"cat", "deps/hit.txt"}}, &out, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	if out.String() != "cached\n" {
		t.Errorf("the mount was not reachable from the sandbox: %q", out.String())
	}
}

func TestAStepWritesThroughAMountIntoTheWorkspaceDirectory(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws-out")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "out", At: "/out", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	exit, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"sh", "-c", "echo built > out/app"}}, io.Discard, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	b, err := os.ReadFile(filepath.Join(wsDir, "app"))
	if err != nil {
		t.Fatalf("the write did not land in the workspace directory: %v", err)
	}
	if string(b) != "built\n" {
		t.Errorf("workspace file = %q", b)
	}
}

func TestSnapshotReturnsARealDigestForAMountedWorkspace(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "ws", At: "/ws", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !cas.Digest(snap.Digest).Valid() {
		t.Errorf("Snapshot returned %q, which is not a digest", snap.Digest)
	}
	if !cas.Digest(snap.Index).Valid() {
		t.Errorf("Snapshot returned index %q, which is not a digest", snap.Index)
	}
	if snap.Files != 1 || snap.Bytes != 2 {
		t.Errorf("Snapshot = %+v, want one file of two bytes", snap)
	}
}

// The negative half. A snapshot of something the sandbox does not have must
// be an error, not an empty digest: an empty digest is a perfectly stable
// content address for "nothing" and would poison every key downstream.
func TestSnapshotOfAnUnmountedNameIsAnError(t *testing.T) {
	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "nope")
	if err == nil {
		t.Fatalf("Snapshot of an unmounted workspace returned %+v and no error", snap)
	}
	if snap.Digest != "" {
		t.Errorf("a failed Snapshot still returned a digest: %q", snap.Digest)
	}
}

func TestSnapshotHonoursAMountsExcludes(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	for _, p := range []string{"keep.go", "drop.tmp"} {
		if err := os.MkdirAll(wsDir, 0o755); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := os.WriteFile(filepath.Join(wsDir, p), []byte("x\n"), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "ws", At: "/ws", Path: wsDir, Exclude: []string{"**/*.tmp"}}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Files != 1 {
		t.Errorf("Snapshot included %d files, want 1: the exclude was ignored", snap.Files)
	}
}

// .git and node_modules are excluded from every workspace whether or not the
// pipeline says so (design.md §4.2 lists it as a mandatory mitigation).
func TestSnapshotAlwaysExcludesTheDefaultDirectories(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(filepath.Join(wsDir, ".git"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, ".git", "HEAD"), []byte("ref\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "a.go"), []byte("package a\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "ws", At: "/ws", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Files != 1 {
		t.Errorf("Snapshot included %d entries, want 1: .git reached the snapshot", snap.Files)
	}
}

// A mount whose path escapes the sandbox is a declaration senro built, not
// user input, but a symlink written outside the sandbox is a filesystem
// write nobody asked for, so it is refused rather than trusted.
func TestAMountPathThatEscapesTheSandboxIsRefused(t *testing.T) {
	ex := newLocal(t, t.TempDir())
	_, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "bad", At: "../../escape", Path: t.TempDir()}},
	})
	if err == nil {
		t.Fatal("a mount at a path escaping the sandbox was accepted")
	}
}
```

Update `internal/executor/executor_test.go`'s conformance suite so `Snapshot` is exercised against the new signature, and update every existing `localexec.New(root)` call in `localexec_test.go` to `newLocal(t, root)`.

- [ ] **Step 2: Run to verify it fails** with `go test ./internal/executor/...`. Expect `too many arguments in call to localexec.New`.

- [ ] **Step 3: Extend `executor.Mount` and change `Sandbox.Snapshot`.** In `internal/executor/executor.go`:

```go
// Mount declares a workspace or scratch cache the sandbox must provide. It is
// a declaration, not an instruction to push bytes: an executor may realise it
// however it can, including having the target pull from a content-addressed
// store itself.
type Mount struct {
	Name string
	// Digest names the content to realize for an executor that pulls from
	// the CAS itself. Empty when the workspace starts from nothing.
	Digest string
	// Path is the coordinator-side directory holding this workspace, for an
	// executor that shares the coordinator's filesystem. A container
	// executor bind-mounts it; the local executor makes the sandbox reach
	// it. An executor that shares no filesystem ignores Path and uses
	// Digest.
	Path string
	At   string
	RO   bool
	// Exclude keeps paths out of this workspace's snapshots. It travels with
	// the mount because the executor is what takes the snapshot, and the
	// exclusion set is a property of the workspace rather than of the step.
	Exclude []string
}

// Snapshot is one captured workspace, in the form an executor reports it.
//
// Deliberately a plain struct rather than internal/workspace's own type:
// this package must stay free of the tar and index code so a future executor
// can report a digest that something else computed, such as a Kubernetes
// init container or an ssh-side wrapper.
type Snapshot struct {
	Digest string
	Index  string
	Bytes  int64
	Files  int
}
```

and on `Sandbox`:

```go
	// Snapshot captures a mounted workspace by name and returns its content
	// address. It is imperative, unlike a Mount, because the coordinator
	// genuinely needs the resulting digest back: it goes into the step's
	// result, into the event log, and into the next step's cache key.
	Snapshot(ctx context.Context, name string) (Snapshot, error)
```

- [ ] **Step 4: Implement realization in `internal/executor/localexec/localexec.go`.** Change `New` and the sandbox:

```go
type local struct {
	root string
	snap *workspace.Snapshotter
}

// New returns an Executor that runs steps on this host, with step working
// directories created under root and workspace snapshots taken through snap.
//
// snap may be nil, which means this executor cannot snapshot: Sandbox
// refuses a spec carrying mounts rather than running the step with the
// workspaces silently absent. A step that believes it has a workspace and
// does not is the shape of failure this whole slice exists to avoid.
func New(root string, snap *workspace.Snapshotter) senroexec.Executor {
	return &local{root: root, snap: snap}
}

func (l *local) Sandbox(_ context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	dir := filepath.Join(l.root, "work", stepid.Encode(spec.StepID), strconv.Itoa(spec.Attempt))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	if len(spec.Mounts) > 0 && l.snap == nil {
		return nil, fmt.Errorf(
			"localexec: %w: step %q declares %d mount(s) but this executor has no snapshotter",
			senroexec.ErrInfra, spec.StepID, len(spec.Mounts))
	}
	s := &sandbox{dir: dir, spec: spec, snap: l.snap, mounts: map[string]senroexec.Mount{}}
	if err := s.realize(); err != nil {
		return nil, err
	}
	return s, nil
}

type sandbox struct {
	dir    string
	spec   senroexec.SandboxSpec
	snap   *workspace.Snapshotter
	mounts map[string]senroexec.Mount
	// workDir is where Run starts the command: the sandbox directory, or the
	// directory of the mount realized at the step's WorkDir. See realize.
	workDir string
}

// realize makes every declared mount reachable from the sandbox.
//
// A Mount.At is interpreted relative to the sandbox, with a leading separator
// stripped, so "/src" lands at <sandbox>/src. A container executor binds the
// same host directory at the same absolute path, which is why the
// declaration is written as an absolute path in the first place.
//
// The mount at the step's working directory is special: the sandbox's working
// directory becomes the workspace directory itself, so an ordinary directory
// walk of the step's cwd sees the workspace with no symlink to follow. That
// is what makes a Pure() step's declared Inputs resolvable, and the engine's
// own input-root rule resolves to exactly the same directory.
//
// Every other mount is a symlink. The coordinator cannot bind-mount without
// privileges, copying would make the workspace two directories that disagree,
// and hardlinking from the CAS is refused outright: a step that writes
// through a hardlink corrupts the store silently and for every future run.
func (s *sandbox) realize() error {
	for _, m := range s.spec.Mounts {
		s.mounts[m.Name] = m
		rel := strings.TrimPrefix(filepath.ToSlash(m.At), "/")
		if rel == "" || rel == "." {
			rel = "."
		}
		target := filepath.Join(s.dir, filepath.FromSlash(rel))
		if !withinDir(s.dir, target) {
			return fmt.Errorf("localexec: %w: mount %q at %q escapes the sandbox",
				senroexec.ErrInfra, m.Name, m.At)
		}
		if m.Path == "" {
			return fmt.Errorf("localexec: %w: mount %q has no coordinator-side path",
				senroexec.ErrInfra, m.Name)
		}
		if s.isWorkDirMount(m) {
			s.workDir = m.Path
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
		}
		if err := os.Symlink(m.Path, target); err != nil {
			return fmt.Errorf("localexec: %w: realize mount %q: %w", senroexec.ErrInfra, m.Name, err)
		}
	}
	if s.workDir == "" {
		s.workDir = s.dir
	}
	return nil
}

// isWorkDirMount reports whether m is realized exactly where the step runs.
// It resolves the same way plan.mountsAtWorkDir does; the two must agree, or
// the engine's input root and the sandbox's cwd would be different
// directories and a Pure() step would hash files it never read.
func (s *sandbox) isWorkDirMount(m senroexec.Mount) bool {
	if m.At == s.spec.WorkDir {
		return true
	}
	return s.spec.WorkDir == "" && (m.At == "." || m.At == "/" || m.At == "")
}

func withinDir(base, p string) bool {
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
```

Replace the placeholder `Snapshot`:

```go
// Snapshot captures a mounted workspace. An unmounted name is an error and
// never an empty digest: an empty digest is a perfectly valid content
// address for "nothing", so returning one would put a stable, wrong value
// into the next step's cache key.
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	m, ok := s.mounts[name]
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"localexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	// DefaultExcludes come first and are not optional: design.md §4.2 lists
	// excluding .git and node_modules as a mandatory mitigation, so a
	// pipeline that forgot still gets it.
	patterns := append(append([]string(nil), workspace.DefaultExcludes...), m.Exclude...)
	extra, err := workspace.LoadIgnoreFile(m.Path)
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	snap, err := s.snap.Snapshot(ctx, m.Path, workspace.NewExcluder(append(patterns, extra...)...))
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return senroexec.Snapshot{
		Digest: string(snap.Digest), Index: string(snap.Index),
		Bytes: snap.Bytes, Files: snap.Files,
	}, nil
}
```

and in `Run`, replace the directory resolution so it starts from `s.workDir` rather than `s.dir`:

```go
	// A relative Dir is relative to the sandbox's working directory, which is
	// the mounted workspace when the step declared one there. Only an
	// absolute path escapes.
	dir := s.workDir
	if c.Dir != "" {
		if filepath.IsAbs(c.Dir) {
			dir = c.Dir
		} else {
			dir = filepath.Join(s.workDir, c.Dir)
		}
	}
```

Note that a step whose `WorkDir` is `/src` and which mounts `src` there now runs with `cmd.Dir` set to the workspace directory, and `c.Dir` is that same `/src`, absolute, which would send it back to a host path. The engine is what passes `Cmd.Dir`, and Task 6 stops passing a `WorkDir` that a mount already realized. Add the guard here too, so the executor is correct on its own:

```go
	// A WorkDir that a mount already realized is not a host path to chdir
	// into: the sandbox has already made it the working directory.
	if c.Dir != "" && c.Dir == s.spec.WorkDir && s.workDir != s.dir {
		dir = s.workDir
	}
```

- [ ] **Step 5: Update the call site in `run.go`** to `localexec.New(dir, store.Snapshotter)`.

- [ ] **Step 6: Run to verify it passes** with `go test ./internal/executor/... . -race`.

- [ ] **Step 7: Prove the working-directory rule.** Temporarily make `isWorkDirMount` always return false, so every mount becomes a symlink. Confirm `TestAMountAtTheWorkDirBecomesTheWorkingDirectory` fails. Restore, and record it.

- [ ] **Step 8: Run the gates**

```bash
go test ./... -race
make all
golangci-lint run ./...
```

- [ ] **Step 9: Commit**

```bash
git add internal/executor run.go
git commit -m "feat(localexec): realize mounts and snapshot workspaces

A mount at the step's WorkDir becomes the sandbox's working directory, so a
Pure() step's declared Inputs resolve by an ordinary walk with no symlink to
follow; every other mount is a symlink. No hardlinking from the CAS: a step
writing through one corrupts the store silently and for every future run.
Snapshot returns an error for an unmounted name rather than an empty digest,
which would be a stable content address for nothing."
```

---

### Task 6: The run's workspace manager

**Files:**
- Create `internal/engine/workspaces.go`, `internal/engine/workspaces_test.go`
- Modify `internal/engine/engine.go` (`Run` around lines 82 to 137, `runCore` around lines 254 to 287, `schedule` around line 409)
- Modify `internal/engine/attempt.go` (`runAttempt` around lines 267 to 331, `attemptResult` around lines 30 to 35)
- Modify `internal/plan/validate.go` (one added rule in `validateNodeStorage`)

**Interfaces:**
- Consumes: Task 3's `*workspace.Snapshotter`; Task 5's `executor.Mount`, `executor.Snapshot`, `Sandbox.Snapshot`; Task 1's `cas.Digest`, `storage.Storage`.
- Produces:
  ```go
  package engine
  type wsSnapshot struct {
      Name   string
      Digest cas.Digest
      Index  cas.Digest
      Bytes  int64
      Files  int
      RO     bool
  }
  type wsManager struct { /* unexported */ }
  func newWSManager(runDir string, p *plan.Plan, snap *workspace.Snapshotter) (*wsManager, error)
  func (m *wsManager) path(name string) string
  func (m *wsManager) mounts(n *plan.Node) ([]executor.Mount, error)
  func (m *wsManager) record(snaps []wsSnapshot)
  func (m *wsManager) digests(n *plan.Node) []wsSnapshot
  func (m *wsManager) restore(ctx context.Context, name string, d cas.Digest) error
  func (m *wsManager) inputRoot(n *plan.Node) string
  // attemptResult gains: snapshots []wsSnapshot
  ```

**Wiring.** `engine.Run` builds the manager from `Options.Storage` and the plan, `runAttempt` mounts through it and snapshots through the sandbox, and `run.go` already supplies the storage. The test is a real `engine.Run` over a two-step plan where the first step writes into a workspace and the second reads it.

**Class, not instance.** A workspace mounted read-only is snapshotted after the step too, and a changed digest fails the step. The local executor cannot enforce read-only, and design.md §4.3 names the hazard directly: a step that mutates a read-only input leaves a workspace digest that does not describe what the step actually saw, so every key computed from it afterwards is wrong. Detecting it costs one snapshot and turns a silent, permanent cache corruption into a step failure that names the workspace.

**The check that catches it.** `engine.Run` refuses a plan that declares a workspace or a `Pure()` step when `Options.Storage` is nil. Without that, a caller who forgot to supply storage gets a run that executes every step with no workspaces and no caching and reports success, which is indistinguishable from a working run until someone notices the cache never hits.

- [ ] **Step 1: Close the last plan-time gap.** In `internal/plan/validate.go`, inside `validateNodeStorage`'s `n.Pure` branch, after the Inputs check:

```go
	if len(n.Outputs) > 0 && workspaceMounts == 0 {
		return fmt.Errorf(
			"plan: step %q declares Outputs but mounts no workspace, so nothing would survive the step to be "+
				"stored: mount a workspace and write the outputs into it", n.ID)
	}
```

The rule exists because a step's declared Inputs and Outputs are resolved against different roots depending on whether it mounts a workspace, and only one of those roots can hold outputs:

- A step that mounts a workspace resolves both against the workspace directory realized at its `WorkDir`, or against its single workspace when it has exactly one. This is the same directory the local executor makes the sandbox's working directory, so the two cannot disagree.
- A step with no workspace resolves Inputs against the coordinator's working directory, which is where a repository's sources are. Its Outputs would have to be resolved there too, and a step's sandbox is not there, so it can produce none. Refusing the declaration is the alternative to silently storing nothing.

- [ ] **Step 2: Write the failing test**

Create `internal/engine/workspaces_test.go`:

```go
package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

func runWithStorage(t *testing.T, p *senro.Plan) (api.RunStatus, []api.Event, string, *storage.Storage) {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "run")
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:      runDir,
		Executor: localexec.New(runDir, store.Snapshotter),
		Sink:     rec,
		Storage:  store,
		RunID:    "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	return status, rec.Events(), runDir, store
}

// One workspace, two steps, and the second sees what the first wrote. This
// is what "shared across steps within a run" means (design.md §4.1).
func TestAScopeRunWorkspaceCarriesDataBetweenSteps(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("produce", exec.Command("sh", "-c", "echo artifact > out.txt")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW))
	l.Step("consume", exec.Command("sh", "-c", "cat out.txt")).
		Needs("produce").WorkDir("/build").Mount(ws.At("/build", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, runDir, _ := runWithStorage(t, p)
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}
	b, err := os.ReadFile(filepath.Join(runDir, "logs", "consume", "1", "stdout"))
	if err != nil {
		t.Fatalf("read consume stdout: %v", err)
	}
	if strings.TrimSpace(string(b)) != "artifact" {
		t.Errorf("consume saw %q, want the file produce wrote", b)
	}
	if countType(events, api.WSSnapshot) == 0 {
		t.Error("no ws.snapshot was emitted")
	}
}

func TestWSSnapshotCarriesTheDigestTheIndexAndTheCounts(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("produce", exec.Command("sh", "-c", "echo hi > a.txt")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, events, _, _ := runWithStorage(t, p)
	var body api.WSSnapshotBody
	var found bool
	for _, e := range events {
		if e.Type == api.WSSnapshot {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode ws.snapshot: %v", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no ws.snapshot event")
	}
	if body.Name != "build" || body.Digest == "" || body.Index == "" {
		t.Errorf("ws.snapshot = %+v, want a named workspace with both digests", body)
	}
	if body.Files != 1 || body.Bytes != 3 {
		t.Errorf("ws.snapshot = %+v, want one file of three bytes", body)
	}
}

// design.md §7.6: failure is when the workspace matters most.
func TestAFailedStepStillSnapshotsItsWorkspace(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("boom", exec.Command("sh", "-c", "echo evidence > clue.txt; exit 7")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	if countType(events, api.WSSnapshot) == 0 {
		t.Error("a failed step left no ws.snapshot, so the evidence it wrote is unaddressable")
	}
}

func TestNoSnapshotSuppressesTheSnapshot(t *testing.T) {
	ws := senro.Workspace("scratchpad", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("noisy", exec.Command("sh", "-c", "echo junk > j.txt")).
		WorkDir("/w").Mount(ws.At("/w", senro.RW)).NoSnapshot()
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, events, _, _ := runWithStorage(t, p)
	if n := countType(events, api.WSSnapshot); n != 0 {
		t.Errorf("NoSnapshot() still produced %d ws.snapshot events", n)
	}
}

// The class fix. Local cannot enforce read-only, so it detects the breach
// instead of pretending it cannot happen.
func TestAStepThatWritesThroughAReadOnlyMountFails(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "echo original > f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("sneaky", exec.Command("sh", "-c", "echo tampered > f.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed: a step wrote through a read-only mount", status)
	}
	var msg string
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "sneaky" {
			var b api.StepFinishedBody
			_ = e.Decode(&b)
			msg = b.Error
		}
	}
	if !strings.Contains(msg, "read-only") || !strings.Contains(msg, "src") {
		t.Errorf("the failure does not name the breach or the workspace: %q", msg)
	}
}

// The negative half of the read-only check: a step that only reads must not
// be failed by it, or every read-only mount in every pipeline breaks.
func TestAStepThatOnlyReadsThroughAReadOnlyMountSucceeds(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "echo original > f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("reader", exec.Command("cat", "f.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if status, _, _, _ := runWithStorage(t, p); status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

// The check that catches a caller who forgot to supply storage. A run that
// silently drops every workspace and every cache is indistinguishable from a
// working one until somebody notices the cache never hits.
func TestRunRefusesAPlanNeedingStorageWhenThereIsNone(t *testing.T) {
	ws := senro.Workspace("w", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).WorkDir("/w").Mount(ws.At("/w", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir := filepath.Join(t.TempDir(), "run")
	_, err = engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, nil), RunID: "r1",
	})
	if err == nil {
		t.Fatal("a plan declaring a workspace ran with no storage configured")
	}
	if !strings.Contains(err.Error(), "storage") {
		t.Errorf("the error does not name what is missing: %v", err)
	}
}

func TestAPlanNeedingNoStorageStillRunsWithNone(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	runDir := filepath.Join(t.TempDir(), "run")
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, nil), RunID: "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

// countType is a helper this file adds for the whole package.
func countType(events []api.Event, ty api.Type) int {
	var n int
	for _, e := range events {
		if e.Type == ty {
			n++
		}
	}
	return n
}
```

- [ ] **Step 3: Run to verify it fails** with `go test ./internal/engine/ -run Workspace`. Expect `unknown field Storage in struct literal`.

- [ ] **Step 4: Implement `internal/engine/workspaces.go`**

```go
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// wsSnapshot is one workspace captured after one attempt.
type wsSnapshot struct {
	Name   string
	Digest cas.Digest
	Index  cas.Digest
	Bytes  int64
	Files  int
	// RO records how the step mounted it, so the caller can tell a legitimate
	// write from a breach of a read-only mount.
	RO bool
}

// wsManager owns a run's workspace directories and their current digests.
//
// ScopeRun means one directory per workspace for the whole run, under
// <run>/ws/<name>, shared by every step that mounts it. Nothing restores
// between local steps because nothing needs to: the directory is already
// there. Snapshots exist for three other reasons, all of which outlive the
// step that took them: they feed the next step's cache key, they let a cache
// hit put back what the skipped step would have produced, and they make a
// failed step's filesystem addressable afterwards (design.md §4.2).
type wsManager struct {
	dir   string
	snap  *workspace.Snapshotter
	specs map[string]plan.WorkspaceSpec

	mu    sync.Mutex
	state map[string]cas.Digest
}

// newWSManager prepares one directory per declared workspace. Every
// directory is created up front rather than on first mount, so a step never
// races another step into creating the same one.
func newWSManager(runDir string, p *plan.Plan, snap *workspace.Snapshotter) (*wsManager, error) {
	m := &wsManager{
		dir:   filepath.Join(runDir, "ws"),
		snap:  snap,
		specs: make(map[string]plan.WorkspaceSpec, len(p.Workspaces)),
		state: make(map[string]cas.Digest, len(p.Workspaces)),
	}
	for _, w := range p.Workspaces {
		m.specs[w.Name] = w
		if err := os.MkdirAll(m.path(w.Name), 0o755); err != nil {
			return nil, fmt.Errorf("engine: create workspace %q: %w", w.Name, err)
		}
	}
	return m, nil
}

// path is a workspace's directory for this run.
func (m *wsManager) path(name string) string { return filepath.Join(m.dir, name) }

// mounts turns a node's declared mounts into the executor's form. Scratch
// mounts are skipped here and handled separately, because their lifetime and
// their semantics are different: a scratch cache is best-effort and never an
// input to a cache key (design.md §4.4).
func (m *wsManager) mounts(n *plan.Node) ([]executor.Mount, error) {
	var out []executor.Mount
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		spec, ok := m.specs[ms.Workspace]
		if !ok {
			return nil, fmt.Errorf("engine: step %q mounts unknown workspace %q", n.ID, ms.Workspace)
		}
		out = append(out, executor.Mount{
			Name:    ms.Workspace,
			Digest:  string(m.state[ms.Workspace]),
			Path:    m.path(ms.Workspace),
			At:      ms.At,
			RO:      ms.Mode == "ro",
			Exclude: spec.Exclude,
		})
	}
	return out, nil
}

// record stores the digests an attempt produced, so a later step's cache key
// and a later mount's Digest field see them.
func (m *wsManager) record(snaps []wsSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range snaps {
		m.state[s.Name] = s.Digest
	}
}

// digests reports the current digest of every workspace a node mounts, in
// the node's declared order. This is the workspaceDigests component of the
// cache key (design.md §3.3), read BEFORE the step runs.
func (m *wsManager) digests(n *plan.Node) []wsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []wsSnapshot
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		out = append(out, wsSnapshot{
			Name: ms.Workspace, Digest: m.state[ms.Workspace], RO: ms.Mode == "ro",
		})
	}
	return out
}

// restore materializes a digest into a workspace's directory, replacing what
// is there, and records it as the workspace's current state.
//
// Coordinator-side, because a cache hit skips the step entirely and there is
// no sandbox to ask. That is a property of every executor v0 has: they all
// share the coordinator's filesystem. An executor that does not is served by
// the Digest field on executor.Mount, which is populated on every mount
// already so that path needs no new plumbing when it arrives.
func (m *wsManager) restore(ctx context.Context, name string, d cas.Digest) error {
	if _, ok := m.specs[name]; !ok {
		return fmt.Errorf("engine: cannot restore unknown workspace %q", name)
	}
	if err := m.snap.Restore(ctx, d, m.path(name)); err != nil {
		return fmt.Errorf("engine: restore workspace %q: %w", name, err)
	}
	m.mu.Lock()
	m.state[name] = d
	m.mu.Unlock()
	return nil
}

// inputRoot is the directory a node's declared Inputs and Outputs resolve
// against.
//
// plan.Validate has already refused every ambiguous case, so this resolves
// rather than guesses. The order matters and mirrors plan.mountsAtWorkDir
// exactly: the mount at the step's WorkDir wins, then the single workspace a
// step mounts, and a step with no workspace resolves against the
// coordinator's working directory, which is where a repository's sources
// are. That last case can only carry Inputs, never Outputs; Validate refuses
// the other combination.
func (m *wsManager) inputRoot(n *plan.Node) string {
	var only string
	var count int
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		count++
		only = ms.Workspace
		if ms.At == n.WorkDir || (n.WorkDir == "" && (ms.At == "." || ms.At == "/" || ms.At == "")) {
			return m.path(ms.Workspace)
		}
	}
	if count == 1 {
		return m.path(only)
	}
	cwd, err := os.Getwd()
	if err != nil {
		// Getwd fails only when the working directory has been removed under
		// the process. "." is the same directory by definition, and every
		// caller opens paths relative to it anyway.
		return "."
	}
	return cwd
}

// planNeedsStorage reports whether p declares anything that cannot work
// without a store. Run refuses such a plan when Options.Storage is nil,
// rather than executing it with every workspace and every cache silently
// absent, which would look exactly like a working run.
func planNeedsStorage(p *plan.Plan) bool {
	if len(p.Workspaces) > 0 || len(p.Scratch) > 0 {
		return true
	}
	for i := range p.Nodes {
		if p.Nodes[i].Pure {
			return true
		}
	}
	return false
}
```

- [ ] **Step 5: Wire the manager into `engine.Run`.** After the `ValidateWithGrace` call:

```go
	if opts.Storage == nil && planNeedsStorage(p) {
		return "", fmt.Errorf(
			"engine: this plan declares workspaces, scratch caches or Pure() steps but no storage was " +
				"configured; running it anyway would drop every workspace and every cache result silently")
	}
```

and after `logs := eventlog.NewLogSet(opts.Dir)`:

```go
	var ws *wsManager
	if opts.Storage != nil {
		ws, err = newWSManager(opts.Dir, p, opts.Storage.Snapshotter)
		if err != nil {
			_ = ledger.Close()
			return "", err
		}
	}
```

Add `ws *wsManager` to `runCore` and set it in the `runCore` literal. `runAttempt` reaches it through `rc.ws`, so no signature grows.

- [ ] **Step 6: Snapshot in `runAttempt`.** Add `snapshots []wsSnapshot` to `attemptResult`. Build the mounts into the `SandboxSpec`:

```go
	var mounts []executor.Mount
	if rc.ws != nil {
		mounts, err = rc.ws.mounts(n)
		if err != nil {
			return attemptResult{state: api.StateFailed, err: err}
		}
	}
	sb, err := opts.Executor.Sandbox(attemptCtx, executor.SandboxSpec{
		StepID: n.ID, Attempt: attempt, Env: n.Env, WorkDir: n.WorkDir, Mounts: mounts,
	})
```

and after `sb.Run` returns, before the classification switch:

```go
	// Snapshotting happens while the sandbox is still open and on EVERY
	// path, success or failure or cancellation: design.md §7.6 is explicit
	// that failure is when the workspace matters most, and a snapshot taken
	// after the sandbox closed would be a snapshot of whatever teardown left.
	//
	// Each attempt snapshots for itself. An attempt already gets its own
	// sandbox, log files and event range; giving it its own workspace digest
	// keeps that consistent and means a retry does not erase the evidence of
	// the attempt before it.
	snaps, snapErr := rc.snapshotMounts(context.WithoutCancel(ctx), sb, n, mounts, attempt)
	if snapErr != nil && runErr == nil && exit == 0 {
		// A step whose workspace could not be captured has produced a result
		// nothing downstream can key on. Reporting it as a step failure is
		// the honest outcome; masking a genuine workload failure with it is
		// not, which is why this only applies when the step itself passed.
		return attemptResult{state: api.StateFailed, err: snapErr, logTail: tail.String(), snapshots: snaps}
	}
	rc.ws.record(snaps)
```

guarding `rc.ws.record` behind `rc.ws != nil`. Then add `snapshots: snaps` to each of the four returned `attemptResult` values.

- [ ] **Step 7: Implement `snapshotMounts` in `internal/engine/workspaces.go`**

```go
// snapshotMounts captures every workspace a node mounted, emits one
// ws.snapshot per workspace, and reports a read-only mount whose content
// changed.
//
// Read-only mounts are snapshotted too, and that is the point. The local
// executor cannot enforce read-only, and design.md §4.3 names the hazard: a
// step that mutates a read-only input leaves a workspace digest that does not
// describe what the step actually saw, so every cache key computed from it
// afterwards is wrong, permanently and silently. One extra snapshot turns
// that into a step failure that names the workspace.
func (rc *runCore) snapshotMounts(
	ctx context.Context, sb executor.Sandbox, n *plan.Node, mounts []executor.Mount, attempt int,
) ([]wsSnapshot, error) {
	if rc.ws == nil || n.NoSnapshot || len(mounts) == 0 {
		return nil, nil
	}
	out := make([]wsSnapshot, 0, len(mounts))
	var violation error
	for _, mt := range mounts {
		snap, err := sb.Snapshot(ctx, mt.Name)
		if err != nil {
			return out, fmt.Errorf("engine: step %q: %w", n.ID, err)
		}
		s := wsSnapshot{
			Name: mt.Name, Digest: cas.Digest(snap.Digest), Index: cas.Digest(snap.Index),
			Bytes: snap.Bytes, Files: snap.Files, RO: mt.RO,
		}
		out = append(out, s)
		rc.emit(api.Event{
			Type: api.WSSnapshot, Step: n.ID, Attempt: attempt,
			Payload: mustMarshal(api.WSSnapshotBody{
				Name: s.Name, Digest: string(s.Digest), Index: string(s.Index),
				Bytes: s.Bytes, Files: s.Files,
			}),
		})
		if mt.RO && mt.Digest != "" && mt.Digest != snap.Digest && violation == nil {
			violation = fmt.Errorf(
				"engine: step %q wrote through its read-only mount of workspace %q (%s became %s); "+
					"a read-only mount that changes makes every cache key computed from it wrong",
				n.ID, mt.Name, cas.Digest(mt.Digest).Short(), cas.Digest(snap.Digest).Short())
		}
	}
	return out, violation
}
```

`internal/engine/workspaces.go` therefore also imports `github.com/xavidop/senro/api`.

- [ ] **Step 8: Do not pass a realized WorkDir back to the executor.** In `runAttempt`, the `executor.Cmd` currently carries `Dir: n.WorkDir`. When a mount realized that path, the sandbox has already made it the working directory, so passing it again would send the command to a host path that does not exist. Replace with:

```go
	cmdDir := n.WorkDir
	for _, mt := range mounts {
		if mt.At == n.WorkDir {
			// The sandbox already runs here; naming it again would resolve
			// against the host rather than the sandbox.
			cmdDir = ""
			break
		}
	}
	exit, runErr := sb.Run(attemptCtx, executor.Cmd{Args: n.Cmd, Env: n.Env, Dir: cmdDir}, stdout, stderr)
```

- [ ] **Step 9: Run to verify it passes** with `go test ./internal/engine/... -race`.

- [ ] **Step 10: Prove the read-only check.** Temporarily skip read-only mounts in `snapshotMounts`. Confirm `TestAStepThatWritesThroughAReadOnlyMountFails` fails and `TestAStepThatOnlyReadsThroughAReadOnlyMountSucceeds` still passes. Restore, and record both.

- [ ] **Step 11: Run the gates**

```bash
go test ./... -race
make all
golangci-lint run ./...
```

- [ ] **Step 12: Commit**

```bash
git add internal/engine internal/plan/validate.go
git commit -m "feat(engine): the run's workspace manager

One directory per ScopeRun workspace, shared by every step that mounts it,
snapshotted through the sandbox on every path including failure, because
design.md section 7.6 says failure is when the workspace matters most. A
read-only mount is snapshotted too and a changed digest fails the step: the
local executor cannot enforce read-only, and a silently mutated input makes
every key computed from it wrong. Run refuses a plan needing storage when
none was configured, rather than dropping every workspace and reporting
success."
```

---

### Task 7: The cache key, its components, and Explain

**Files:**
- Create `internal/cache/key.go`, `internal/cache/key_test.go`

**Interfaces:**
- Consumes: Task 1's `cas.Digest`, `cas.FromBytes`.
- Produces:
  ```go
  package cache
  const KeyVersion = 1
  type Key struct {
      Command          string `json:"command"`
      Env              string `json:"env"`
      Secrets          string `json:"secrets"`
      ExecutorClass    string `json:"executor_class"`
      Platform         string `json:"platform"`
      InputDigests     string `json:"input_digests"`
      WorkspaceDigests string `json:"workspace_digests"`
      FuncIdentity     string `json:"func_identity"`
      ToolVersions     string `json:"tool_versions"`
      Version          int    `json:"version"`
  }
  type Component struct{ Name, Value string }
  func (k Key) Components() []Component
  func (k Key) Digest() cas.Digest
  type Diff struct {
      Name string `json:"name"`
      From string `json:"from"`
      To   string `json:"to"`
  }
  func Explain(prev, cur Key) []Diff
  type FileDigest struct {
      Path   string     `json:"path"`
      Digest cas.Digest `json:"digest"`
  }
  type WorkspaceDigest struct {
      Name   string     `json:"name"`
      Digest cas.Digest `json:"digest"`
  }
  func CommandComponent(kind string, cmd []string, workDir string) string
  func EnvComponent(env []string, allow []string) string
  func InputsComponent(files []FileDigest) string
  func WorkspacesComponent(ws []WorkspaceDigest) string
  ```

**Wiring.** Task 9 is the production caller and it is in this plan. This task is deliberately pure computation with no I/O, because the key is the one thing in the slice that has to be reasoned about rather than observed, and a package that cannot touch a disk is a package whose tests cannot be flaky.

**Secrets.** Two of the components exist to keep values out. `Env` holds `NAME=<8 hex of sha256(value)>` for allowlisted names only, so a credential that reached a step's environment cannot reach a cache entry that outlives the run. `Secrets` is reserved for the `provider:key:version:digest8` identity form design.md §1.6 specifies and is empty in this build, which is what lets the secrets subsystem populate it later without changing the key's shape. Neither ever holds a value.

**The check that catches it.** A table test that mutates each of the ten components in turn and asserts the digest changed. That is what catches the failure mode where somebody adds a field to `Key` and forgets to add it to `Components()`, which would produce a key that silently ignores whatever the new field describes. The design's own instruction is to build the key "from a structured, ordered set of named components rather than a hash of a blob", and this test is what makes the ordered set real.

- [ ] **Step 1: Write the failing test**

Create `internal/cache/key_test.go`:

```go
package cache_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func sampleKey() cache.Key {
	return cache.Key{
		Command:          cache.CommandComponent("exec", []string{"go", "test", "./..."}, "/src"),
		Env:              cache.EnvComponent([]string{"CGO_ENABLED=0", "HOME=/root"}, []string{"CGO_ENABLED"}),
		Secrets:          "",
		ExecutorClass:    "local/linux/amd64",
		Platform:         "linux/amd64",
		InputDigests:     cache.InputsComponent([]cache.FileDigest{{Path: "a.go", Digest: cas.FromBytes([]byte("a"))}}),
		WorkspaceDigests: cache.WorkspacesComponent([]cache.WorkspaceDigest{{Name: "src", Digest: cas.FromBytes([]byte("w"))}}),
		FuncIdentity:     "",
		ToolVersions:     "",
		Version:          cache.KeyVersion,
	}
}

func TestDigestIsStableForTheSameKey(t *testing.T) {
	if sampleKey().Digest() != sampleKey().Digest() {
		t.Error("the same key digested twice gave two answers")
	}
	if !sampleKey().Digest().Valid() {
		t.Errorf("Digest() = %q, which is not a digest", sampleKey().Digest())
	}
}

// The check that catches a field added to Key and forgotten in Components().
// Such a key would silently ignore whatever the new field describes, which is
// a wrong cache hit, which design.md §3.1 calls a wrong build.
func TestEveryComponentIsLoadBearing(t *testing.T) {
	base := sampleKey()
	for _, tc := range []struct {
		name string
		mut  func(*cache.Key)
	}{
		{"command", func(k *cache.Key) { k.Command = "changed" }},
		{"env", func(k *cache.Key) { k.Env = "changed" }},
		{"secrets", func(k *cache.Key) { k.Secrets = "changed" }},
		{"executor_class", func(k *cache.Key) { k.ExecutorClass = "changed" }},
		{"platform", func(k *cache.Key) { k.Platform = "changed" }},
		{"input_digests", func(k *cache.Key) { k.InputDigests = "changed" }},
		{"workspace_digests", func(k *cache.Key) { k.WorkspaceDigests = "changed" }},
		{"func_identity", func(k *cache.Key) { k.FuncIdentity = "changed" }},
		{"tool_versions", func(k *cache.Key) { k.ToolVersions = "changed" }},
		{"version", func(k *cache.Key) { k.Version = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := base
			tc.mut(&mutated)
			if mutated.Digest() == base.Digest() {
				t.Errorf("changing %s did not change the key digest, so it is not in Components()", tc.name)
			}
		})
	}
}

func TestComponentsAreNamedAndOrdered(t *testing.T) {
	want := []string{
		"command", "env", "secrets", "executor_class", "platform",
		"input_digests", "workspace_digests", "func_identity", "tool_versions", "version",
	}
	got := sampleKey().Components()
	if len(got) != len(want) {
		t.Fatalf("Components() returned %d components, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name != want[i] {
			t.Errorf("component %d is %q, want %q: the order is part of the digest", i, got[i].Name, want[i])
		}
	}
}

// design.md §3.3: sorted(env ∩ envAllowlist). Only allowlisted names enter,
// and the VALUE never does.
func TestEnvComponentDigestsValuesAndHonoursTheAllowlist(t *testing.T) {
	const token = "super-secret-value-nobody-should-see" //nolint:gosec // a test fixture, not a credential
	got := cache.EnvComponent(
		[]string{"BUILD_TOKEN=" + token, "CGO_ENABLED=0", "HOME=/root"},
		[]string{"BUILD_TOKEN", "CGO_ENABLED"})

	if strings.Contains(got, token) {
		t.Fatalf("the env component contains a value verbatim: %q", got)
	}
	if !strings.Contains(got, "BUILD_TOKEN=") || !strings.Contains(got, "CGO_ENABLED=") {
		t.Errorf("the env component dropped an allowlisted name: %q", got)
	}
	if strings.Contains(got, "HOME") {
		t.Errorf("the env component included a name nobody allowlisted: %q", got)
	}
	if strings.Contains(got, "/root") {
		t.Errorf("the env component leaked an unallowlisted value: %q", got)
	}
}

func TestEnvComponentChangesWhenAnAllowlistedValueChanges(t *testing.T) {
	a := cache.EnvComponent([]string{"CGO_ENABLED=0"}, []string{"CGO_ENABLED"})
	b := cache.EnvComponent([]string{"CGO_ENABLED=1"}, []string{"CGO_ENABLED"})
	if a == b {
		t.Error("changing an allowlisted value did not change the env component, so the cache would serve the wrong build")
	}
}

// The negative half, and a deliberate trade-off worth being explicit about:
// an unallowlisted variable does NOT invalidate. That is what stops every
// machine-specific variable putting a unique key on every host, which is the
// "cache that never hits" failure in its other clothes.
func TestEnvComponentIgnoresAnUnallowlistedValueChange(t *testing.T) {
	a := cache.EnvComponent([]string{"HOSTNAME=build-07"}, []string{"CGO_ENABLED"})
	b := cache.EnvComponent([]string{"HOSTNAME=build-08"}, []string{"CGO_ENABLED"})
	if a != b {
		t.Error("an unallowlisted variable changed the env component")
	}
}

func TestEnvComponentIsOrderIndependent(t *testing.T) {
	a := cache.EnvComponent([]string{"A=1", "B=2"}, []string{"A", "B"})
	b := cache.EnvComponent([]string{"B=2", "A=1"}, []string{"B", "A"})
	if a != b {
		t.Errorf("declaration order changed the env component: %q vs %q", a, b)
	}
}

func TestInputsAndWorkspacesComponentsAreOrderIndependent(t *testing.T) {
	x := cas.FromBytes([]byte("x"))
	y := cas.FromBytes([]byte("y"))
	if cache.InputsComponent([]cache.FileDigest{{Path: "a", Digest: x}, {Path: "b", Digest: y}}) !=
		cache.InputsComponent([]cache.FileDigest{{Path: "b", Digest: y}, {Path: "a", Digest: x}}) {
		t.Error("input order changed the component")
	}
	if cache.WorkspacesComponent([]cache.WorkspaceDigest{{Name: "a", Digest: x}, {Name: "b", Digest: y}}) !=
		cache.WorkspacesComponent([]cache.WorkspaceDigest{{Name: "b", Digest: y}, {Name: "a", Digest: x}}) {
		t.Error("workspace order changed the component")
	}
}

func TestCommandComponentDistinguishesArgumentBoundaries(t *testing.T) {
	a := cache.CommandComponent("exec", []string{"go", "test ./..."}, "")
	b := cache.CommandComponent("exec", []string{"go", "test", "./..."}, "")
	if a == b {
		t.Error("two different argument vectors produced the same component, so a quoting change would hit a stale entry")
	}
}

func TestExplainNamesEveryDifferingComponentInOrder(t *testing.T) {
	prev := sampleKey()
	cur := prev
	cur.Platform = "darwin/arm64"
	cur.InputDigests = cache.InputsComponent([]cache.FileDigest{{Path: "a.go", Digest: cas.FromBytes([]byte("changed"))}})

	diffs := cache.Explain(prev, cur)
	if len(diffs) != 2 {
		t.Fatalf("Explain returned %d diffs, want 2: %+v", len(diffs), diffs)
	}
	if diffs[0].Name != "platform" || diffs[1].Name != "input_digests" {
		t.Errorf("diffs are out of component order: %+v", diffs)
	}
	if diffs[0].From != prev.Platform || diffs[0].To != cur.Platform {
		t.Errorf("diff does not carry both sides: %+v", diffs[0])
	}
}

// The negative half. §3.5's whole promise is that a miss can be explained,
// which is worth nothing if a hit reports phantom differences.
func TestExplainReportsNothingForIdenticalKeys(t *testing.T) {
	if diffs := cache.Explain(sampleKey(), sampleKey()); len(diffs) != 0 {
		t.Errorf("Explain over two identical keys returned %+v", diffs)
	}
}
```

- [ ] **Step 2: Run to verify it fails** with `go test ./internal/cache/...`. Expect a build failure: package does not exist.

- [ ] **Step 3: Implement `internal/cache/key.go`**

```go
// Package cache is senro's action cache: the one that skips a step entirely
// because nothing it depends on changed.
//
// It is correctness-critical, and deliberately not the same thing as the
// scratch cache in internal/scratch. A wrong hit here is a wrong build; a
// wrong hit there costs time (design.md §3.1). Conflating the two is the
// most common design error in this area, which is why they are two packages
// that share nothing but the CAS underneath them.
//
// A Key is a struct of NAMED components stored alongside the entry, never an
// opaque hash. That is what makes `senro cache explain` tractable: the
// question "why did this miss" is answered by diffing two structs rather
// than by re-deriving anything. design.md §3.5 says to build it that way
// precisely because every cache system that cannot explain a miss acquires a
// reputation for being broken, whether or not it is.
package cache

import (
	"bytes"
	"sort"
	"strings"

	"github.com/xavidop/senro/internal/cas"
)

// KeyVersion is the engine-side salt from design.md §3.3. Bump it whenever
// the MEANING of a component changes without its value changing, which is
// the one kind of cache invalidation nothing else can express.
const KeyVersion = 1

// Key is one step's cache key, component by component.
//
// Every field is a string on purpose. A component is whatever canonical text
// its builder produced, so the digest depends on the builders rather than on
// Go's struct layout, JSON field order, or the encoding of an int.
type Key struct {
	// Command is the step's kind, argument vector and working directory.
	Command string `json:"command"`
	// Env is the allowlisted environment as NAME=<digest8 of value> pairs,
	// sorted. Never a value: see EnvComponent.
	Env string `json:"env"`
	// Secrets is the provider:key:version:digest8 identity form from
	// design.md §1.6. Empty in this build, and declared now so the secrets
	// subsystem populates an existing component rather than changing the
	// key's shape and invalidating every entry in the fleet. Never a value.
	Secrets string `json:"secrets"`
	// ExecutorClass is the cache equivalence class, deliberately not host
	// identity (design.md §3.3). Host identity would mean an ssh executor
	// never shares a cache entry across two machines that are the same in
	// every way that matters.
	ExecutorClass string `json:"executor_class"`
	// Platform is the DECLARED platform, resolved before a sandbox exists.
	// The observed platform is verified rather than keyed; see the v0 spec
	// §2.5 for why those cannot be the same value.
	Platform string `json:"platform"`
	// InputDigests is the sorted (path, digest) set of the step's declared
	// inputs.
	InputDigests string `json:"input_digests"`
	// WorkspaceDigests is the sorted (name, digest) set of the workspaces the
	// step mounts, read before it runs. This is what content-addresses the
	// DAG end to end (design.md §4.2).
	WorkspaceDigests string `json:"workspace_digests"`
	// FuncIdentity is a Func step's binary digest, registered name and
	// parameter digest. Empty in this build, which executes no Func steps,
	// and declared for the same reason Secrets is.
	FuncIdentity string `json:"func_identity"`
	// ToolVersions is the declared toolchain fingerprint. Empty in this
	// build, and declared for the same reason.
	ToolVersions string `json:"tool_versions"`
	// Version is KeyVersion at the time the key was built.
	Version int `json:"version"`
}

// Component is one named piece of a key.
type Component struct {
	Name  string
	Value string
}

// Components returns the key's pieces in the ONE canonical order. The order
// is part of the digest and part of Explain's output, so it is defined here
// and nowhere else.
func (k Key) Components() []Component {
	return []Component{
		{"command", k.Command},
		{"env", k.Env},
		{"secrets", k.Secrets},
		{"executor_class", k.ExecutorClass},
		{"platform", k.Platform},
		{"input_digests", k.InputDigests},
		{"workspace_digests", k.WorkspaceDigests},
		{"func_identity", k.FuncIdentity},
		{"tool_versions", k.ToolVersions},
		{"version", itoa(k.Version)},
	}
}

// Digest is the key's content address, over the canonical component
// encoding rather than over a JSON marshalling, so it cannot move because
// somebody reordered a struct field or changed a tag.
func (k Key) Digest() cas.Digest {
	var b bytes.Buffer
	for _, c := range k.Components() {
		b.WriteString(c.Name)
		b.WriteByte(0)
		b.WriteString(c.Value)
		b.WriteByte(0)
	}
	return cas.FromBytes(b.Bytes())
}

// Diff is one component that changed between two keys.
type Diff struct {
	Name string `json:"name"`
	From string `json:"from"`
	To   string `json:"to"`
}

// Explain reports every component that differs, in canonical order. An empty
// result means the two keys are the same key.
func Explain(prev, cur Key) []Diff {
	p, c := prev.Components(), cur.Components()
	var out []Diff
	for i := range p {
		if p[i].Value != c[i].Value {
			out = append(out, Diff{Name: p[i].Name, From: p[i].Value, To: c[i].Value})
		}
	}
	return out
}

// FileDigest is one declared input or output.
type FileDigest struct {
	Path   string     `json:"path"`
	Digest cas.Digest `json:"digest"`
}

// WorkspaceDigest is one mounted workspace's content address.
type WorkspaceDigest struct {
	Name   string     `json:"name"`
	Digest cas.Digest `json:"digest"`
}

// CommandComponent canonicalizes what the step will actually execute.
//
// The argument vector is joined with a NUL rather than a space, so
// ["go", "test ./..."] and ["go", "test", "./..."] cannot collide: they are
// different commands, and a quoting change that made them collide would
// serve a stale result for a command nobody ran.
func CommandComponent(kind string, cmd []string, workDir string) string {
	var b strings.Builder
	b.WriteString(kind)
	b.WriteByte(0)
	for _, a := range cmd {
		b.WriteString(a)
		b.WriteByte(0)
	}
	b.WriteString("workdir=")
	b.WriteString(workDir)
	return b.String()
}

// EnvComponent renders the allowlisted environment as sorted NAME=<digest8>
// pairs.
//
// The value is NEVER included, only the first eight hex digits of its
// sha256. A cache entry persists across runs and outlives the run directory,
// so a credential that reached a step's environment by mistake would
// otherwise be readable from a shared cache long after the run that leaked
// it. Eight digits is enough to distinguish two values in practice and short
// enough to print in `cache explain` output.
//
// Only names in allow are considered. Nothing is allowlisted by default: an
// environment that entered keys wholesale would put every variable that
// differs between two machines into every key, and the cache would never hit
// for a reason nobody could see.
func EnvComponent(env []string, allow []string) string {
	want := make(map[string]bool, len(allow))
	for _, n := range allow {
		want[n] = true
	}
	pairs := make([]string, 0, len(allow))
	for _, kv := range env {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !want[name] {
			continue
		}
		pairs = append(pairs, name+"="+cas.FromBytes([]byte(value)).Short())
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "\n")
}

// InputsComponent renders declared inputs as sorted "path digest" lines.
func InputsComponent(files []FileDigest) string {
	lines := make([]string, 0, len(files))
	for _, f := range files {
		lines = append(lines, f.Path+" "+string(f.Digest))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// WorkspacesComponent renders mounted workspaces as sorted "name digest"
// lines.
func WorkspacesComponent(ws []WorkspaceDigest) string {
	lines := make([]string, 0, len(ws))
	for _, w := range ws {
		lines = append(lines, w.Name+" "+string(w.Digest))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// itoa avoids importing strconv for one call and keeps the version's
// rendering in the same file as everything else the digest depends on.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
```

- [ ] **Step 4: Run to verify it passes** with `go test ./internal/cache/... -race`.

- [ ] **Step 5: Prove the load-bearing check.** Temporarily drop the `platform` entry from `Components()`. Confirm `TestEveryComponentIsLoadBearing/platform` and `TestComponentsAreNamedAndOrdered` both fail. Restore, and record it.

- [ ] **Step 6: Run the gates**

```bash
make all
golangci-lint run ./...
```

- [ ] **Step 7: Commit**

```bash
git add internal/cache
git commit -m "feat(cache): the action cache key as named, ordered components

A struct stored alongside the entry rather than an opaque hash, which is what
makes cache explain a diff of two structs instead of a re-derivation. The env
component holds NAME=digest8 for allowlisted names only, so a value cannot
reach a cache entry that outlives the run, and the secrets and funcIdentity
components are declared empty now so populating them later does not change
the key's shape. A table test mutates each of the ten components and asserts
the digest moved, which is what catches a field added to Key and forgotten in
Components()."
```

---

### Task 8: The action cache store, its Result, and the per-run key record

**Files:**
- Create `internal/cache/action.go`, `internal/cache/action_test.go`
- Create `internal/cache/inputs.go`, `internal/cache/inputs_test.go`
- Create `internal/cache/record.go`, `internal/cache/record_test.go`
- Modify `internal/workspace/exclude.go` (export `MatchGlob`)
- Modify `internal/storage/storage.go` (add `Action`), `internal/storage/storage_test.go`

**Interfaces:**
- Consumes: Task 1's `cas.Digest`, `cas.Dir`; Task 7's `Key`, `Diff`, `Explain`, `FileDigest`, `WorkspaceDigest`; Task 2's glob matcher; the `artifact` package from Task 4.
- Produces:
  ```go
  package workspace
  func MatchGlob(pattern, rel string) bool

  package cache
  const HermeticityTrusted = "trusted"
  const (
      ReasonNoPreviousEntry = "no_previous_entry"
      ReasonKeyChanged      = "key_changed"
      ReasonEntryIncomplete = "entry_incomplete"
  )
  type LogRef struct {
      Stream string     `json:"stream"`
      Digest cas.Digest `json:"digest"`
      Bytes  int64      `json:"bytes"`
  }
  type Result struct {
      ExitCode    int               `json:"exit_code"`
      Outputs     []FileDigest      `json:"outputs,omitempty"`
      Workspaces  []WorkspaceDigest `json:"workspaces,omitempty"`
      Logs        []LogRef          `json:"logs,omitempty"`
      DurationNS  int64             `json:"duration_ns"`
      RunID       string            `json:"run_id"`
      Hermeticity string            `json:"hermeticity"`
      SavedAt     time.Time         `json:"saved_at"`
      Bytes       int64             `json:"bytes"`
  }
  type Entry struct {
      Key    Key    `json:"key"`
      Result Result `json:"result"`
  }
  type ActionCache interface {
      Lookup(ctx context.Context, step string, k Key) (*Result, bool, error)
      Save(ctx context.Context, step string, k Key, r *Result) error
      Previous(ctx context.Context, step string) (*Entry, bool, error)
      Forget(ctx context.Context, k Key) error
  }
  type Dir struct { /* unexported */ }
  func Open(root string) (*Dir, error)
  func (d *Dir) Root() string
  func (d *Dir) EntryPath(k cas.Digest) string
  func (d *Dir) Walk(fn func(path string, e Entry, accessed time.Time) error) error
  func Resolve(root string, selectors []string) ([]FileDigest, error)
  type Record struct {
      Step           string     `json:"step"`
      Digest         cas.Digest `json:"digest"`
      Key            Key        `json:"key"`
      Hit            bool       `json:"hit"`
      Reason         string     `json:"reason,omitempty"`
      PreviousDigest cas.Digest `json:"previous_digest,omitempty"`
      Diffs          []Diff     `json:"diffs,omitempty"`
  }
  func WriteRecord(dir string, r Record) error
  func ReadRecord(dir, step string) (Record, error)
  func ReadRecords(dir string) ([]Record, error)
  func FormatExplain(w io.Writer, r Record) error

  package storage
  // Storage gains: Action *cache.Dir
  ```

**Wiring.** `storage.Open` constructs the action cache alongside the CAS, so Task 1's handle now carries it and Task 9 calls it. `FormatExplain` lives here rather than in `cmd/senro` so the engine's own tests pin the exact rendering, and the CLI in Task 12 is a caller rather than a second implementation.

**Why `Lookup` and `Save` take a step ID.** design.md §3.6's signatures do not. §3.5's `cache explain` requires "the most recent entry for the same step", which cannot be found in a store indexed only by key digest. The step ID is therefore part of the store's interface, and `Previous` is the operation §3.5 actually needs.

**Class, not instance.** A cache entry whose referenced CAS objects have been collected is not a hit. `Forget` exists so Task 9 can turn a broken entry into an ordinary miss rather than failing the step, and the whole category is handled in one place rather than as a special case for whichever object went missing first.

- [ ] **Step 1: Export the glob matcher.** In `internal/workspace/exclude.go`:

```go
// MatchGlob reports whether pattern matches the forward-slash relative path
// rel, using senro's one glob syntax: "*" and "?" within a segment, "**"
// across segments, and a pattern without "/" matching the last segment only.
//
// Exported so that input selection (internal/cache) and workspace exclusion
// use the same matcher. Two implementations of "what does this pattern mean"
// is how a file ends up in a cache key but out of a snapshot.
func MatchGlob(pattern, rel string) bool {
	if strings.Contains(pattern, "/") {
		return matchPath(pattern, rel)
	}
	base := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		base = rel[i+1:]
	}
	return matchSegments(pattern, base)
}
```

- [ ] **Step 2: Write the failing test for input resolution**

Create `internal/cache/inputs_test.go`:

```go
package cache_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func seed(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	return root
}

func TestResolveHashesGlobsAndFiles(t *testing.T) {
	root := seed(t, map[string]string{
		"main.go":         "package main\n",
		"pkg/a/a.go":      "package a\n",
		"go.sum":          "h1:abc\n",
		"README.md":       "hi\n",
	})
	got, err := cache.Resolve(root, []string{
		artifact.Glob("**/*.go").Serial(),
		artifact.File("go.sum").Serial(),
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := map[string]cas.Digest{
		"main.go":    cas.FromBytes([]byte("package main\n")),
		"pkg/a/a.go": cas.FromBytes([]byte("package a\n")),
		"go.sum":     cas.FromBytes([]byte("h1:abc\n")),
	}
	if len(got) != len(want) {
		t.Fatalf("Resolve returned %d files, want %d: %+v", len(got), len(want), got)
	}
	for _, f := range got {
		if want[f.Path] != f.Digest {
			t.Errorf("%s = %s, want %s", f.Path, f.Digest, want[f.Path])
		}
	}
}

func TestResolveIsSortedAndDeduplicated(t *testing.T) {
	root := seed(t, map[string]string{"b.go": "b\n", "a.go": "a\n"})
	got, err := cache.Resolve(root, []string{
		artifact.Glob("**/*.go").Serial(),
		artifact.File("a.go").Serial(),
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Resolve returned %d files, want 2 after dedup: %+v", len(got), got)
	}
	if got[0].Path != "a.go" || got[1].Path != "b.go" {
		t.Errorf("Resolve is not sorted: %+v", got)
	}
}

// A declared input that selects nothing is a typo, and its consequence is a
// key that cannot change when the sources do. Loud beats silent.
func TestResolveRefusesASelectorThatMatchesNothing(t *testing.T) {
	root := seed(t, map[string]string{"a.go": "a\n"})
	for _, sel := range []string{
		artifact.Glob("**/*.rs").Serial(),
		artifact.File("absent.txt").Serial(),
	} {
		_, err := cache.Resolve(root, []string{sel})
		if err == nil {
			t.Errorf("Resolve(%q) matched nothing and returned no error", sel)
			continue
		}
		if !strings.Contains(err.Error(), sel) {
			t.Errorf("the error does not name the selector: %v", err)
		}
	}
}

func TestResolveSkipsDirectoriesAndDefaultExclusions(t *testing.T) {
	root := seed(t, map[string]string{
		"a.go":              "a\n",
		".git/objects/x.go": "junk\n",
		"node_modules/m.go": "junk\n",
	})
	got, err := cache.Resolve(root, []string{artifact.Glob("**/*.go").Serial()})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 1 || got[0].Path != "a.go" {
		t.Errorf("Resolve = %+v, want only a.go: .git and node_modules must not enter a cache key", got)
	}
}

func TestResolveRefusesASelectorThatEscapesTheRoot(t *testing.T) {
	root := seed(t, map[string]string{"a.go": "a\n"})
	if _, err := cache.Resolve(root, []string{artifact.File("../outside.txt").Serial()}); err == nil {
		t.Error("a file selector pointing outside the input root was accepted")
	}
}
```

- [ ] **Step 3: Implement `internal/cache/inputs.go`**

```go
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// Resolve turns a step's declared selectors into sorted (path, digest)
// pairs, relative to root.
//
// A selector that matches nothing is an ERROR. design.md §3.4 makes input
// declaration the pipeline author's responsibility, and the consequence of a
// typo is a key that cannot change when the sources do: the step would be
// served from cache forever. A loud refusal at the first run beats a cache
// that appears to work.
//
// The default exclusions apply here as well as to snapshots, and for the
// same reason: .git changes on every commit, so letting it into an input set
// would mean no pure step ever hits.
func Resolve(root string, selectors []string) ([]FileDigest, error) {
	if len(selectors) == 0 {
		return nil, nil
	}
	sels := make([]artifact.Selector, 0, len(selectors))
	for _, s := range selectors {
		sel, err := artifact.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("cache: %w", err)
		}
		sels = append(sels, sel)
	}

	ex := workspace.NewExcluder(workspace.DefaultExcludes...)
	found := make(map[string]cas.Digest)
	matched := make([]bool, len(sels))

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		relOS, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(relOS)
		if ex.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() || !d.Type().IsRegular() {
			// Directories carry no content of their own, and an irregular
			// file is not portable, so neither belongs in a key.
			return nil
		}
		for i, sel := range sels {
			if !selects(sel, rel) {
				continue
			}
			matched[i] = true
			if _, seen := found[rel]; seen {
				continue
			}
			dg, err := digestFile(p)
			if err != nil {
				return err
			}
			found[rel] = dg
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cache: resolve inputs under %s: %w", root, err)
	}

	for i, ok := range matched {
		if !ok {
			return nil, fmt.Errorf(
				"cache: declared selector %q matched no files under %s; a selector that matches nothing "+
					"leaves a key that cannot change when the sources do", sels[i].Serial(), root)
		}
	}

	out := make([]FileDigest, 0, len(found))
	for p, dg := range found {
		out = append(out, FileDigest{Path: p, Digest: dg})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// selects reports whether sel picks rel. A file selector is an exact
// relative path and never a pattern, so a path containing a glob character
// still means itself.
func selects(sel artifact.Selector, rel string) bool {
	switch sel.Kind() {
	case "file":
		return sel.Pattern() == rel
	default:
		return workspace.MatchGlob(sel.Pattern(), rel)
	}
}

func digestFile(p string) (cas.Digest, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil))), nil
}

// SafeRelative rejects a declared path that leaves the input root. A
// selector is written by a pipeline author, not by an attacker, but a
// selector that reaches outside the root would put a file nobody declared
// into the key and would fail on the next machine.
func SafeRelative(rel string) error {
	if filepath.IsAbs(rel) || strings.HasPrefix(rel, "..") || strings.Contains(rel, "/../") {
		return fmt.Errorf("cache: %q leaves the input root", rel)
	}
	return nil
}
```

Call `SafeRelative` on each `file:` selector's pattern at the top of `Resolve`, before the walk, so an escaping declaration is refused before any I/O happens.

- [ ] **Step 4: Write the failing test for the store**

Create `internal/cache/action_test.go`:

```go
package cache_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func openCache(t *testing.T) *cache.Dir {
	t.Helper()
	d, err := cache.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	return d
}

func sampleResult() *cache.Result {
	return &cache.Result{
		ExitCode:    0,
		Outputs:     []cache.FileDigest{{Path: "out/app", Digest: cas.FromBytes([]byte("app"))}},
		Workspaces:  []cache.WorkspaceDigest{{Name: "build", Digest: cas.FromBytes([]byte("ws"))}},
		Logs:        []cache.LogRef{{Stream: "stdout", Digest: cas.FromBytes([]byte("log")), Bytes: 3}},
		DurationNS:  1500,
		RunID:       "r1",
		Hermeticity: cache.HermeticityTrusted,
		SavedAt:     time.Now().UTC(),
		Bytes:       6,
	}
}

func TestSaveThenLookupReturnsTheResult(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()

	if _, ok, err := d.Lookup(ctx, "build", k); err != nil || ok {
		t.Fatalf("Lookup before Save = %v, %v; want false, nil", ok, err)
	}
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := d.Lookup(ctx, "build", k)
	if err != nil || !ok {
		t.Fatalf("Lookup after Save = %v, %v; want true, nil", ok, err)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Path != "out/app" {
		t.Errorf("result outputs = %+v", got.Outputs)
	}
	if got.Hermeticity != cache.HermeticityTrusted {
		t.Errorf("hermeticity = %q, want %q: Pure() is trusted rather than enforced, and the entry must say so",
			got.Hermeticity, cache.HermeticityTrusted)
	}
}

// The negative half, and the one that matters most: a key that differs in
// any component must not hit.
func TestADifferentKeyDoesNotHit(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	other := k
	other.InputDigests = cache.InputsComponent([]cache.FileDigest{{Path: "a.go", Digest: cas.FromBytes([]byte("edited"))}})
	if _, ok, err := d.Lookup(ctx, "build", other); err != nil || ok {
		t.Errorf("a changed input hit the cache: %v, %v", ok, err)
	}
}

// design.md §3.5 needs "the most recent entry for the same step", which is
// the whole reason the store is indexed by step as well as by key.
func TestPreviousReturnsTheMostRecentEntryForAStep(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()

	first := sampleKey()
	if err := d.Save(ctx, "build", first, sampleResult()); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	second := first
	second.Platform = "darwin/arm64"
	if err := d.Save(ctx, "build", second, sampleResult()); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	e, ok, err := d.Previous(ctx, "build")
	if err != nil || !ok {
		t.Fatalf("Previous = %v, %v", ok, err)
	}
	if e.Key.Digest() != second.Digest() {
		t.Errorf("Previous returned the older entry")
	}

	if _, ok, err := d.Previous(ctx, "never-run"); err != nil || ok {
		t.Errorf("Previous for an unknown step = %v, %v; want false, nil", ok, err)
	}
}

// Step IDs contain "/" and "[]" (see internal/stepid). A store that used
// them as filenames unescaped would collide or fail.
func TestPreviousHandlesAnExpandedStepID(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	const id = "build/test[unit=services/api]"
	if err := d.Save(ctx, id, sampleKey(), sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, ok, err := d.Previous(ctx, id); err != nil || !ok {
		t.Errorf("Previous for %q = %v, %v", id, ok, err)
	}
}

func TestForgetRemovesAnEntry(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := d.Forget(ctx, k); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, ok, _ := d.Lookup(ctx, "build", k); ok {
		t.Error("Lookup still hits after Forget")
	}
	if err := d.Forget(ctx, k); err != nil {
		t.Errorf("Forget is not idempotent: %v", err)
	}
}

// An entry file that is not readable JSON is treated as absent rather than
// as an error, so one corrupt file cannot fail every run on the machine.
func TestACorruptEntryReadsAsAMiss(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k := sampleKey()
	if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := os.WriteFile(d.EntryPath(k.Digest()), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("scribble: %v", err)
	}
	_, ok, err := d.Lookup(ctx, "build", k)
	if err != nil {
		t.Errorf("a corrupt entry returned an error rather than a miss: %v", err)
	}
	if ok {
		t.Error("a corrupt entry reported a hit")
	}
}

func TestWalkVisitsEveryEntry(t *testing.T) {
	d := openCache(t)
	ctx := context.Background()
	k1 := sampleKey()
	k2 := k1
	k2.Platform = "darwin/arm64"
	for _, k := range []cache.Key{k1, k2} {
		if err := d.Save(ctx, "build", k, sampleResult()); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	var n int
	if err := d.Walk(func(_ string, e cache.Entry, accessed time.Time) error {
		n++
		if accessed.IsZero() {
			t.Error("Walk reported an entry with no access time, so the GC has no clock to sort by")
		}
		if e.Result.RunID != "r1" {
			t.Errorf("Walk decoded a result with RunID %q", e.Result.RunID)
		}
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if n != 2 {
		t.Errorf("Walk saw %d entries, want 2", n)
	}
}

func TestWalkStopsOnTheCallbacksError(t *testing.T) {
	d := openCache(t)
	if err := d.Save(context.Background(), "build", sampleKey(), sampleResult()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	sentinel := errors.New("stop")
	if err := d.Walk(func(string, cache.Entry, time.Time) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Errorf("Walk = %v, want the callback's error", err)
	}
}
```

- [ ] **Step 5: Implement `internal/cache/action.go`**

```go
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/stepid"
)

// HermeticityTrusted records that Pure() was taken at its word.
//
// design.md §11 item 2 resolves that Pure() is trusted, not enforced: there
// is no network sandboxing in this build. The field is written now so that
// entries produced under enforcement, if it ever arrives, are distinguishable
// from these without a migration.
const HermeticityTrusted = "trusted"

// Miss reasons, as they appear in cache.miss events and in `cache explain`.
const (
	ReasonNoPreviousEntry = "no_previous_entry"
	ReasonKeyChanged      = "key_changed"
	// ReasonEntryIncomplete means an entry was found and its content was
	// not: a GC collected an object the entry referenced. It is a miss, not
	// a failure. Treating it as a failure would make a cache sweep able to
	// break a build.
	ReasonEntryIncomplete = "entry_incomplete"
)

// LogRef points at one stream of a cached step's output, stored in the CAS
// so a hit can replay what the step would have printed (design.md §3.6).
type LogRef struct {
	Stream string     `json:"stream"`
	Digest cas.Digest `json:"digest"`
	Bytes  int64      `json:"bytes"`
}

// Result is everything a hit has to reproduce.
type Result struct {
	ExitCode   int               `json:"exit_code"`
	Outputs    []FileDigest      `json:"outputs,omitempty"`
	Workspaces []WorkspaceDigest `json:"workspaces,omitempty"`
	Logs       []LogRef          `json:"logs,omitempty"`
	DurationNS int64             `json:"duration_ns"`
	// RunID is the run that produced this result, so a cached step can be
	// traced back to the one execution that actually happened.
	RunID       string    `json:"run_id"`
	Hermeticity string    `json:"hermeticity"`
	SavedAt     time.Time `json:"saved_at"`
	// Bytes is the total uncompressed size stored for this entry. It is what
	// cache.saved reports and what the GC budgets against.
	Bytes int64 `json:"bytes"`
}

// Entry is a key and its result, stored together. Storing the key rather
// than only its digest is what makes `cache explain` a diff instead of a
// re-derivation (design.md §3.5).
//
// Storing the components leaks the SHAPE of a build and never a secret: the
// env component holds digests, not values, and the secrets component holds
// the provider:key:version:digest8 identity form (design.md §1.6).
type Entry struct {
	Key    Key    `json:"key"`
	Result Result `json:"result"`
}

// ActionCache is the correctness-critical cache: a hit skips the step.
//
// The step ID is part of every operation, which design.md §3.6's sketch does
// not show. §3.5 requires "the most recent entry for the same step", and
// that is unfindable in a store indexed by key digest alone.
type ActionCache interface {
	Lookup(ctx context.Context, step string, k Key) (*Result, bool, error)
	Save(ctx context.Context, step string, k Key, r *Result) error
	Previous(ctx context.Context, step string) (*Entry, bool, error)
	// Forget removes an entry. Used when a hit turns out to reference
	// content a GC has collected, so the next run misses cleanly instead of
	// finding the same broken entry again.
	Forget(ctx context.Context, k Key) error
}

// Dir is the local-directory action cache: <root>/entries/<aa>/<hex> for the
// entries, <root>/recent/<encoded-step> naming each step's latest key.
type Dir struct {
	root    string
	entries string
	recent  string
}

var _ ActionCache = (*Dir)(nil)

// Open prepares root.
func Open(root string) (*Dir, error) {
	d := &Dir{
		root:    root,
		entries: filepath.Join(root, "entries"),
		recent:  filepath.Join(root, "recent"),
	}
	for _, p := range []string{d.entries, d.recent} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			return nil, fmt.Errorf("cache: open %s: %w", root, err)
		}
	}
	return d, nil
}

// Root is the directory this cache lives in.
func (d *Dir) Root() string { return d.root }

// EntryPath is where the entry for a key digest is stored.
func (d *Dir) EntryPath(k cas.Digest) string {
	if !k.Valid() {
		return ""
	}
	h := k.Hex()
	return filepath.Join(d.entries, h[0:2], h)
}

// Lookup returns the stored result for k, if there is one.
//
// A corrupt or unreadable entry reads as a MISS rather than as an error. A
// cache is an optimization, and one damaged file on a developer's machine
// must not be able to fail every run on it. The damaged file is left in
// place for a GC to reclaim rather than deleted here, so a Lookup stays a
// read.
func (d *Dir) Lookup(_ context.Context, _ string, k Key) (*Result, bool, error) {
	p := d.EntryPath(k.Digest())
	if p == "" {
		return nil, false, nil
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: %w", err)
	}
	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, false, nil
	}
	now := time.Now()
	// mtime is this store's access clock, for the same reason it is the
	// CAS's: an entry is immutable, so mtime carries no other meaning, and
	// atime needs a build-tagged syscall and is unreliable under relatime.
	_ = os.Chtimes(p, now, now)
	return &e.Result, true, nil
}

// Save writes the entry and records it as the step's most recent.
func (d *Dir) Save(_ context.Context, step string, k Key, r *Result) error {
	if r == nil {
		return fmt.Errorf("cache: refusing to save a nil result for step %q", step)
	}
	dg := k.Digest()
	b, err := json.MarshalIndent(Entry{Key: k, Result: *r}, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: marshal entry for step %q: %w", step, err)
	}
	p := d.EntryPath(dg)
	if err := writeAtomic(p, b); err != nil {
		return fmt.Errorf("cache: save entry for step %q: %w", step, err)
	}
	// The recent pointer is written second and separately. If the process
	// dies between the two, the entry is still a valid hit and only `cache
	// explain` loses one generation of history, which is the right way round.
	if err := writeAtomic(filepath.Join(d.recent, stepid.Encode(step)), []byte(dg)); err != nil {
		return fmt.Errorf("cache: record most recent entry for step %q: %w", step, err)
	}
	return nil
}

// Previous returns the most recent entry saved for a step.
func (d *Dir) Previous(_ context.Context, step string) (*Entry, bool, error) {
	b, err := os.ReadFile(filepath.Join(d.recent, stepid.Encode(step)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: %w", err)
	}
	eb, err := os.ReadFile(d.EntryPath(cas.Digest(b)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// The pointer outlived the entry, which is what a GC leaves
			// behind. Not an error: there simply is no previous entry.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: %w", err)
	}
	var e Entry
	if err := json.Unmarshal(eb, &e); err != nil {
		return nil, false, nil
	}
	return &e, true, nil
}

// Forget removes an entry, leaving the recent pointer to fall through to
// "no previous entry" on its own.
func (d *Dir) Forget(_ context.Context, k Key) error {
	p := d.EntryPath(k.Digest())
	if p == "" {
		return nil
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache: %w", err)
	}
	return nil
}

// Walk calls fn for every stored entry. Unreadable files are skipped for the
// same reason Lookup treats them as a miss.
func (d *Dir) Walk(fn func(path string, e Entry, accessed time.Time) error) error {
	err := filepath.WalkDir(d.entries, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && p == d.entries {
				return fs.SkipAll
			}
			return err
		}
		if de.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		var e Entry
		if err := json.Unmarshal(b, &e); err != nil {
			return nil
		}
		info, err := de.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		return fn(p, e, info.ModTime())
	})
	if err != nil && !errors.Is(err, fs.SkipAll) {
		return err
	}
	return nil
}

// writeAtomic writes b to p through a temp file and a rename, so a reader
// never sees a partial entry.
func writeAtomic(p string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(b); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}
```

`Walk`'s error handling returns the callback's error untouched, so `TestWalkStopsOnTheCallbacksError` passes: `filepath.WalkDir` propagates it and the `fs.SkipAll` guard does not match it.

- [ ] **Step 6: Write the failing test for the per-run record**

Create `internal/cache/record_test.go`:

```go
package cache_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func TestRecordsRoundTripThroughARunDirectory(t *testing.T) {
	dir := t.TempDir()
	want := cache.Record{
		Step:   "build/test[unit=services/api]",
		Digest: sampleKey().Digest(),
		Key:    sampleKey(),
		Hit:    false,
		Reason: cache.ReasonKeyChanged,
		Diffs:  []cache.Diff{{Name: "input_digests", From: "a.go " + string(cas.FromBytes([]byte("a"))), To: "a.go " + string(cas.FromBytes([]byte("b")))}},
	}
	if err := cache.WriteRecord(dir, want); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	got, err := cache.ReadRecord(dir, want.Step)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if got.Step != want.Step || got.Digest != want.Digest || got.Hit != want.Hit {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
	all, err := cache.ReadRecords(dir)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(all) != 1 || all[0].Step != want.Step {
		t.Errorf("ReadRecords = %+v", all)
	}
}

func TestReadRecordForAnUnknownStepIsAnError(t *testing.T) {
	if _, err := cache.ReadRecord(t.TempDir(), "never-run"); err == nil {
		t.Error("ReadRecord for a step with no record returned no error")
	}
}

// design.md §3.5's output shape: the miss, both key digests, the first
// differing field with both sides, and a line confirming what did not move.
func TestFormatExplainRendersAMiss(t *testing.T) {
	var buf bytes.Buffer
	err := cache.FormatExplain(&buf, cache.Record{
		Step:           "build/test",
		Digest:         cas.FromBytes([]byte("current")),
		Key:            sampleKey(),
		Hit:            false,
		Reason:         cache.ReasonKeyChanged,
		PreviousDigest: cas.FromBytes([]byte("previous")),
		Diffs: []cache.Diff{{
			Name: "input_digests",
			From: "services/api/handler.go " + string(cas.FromBytes([]byte("old"))),
			To:   "services/api/handler.go " + string(cas.FromBytes([]byte("new"))),
		}},
	})
	if err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"MISS", "build/test",
		cas.FromBytes([]byte("current")).Short(),
		cas.FromBytes([]byte("previous")).Short(),
		"input_digests", "services/api/handler.go",
		"executor_class", "unchanged",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q:\n%s", want, out)
		}
	}
}

func TestFormatExplainRendersAHit(t *testing.T) {
	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "build/test", Digest: sampleKey().Digest(), Key: sampleKey(), Hit: true,
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "HIT") {
		t.Errorf("a hit did not render as a hit:\n%s", out)
	}
	if strings.Contains(out, "MISS") {
		t.Errorf("a hit rendered as a miss:\n%s", out)
	}
}

func TestFormatExplainSaysSoWhenThereIsNoPreviousEntry(t *testing.T) {
	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "build/test", Digest: sampleKey().Digest(), Key: sampleKey(),
		Hit: false, Reason: cache.ReasonNoPreviousEntry,
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "no previous entry") {
		t.Errorf("a first run must say there was nothing to compare against:\n%s", out)
	}
	if strings.Contains(out, "unchanged") {
		t.Errorf("a first run must not claim components were unchanged:\n%s", out)
	}
}

// A value never reaches this output, because a value never reaches a key.
func TestFormatExplainCannotPrintAnEnvironmentValue(t *testing.T) {
	const token = "super-secret-value-nobody-should-see" //nolint:gosec // a test fixture, not a credential
	k := sampleKey()
	k.Env = cache.EnvComponent([]string{"BUILD_TOKEN=" + token}, []string{"BUILD_TOKEN"})
	prev := k
	prev.Env = cache.EnvComponent([]string{"BUILD_TOKEN=other"}, []string{"BUILD_TOKEN"})

	var buf bytes.Buffer
	if err := cache.FormatExplain(&buf, cache.Record{
		Step: "s", Digest: k.Digest(), Key: k, Hit: false,
		Reason: cache.ReasonKeyChanged, PreviousDigest: prev.Digest(),
		Diffs: cache.Explain(prev, k),
	}); err != nil {
		t.Fatalf("FormatExplain: %v", err)
	}
	if strings.Contains(buf.String(), token) {
		t.Fatalf("cache explain printed an environment value:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "BUILD_TOKEN") {
		t.Errorf("cache explain did not name the variable that changed:\n%s", buf.String())
	}
}
```

- [ ] **Step 7: Implement `internal/cache/record.go`**

```go
package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/stepid"
)

// Record is what one step's cache decision looked like during one run,
// written to <run>/cache/<encoded-step>.json.
//
// It exists so `senro cache explain` is a formatter over facts the engine
// already recorded rather than a re-planning exercise. Re-deriving a key
// after the fact would need the pipeline binary, its inputs and the same
// working tree, and would answer a subtly different question from the one
// the run actually asked. It also means the CLI and the engine cannot
// disagree about why a step missed.
type Record struct {
	Step   string     `json:"step"`
	Digest cas.Digest `json:"digest"`
	Key    Key        `json:"key"`
	Hit    bool       `json:"hit"`
	Reason string     `json:"reason,omitempty"`
	// PreviousDigest and Diffs describe the entry this key was compared
	// against, captured at LOOKUP time. After a save the store's most recent
	// entry is this key, so a comparison made later would always come back
	// empty.
	PreviousDigest cas.Digest `json:"previous_digest,omitempty"`
	Diffs          []Diff     `json:"diffs,omitempty"`
}

// WriteRecord stores r under dir, which is the run's cache directory.
func WriteRecord(dir string, r Record) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: marshal record for step %q: %w", r.Step, err)
	}
	if err := writeAtomic(filepath.Join(dir, stepid.Encode(r.Step)+".json"), b); err != nil {
		return fmt.Errorf("cache: write record for step %q: %w", r.Step, err)
	}
	return nil
}

// ReadRecord loads one step's record.
func ReadRecord(dir, step string) (Record, error) {
	b, err := os.ReadFile(filepath.Join(dir, stepid.Encode(step)+".json"))
	if err != nil {
		return Record{}, fmt.Errorf("cache: no cache record for step %q: %w", step, err)
	}
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, fmt.Errorf("cache: read record for step %q: %w", step, err)
	}
	return r, nil
}

// ReadRecords loads every record in a run, sorted by step ID.
func ReadRecords(dir string) ([]Record, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache: read records: %w", err)
	}
	var out []Record
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("cache: read records: %w", err)
		}
		var r Record
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("cache: read record %s: %w", e.Name(), err)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Step < out[j].Step })
	return out, nil
}

// FormatExplain renders one record in the shape design.md §3.5 specifies:
// the verdict, both key digests, every differing component with both sides,
// and a line naming what did not move. The last line is not decoration: "the
// inputs changed" and "everything changed" are very different diagnoses, and
// without it a reader cannot tell them apart.
func FormatExplain(w io.Writer, r Record) error {
	verdict := "MISS"
	if r.Hit {
		verdict = "HIT"
	}
	line := fmt.Sprintf("%s  %s  key %s", verdict, r.Step, r.Digest.Short())
	if r.PreviousDigest != "" {
		line += fmt.Sprintf(" (previous %s)", r.PreviousDigest.Short())
	}
	if _, err := fmt.Fprintln(w, line); err != nil {
		return err
	}

	switch {
	case r.Hit:
		return nil
	case r.Reason == ReasonNoPreviousEntry:
		_, err := fmt.Fprintln(w, "  no previous entry for this step: nothing to compare against")
		return err
	case r.Reason == ReasonEntryIncomplete:
		_, err := fmt.Fprintln(w,
			"  the entry was found but its content was not: a cache sweep collected an object it referenced")
		return err
	}

	changed := make(map[string]bool, len(r.Diffs))
	for _, d := range r.Diffs {
		changed[d.Name] = true
		for _, detail := range componentDiffLines(d) {
			if _, err := fmt.Fprintf(w, "  ✗ %s: %s\n", d.Name, detail); err != nil {
				return err
			}
		}
	}

	var same []string
	for _, c := range r.Key.Components() {
		if !changed[c.Name] {
			same = append(same, c.Name)
		}
	}
	if len(same) > 0 {
		if _, err := fmt.Fprintf(w, "  ✓ %s unchanged\n", strings.Join(same, ", ")); err != nil {
			return err
		}
	}
	return nil
}

// componentDiffLines breaks a multi-line component down to the lines that
// actually differ, so an input set of 4000 files reports the one file that
// changed rather than 4000 lines of identical text.
func componentDiffLines(d Diff) []string {
	from := splitLines(d.From)
	to := splitLines(d.To)
	if len(from) <= 1 && len(to) <= 1 {
		return []string{fmt.Sprintf("%s → %s", shorten(d.From), shorten(d.To))}
	}

	fromByLabel := labelled(from)
	toByLabel := labelled(to)
	labels := make([]string, 0, len(fromByLabel)+len(toByLabel))
	seen := map[string]bool{}
	for _, m := range []map[string]string{fromByLabel, toByLabel} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				labels = append(labels, k)
			}
		}
	}
	sort.Strings(labels)

	var out []string
	for _, l := range labels {
		a, inA := fromByLabel[l]
		b, inB := toByLabel[l]
		switch {
		case inA && inB && a != b:
			out = append(out, fmt.Sprintf("%s  %s → %s", l, shorten(a), shorten(b)))
		case inA && !inB:
			out = append(out, fmt.Sprintf("%s  removed", l))
		case !inA && inB:
			out = append(out, fmt.Sprintf("%s  added", l))
		}
	}
	if len(out) == 0 {
		out = append(out, fmt.Sprintf("%s → %s", shorten(d.From), shorten(d.To)))
	}
	return out
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// labelled keys each line of a component by everything before its last
// space, which is the path in an inputs component, the workspace name in a
// workspaces component, and the variable name in an env component. All three
// are written as "<label><separator><value>" by their builders in key.go.
func labelled(lines []string) map[string]string {
	out := make(map[string]string, len(lines))
	for _, l := range lines {
		if i := strings.LastIndex(l, " "); i >= 0 {
			out[l[:i]] = l[i+1:]
			continue
		}
		if k, v, ok := strings.Cut(l, "="); ok {
			out[k] = v
			continue
		}
		out[l] = ""
	}
	return out
}

// shorten trims a digest to its short form for display, and leaves anything
// else alone.
func shorten(s string) string {
	if d := cas.Digest(s); d.Valid() {
		return d.Short()
	}
	if len(s) > 32 {
		return s[:32] + "..."
	}
	return s
}
```

- [ ] **Step 8: Wire the action cache into `internal/storage`.** Add the field and construct it:

```go
	// Action is the correctness-critical cache: a hit skips a step entirely.
	Action *cache.Dir
```

```go
	action, err := cache.Open(filepath.Join(root, "action"))
	if err != nil {
		return nil, err
	}
	return &Storage{
		Root: root, CAS: store, Action: action,
		Snapshotter: workspace.NewSnapshotter(store),
	}, nil
```

and extend `TestOpenCreatesTheStoreLayout`:

```go
	if s.Action == nil {
		t.Error("Open returned a Storage with no action cache")
	}
```

- [ ] **Step 9: Run to verify it passes** with `go test ./internal/cache/... ./internal/storage/... ./internal/workspace/... -race`.

- [ ] **Step 10: Prove the corrupt-entry behaviour.** Temporarily make `Lookup` return the unmarshal error. Confirm `TestACorruptEntryReadsAsAMiss` fails. Restore, and record it.

- [ ] **Step 11: Run the gates**

```bash
make all
golangci-lint run ./...
```

- [ ] **Step 12: Commit**

```bash
git add internal/cache internal/storage internal/workspace/exclude.go
git commit -m "feat(cache): the action cache store, its Result and the per-run record

Entries hold the key's components alongside the result, indexed by key digest
and by step, because cache explain needs the most recent entry for a step and
that is unfindable in a store keyed only by digest. A corrupt entry reads as a
miss rather than an error, so one damaged file cannot fail every run on the
machine. FormatExplain lives here, not in the CLI, so the engine's own tests
pin the rendering and the two cannot disagree."
```

---

### Task 9: Engine wiring: lookup, hit, replay, save

**This is the task the rest of the plan exists to make possible**, and it is the one where an unwired capability would be invisible: the cache would be built, tested, and never consulted.

**Files:**
- Create `internal/engine/cache.go`, `internal/engine/cache_test.go`
- Modify `internal/engine/engine.go` (`runCore` around lines 254 to 287, `Run` around lines 106 to 137)
- Modify `internal/engine/attempt.go` (`runStep` around lines 64 to 246, `finishStep` around lines 364 to 377)

**Interfaces:**
- Consumes: Task 6's `wsManager`, `wsSnapshot`, `attemptResult.snapshots`; Task 7's `Key`, `Explain`, `FileDigest`, `WorkspaceDigest`, the component builders; Task 8's `ActionCache`, `Result`, `LogRef`, `Resolve`, `Record`, `WriteRecord`, the reason constants; Task 1's `cas`.
- Produces:
  ```go
  package engine
  type cacheDecision struct {
      key    cache.Key
      digest cas.Digest
      result *cache.Result
      hit    bool
      reason string
      prev   cas.Digest
      diffs  []cache.Diff
  }
  func (rc *runCore) cacheLookup(ctx context.Context, n *plan.Node, opts Options) (cacheDecision, error)
  func (rc *runCore) serveFromCache(ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, dec cacheDecision) bool
  func (rc *runCore) cacheSave(ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, dec cacheDecision, res attemptResult, attempt int, dur time.Duration) error
  // finishStep gains a trailing `cached bool` parameter.
  ```

**Event order, and why.** On a hit: `cache.hit`, then one `ws.restored` per restored workspace, then the replayed `step.log.appended` markers, then `step.finished` with `state: cached` and `cached: true`. On a miss: `cache.miss`, then the ordinary `step.started` through `ws.snapshot` sequence, then `cache.saved`, then `step.finished`.

**No `step.started` on a hit.** A `step.started` means a sandbox was created and a command was launched, and on a hit neither happened. The fold materializes a step from any step-scoped event, so nothing downstream needs one; a renderer reads `State` to learn the step was cached. Emitting one would put a start time on a step that never started, which is the sort of small lie that makes a duration column meaningless.

**The seq rule.** Replayed log markers are the CURRENT run's events with the current run's sequence numbers, emitted through `rc.emit` like everything else. They are not the stored run's events replayed verbatim, and the stored run's ID travels in `cache.hit`'s `from_run` field instead. Anything else would put two sequence spaces in one ledger.

**Class, not instance.** A hit whose content cannot be restored degrades to a miss: the entry is forgotten, `cache.miss` is emitted with `ReasonEntryIncomplete`, and the step runs. Every reason an entry can be incomplete is handled at once, because they all have the same correct answer. Failing the step instead would mean a cache sweep could break a build, which is the worst possible property for an optimization to have.

- [ ] **Step 1: Write the failing test**

Create `internal/engine/cache_test.go`:

```go
package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// runTwice runs the same plan twice against ONE shared cache root and two
// separate run directories, which is the shape every question about a cache
// actually asks.
func runTwice(t *testing.T, p *senro.Plan) (first, second []api.Event, dirs [2]string) {
	t.Helper()
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	for i := 0; i < 2; i++ {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		runDir := filepath.Join(t.TempDir(), "run")
		dirs[i] = runDir
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir:      runDir,
			Executor: localexec.New(runDir, store.Snapshotter),
			Sink:     rec,
			Storage:  store,
			RunID:    []string{"r1", "r2"}[i],
		}); err != nil {
			t.Fatalf("engine.Run %d: %v", i+1, err)
		}
		if i == 0 {
			first = rec.Events()
		} else {
			second = rec.Events()
		}
		_ = store.Close()
	}
	return first, second, dirs
}

func purePipeline(t *testing.T, cmd string) *senro.Plan {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("compile", exec.Command("sh", "-c", cmd)).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

func TestASecondIdenticalRunHitsTheCache(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	first, second, dirs := runTwice(t, p)

	if countType(first, api.CacheMiss) != 1 || countType(first, api.CacheSaved) != 1 {
		t.Errorf("the first run should miss once and save once: miss=%d saved=%d",
			countType(first, api.CacheMiss), countType(first, api.CacheSaved))
	}
	if countType(first, api.CacheHit) != 0 {
		t.Error("the first run hit a cache that was empty")
	}
	if countType(second, api.CacheHit) != 1 {
		t.Fatalf("the second run did not hit: %d hits, %d misses",
			countType(second, api.CacheHit), countType(second, api.CacheMiss))
	}
	if countType(second, api.StepStarted) != 1 {
		t.Errorf("a cached step still emitted step.started: %d starts in the second run",
			countType(second, api.StepStarted))
	}

	// The step is cached AND its filesystem effect is back.
	if b, err := os.ReadFile(filepath.Join(dirs[1], "ws", "src", "out.txt")); err != nil {
		t.Errorf("a cache hit did not restore the output the step would have produced: %v", err)
	} else if strings.TrimSpace(string(b)) != "compiled" {
		t.Errorf("restored output = %q", b)
	}
}

func TestACachedStepFoldsToStateCached(t *testing.T) {
	_, second, _ := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	st := api.NewRunState()
	for _, e := range second {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	step, ok := st.Steps["compile"]
	if !ok {
		t.Fatal("compile is missing from the folded state")
	}
	if step.State != api.StateCached {
		t.Errorf("state = %s, want cached", step.State)
	}
	if !step.Cached {
		t.Error("the step folded without the cached flag, so a renderer would count it as an ordinary success")
	}
	if !step.Started.IsZero() {
		t.Error("a cached step has a start time, which means step.started was emitted for a step that never started")
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("a fully cached run rolled up to %s, want succeeded", st.Run.Status)
	}
}

// design.md §3.6: restoring a hit replays the stored logs so the UI shows
// what would have happened.
func TestACacheHitReplaysTheStoredLogs(t *testing.T) {
	_, second, dirs := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))

	b, err := os.ReadFile(filepath.Join(dirs[1], "logs", "compile", "1", "stdout"))
	if err != nil {
		t.Fatalf("a cached step left no log file: %v", err)
	}
	if !strings.Contains(string(b), "compiled") {
		t.Errorf("the replayed log does not contain the cached output: %q", b)
	}
	var markers int
	for _, e := range second {
		if e.Type == api.StepLogAppended && e.Step == "compile" {
			markers++
		}
	}
	if markers == 0 {
		t.Error("no step.log.appended marker was emitted for the replayed log, so an attached client sees nothing")
	}
}

// Sequence numbers stay this run's own. Replaying a stored run's events
// verbatim would put two sequence spaces in one ledger.
func TestReplayedEventsUseThisRunsSequence(t *testing.T) {
	_, second, _ := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	var last uint64
	for _, e := range second {
		if e.Seq <= last {
			t.Fatalf("sequence regressed at %s: %d after %d", e.Type, e.Seq, last)
		}
		last = e.Seq
		if e.Run != "r2" {
			t.Errorf("event %s carries run %q, want the current run", e.Type, e.Run)
		}
	}
}

func TestCacheHitNamesTheRunThatProducedTheResult(t *testing.T) {
	_, second, _ := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	for _, e := range second {
		if e.Type != api.CacheHit {
			continue
		}
		var b api.CacheHitBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode cache.hit: %v", err)
		}
		if b.FromRun != "r1" {
			t.Errorf("cache.hit from_run = %q, want the run that produced the result", b.FromRun)
		}
		if b.Key == "" {
			t.Error("cache.hit carries no key")
		}
	}
}

// The negative half. An input change must miss, and the miss must say which
// component moved.
func TestAChangedInputMissesAndNamesTheComponent(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	run := func(body string, runID string) []api.Event {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf '"+body+"' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "cat main.go > out.txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("package main\\n", "r1")
	second := run("package other\\n", "r2")

	if countType(second, api.CacheHit) != 0 {
		t.Fatal("a changed input still hit the cache, which is a wrong build")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheMiss {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode cache.miss: %v", err)
			}
		}
	}
	if body.Reason != cache.ReasonKeyChanged {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonKeyChanged)
	}
	if body.Differing != "input_digests" && body.Differing != "workspace_digests" {
		t.Errorf("miss differing = %q, want the component that actually moved", body.Differing)
	}
}

func TestAnImpureStepIsNeverCachedAndEmitsNoCacheEvents(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	first, second, _ := runTwice(t, p)
	for i, events := range [][]api.Event{first, second} {
		for _, ty := range []api.Type{api.CacheHit, api.CacheMiss, api.CacheSaved} {
			if n := countType(events, ty); n != 0 {
				t.Errorf("run %d emitted %d %s events for an impure step", i+1, n, ty)
			}
		}
		if countType(events, api.StepStarted) != 1 {
			t.Errorf("run %d did not run the impure step", i+1)
		}
	}
}

func TestAFailedPureStepIsNotSaved(t *testing.T) {
	p := purePipeline(t, "echo partial > out.txt; exit 3")
	first, second, _ := runTwice(t, p)
	if countType(first, api.CacheSaved) != 0 {
		t.Error("a failed step saved a cache entry, so the failure would be served to every future run")
	}
	if countType(second, api.CacheHit) != 0 {
		t.Error("a failed step was served from cache")
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the second run did not re-execute the failed step: %d starts", countType(second, api.StepStarted))
	}
}

// The class fix. A GC that collected an object an entry references must not
// be able to break a build.
func TestAHitWithMissingContentDegradesToAMiss(t *testing.T) {
	p := purePipeline(t, "echo compiled | tee out.txt")
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), p, engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	// Empty the CAS while leaving every cache entry in place, which is
	// exactly what an over-eager sweep produces.
	if err := os.RemoveAll(filepath.Join(cacheRoot, "cas", "sha256")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	second := run("r2")

	if countType(second, api.CacheHit) != 0 {
		t.Error("an entry whose content was collected still reported a hit")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheMiss {
			_ = e.Decode(&body)
		}
	}
	if body.Reason != cache.ReasonEntryIncomplete {
		t.Errorf("miss reason = %q, want %q", body.Reason, cache.ReasonEntryIncomplete)
	}
	if countType(second, api.StepStarted) != 2 {
		t.Errorf("the run did not re-execute after the degraded hit: %d starts", countType(second, api.StepStarted))
	}
}

func TestTheRunRecordsItsKeyForEveryPureStep(t *testing.T) {
	_, _, dirs := runTwice(t, purePipeline(t, "echo compiled | tee out.txt"))
	r, err := cache.ReadRecord(filepath.Join(dirs[1], "cache"), "compile")
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if !r.Hit {
		t.Errorf("the second run's record says miss: %+v", r)
	}
	if r.Digest == "" || r.Key.Version != cache.KeyVersion {
		t.Errorf("record = %+v, want the key it looked up", r)
	}
}
```

- [ ] **Step 2: Run to verify it fails** with `go test ./internal/engine/ -run Cache`. Expect no `cache.hit` events at all, since nothing consults a cache yet.

- [ ] **Step 3: Implement `internal/engine/cache.go`**

```go
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/plan"
)

// cacheDecision is what one lookup concluded, carried from the lookup to the
// hit or to the save so neither has to recompute a key.
type cacheDecision struct {
	key    cache.Key
	digest cas.Digest
	result *cache.Result
	hit    bool
	reason string
	prev   cas.Digest
	diffs  []cache.Diff
}

// cacheable reports whether this build will consult the action cache for n.
func (rc *runCore) cacheable(n *plan.Node) bool {
	return n.Pure && rc.cache != nil && rc.ws != nil
}

// cacheLookup builds n's key, consults the store, and records the decision in
// the run directory.
//
// The key is built HERE, immediately before the step would run, rather than
// at plan time. Two of its components are only knowable now: the digests of
// the workspaces the step mounts, which depend on what upstream steps wrote,
// and the digests of the declared inputs, which are read off the filesystem
// those steps left behind.
func (rc *runCore) cacheLookup(ctx context.Context, n *plan.Node, opts Options) (cacheDecision, error) {
	class, err := opts.Executor.Class(ctx)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: executor class: %w", n.ID, err)
	}
	platform, err := opts.Executor.DeclaredPlatform(ctx)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: declared platform: %w", n.ID, err)
	}

	root := rc.ws.inputRoot(n)
	inputs, err := cache.Resolve(root, n.Inputs)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: %w", n.ID, err)
	}

	var wsDigests []cache.WorkspaceDigest
	for _, s := range rc.ws.digests(n) {
		wsDigests = append(wsDigests, cache.WorkspaceDigest{Name: s.Name, Digest: s.Digest})
	}

	k := cache.Key{
		Command:          cache.CommandComponent(n.Kind, n.Cmd, n.WorkDir),
		Env:              cache.EnvComponent(n.Env, n.CacheEnv),
		Secrets:          "", // populated by the secrets subsystem; identities only, never values
		ExecutorClass:    class,
		Platform:         platform.String(),
		InputDigests:     cache.InputsComponent(inputs),
		WorkspaceDigests: cache.WorkspacesComponent(wsDigests),
		FuncIdentity:     "", // this build executes no Func steps
		ToolVersions:     "", // no tool declarations in this build
		Version:          cache.KeyVersion,
	}
	dec := cacheDecision{key: k, digest: k.Digest()}

	res, ok, err := rc.cache.Lookup(ctx, n.ID, k)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: cache lookup: %w", n.ID, err)
	}
	dec.hit, dec.result = ok, res

	if !ok {
		// Why it missed is computed HERE, against the entry that was most
		// recent at lookup time. After a save the store's most recent entry
		// is this key, so the same comparison made later always comes back
		// empty and `cache explain` reports nothing.
		prev, hasPrev, err := rc.cache.Previous(ctx, n.ID)
		if err != nil {
			return cacheDecision{}, fmt.Errorf("engine: step %q: cache history: %w", n.ID, err)
		}
		if hasPrev {
			dec.reason = cache.ReasonKeyChanged
			dec.prev = prev.Key.Digest()
			dec.diffs = cache.Explain(prev.Key, k)
		} else {
			dec.reason = cache.ReasonNoPreviousEntry
		}
	}
	return dec, nil
}

// recordDecision writes the run's copy of a key and its verdict, which is
// what `senro cache explain` reads.
func (rc *runCore) recordDecision(dir string, n *plan.Node, dec cacheDecision) {
	rec := cache.Record{
		Step: n.ID, Digest: dec.digest, Key: dec.key, Hit: dec.hit,
		Reason: dec.reason, PreviousDigest: dec.prev, Diffs: dec.diffs,
	}
	if err := cache.WriteRecord(filepath.Join(dir, "cache"), rec); err != nil {
		// A record is a diagnostic. Failing a run because one could not be
		// written would make `cache explain` able to break a build, which is
		// exactly backwards.
		rc.emit(api.Event{
			Type: api.CacheMiss, Step: n.ID,
			Payload: mustMarshal(api.CacheMissBody{
				Key: string(dec.digest), Reason: "record_write_failed", Differing: err.Error(),
			}),
		})
	}
}

// emitMiss records a miss and the component that moved.
func (rc *runCore) emitMiss(n *plan.Node, dec cacheDecision) {
	var differing string
	if len(dec.diffs) > 0 {
		// The FIRST differing component in canonical order, which is what
		// design.md §3.5's example prints. cache explain shows all of them.
		differing = dec.diffs[0].Name
	}
	rc.emit(api.Event{
		Type: api.CacheMiss, Step: n.ID,
		Payload: mustMarshal(api.CacheMissBody{
			Key: string(dec.digest), Reason: dec.reason, Differing: differing,
		}),
	})
}

// serveFromCache reproduces a cached result: the workspaces the step would
// have written, the logs it would have printed, and its exit code.
//
// It reports whether it succeeded. A false return means the entry could not
// be reproduced, the entry has been forgotten, and the caller must run the
// step. That degradation is deliberate and covers every way an entry can be
// incomplete at once: they all have the same right answer, and failing the
// step instead would let a cache sweep break a build.
func (rc *runCore) serveFromCache(
	ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, dec cacheDecision,
) bool {
	rc.emit(api.Event{
		Type: api.CacheHit, Step: n.ID,
		Payload: mustMarshal(api.CacheHitBody{Key: string(dec.digest), FromRun: dec.result.RunID}),
	})

	for _, w := range dec.result.Workspaces {
		if err := rc.ws.restore(ctx, w.Name, w.Digest); err != nil {
			return rc.degradeToMiss(ctx, n, dec, err)
		}
		rc.emit(api.Event{
			Type: api.WSRestored, Step: n.ID,
			Payload: mustMarshal(api.WSRestoredBody{Name: w.Name, Digest: string(w.Digest)}),
		})
	}

	root := rc.ws.inputRoot(n)
	for _, o := range dec.result.Outputs {
		if err := rc.restoreOutput(ctx, opts, root, o); err != nil {
			return rc.degradeToMiss(ctx, n, dec, err)
		}
	}

	for _, l := range dec.result.Logs {
		if err := rc.replayLog(ctx, opts, logs, n, l); err != nil {
			return rc.degradeToMiss(ctx, n, dec, err)
		}
	}
	return true
}

// degradeToMiss turns a broken entry into an ordinary miss.
func (rc *runCore) degradeToMiss(ctx context.Context, n *plan.Node, dec cacheDecision, cause error) bool {
	_ = rc.cache.Forget(ctx, dec.key)
	rc.emit(api.Event{
		Type: api.CacheMiss, Step: n.ID,
		Payload: mustMarshal(api.CacheMissBody{
			Key: string(dec.digest), Reason: cache.ReasonEntryIncomplete, Differing: cause.Error(),
		}),
	})
	return false
}

// restoreOutput puts a declared output file back where the step would have
// written it.
func (rc *runCore) restoreOutput(ctx context.Context, opts Options, root string, o cache.FileDigest) error {
	if err := cache.SafeRelative(o.Path); err != nil {
		return err
	}
	rc2, err := opts.Storage.CAS.Get(ctx, o.Digest)
	if err != nil {
		return err
	}
	defer func() { _ = rc2.Close() }()

	target := filepath.Join(root, filepath.FromSlash(o.Path))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, rc2)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

// replayLog writes a cached step's stored output into this run's own log
// files and emits the byte-range markers for it.
//
// Attempt 1, always: a cached step has exactly one notional attempt, and
// carrying the stored run's attempt number across would produce a log path
// that does not match this run's step.finished.
func (rc *runCore) replayLog(
	ctx context.Context, opts Options, logs *eventlog.LogSet, n *plan.Node, l cache.LogRef,
) error {
	body, err := opts.Storage.CAS.Get(ctx, l.Digest)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	w, err := logs.Writer(n.ID, 1, l.Stream)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	// Through logMarker, exactly as a live step's output goes, so a client
	// range-requesting the replayed log reads the same shape of marker it
	// would for a step that ran.
	m := &logMarker{rc: rc, w: w, step: n.ID, attempt: 1, stream: l.Stream}
	if _, err := io.Copy(m, body); err != nil {
		return err
	}
	return nil
}

// cacheSave stores what a successful pure step produced.
//
// Only on success, and only for a step that actually ran: a cached step has
// nothing new to store, and a failed step must never be saved, or the failure
// is served to every future run with the same key.
func (rc *runCore) cacheSave(
	ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet,
	dec cacheDecision, res attemptResult, attempt int, dur time.Duration,
) error {
	result := &cache.Result{
		ExitCode:    res.exitCode,
		DurationNS:  dur.Nanoseconds(),
		RunID:       rc.runID,
		Hermeticity: cache.HermeticityTrusted,
		SavedAt:     time.Now().UTC(),
	}

	for _, s := range res.snapshots {
		result.Workspaces = append(result.Workspaces,
			cache.WorkspaceDigest{Name: s.Name, Digest: s.Digest})
		result.Bytes += s.Bytes
	}

	root := rc.ws.inputRoot(n)
	outputs, err := cache.Resolve(root, n.Outputs)
	if err != nil {
		return fmt.Errorf("engine: step %q: declared outputs: %w", n.ID, err)
	}
	for _, o := range outputs {
		if _, err := putFile(ctx, opts, filepath.Join(root, filepath.FromSlash(o.Path))); err != nil {
			return fmt.Errorf("engine: step %q: store output %s: %w", n.ID, o.Path, err)
		}
		result.Outputs = append(result.Outputs, o)
	}

	for _, stream := range []string{api.StreamStdout, api.StreamStderr} {
		p := logs.Path(n.ID, attempt, stream)
		d, size, err := putFileIfPresent(ctx, opts, p)
		if err != nil {
			return fmt.Errorf("engine: step %q: store %s log: %w", n.ID, stream, err)
		}
		if d == "" {
			continue
		}
		result.Logs = append(result.Logs, cache.LogRef{Stream: stream, Digest: d, Bytes: size})
		result.Bytes += size
	}

	if err := rc.cache.Save(ctx, n.ID, dec.key, result); err != nil {
		return fmt.Errorf("engine: step %q: cache save: %w", n.ID, err)
	}
	rc.emit(api.Event{
		Type: api.CacheSaved, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(api.CacheSavedBody{Key: string(dec.digest), Bytes: result.Bytes}),
	})
	return nil
}

func putFile(ctx context.Context, opts Options, p string) (cas.Digest, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	return opts.Storage.CAS.Put(ctx, f)
}

// putFileIfPresent stores a file that may legitimately not exist, which is
// the case for a stream a step never wrote to. It returns an empty digest
// for that case rather than an error.
func putFileIfPresent(ctx context.Context, opts Options, p string) (cas.Digest, int64, error) {
	fi, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, nil
		}
		return "", 0, err
	}
	d, err := putFile(ctx, opts, p)
	if err != nil {
		return "", 0, err
	}
	return d, fi.Size(), nil
}
```

- [ ] **Step 4: Add the cache handle to `runCore`.** In `internal/engine/engine.go`, add `cache cache.ActionCache` to `runCore` and set it in the literal from `opts.Storage`:

```go
	rc := &runCore{
		ledger: ledger, sink: opts.Sink, runID: opts.RunID, cancel: cancel,
		oc: newOutcomes(len(p.Nodes)), ws: ws,
	}
	if opts.Storage != nil {
		rc.cache = opts.Storage.Action
	}
```

- [ ] **Step 5: Give `finishStep` a `cached` flag.** In `internal/engine/attempt.go`:

```go
func (rc *runCore) finishStep(
	n *plan.Node, start time.Time, attempt int, state api.State, exitCode int, errMsg string, cached bool,
) api.State {
```

and set `Cached: cached` in the emitted `StepFinishedBody`. Update the one existing call site to pass `false`. `api.StepFinishedBody.Cached`'s own doc already says an engine restoring from cache should emit `State: StateCached` and `Cached: true` together, which is exactly what the hit path below does.

- [ ] **Step 6: Wire the lookup and the save into `runStep`.** At the top of `runStep`, after the `giveBack` defer and before the retry policy is parsed:

```go
	var dec cacheDecision
	if rc.cacheable(n) {
		var err error
		dec, err = rc.cacheLookup(ctx, n, opts)
		if err != nil {
			// A key that cannot be built is a step that cannot be trusted to
			// run correctly: an undeclared or missing input is exactly the
			// condition design.md §3.4 makes the author responsible for. It
			// settles as an ordinary failure, so its handlers still run.
			return rc.finishStep(n, stepStart, 1, api.StateFailed, 0, err.Error(), false)
		}
		if dec.hit {
			rc.recordDecision(opts.Dir, n, dec)
			if rc.serveFromCache(ctx, n, opts, logs, dec) {
				return rc.finishStep(n, stepStart, 1, api.StateCached, dec.result.ExitCode, "", true)
			}
			// The entry could not be reproduced. serveFromCache has already
			// emitted cache.miss and forgotten the entry; fall through and
			// run the step.
			dec.hit = false
		} else {
			rc.emitMiss(n, dec)
			rc.recordDecision(opts.Dir, n, dec)
		}
	}
```

and after the retry loop, once `finalState` is known and before `finishStep` is called:

```go
	if rc.cacheable(n) && finalState == api.StateSucceeded {
		if err := rc.cacheSave(ctx, n, opts, logs, dec, res, attempt, time.Since(stepStart)); err != nil {
			// A step that ran correctly and could not be stored is still a
			// step that ran correctly. The run continues and the next one
			// misses, which is a slower build rather than a broken one.
			rc.emit(api.Event{
				Type: api.CacheMiss, Step: n.ID, Attempt: attempt,
				Payload: mustMarshal(api.CacheMissBody{
					Key: string(dec.digest), Reason: "save_failed", Differing: err.Error(),
				}),
			})
		}
	}
```

Note the state tested is `api.StateSucceeded` and not `api.StateRecovered`: a step that failed and then passed on retry is not evidence that its declared inputs describe it, and saving it would serve the flaky step's one lucky attempt to every future run.

- [ ] **Step 7: Run to verify it passes** with `go test ./internal/engine/... -race`.

- [ ] **Step 8: Prove the degradation.** Temporarily make `serveFromCache` return `true` when a workspace restore fails. Confirm `TestAHitWithMissingContentDegradesToAMiss` fails. Restore, and record it.

- [ ] **Step 9: Prove the save gate.** Temporarily save on `finalState.Failed()` as well. Confirm `TestAFailedPureStepIsNotSaved` fails. Restore, and record it.

- [ ] **Step 10: Run the whole suite, including the goldens.**

```bash
go test ./... -race && (cd api && go test ./... -race)
```

The existing goldens contain no pure steps and no workspaces, so they must be unchanged. If one moved, a cache event is being emitted for an impure step.

- [ ] **Step 11: Run the gates**

```bash
make all
golangci-lint run ./...
```

- [ ] **Step 12: Commit**

```bash
git add internal/engine
git commit -m "feat(engine): consult the action cache, replay a hit, save a miss

A hit restores the workspaces and the declared outputs, replays the stored
logs into this run's own log files with this run's sequence numbers, and
settles the step as cached without ever emitting step.started for a step that
never started. A hit whose content a sweep collected degrades to a miss and
runs the step, because an optimization must never be able to break a build. A
failed or recovered step is never saved."
```

---

### Task 10: The scratch cache

**Files:**
- Create `internal/scratch/scratch.go`, `internal/scratch/template.go`, `internal/scratch/record.go`
- Create `internal/scratch/scratch_test.go`, `internal/scratch/template_test.go`
- Modify `internal/storage/storage.go` (add `Scratch`), `internal/storage/storage_test.go`
- Modify `internal/engine/workspaces.go` (scratch mounts), `internal/engine/engine.go` (expand keys, save at run end)
- Modify `internal/engine/attempt.go` (add scratch mounts to the `SandboxSpec`)
- Test `internal/engine/scratch_test.go`

**Interfaces:**
- Consumes: Task 3's `*workspace.Snapshotter`; Task 1's `cas.Digest`.
- Produces:
  ```go
  package scratch
  type Match struct {
      Key    string
      Digest cas.Digest
      Exact  bool
  }
  type Cache interface {
      Restore(ctx context.Context, key string, restoreKeys []string, dest string) (Match, bool, error)
      Save(ctx context.Context, key string, src string) (bool, error)
  }
  type Dir struct { /* unexported */ }
  func Open(root string, snap *workspace.Snapshotter) (*Dir, error)
  func ExpandKey(template, root string) (string, error)
  type Record struct {
      Name         string `json:"name"`
      Key          string `json:"key"`
      RestoredFrom string `json:"restored_from,omitempty"`
      Restored     bool   `json:"restored"`
      Saved        bool   `json:"saved"`
  }
  func WriteRecords(dir string, recs []Record) error
  func ReadRecords(dir string) ([]Record, error)

  package storage
  // Storage gains: Scratch *scratch.Dir

  package engine
  func (m *wsManager) scratchMounts(ctx context.Context, n *plan.Node) ([]executor.Mount, error)
  func (m *wsManager) saveScratch(ctx context.Context, runDir string, succeeded bool)
  ```

**Wiring.** `storage.Open` constructs it, the engine restores on first mount and saves at run end, and `senro cache explain` in Task 12 prints the records. The test runs a real pipeline twice and asserts the second run restored what the first left.

**Two deliberate deviations from §4.4, both stated rather than assumed.**

`Save` happens once, at run end, gated on the RUN's status rather than on each step's. §4.4 says "only on step success", and a per-step save is right when a cache entry belongs to one step. A scratch cache does not: it is one directory shared by every step in the run that mounts it, so per-step saves would race each other within a single run and store intermediate states under a key that names none of them. Saving once, only when the run succeeded, is the same rule at the granularity the directory actually has.

Scratch outcomes are recorded in the run directory rather than emitted as events. There is no scratch event type in `api`, and inventing one would be a schema change for a best-effort mechanism whose miss is not an error. `senro cache explain` reads the records, so the behaviour is visible without a wire change.

**Class, not instance.** `Save` is `O_EXCL` and a lost race is silent success. §4.4 is explicit that entries are immutable, because mutating one under concurrent runs is how a `node_modules` gets corrupted. The rule covers every concurrent writer, not the one that was noticed.

- [ ] **Step 1: Write the failing test for the key template**

Create `internal/scratch/template_test.go`:

```go
package scratch_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/scratch"
)

func TestExpandKeyHashesFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.sum"), []byte("h1:abc\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := scratch.ExpandKey(`gomod-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	if !strings.HasPrefix(got, "gomod-") || len(got) <= len("gomod-") {
		t.Fatalf("ExpandKey = %q", got)
	}

	again, err := scratch.ExpandKey(`gomod-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey again: %v", err)
	}
	if again != got {
		t.Errorf("the same tree hashed to %q then %q", got, again)
	}
}

func TestExpandKeyChangesWhenAHashedFileChanges(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "go.sum")
	if err := os.WriteFile(p, []byte("one\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before, err := scratch.ExpandKey(`k-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	if err := os.WriteFile(p, []byte("two\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	after, err := scratch.ExpandKey(`k-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey again: %v", err)
	}
	if before == after {
		t.Error("changing a hashed file did not change the key")
	}
}

func TestExpandKeyHashesGlobsInAStableOrder(t *testing.T) {
	root := t.TempDir()
	for _, n := range []string{"b.lock", "a.lock"} {
		if err := os.WriteFile(filepath.Join(root, n), []byte(n+"\n"), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	a, err := scratch.ExpandKey(`k-{{ hashFiles "*.lock" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	b, err := scratch.ExpandKey(`k-{{ hashFiles "*.lock" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey again: %v", err)
	}
	if a != b {
		t.Errorf("a glob hashed to %q then %q, so the walk order reached the key", a, b)
	}
}

// A key that quietly becomes "gomod-" when go.sum is missing would collide
// with every other project's empty key on a shared cache.
func TestExpandKeyRefusesAPatternThatMatchesNothing(t *testing.T) {
	if _, err := scratch.ExpandKey(`k-{{ hashFiles "absent.lock" }}`, t.TempDir()); err == nil {
		t.Error("hashFiles over a pattern matching nothing returned no error")
	}
}

func TestExpandKeyRefusesAnUnknownFunction(t *testing.T) {
	if _, err := scratch.ExpandKey(`k-{{ env "HOME" }}`, t.TempDir()); err == nil {
		t.Error("an unknown template function was accepted")
	}
}

func TestExpandKeyPassesAPlainStringThrough(t *testing.T) {
	got, err := scratch.ExpandKey("plain-key", t.TempDir())
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	if got != "plain-key" {
		t.Errorf("ExpandKey = %q, want the literal", got)
	}
}
```

- [ ] **Step 2: Implement `internal/scratch/template.go`**

```go
package scratch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/xavidop/senro/internal/workspace"
)

// ExpandKey evaluates a scratch cache key template.
//
// One function is available, hashFiles, and its patterns are relative to
// root, which is the pipeline process's working directory. That is where a
// lock file lives: go.sum and pnpm-lock.yaml are repository files, not
// workspace contents, and a key that could only be computed after some step
// populated a workspace would be useless for deciding what to restore
// BEFORE the first step runs.
//
// Deliberately not a general template environment. An env function would put
// whatever the machine happened to export into a shared cache key, and a
// date function would guarantee a miss every midnight. Anything unknown is
// an error rather than an empty string, so a typo cannot silently collapse
// every project's key to the same prefix.
func ExpandKey(tmpl, root string) (string, error) {
	t, err := template.New("scratch-key").
		Option("missingkey=error").
		Funcs(template.FuncMap{
			"hashFiles": func(patterns ...string) (string, error) { return hashFiles(root, patterns) },
		}).
		Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("scratch: key template %q: %w", tmpl, err)
	}
	var b strings.Builder
	if err := t.Execute(&b, nil); err != nil {
		return "", fmt.Errorf("scratch: key template %q: %w", tmpl, err)
	}
	return b.String(), nil
}

// hashFiles digests every file matching any pattern, in sorted path order so
// the walk order cannot reach the key.
//
// A pattern matching nothing is an error. The alternative is a key that
// quietly becomes its own prefix, which on a shared cache root collides with
// every other project whose lock file is also missing.
func hashFiles(root string, patterns []string) (string, error) {
	if len(patterns) == 0 {
		return "", fmt.Errorf("scratch: hashFiles needs at least one pattern")
	}
	var paths []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		relOS, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(relOS)
		if d.IsDir() {
			for _, skip := range workspace.DefaultExcludes {
				if workspace.MatchGlob(strings.TrimSuffix(skip, "/"), rel) {
					return fs.SkipDir
				}
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		for _, pat := range patterns {
			if workspace.MatchGlob(pat, rel) {
				paths = append(paths, rel)
				break
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("scratch: hashFiles under %s: %w", root, err)
	}
	if len(paths) == 0 {
		return "", fmt.Errorf(
			"scratch: hashFiles matched no files under %s for %v; a key that silently drops its hash "+
				"collides with every other project's", root, patterns)
	}
	sort.Strings(paths)

	h := sha256.New()
	for _, rel := range paths {
		// The path goes into the hash as well as the content, so moving a
		// file changes the key even when the bytes do not.
		h.Write([]byte(rel))
		h.Write([]byte{0})
		f, err := os.Open(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return "", fmt.Errorf("scratch: hashFiles: %w", err)
		}
		_, copyErr := io.Copy(h, f)
		_ = f.Close()
		if copyErr != nil {
			return "", fmt.Errorf("scratch: hashFiles: %w", copyErr)
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:32], nil
}
```

- [ ] **Step 3: Write the failing test for the store**

Create `internal/scratch/scratch_test.go`:

```go
package scratch_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/workspace"
)

func openScratch(t *testing.T) *scratch.Dir {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	d, err := scratch.Open(filepath.Join(t.TempDir(), "scratch"), workspace.NewSnapshotter(store))
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}
	return d
}

func withFile(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return dir
}

func TestSaveThenRestoreByExactKey(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	src := withFile(t, "mod.txt", "downloaded\n")

	saved, err := d.Save(ctx, "gomod-abc", src)
	if err != nil || !saved {
		t.Fatalf("Save = %v, %v", saved, err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, "gomod-abc", nil, dest)
	if err != nil || !ok {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	if !m.Exact || m.Key != "gomod-abc" {
		t.Errorf("Match = %+v, want an exact hit on the key asked for", m)
	}
	b, err := os.ReadFile(filepath.Join(dest, "mod.txt"))
	if err != nil {
		t.Fatalf("restored content: %v", err)
	}
	if string(b) != "downloaded\n" {
		t.Errorf("restored %q", b)
	}
}

// design.md §4.4: exact key, then each restore key as a prefix match, newest
// first. Miss is not an error.
func TestRestoreFallsBackToARestoreKeyPrefixNewestFirst(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()

	if _, err := d.Save(ctx, "gomod-old", withFile(t, "which.txt", "old\n")); err != nil {
		t.Fatalf("Save old: %v", err)
	}
	if _, err := d.Save(ctx, "gomod-new", withFile(t, "which.txt", "new\n")); err != nil {
		t.Fatalf("Save new: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, "gomod-absent", []string{"gomod-"}, dest)
	if err != nil || !ok {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	if m.Exact {
		t.Error("a prefix fallback reported itself as an exact match")
	}
	b, err := os.ReadFile(filepath.Join(dest, "which.txt"))
	if err != nil {
		t.Fatalf("restored content: %v", err)
	}
	if string(b) != "new\n" {
		t.Errorf("the fallback restored %q, want the newest entry under the prefix", b)
	}
}

func TestRestoreTriesRestoreKeysInDeclaredOrder(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "second-1", withFile(t, "w.txt", "second\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := d.Save(ctx, "first-1", withFile(t, "w.txt", "first\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, ok, err := d.Restore(ctx, "nope", []string{"first-", "second-"}, dest); err != nil || !ok {
		t.Fatalf("Restore = %v, %v", ok, err)
	}
	b, _ := os.ReadFile(filepath.Join(dest, "w.txt"))
	if string(b) != "first\n" {
		t.Errorf("restored %q, want the first declared restore key to win", b)
	}
}

// The negative half, and the one that defines the whole mechanism: a miss is
// not an error. A pipeline whose module cache is cold must still run.
func TestARestoreMissIsNotAnError(t *testing.T) {
	d := openScratch(t)
	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(context.Background(), "cold", []string{"warm-"}, dest)
	if err != nil {
		t.Fatalf("a scratch miss returned an error: %v", err)
	}
	if ok {
		t.Errorf("a cold cache reported a hit: %+v", m)
	}
}

// design.md §4.4: entries are immutable. Mutating one under concurrent runs
// is how a node_modules gets corrupted, so a second Save under an existing
// key loses silently.
func TestSaveUnderAnExistingKeyLosesSilently(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "k", withFile(t, "v.txt", "first\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	saved, err := d.Save(ctx, "k", withFile(t, "v.txt", "second\n"))
	if err != nil {
		t.Fatalf("the losing Save returned an error: %v", err)
	}
	if saved {
		t.Error("the second Save reported that it stored an entry")
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, _, err := d.Restore(ctx, "k", nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	b, _ := os.ReadFile(filepath.Join(dest, "v.txt"))
	if string(b) != "first\n" {
		t.Errorf("the entry was overwritten: %q", b)
	}
}

// Keys come from a user template and land in filenames.
func TestKeysWithPathCharactersAreStoredSafely(t *testing.T) {
	d := openScratch(t)
	ctx := context.Background()
	const key = "deps/../../escape linux+amd64"
	if _, err := d.Save(ctx, key, withFile(t, "v.txt", "x\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, ok, err := d.Restore(ctx, key, nil, dest); err != nil || !ok {
		t.Errorf("Restore of an awkward key = %v, %v", ok, err)
	}
}

func TestRestoreOfAnEntryWhoseContentIsGoneIsAMissNotAFailure(t *testing.T) {
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	root := filepath.Join(t.TempDir(), "scratch")
	d, err := scratch.Open(root, workspace.NewSnapshotter(store))
	if err != nil {
		t.Fatalf("scratch.Open: %v", err)
	}
	ctx := context.Background()
	if _, err := d.Save(ctx, "k", withFile(t, "v.txt", "x\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(store.Root(), "sha256")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, ok, err := d.Restore(ctx, "k", nil, filepath.Join(t.TempDir(), "dest")); err != nil {
		t.Errorf("a swept scratch entry returned an error: %v", err)
	} else if ok {
		t.Error("a swept scratch entry reported a hit")
	}
}
```

- [ ] **Step 4: Implement `internal/scratch/scratch.go`**

```go
// Package scratch is senro's best-effort cache: a mutable directory such as
// a module cache, restored by key with prefix fallbacks.
//
// Deliberately not the action cache (internal/cache), and design.md §3.1
// calls conflating the two the most common design error in this area. A
// wrong hit there is a wrong build; a stale hit here only costs time. The
// consequences run all the way down: a miss here is not an error, entries
// are immutable so a concurrent save loses silently, and a scratch cache is
// NEVER an input to an action cache key (design.md §4.4). If a scratch cache
// can change a step's output, the step is not pure and should not say it is.
package scratch

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// Match is which entry a restore actually used.
type Match struct {
	Key    string
	Digest cas.Digest
	Exact  bool
}

// Cache is the best-effort cache interface.
type Cache interface {
	Restore(ctx context.Context, key string, restoreKeys []string, dest string) (Match, bool, error)
	Save(ctx context.Context, key string, src string) (bool, error)
}

// Dir is the local-directory scratch cache. Each entry is one small file
// named for its key and holding a workspace digest, so the content itself
// lives in the CAS and is shared with everything else stored there.
type Dir struct {
	root string
	snap *workspace.Snapshotter
}

var _ Cache = (*Dir)(nil)

// Open prepares root.
func Open(root string, snap *workspace.Snapshotter) (*Dir, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("scratch: open %s: %w", root, err)
	}
	return &Dir{root: root, snap: snap}, nil
}

// entryPath is where a key is stored. Keys come from a user-authored
// template and routinely contain "/" and spaces, so they are percent-encoded
// into one path segment rather than trusted as filenames.
func (d *Dir) entryPath(key string) string {
	return filepath.Join(d.root, url.PathEscape(key))
}

// Restore materializes the newest entry matching key, or matching one of
// restoreKeys as a prefix, into dest.
//
// A miss returns (Match{}, false, nil). Not an error: a cold module cache is
// the ordinary state of a fresh machine, and a pipeline that fails because a
// best-effort cache was empty is worse than no cache at all. An entry whose
// content a sweep collected is treated the same way, for the same reason.
func (d *Dir) Restore(ctx context.Context, key string, restoreKeys []string, dest string) (Match, bool, error) {
	if dg, ok, err := d.read(key); err != nil {
		return Match{}, false, err
	} else if ok {
		m := Match{Key: key, Digest: dg, Exact: true}
		return d.materialize(ctx, m, dest)
	}
	for _, prefix := range restoreKeys {
		m, ok, err := d.newestUnder(prefix)
		if err != nil {
			return Match{}, false, err
		}
		if !ok {
			continue
		}
		return d.materialize(ctx, m, dest)
	}
	return Match{}, false, nil
}

func (d *Dir) materialize(ctx context.Context, m Match, dest string) (Match, bool, error) {
	if err := d.snap.Restore(ctx, m.Digest, dest); err != nil {
		if errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrCorrupt) {
			// The entry outlived its content. Best-effort means this is a
			// miss, and the step repopulates the directory itself.
			return Match{}, false, nil
		}
		return Match{}, false, fmt.Errorf("scratch: restore %q: %w", m.Key, err)
	}
	return m, true, nil
}

// newestUnder finds the most recently saved entry whose key starts with
// prefix. Newest by the entry file's mtime, which is written once and never
// touched again, so it is the entry's creation time.
func (d *Dir) newestUnder(prefix string) (Match, bool, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Match{}, false, nil
		}
		return Match{}, false, fmt.Errorf("scratch: %w", err)
	}
	type candidate struct {
		key  string
		when time.Time
	}
	var found []candidate
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		key, err := url.PathUnescape(e.Name())
		if err != nil || !strings.HasPrefix(key, prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		found = append(found, candidate{key: key, when: info.ModTime()})
	}
	if len(found) == 0 {
		return Match{}, false, nil
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].when.Equal(found[j].when) {
			// A deterministic tiebreak, so two entries written in the same
			// filesystem timestamp tick do not restore differently run to run.
			return found[i].key > found[j].key
		}
		return found[i].when.After(found[j].when)
	})
	dg, ok, err := d.read(found[0].key)
	if err != nil || !ok {
		return Match{}, false, err
	}
	return Match{Key: found[0].key, Digest: dg}, true, nil
}

func (d *Dir) read(key string) (cas.Digest, bool, error) {
	b, err := os.ReadFile(d.entryPath(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("scratch: %w", err)
	}
	dg := cas.Digest(strings.TrimSpace(string(b)))
	if !dg.Valid() {
		return "", false, nil
	}
	return dg, true, nil
}

// Save snapshots src and stores it under key, unless key already exists.
//
// Reports whether it stored anything. Losing the race is silent success:
// design.md §4.4 makes entries immutable because mutating one under
// concurrent runs is how a node_modules gets corrupted, and a run that
// discovers another run got there first has nothing to do about it.
func (d *Dir) Save(ctx context.Context, key, src string) (bool, error) {
	p := d.entryPath(key)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	// The placeholder exists from here on, which is what claims the key. It
	// is removed on every failure path so a crash mid-save does not leave a
	// key that can never be filled and never be retried.
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(p)
		}
	}()

	snap, err := d.snap.Snapshot(ctx, src, workspace.NewExcluder())
	if err != nil {
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	if _, err := f.Write([]byte(snap.Digest)); err != nil {
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	if err := f.Sync(); err != nil {
		return false, fmt.Errorf("scratch: save %q: %w", key, err)
	}
	ok = true
	return true, nil
}
```

Note the exclusion set: a scratch cache is snapshotted with an EMPTY excluder, not with `DefaultExcludes`. A `node_modules` scratch cache would otherwise store nothing, since `node_modules` is exactly what it holds. This asymmetry with a workspace is deliberate and is why the two go through different call sites.

- [ ] **Step 5: Implement `internal/scratch/record.go`**

```go
package scratch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Record is what one scratch cache did during one run, written to
// <run>/cache/scratch.json.
//
// A record rather than an event: there is no scratch event type in the api
// module, and adding one would be a schema change for a mechanism whose miss
// is not an error and whose outcome nothing downstream branches on. `senro
// cache explain` reads these, so the behaviour is still visible without a
// wire change.
type Record struct {
	Name         string `json:"name"`
	Key          string `json:"key"`
	RestoredFrom string `json:"restored_from,omitempty"`
	Restored     bool   `json:"restored"`
	Saved        bool   `json:"saved"`
}

const recordFile = "scratch.json"

// WriteRecords stores recs under dir, the run's cache directory.
func WriteRecords(dir string, recs []Record) error {
	sort.Slice(recs, func(i, j int) bool { return recs[i].Name < recs[j].Name })
	b, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return fmt.Errorf("scratch: marshal records: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("scratch: write records: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, recordFile), b, 0o644); err != nil {
		return fmt.Errorf("scratch: write records: %w", err)
	}
	return nil
}

// ReadRecords loads a run's scratch records. A run that mounted none has
// none, which is not an error.
func ReadRecords(dir string) ([]Record, error) {
	b, err := os.ReadFile(filepath.Join(dir, recordFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("scratch: read records: %w", err)
	}
	var recs []Record
	if err := json.Unmarshal(b, &recs); err != nil {
		return nil, fmt.Errorf("scratch: read records: %w", err)
	}
	return recs, nil
}
```

- [ ] **Step 6: Wire the scratch cache into `internal/storage`.** Add `Scratch *scratch.Dir` with a doc naming it best-effort, construct it with `scratch.Open(filepath.Join(root, "scratch"), snapshotter)`, and extend `TestOpenCreatesTheStoreLayout` to assert it is non-nil.

- [ ] **Step 7: Write the failing engine test**

Create `internal/engine/scratch_test.go`:

```go
package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

func TestAScratchCacheSurvivesBetweenRuns(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")

	build := func() *senro.Plan {
		c := senro.ScratchCache("deps", senro.Key("deps-v1"), senro.RestoreKeys("deps-"))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("install", exec.Command("sh", "-c",
			"if [ -f m/marker ]; then echo warm; else echo cold; mkdir -p m; touch m/marker; fi")).
			Mount(c.At("/m"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	run := func(runID string) (string, []api.Event) {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return runDir, rec.Events()
	}

	firstDir, _ := run("r1")
	b, err := os.ReadFile(filepath.Join(firstDir, "logs", "install", "1", "stdout"))
	if err != nil {
		t.Fatalf("read first log: %v", err)
	}
	if strings.TrimSpace(string(b)) != "cold" {
		t.Fatalf("the first run saw %q, want a cold cache", b)
	}

	secondDir, _ := run("r2")
	b, err = os.ReadFile(filepath.Join(secondDir, "logs", "install", "1", "stdout"))
	if err != nil {
		t.Fatalf("read second log: %v", err)
	}
	if strings.TrimSpace(string(b)) != "warm" {
		t.Errorf("the second run saw %q, want the scratch cache restored", b)
	}

	recs, err := scratch.ReadRecords(filepath.Join(secondDir, "cache"))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || !recs[0].Restored || recs[0].Name != "deps" {
		t.Errorf("scratch records = %+v, want one restored entry named deps", recs)
	}
}

// A scratch cache is never an input to an action cache key (design.md §4.4).
// If it were, a warm cache and a cold one would key differently and a pure
// step would never hit twice on different machines.
func TestAScratchCacheDoesNotEnterAnActionCacheKey(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	build := func() *senro.Plan {
		c := senro.ScratchCache("deps", senro.Key("deps-v1"))
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "date > /dev/null; touch /m/blob 2>/dev/null || true")).
			Needs("seed").WorkDir("/src").
			Mount(ws.At("/src", senro.RW), c.At("/m")).
			Pure().Inputs(senroGlob())
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}
	run := func(runID string) []api.Event {
		store, err := storage.Open(cacheRoot)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run: %v", err)
		}
		return rec.Events()
	}

	_ = run("r1")
	second := run("r2")
	if countType(second, api.CacheHit) != 1 {
		t.Errorf("the second run did not hit, so the scratch cache's contents reached the key: %d hits",
			countType(second, api.CacheHit))
	}
}

// Nothing is saved from a failed run: a half-populated module cache stored
// under a key that names a complete one is worse than no entry.
func TestAFailedRunDoesNotSaveItsScratchCache(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	c := senro.ScratchCache("deps", senro.Key("deps-v1"))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("install", exec.Command("sh", "-c", "touch /m/partial 2>/dev/null; exit 4")).Mount(c.At("/m"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	runDir := filepath.Join(t.TempDir(), "run")
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
		Sink: rec, Storage: store, RunID: "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	_ = store.Close()

	recs, err := scratch.ReadRecords(filepath.Join(runDir, "cache"))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].Saved {
		t.Errorf("scratch records = %+v, want one entry that was NOT saved", recs)
	}
}
```

Add the small helper the second test uses, next to it in the same file:

```go
// senroGlob keeps the import of package artifact in one place in this file.
func senroGlob() artifact.Selector { return artifact.Glob("**/*.go") }
```

- [ ] **Step 8: Implement the engine side.** In `internal/engine/workspaces.go`, add scratch state to `wsManager` and two methods:

```go
	scratchSpecs map[string]plan.ScratchSpec
	scratchKeys  map[string]string   // resolved once per run
	scratchDone  map[string]bool     // restored already
	scratchRecs  map[string]scratch.Record
	scratchStore scratch.Cache
```

```go
// scratchMounts realizes a node's scratch caches, restoring each one the
// first time any step in the run mounts it.
//
// Once per run, not once per step: a scratch cache is one directory shared
// by every step that mounts it, exactly like a ScopeRun workspace, and
// restoring it again mid-run would throw away what an earlier step put
// there.
func (m *wsManager) scratchMounts(ctx context.Context, n *plan.Node) ([]executor.Mount, error) {
	var out []executor.Mount
	for _, ms := range n.Mounts {
		if ms.Scratch == "" {
			continue
		}
		spec, ok := m.scratchSpecs[ms.Scratch]
		if !ok {
			return nil, fmt.Errorf("engine: step %q mounts unknown scratch cache %q", n.ID, ms.Scratch)
		}
		dir := filepath.Join(m.dir, "..", "scratch", ms.Scratch)
		if err := m.ensureScratch(ctx, spec, dir); err != nil {
			return nil, err
		}
		out = append(out, executor.Mount{Name: ms.Scratch, Path: dir, At: ms.At})
	}
	return out, nil
}

// ensureScratch restores a scratch cache once, and never fails a step
// because of it: a miss, a swept entry, or a restore that could not complete
// all leave an empty directory the step repopulates itself. That is what
// "best effort" means (design.md §4.4).
func (m *wsManager) ensureScratch(ctx context.Context, spec plan.ScratchSpec, dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scratchDone[spec.Name] {
		return nil
	}
	m.scratchDone[spec.Name] = true

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("engine: create scratch cache %q: %w", spec.Name, err)
	}
	key := m.scratchKeys[spec.Name]
	rec := scratch.Record{Name: spec.Name, Key: key}
	match, ok, err := m.scratchStore.Restore(ctx, key, spec.RestoreKeys, dir)
	if err == nil && ok {
		rec.Restored = true
		rec.RestoredFrom = match.Key
	}
	// A restore error is recorded and swallowed. The alternative is a
	// pipeline that cannot run because a module cache was unreadable.
	m.scratchRecs[spec.Name] = rec
	return nil
}

// saveScratch stores every scratch cache the run touched, once, at run end.
//
// Gated on the RUN's outcome rather than on each step's. A scratch directory
// is shared by every step that mounts it, so per-step saves would race each
// other inside one run and store intermediate states under a key that names
// none of them. Gating on the run is design.md §4.4's "only on success" at
// the granularity the directory actually has.
func (m *wsManager) saveScratch(ctx context.Context, runDir string, succeeded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	recs := make([]scratch.Record, 0, len(m.scratchRecs))
	for name, rec := range m.scratchRecs {
		if succeeded && !rec.Restored {
			// Nothing to store when the exact key was already there: entries
			// are immutable, so a save would lose the race with itself.
			dir := filepath.Join(runDir, "scratch", name)
			if saved, err := m.scratchStore.Save(ctx, rec.Key, dir); err == nil {
				rec.Saved = saved
			}
		}
		recs = append(recs, rec)
	}
	if err := scratch.WriteRecords(filepath.Join(runDir, "cache"), recs); err != nil {
		// A record is a diagnostic; losing one must not change a run's
		// outcome.
		_ = err
	}
}
```

Adjust `newWSManager` to take the plan's `Scratch` specs and a `scratch.Cache`, expand each key once with `scratch.ExpandKey(spec.Key, cwd)` where `cwd` is the process's working directory, and store the results. A key that will not expand is a run-level error: it names files the pipeline said were there, and guessing at a substitute key would poison a shared cache.

- [ ] **Step 9: Add scratch mounts to the `SandboxSpec`.** In `runAttempt`, after the workspace mounts are built:

```go
	var scratchMounts []executor.Mount
	if rc.ws != nil {
		scratchMounts, err = rc.ws.scratchMounts(attemptCtx, n)
		if err != nil {
			return attemptResult{state: api.StateFailed, err: err}
		}
	}
```

and pass `append(append([]executor.Mount(nil), mounts...), scratchMounts...)` as `SandboxSpec.Mounts`, while continuing to pass only `mounts` to `snapshotMounts`. A scratch cache must not produce a `ws.snapshot`: it is not a workspace, its digest is not part of anything's identity, and emitting one would put it in front of every reader as though it were.

- [ ] **Step 10: Save at run end.** In `engine.Run`, after `status` is computed and before `rc.emitFinal`:

```go
	if ws != nil {
		ws.saveScratch(context.WithoutCancel(ctx), opts.Dir,
			status == api.RunSucceeded || status == api.RunSucceededWithRecovery)
	}
```

`context.WithoutCancel`, because saving is cleanup and a cancelled run still wants the module cache it warmed. On the engine-failure path, where no status is computed, `saveScratch` is called with `succeeded` false so the records are still written.

- [ ] **Step 11: Run to verify it passes** with `go test ./internal/scratch/... ./internal/engine/... -race`.

- [ ] **Step 12: Prove the immutability rule.** Temporarily replace the `O_EXCL` in `Save` with a plain create. Confirm `TestSaveUnderAnExistingKeyLosesSilently` fails. Restore, and record it.

- [ ] **Step 13: Run the gates**

```bash
go test ./... -race
make all
golangci-lint run ./...
```

- [ ] **Step 14: Commit**

```bash
git add internal/scratch internal/storage internal/engine
git commit -m "feat(scratch): the best-effort cache, restored by key with prefixes

Restored once per run on first mount, saved once at run end and only when the
run succeeded, because the directory is shared by every step that mounts it
and per-step saves would race inside one run. Entries are immutable and a
lost race is silent success, which is what stops a concurrent run corrupting
a node_modules. Never an input to an action cache key, and there is a test
that a warm cache and a cold one produce the same key."
```

---

### Task 11: `senro cache gc`, pins and retention

**Files:**
- Create `internal/cache/gc.go`, `internal/cache/pin.go`, `internal/cache/gc_test.go`
- Modify `internal/engine/engine.go` (write a pin at run end)
- Create `cmd/senro/cmd_cache.go` (the `gc` subcommand), `cmd/senro/cmd_cache_test.go`
- Modify `cmd/senro/main.go` (dispatch), `cmd/senro/main_test.go`

**Interfaces:**
- Consumes: Task 1's `cas.Dir`, `cas.Object`, `cas.Digest`; Task 8's `Dir`, `Entry`, `Walk`; Task 6's `wsSnapshot`.
- Produces:
  ```go
  package cache
  type Pin struct {
      RunID    string       `json:"run_id"`
      Status   string       `json:"status"`
      Finished time.Time    `json:"finished_at"`
      Digests  []cas.Digest `json:"digests"`
  }
  func WritePin(dir string, p Pin) error
  func ReadPins(dir string) ([]Pin, error)
  type GCOptions struct {
      CAS        *cas.Dir
      Action     *Dir
      PinsDir    string
      MaxSize    int64
      KeepFailed time.Duration
      Now        time.Time
      DryRun     bool
  }
  type GCStats struct {
      ObjectsScanned int
      ObjectsDeleted int
      EntriesScanned int
      EntriesEvicted int
      PinsExpired    int
      PinnedObjects  int
      BytesBefore    int64
      BytesFreed     int64
  }
  func GC(ctx context.Context, opts GCOptions) (GCStats, error)

  package main
  func cmdCache(args []string, stdout, stderr io.Writer) int
  func parseSize(s string) (int64, error)
  ```

**Wiring.** The engine writes a pin for every run that did not succeed, `senro cache gc` is dispatched from `main.go`, and the CLI test drives `cmdCache` over a cache root a real run produced.

**Why pins exist.** design.md §3.6 sets LRU against a size budget. The v0 spec §4.3 adds the correction that matters: workspaces snapshot on failure as well as success, so an LRU sweep run at the wrong moment deletes exactly the snapshot somebody is debugging. A pin is the run's own list of the digests it produced, written when the run ends badly, and the sweep skips them until `--keep-failed` has elapsed.

**Class, not instance.** The live set is computed from every reference an entry holds: workspaces, outputs, and log refs alike. Enumerating them in one place means a field added to `Result` later has one function to update rather than three call sites to find, and the failure it prevents is deleting the logs of an entry that stays a valid hit.

- [ ] **Step 1: Write the failing test**

Create `internal/cache/gc_test.go`:

```go
package cache_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

type gcFixture struct {
	store   *cas.Dir
	action  *cache.Dir
	pinsDir string
}

func newGCFixture(t *testing.T) gcFixture {
	t.Helper()
	root := t.TempDir()
	store, err := cas.Open(filepath.Join(root, "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	action, err := cache.Open(filepath.Join(root, "action"))
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	return gcFixture{store: store, action: action, pinsDir: filepath.Join(root, "pins")}
}

func (f gcFixture) put(t *testing.T, body string) cas.Digest {
	t.Helper()
	d, err := f.store.Put(context.Background(), strings.NewReader(body))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return d
}

func (f gcFixture) saveEntry(t *testing.T, step string, k cache.Key, digests ...cas.Digest) {
	t.Helper()
	r := &cache.Result{RunID: "r1", Hermeticity: cache.HermeticityTrusted, SavedAt: time.Now().UTC()}
	for i, d := range digests {
		if i == 0 {
			r.Workspaces = append(r.Workspaces, cache.WorkspaceDigest{Name: "ws", Digest: d})
			continue
		}
		r.Logs = append(r.Logs, cache.LogRef{Stream: "stdout", Digest: d})
	}
	if err := f.action.Save(context.Background(), step, k, r); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func (f gcFixture) has(t *testing.T, d cas.Digest) bool {
	t.Helper()
	ok, err := f.store.Has(context.Background(), d)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	return ok
}

func TestGCDeletesUnreferencedObjectsAndKeepsReferencedOnes(t *testing.T) {
	f := newGCFixture(t)
	live := f.put(t, "referenced by a live entry")
	logRef := f.put(t, "the log of that entry")
	orphan := f.put(t, "referenced by nothing")
	f.saveEntry(t, "build", sampleKey(), live, logRef)

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if !f.has(t, live) {
		t.Error("GC deleted an object a live entry references")
	}
	if !f.has(t, logRef) {
		t.Error("GC deleted a log an entry references, so a future hit would replay nothing")
	}
	if f.has(t, orphan) {
		t.Error("GC kept an object nothing references")
	}
	if stats.ObjectsDeleted != 1 || stats.BytesFreed <= 0 {
		t.Errorf("stats = %+v, want one object deleted and a non-zero size", stats)
	}
}

// The size budget. The oldest-accessed entry goes first, which is what LRU
// means, and everything it alone referenced goes with it.
func TestGCEvictsTheLeastRecentlyUsedEntryUnderASizeBudget(t *testing.T) {
	f := newGCFixture(t)
	oldBody := f.put(t, strings.Repeat("old", 4000))
	newBody := f.put(t, strings.Repeat("new", 4000))

	oldKey := sampleKey()
	newKey := oldKey
	newKey.Platform = "darwin/arm64"
	f.saveEntry(t, "old-step", oldKey, oldBody)
	f.saveEntry(t, "new-step", newKey, newBody)

	// Age the older entry so the LRU order is unambiguous rather than
	// dependent on filesystem timestamp resolution.
	ageEntry(t, f.action, oldKey, time.Now().Add(-48*time.Hour))

	var total int64
	if err := f.store.Walk(func(o cas.Object) error { total += o.Bytes; return nil }); err != nil {
		t.Fatalf("Walk: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir,
		MaxSize: total / 2, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if f.has(t, oldBody) {
		t.Error("GC kept the least recently used entry's content under a budget that could not hold both")
	}
	if !f.has(t, newBody) {
		t.Error("GC evicted the most recently used entry")
	}
	if stats.EntriesEvicted != 1 {
		t.Errorf("stats = %+v, want one entry evicted", stats)
	}
	if _, ok, _ := f.action.Lookup(context.Background(), "old-step", oldKey); ok {
		t.Error("the evicted entry still reports a hit, so a future run would restore content that is gone")
	}
}

// The correction from the v0 spec §4.3: an LRU sweep must not delete the
// snapshot somebody is debugging.
func TestGCKeepsAPinnedFailedRunsWorkspaceEvenWhenOverBudget(t *testing.T) {
	f := newGCFixture(t)
	debugging := f.put(t, strings.Repeat("the failed run's workspace", 500))

	if err := cache.WritePin(f.pinsDir, cache.Pin{
		RunID: "r-failed", Status: "failed", Finished: time.Now(), Digests: []cas.Digest{debugging},
	}); err != nil {
		t.Fatalf("WritePin: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir,
		MaxSize: 1, KeepFailed: 7 * 24 * time.Hour, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if !f.has(t, debugging) {
		t.Fatal("GC deleted the workspace of a failed run inside its retention window, which is exactly the snapshot someone is looking at")
	}
	if stats.PinnedObjects != 1 {
		t.Errorf("stats = %+v, want one pinned object", stats)
	}
}

// The negative half. Retention has to end, or a cache root grows forever on
// a machine with a flaky test suite.
func TestGCCollectsAPinnedWorkspaceOnceRetentionHasElapsed(t *testing.T) {
	f := newGCFixture(t)
	stale := f.put(t, "an old failed run's workspace")

	if err := cache.WritePin(f.pinsDir, cache.Pin{
		RunID: "r-old", Status: "failed",
		Finished: time.Now().Add(-30 * 24 * time.Hour), Digests: []cas.Digest{stale},
	}); err != nil {
		t.Fatalf("WritePin: %v", err)
	}

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir,
		KeepFailed: 7 * 24 * time.Hour, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if f.has(t, stale) {
		t.Error("GC kept a pinned workspace past its retention window")
	}
	if stats.PinsExpired != 1 {
		t.Errorf("stats = %+v, want one expired pin", stats)
	}
	pins, err := cache.ReadPins(f.pinsDir)
	if err != nil {
		t.Fatalf("ReadPins: %v", err)
	}
	if len(pins) != 0 {
		t.Errorf("an expired pin file survived: %+v", pins)
	}
}

func TestGCDryRunDeletesNothingAndReportsTheSameNumbers(t *testing.T) {
	f := newGCFixture(t)
	orphan := f.put(t, "unreferenced")

	dry, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(), DryRun: true,
	})
	if err != nil {
		t.Fatalf("GC dry run: %v", err)
	}
	if !f.has(t, orphan) {
		t.Fatal("a dry run deleted an object")
	}
	wet, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if dry.ObjectsDeleted != wet.ObjectsDeleted || dry.BytesFreed != wet.BytesFreed {
		t.Errorf("dry run reported %+v, the real sweep did %+v", dry, wet)
	}
	if f.has(t, orphan) {
		t.Error("the real sweep kept the object the dry run promised to delete")
	}
}

func TestGCOnAnEmptyStoreIsNotAnError(t *testing.T) {
	f := newGCFixture(t)
	if _, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: f.store, Action: f.action, PinsDir: f.pinsDir, Now: time.Now(),
	}); err != nil {
		t.Errorf("GC over an empty store: %v", err)
	}
}

// ageEntry backdates an entry file so LRU order does not depend on
// filesystem timestamp resolution.
func ageEntry(t *testing.T, d *cache.Dir, k cache.Key, when time.Time) {
	t.Helper()
	if err := osChtimes(d.EntryPath(k.Digest()), when); err != nil {
		t.Fatalf("age entry: %v", err)
	}
}
```

Add `osChtimes` next to it as a one-line wrapper over `os.Chtimes(p, when, when)`, so the test file's intent reads at the call site.

- [ ] **Step 2: Run to verify it fails** with `go test ./internal/cache/ -run GC`. Expect `undefined: cache.GC`.

- [ ] **Step 3: Implement `internal/cache/pin.go`**

```go
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/stepid"
)

// Pin protects the content a run produced from a size-budget sweep.
//
// Workspaces snapshot on failure as well as on success, because failure is
// when a workspace matters most (design.md §7.6). That puts the two features
// in direct conflict: an LRU sweep against a size budget will happily delete
// the snapshot of the failed run somebody is in the middle of debugging,
// since nothing references it. A pin is that run's own list of what it
// produced, and the sweep skips it until the retention window closes.
type Pin struct {
	RunID    string       `json:"run_id"`
	Status   string       `json:"status"`
	Finished time.Time    `json:"finished_at"`
	Digests  []cas.Digest `json:"digests"`
}

// WritePin stores p under dir, one file per run.
func WritePin(dir string, p Pin) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: marshal pin for run %q: %w", p.RunID, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cache: write pin for run %q: %w", p.RunID, err)
	}
	// Run IDs are opaque strings from a caller, so they are encoded rather
	// than used as filenames.
	if err := writeAtomic(filepath.Join(dir, stepid.Encode(p.RunID)+".json"), b); err != nil {
		return fmt.Errorf("cache: write pin for run %q: %w", p.RunID, err)
	}
	return nil
}

// ReadPins loads every pin. An unreadable pin file is skipped rather than
// failing the sweep: a sweep that cannot run because of one bad file is a
// disk that fills up.
func ReadPins(dir string) ([]Pin, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache: read pins: %w", err)
	}
	var out []Pin
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var p Pin
		if err := json.Unmarshal(b, &p); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// removePin deletes an expired pin file.
func removePin(dir, runID string) error {
	err := os.Remove(filepath.Join(dir, stepid.Encode(runID)+".json"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache: remove pin for run %q: %w", runID, err)
	}
	return nil
}
```

- [ ] **Step 4: Implement `internal/cache/gc.go`**

```go
package cache

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/xavidop/senro/internal/cas"
)

// GCOptions configures one sweep.
type GCOptions struct {
	CAS    *cas.Dir
	Action *Dir
	// PinsDir holds the per-run pin files; see Pin.
	PinsDir string
	// MaxSize is the disk budget in bytes. Zero means no budget: only
	// unreferenced objects and expired pins are collected, and no entry is
	// evicted. That is the safe default for a sweep somebody runs by hand
	// without deciding on a number first.
	MaxSize int64
	// KeepFailed is how long a failed run's content is protected. Zero means
	// no protection at all, which is deliberately not the CLI's default.
	KeepFailed time.Duration
	Now        time.Time
	DryRun     bool
}

// GCStats is what a sweep did, or would have done under DryRun.
type GCStats struct {
	ObjectsScanned int
	ObjectsDeleted int
	EntriesScanned int
	EntriesEvicted int
	PinsExpired    int
	PinnedObjects  int
	BytesBefore    int64
	BytesFreed     int64
}

// GC reclaims disk space.
//
// The order is the whole algorithm and each step depends on the one before:
//
//  1. Read the pins. A pin inside its retention window contributes its
//     digests to the protected set; an expired one is deleted.
//  2. Measure every object.
//  3. Walk the entries newest-accessed first, keeping each while the
//     protected set plus everything kept so far fits the budget, and
//     evicting the rest. This is the LRU decision, and it is made over
//     ENTRIES rather than over objects, because an entry whose logs were
//     collected is a broken hit and deleting half of one saves nothing.
//  4. Delete every object no surviving entry and no live pin references.
//
// A zero MaxSize skips step 3 entirely, so nothing that is still a valid hit
// is ever evicted by a sweep the operator did not give a number to.
func GC(ctx context.Context, opts GCOptions) (GCStats, error) {
	if err := ctx.Err(); err != nil {
		return GCStats{}, err
	}
	var stats GCStats

	protected := make(map[cas.Digest]bool)
	pins, err := ReadPins(opts.PinsDir)
	if err != nil {
		return stats, err
	}
	for _, p := range pins {
		if opts.Now.Sub(p.Finished) < opts.KeepFailed {
			for _, d := range p.Digests {
				protected[d] = true
			}
			continue
		}
		stats.PinsExpired++
		if !opts.DryRun {
			if err := removePin(opts.PinsDir, p.RunID); err != nil {
				return stats, err
			}
		}
	}
	stats.PinnedObjects = len(protected)

	sizes := make(map[cas.Digest]int64)
	if err := opts.CAS.Walk(func(o cas.Object) error {
		sizes[o.Digest] = o.Bytes
		stats.ObjectsScanned++
		stats.BytesBefore += o.Bytes
		return nil
	}); err != nil {
		return stats, err
	}

	type entryInfo struct {
		path     string
		refs     []cas.Digest
		accessed time.Time
	}
	var entries []entryInfo
	if err := opts.Action.Walk(func(path string, e Entry, accessed time.Time) error {
		stats.EntriesScanned++
		entries = append(entries, entryInfo{path: path, refs: references(e.Result), accessed: accessed})
		return nil
	}); err != nil {
		return stats, err
	}
	// Newest first, so the greedy keep below is least-recently-used eviction.
	sort.Slice(entries, func(i, j int) bool { return entries[i].accessed.After(entries[j].accessed) })

	live := make(map[cas.Digest]bool, len(protected))
	for d := range protected {
		live[d] = true
	}
	used := int64(0)
	for d := range protected {
		used += sizes[d]
	}

	for _, e := range entries {
		if opts.MaxSize > 0 {
			var add int64
			for _, d := range e.refs {
				if !live[d] {
					add += sizes[d]
				}
			}
			if used+add > opts.MaxSize {
				stats.EntriesEvicted++
				if !opts.DryRun {
					if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
						return stats, fmt.Errorf("cache: evict entry: %w", err)
					}
				}
				continue
			}
			used += add
		}
		for _, d := range e.refs {
			live[d] = true
		}
	}

	for d, size := range sizes {
		if live[d] {
			continue
		}
		stats.ObjectsDeleted++
		stats.BytesFreed += size
		if !opts.DryRun {
			if err := opts.CAS.Delete(d); err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}

// references enumerates every content address a result holds, in ONE place.
// A field added to Result later has one function to update rather than three
// call sites to find, and the failure that prevents is deleting the logs of
// an entry that stays a valid hit.
func references(r Result) []cas.Digest {
	out := make([]cas.Digest, 0, len(r.Workspaces)+len(r.Outputs)+len(r.Logs))
	for _, w := range r.Workspaces {
		out = append(out, w.Digest)
	}
	for _, o := range r.Outputs {
		out = append(out, o.Digest)
	}
	for _, l := range r.Logs {
		out = append(out, l.Digest)
	}
	return out
}
```

A workspace snapshot's index object is not in `references`, because a `Result` records only the workspace digest. Add the index to the pin instead: the engine writes both digests into the pin it produces, which is what keeps `ws ls` working for the failed runs anyone actually inspects. Note this explicitly in `references`'s doc comment when implementing, and cover it in the engine step below.

- [ ] **Step 5: Write the pin from the engine.** In `engine.Run`, after `status` is computed and before `rc.emitFinal`, and gathering the digests from `rc.ws`:

```go
	if opts.Storage != nil && ws != nil && status != api.RunSucceeded && status != api.RunSucceededWithRecovery {
		// A run that ended badly is a run somebody is about to debug, and
		// its workspaces are the evidence. Pinning them is what stops a size
		// budget sweep from deleting exactly the snapshot being looked at
		// (v0 spec §4.3). Both digests go in: the body so it can be
		// restored, and the index so `ws ls` still works.
		if err := cache.WritePin(filepath.Join(opts.Storage.Root, "pins"), cache.Pin{
			RunID: opts.RunID, Status: string(status), Finished: time.Now().UTC(),
			Digests: ws.allSnapshotDigests(),
		}); err != nil {
			// A pin is protection, not a result. A run that produced its
			// output and could not write a pin still produced its output.
			_ = err
		}
	}
```

with the accessor on `wsManager`, which needs the manager to accumulate every snapshot it recorded rather than only the latest per workspace:

```go
// allSnapshotDigests is every digest this run's snapshots produced, body and
// index alike, in a deterministic order.
func (m *wsManager) allSnapshotDigests() []cas.Digest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]cas.Digest, 0, len(m.produced))
	for _, d := range m.produced {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
```

Add `produced []cas.Digest` to `wsManager` and append both `s.Digest` and `s.Index` in `record`.

- [ ] **Step 6: Write the failing CLI test**

Create `cmd/senro/cmd_cache_test.go`:

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSizeAcceptsSuffixes(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"1K", 1024},
		{"2M", 2 * 1024 * 1024},
		{"3G", 3 * 1024 * 1024 * 1024},
		{"50g", 50 * 1024 * 1024 * 1024},
	} {
		got, err := parseSize(tc.in)
		if err != nil {
			t.Errorf("parseSize(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"", "-1", "1T B", "many", "1.5G"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) returned no error", bad)
		}
	}
}

func TestCacheGCReportsWhatItFreed(t *testing.T) {
	root := t.TempDir()
	seedCacheRoot(t, root)

	var out, errOut bytes.Buffer
	code := cmdCache([]string{"gc", "--cache-dir", root, "--max-size", "1K"}, &out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	for _, want := range []string{"objects", "freed"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output is missing %q:\n%s", want, out.String())
		}
	}
}

func TestCacheGCDryRunSaysSoAndChangesNothing(t *testing.T) {
	root := t.TempDir()
	seedCacheRoot(t, root)
	before := countFiles(t, filepath.Join(root, "cas"))

	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--cache-dir", root, "--max-size", "1", "--dry-run"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "dry run") {
		t.Errorf("a dry run did not say so:\n%s", out.String())
	}
	if after := countFiles(t, filepath.Join(root, "cas")); after != before {
		t.Errorf("a dry run changed the store: %d files became %d", before, after)
	}
}

func TestCacheRejectsAnUnknownSubcommandWithAUsageCode(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"vacuum"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "vacuum") {
		t.Errorf("the error does not name the unknown subcommand: %s", errOut.String())
	}
}

func TestCacheGCOnAnUnopenableRootIsAUsageError(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocked, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--cache-dir", blocked}, &out, &errOut); code == exitSuccess {
		t.Error("gc over an unopenable cache root reported success")
	}
}
```

Write `seedCacheRoot` and `countFiles` in the same file: `seedCacheRoot` runs a small two-step pure pipeline through `senro.Run` with `WithCacheDir(root)` so the fixture is a cache root a real run produced, and `countFiles` walks a directory counting regular files.

- [ ] **Step 7: Implement `cmd/senro/cmd_cache.go`**

```go
package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/storage"
)

// defaultKeepFailed is how long a failed run's workspaces survive a sweep.
//
// A week, per the v0 spec §4.3, because the question a failed workspace
// answers is "why did this break", and that question is usually asked days
// later by someone who was not there. Shorter would make the debug loop the
// snapshot exists for unreliable in exactly the case it matters.
const defaultKeepFailed = 7 * 24 * time.Hour

func cmdCache(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, cacheUsage)
		return exitUsage
	}
	switch args[0] {
	case "gc":
		return cmdCacheGC(args[1:], stdout, stderr)
	case "explain":
		return cmdCacheExplain(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "senro cache: unknown subcommand %q\n\n%s", args[0], cacheUsage)
		return exitUsage
	}
}

func cmdCacheGC(args []string, stdout, stderr io.Writer) int {
	var (
		cacheDir   string
		maxSize    int64
		keepFailed = defaultKeepFailed
		dryRun     bool
	)
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--dry-run":
			dryRun = true
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir = args[i+1]
			i++
		case a == "--max-size" && i+1 < len(args):
			n, err := parseSize(args[i+1])
			if err != nil {
				_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
				return exitUsage
			}
			maxSize = n
			i++
		case a == "--keep-failed" && i+1 < len(args):
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
				return exitUsage
			}
			keepFailed = d
			i++
		default:
			_, _ = fmt.Fprintf(stderr, "senro cache gc: unknown argument %q\n\n%s", a, cacheUsage)
			return exitUsage
		}
	}

	root, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: store.CAS, Action: store.Action, PinsDir: filepath.Join(root, "pins"),
		MaxSize: maxSize, KeepFailed: keepFailed, Now: time.Now(), DryRun: dryRun,
	})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
		return exitRunFailed
	}

	prefix := ""
	if dryRun {
		prefix = "dry run: "
	}
	_, _ = fmt.Fprintf(stdout,
		"%s%d of %d objects deleted, %s freed of %s; %d of %d entries evicted; %d pinned objects kept, %d pins expired\n",
		prefix, stats.ObjectsDeleted, stats.ObjectsScanned,
		humanBytes(stats.BytesFreed), humanBytes(stats.BytesBefore),
		stats.EntriesEvicted, stats.EntriesScanned, stats.PinnedObjects, stats.PinsExpired)
	return exitSuccess
}

// resolveCacheDir is the one place that turns a --cache-dir flag into a path,
// so the CLI and senro.Run agree on where a cache lives.
func resolveCacheDir(flag string) (string, error) {
	if flag != "" {
		return flag, nil
	}
	return storage.DefaultRoot()
}

// parseSize reads "50G", "500M", "1K" or a plain byte count. Deliberately
// integer-only: "1.5G" is refused rather than rounded, because a budget
// silently rounded to a different number is a budget nobody set.
func parseSize(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mult := int64(1)
	switch strings.ToUpper(s[len(s)-1:]) {
	case "K":
		mult, s = 1024, s[:len(s)-1]
	case "M":
		mult, s = 1024*1024, s[:len(s)-1]
	case "G":
		mult, s = 1024*1024*1024, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q: want a byte count, optionally suffixed K, M or G", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("size must not be negative")
	}
	return n * mult, nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

const cacheUsage = `Usage:
  senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
      Reclaim disk. Least recently used entries go first; the workspaces of a
      failed run are kept for --keep-failed so the snapshot you are debugging
      is still there.

  senro cache explain [--run ID] [STEP]
      Why a step hit or missed, component by component.
`
```

Add `"path/filepath"` to the imports.

- [ ] **Step 8: Dispatch from `main.go`.** Add a `case "cache": return cmdCache(args[1:], stdout, stderr)` and extend `usage` with the two `senro cache` lines. Extend `cmd/senro/main_test.go`'s unknown-command test set so `cache` is no longer reported as unknown.

- [ ] **Step 9: Run to verify it passes** with `go test ./internal/cache/... ./cmd/senro/... ./internal/engine/... -race`.

- [ ] **Step 10: Prove the pin.** Temporarily set `KeepFailed` to zero inside `GC`. Confirm `TestGCKeepsAPinnedFailedRunsWorkspaceEvenWhenOverBudget` fails. Restore, and record it.

- [ ] **Step 11: Run the gates**

```bash
go test ./... -race
make all
golangci-lint run ./...
```

- [ ] **Step 12: Commit**

```bash
git add internal/cache internal/engine cmd/senro
git commit -m "feat(cache): senro cache gc, with pins for failed runs

LRU over entries rather than objects, because half an entry is a broken hit
and saves nothing. A run that ends badly writes a pin listing every digest it
produced, body and index, and the sweep skips those for --keep-failed,
default a week: without it a size budget deletes exactly the snapshot someone
is debugging. A zero --max-size collects only orphans and expired pins, so a
sweep run without a number never evicts a valid hit."
```

---

### Task 12: `senro cache explain` and `senro ws ls`

**Files:**
- Modify `cmd/senro/cmd_cache.go` (add `cmdCacheExplain`)
- Create `cmd/senro/cmd_ws.go`, `cmd/senro/cmd_ws_test.go`
- Create `cmd/senro/rundir.go`, `cmd/senro/rundir_test.go`
- Modify `cmd/senro/cmd_cache_test.go`, `cmd/senro/main.go`, `cmd/senro/main_test.go`

**Interfaces:**
- Consumes: Task 8's `ReadRecord`, `ReadRecords`, `FormatExplain`; Task 10's `scratch.ReadRecords`; Task 3's `Snapshotter.LoadIndex`; Task 1's `storage.Open`; `internal/eventlog.Read`.
- Produces:
  ```go
  package main
  func cmdCacheExplain(args []string, stdout, stderr io.Writer) int
  func cmdWS(args []string, stdout, stderr io.Writer) int
  func resolveRunDir(flag string) (string, error)
  const largeWorkspaceBytes = 2 << 30
  ```

**Wiring.** Both are dispatched from `main.go` and both read what a real run produced. `ws ls <run> <name>` is what gives the file index a production reader: the index is stored by Task 3 and would otherwise be written and never read, which is precisely the defect this plan is organized against.

**The 2 GiB warning.** design.md §4.2 lists warning above a size threshold as a mandatory mitigation. It lands here rather than in the engine because there is no warning event type in `api` and inventing one would be a schema change for a diagnostic. `ws ls` already has every number it needs from `ws.snapshot`, so it flags a workspace over the threshold, and `ws ls <run> <name>` is how the operator then finds what is in it. Naming the top offending directories automatically is not planned: the index makes it a sort the operator can do, and a heuristic that names the wrong directory is worse than a listing that names all of them.

- [ ] **Step 1: Write the failing test for run resolution**

Create `cmd/senro/rundir_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolveRunDirAcceptsAPath(t *testing.T) {
	dir := t.TempDir()
	got, err := resolveRunDir(dir)
	if err != nil {
		t.Fatalf("resolveRunDir: %v", err)
	}
	if got != dir {
		t.Errorf("resolveRunDir(%q) = %q", dir, got)
	}
}

func TestResolveRunDirAcceptsARunIDUnderRuns(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	want := filepath.Join(base, "runs", "r7")
	if err := os.MkdirAll(want, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := resolveRunDir("r7")
	if err != nil {
		t.Fatalf("resolveRunDir: %v", err)
	}
	if got != want {
		t.Errorf("resolveRunDir(\"r7\") = %q, want %q", got, want)
	}
}

func TestResolveRunDirDefaultsToTheNewestRun(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	older := filepath.Join(base, "runs", "a")
	newer := filepath.Join(base, "runs", "b")
	for _, d := range []string{older, newer} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(older, past, past); err != nil {
		t.Fatalf("age: %v", err)
	}
	got, err := resolveRunDir("")
	if err != nil {
		t.Fatalf("resolveRunDir: %v", err)
	}
	if got != newer {
		t.Errorf("resolveRunDir(\"\") = %q, want the newest run %q", got, newer)
	}
}

// The negative half. Silently picking a directory that does not exist would
// turn every later error into a confusing one about a missing file.
func TestResolveRunDirFailsWhenThereIsNoRun(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, err := resolveRunDir(""); err == nil {
		t.Error("resolveRunDir found a run in an empty directory")
	}
	if _, err := resolveRunDir("nope"); err == nil {
		t.Error("resolveRunDir accepted a run ID with no directory")
	}
}
```

- [ ] **Step 2: Implement `cmd/senro/rundir.go`**

```go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// resolveRunDir turns a --run value into a run directory.
//
// Three forms, in order: a path that exists, a run ID under ./runs, and,
// when nothing was given, the newest directory under ./runs. That last one
// is the form an operator uses constantly ("why did the run I just did
// miss"), and it is why this refuses rather than defaulting to a directory
// that does not exist: a wrong guess turns every later message into one
// about a missing file rather than about a missing run.
func resolveRunDir(flag string) (string, error) {
	if flag != "" {
		if fi, err := os.Stat(flag); err == nil && fi.IsDir() {
			return flag, nil
		}
		if strings.ContainsRune(flag, os.PathSeparator) {
			return "", fmt.Errorf("no run directory at %q", flag)
		}
		p := filepath.Join("runs", flag)
		if fi, err := os.Stat(p); err == nil && fi.IsDir() {
			return p, nil
		}
		return "", fmt.Errorf("no run %q (looked for %s)", flag, p)
	}

	entries, err := os.ReadDir("runs")
	if err != nil {
		return "", fmt.Errorf("no runs directory here: name a run with --run, or run from the directory a pipeline ran in")
	}
	type candidate struct {
		name string
		when time.Time
	}
	var found []candidate
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		found = append(found, candidate{name: e.Name(), when: info.ModTime()})
	}
	if len(found) == 0 {
		return "", fmt.Errorf("no runs found under ./runs")
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].when.Equal(found[j].when) {
			return found[i].name > found[j].name
		}
		return found[i].when.After(found[j].when)
	})
	return filepath.Join("runs", found[0].name), nil
}
```

- [ ] **Step 3: Write the failing test for explain**

Append to `cmd/senro/cmd_cache_test.go`:

```go
// The whole point of design.md §3.5: a miss you can explain. seedRuns runs
// the same pure pipeline twice against one cache root, changing an input
// between them, so the second run's records describe a real miss.
func TestCacheExplainNamesTheComponentThatChanged(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	code := cmdCacheExplain([]string{"--run", "r2", "compile"}, &out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	if !strings.Contains(got, "MISS") || !strings.Contains(got, "compile") {
		t.Errorf("output does not report the miss:\n%s", got)
	}
	if !strings.Contains(got, "input_digests") && !strings.Contains(got, "workspace_digests") {
		t.Errorf("output does not name the component that changed:\n%s", got)
	}
	if !strings.Contains(got, "unchanged") {
		t.Errorf("output does not say what stayed the same, so a reader cannot tell one change from all of them:\n%s", got)
	}
}

func TestCacheExplainWithNoStepSummarisesEveryStep(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdCacheExplain([]string{"--run", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "compile") {
		t.Errorf("the summary omits a step with a record:\n%s", out.String())
	}
}

func TestCacheExplainForAStepWithNoRecordIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	code := cmdCacheExplain([]string{"--run", "r2", "not-a-step"}, &out, &errOut)
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "not-a-step") {
		t.Errorf("the error does not name the step: %s", errOut.String())
	}
}

func TestCacheExplainReportsScratchOutcomes(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithScratch(t, base)

	var out, errOut bytes.Buffer
	if code := cmdCacheExplain([]string{"--run", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "scratch") || !strings.Contains(out.String(), "deps") {
		t.Errorf("the summary does not report the scratch cache, which is invisible everywhere else:\n%s", out.String())
	}
}
```

Write `seedRuns` and `seedRunsWithScratch` in the same file. Each builds a pipeline with `senro.New`, runs it twice through `senro.Run` with `WithDir(filepath.Join(base, "runs", "r1"))` then `"r2"`, `WithRunID`, and one shared `WithCacheDir`, changing a source file between the two so the second run misses.

- [ ] **Step 4: Implement `cmdCacheExplain` in `cmd/senro/cmd_cache.go`**

```go
func cmdCacheExplain(args []string, stdout, stderr io.Writer) int {
	var run, step string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--run" && i+1 < len(args):
			run = args[i+1]
			i++
		case strings.HasPrefix(a, "--"):
			_, _ = fmt.Fprintf(stderr, "senro cache explain: unknown flag %q\n\n%s", a, cacheUsage)
			return exitUsage
		case step == "":
			step = a
		default:
			_, _ = fmt.Fprintf(stderr, "senro cache explain: unexpected argument %q\n\n%s", a, cacheUsage)
			return exitUsage
		}
	}

	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
		return exitUsage
	}
	cacheDir := filepath.Join(dir, "cache")

	if step != "" {
		// An address may carry an attempt suffix at the CLI boundary and
		// never inside a record, so it is stripped here and nowhere else.
		id, _, err := stepid.ParseAddress(step)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
			return exitUsage
		}
		rec, err := cache.ReadRecord(cacheDir, id)
		if err != nil {
			_, _ = fmt.Fprintf(stderr,
				"senro cache explain: no cache record for step %q in %s "+
					"(only Pure() steps have one, since nothing else is looked up)\n", id, dir)
			return exitUsage
		}
		if err := cache.FormatExplain(stdout, rec); err != nil {
			_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
			return exitRunFailed
		}
		return exitSuccess
	}

	recs, err := cache.ReadRecords(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
		return exitUsage
	}
	for _, r := range recs {
		if err := cache.FormatExplain(stdout, r); err != nil {
			_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
			return exitRunFailed
		}
	}

	// The scratch cache has no events of its own (see internal/scratch's
	// Record), so this is the one place its behaviour is visible.
	sr, err := scratch.ReadRecords(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
		return exitUsage
	}
	for _, r := range sr {
		state := "cold"
		switch {
		case r.Restored && r.RestoredFrom == r.Key:
			state = "restored (exact)"
		case r.Restored:
			state = "restored from " + r.RestoredFrom
		case r.Saved:
			state = "cold, saved"
		}
		if _, err := fmt.Fprintf(stdout, "scratch  %s  key %s  %s\n", r.Name, r.Key, state); err != nil {
			return exitRunFailed
		}
	}
	if len(recs) == 0 && len(sr) == 0 {
		_, _ = fmt.Fprintf(stdout, "no cache activity recorded in %s: no step declared Pure() and no scratch cache was mounted\n", dir)
	}
	return exitSuccess
}
```

Add `internal/scratch` and `internal/stepid` to the file's imports.

- [ ] **Step 5: Write the failing test for `ws ls`**

Create `cmd/senro/cmd_ws_test.go`:

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestWSLsListsEveryWorkspaceWithItsDigestAndSize(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	for _, want := range []string{"src", "sha256:", "files"} {
		if !strings.Contains(got, want) {
			t.Errorf("output is missing %q:\n%s", want, got)
		}
	}
}

// The index exists so a file list is readable without the body. This is what
// reads it; without this command the index would be stored by every snapshot
// and never opened by anything.
func TestWSLsWithAWorkspaceNameListsItsFilesFromTheIndex(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "main.go") {
		t.Errorf("the file listing does not name a file the workspace holds:\n%s", out.String())
	}
}

func TestWSLsForAnUnknownWorkspaceIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2", "nope"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "nope") {
		t.Errorf("the error does not name the workspace: %s", errOut.String())
	}
}

func TestWSLsOnARunWithNoWorkspacesSaysSo(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunWithoutWorkspaces(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r1"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "no workspaces") {
		t.Errorf("a run with no workspaces produced no explanation:\n%s", out.String())
	}
}

func TestWSRejectsAnUnknownSubcommand(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "r1", "src"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "pull") {
		t.Errorf("the error does not name the unknown subcommand: %s", errOut.String())
	}
}
```

Write `seedRunWithoutWorkspaces` next to the other seeds.

- [ ] **Step 6: Implement `cmd/senro/cmd_ws.go`**

```go
package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/storage"
)

// largeWorkspaceBytes is where ws ls starts flagging a workspace as large.
//
// design.md §4.2 makes a size warning a mandatory mitigation and sets the
// threshold at 2 GiB. It lands in the CLI rather than in the engine because
// there is no warning event type in the api module, and inventing one would
// be a schema change for a diagnostic. Every number this needs is already in
// ws.snapshot.
const largeWorkspaceBytes = 2 << 30

func cmdWS(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "ls" {
		what := "(none)"
		if len(args) > 0 {
			what = args[0]
		}
		_, _ = fmt.Fprintf(stderr, "senro ws: unknown subcommand %q\n\n%s", what, wsUsage)
		return exitUsage
	}
	rest := args[1:]
	var run, name string
	switch len(rest) {
	case 0:
	case 1:
		run = rest[0]
	case 2:
		run, name = rest[0], rest[1]
	default:
		_, _ = fmt.Fprintf(stderr, "senro ws ls: unexpected arguments %v\n\n%s", rest[2:], wsUsage)
		return exitUsage
	}

	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	snaps, err := latestSnapshots(dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	if len(snaps) == 0 {
		_, _ = fmt.Fprintf(stdout, "no workspaces in %s\n", dir)
		return exitSuccess
	}

	names := make([]string, 0, len(snaps))
	for n := range snaps {
		names = append(names, n)
	}
	sort.Strings(names)

	if name == "" {
		for _, n := range names {
			s := snaps[n]
			line := fmt.Sprintf("%-20s %s  %d files  %s", n, cas.Digest(s.Digest).Short(), s.Files, humanBytes(s.Bytes))
			if s.Bytes > largeWorkspaceBytes {
				line += fmt.Sprintf("  LARGE (over %s; senro ws ls %s %s lists what is in it)",
					humanBytes(largeWorkspaceBytes), run, n)
			}
			if _, err := fmt.Fprintln(stdout, line); err != nil {
				return exitRunFailed
			}
		}
		return exitSuccess
	}

	s, ok := snaps[name]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "senro ws ls: %s has no workspace %q (it has %v)\n", dir, name, names)
		return exitUsage
	}
	if s.Index == "" {
		_, _ = fmt.Fprintf(stderr, "senro ws ls: workspace %q was snapshotted by a build that recorded no index\n", name)
		return exitUsage
	}

	root, err := resolveCacheDir("")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	ix, err := store.Snapshotter.LoadIndex(context.Background(), cas.Digest(s.Index))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitRunFailed
	}
	for _, e := range ix.Entries {
		suffix := ""
		if e.Link != "" {
			suffix = " -> " + e.Link
		}
		if _, err := fmt.Fprintf(stdout, "%04o  %10d  %-12s %s%s\n",
			e.Mode, e.Size, cas.Digest(e.Digest).Short(), e.Path, suffix); err != nil {
			return exitRunFailed
		}
	}
	return exitSuccess
}

// latestSnapshots folds a run's ledger down to the last ws.snapshot per
// workspace. The ledger rather than the CAS, because that is what says which
// digest this RUN ended with, and a workspace snapshotted once per attempt
// has several.
func latestSnapshots(dir string) (map[string]api.WSSnapshotBody, error) {
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil && len(events) == 0 {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	out := map[string]api.WSSnapshotBody{}
	for _, e := range events {
		if e.Type != api.WSSnapshot {
			continue
		}
		var b api.WSSnapshotBody
		if err := e.Decode(&b); err != nil {
			continue
		}
		out[b.Name] = b
	}
	return out, nil
}

const wsUsage = `Usage:
  senro ws ls [RUN] [NAME]
      List a run's workspaces with their digests and sizes. With a workspace
      name, list its files from the stored index, without downloading the
      body.
`
```

`eventlog.Read` returns the events it parsed before a torn tail alongside an error, which is why the guard above only fails when nothing at all could be read: a run killed mid-write still has workspaces worth listing.

- [ ] **Step 7: Dispatch `ws` from `main.go`** with `case "ws": return cmdWS(args[1:], stdout, stderr)`, and extend `usage` with the `senro ws ls` lines.

- [ ] **Step 8: Run to verify it passes** with `go test ./cmd/senro/... -race`.

- [ ] **Step 9: Prove the index has a reader.** Temporarily make `Snapshotter.Snapshot` return an empty `Index`. Confirm `TestWSLsWithAWorkspaceNameListsItsFilesFromTheIndex` fails. Restore, and record it. This is the check that the index is not being written for nobody.

- [ ] **Step 10: Run the gates**

```bash
go test ./... -race
make all
golangci-lint run ./...
```

- [ ] **Step 11: Commit**

```bash
git add cmd/senro
git commit -m "feat(cli): senro cache explain and senro ws ls

explain is a formatter over the records the engine already wrote, so the CLI
and the engine cannot disagree about why a step missed, and it is the only
place a scratch cache's behaviour is visible since it has no events. ws ls
reads the file index, which is what gives the index a production reader
rather than leaving it written and never opened, and it flags a workspace
over the 2 GiB threshold design.md section 4.2 asks for."
```

---

### Task 13: End to end: the mtime proof, secret containment, and the cached golden

**This task is the plan's deliverable.** Tasks 2 and 3 prove the snapshot digest is stable in isolation. This one proves it where it matters: two runs of a pipeline whose first step rewrites a file with identical content and a fresh mtime, driven through `senro.Run`, asserting the second run's downstream pure step hits.

**Files:**
- Create `storage_e2e_test.go` (package `senro_test`, the root module's public surface)
- Modify `internal/engine/golden_test.go` (extend `scrub`, add `TestGoldenCachedRun`)
- Create `internal/engine/testdata/golden/cached.jsonl`
- Modify `README.md`

**Interfaces:**
- Consumes: everything. This task adds no new exported identifier.

- [ ] **Step 1: Write the failing test. This is the one that matters.**

Create `storage_e2e_test.go`:

```go
package senro_test

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/eventlog"
)

// e2e runs one pipeline through the public entry point and returns the run's
// events. Every test below shares one cache root across several calls,
// because that is the only shape in which a cache question can be asked.
func e2e(t *testing.T, p *senro.Plan, cacheDir, runID string) (string, []api.Event) {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "run")
	// p is already a resolved *senro.Plan, not a *senro.Pipeline, so this
	// goes through RunPlan rather than Run: see run.go's own doc for why the
	// two entry points exist.
	err := senro.RunPlan(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID(runID), senro.WithCacheDir(cacheDir))
	var runErr *senro.RunError
	if err != nil && !errors.As(err, &runErr) {
		t.Fatalf("senro.RunPlan: %v", err)
	}
	events, readErr := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	if readErr != nil && len(events) == 0 {
		t.Fatalf("read ledger: %v", readErr)
	}
	return runDir, events
}

func count(events []api.Event, ty api.Type) int {
	var n int
	for _, e := range events {
		if e.Type == ty {
			n++
		}
	}
	return n
}

// THE test this whole plan exists for.
//
// design.md §11 item 3: tar records mtimes, `go build` touches files, and an
// unnormalized tar produces a different digest on every run, which silently
// destroys every cache key downstream of a workspace. The pipeline below is
// exactly that shape: `generate` rewrites main.go with byte-identical
// content and a fresh mtime on every run, and `compile` is a Pure() step
// downstream of the workspace it wrote into.
//
// If the snapshot digest carried an mtime, the second run would miss, and
// nothing anywhere would report an error. The assertion is the hit.
func TestRewritingAFileWithTheSameContentStillHitsTheCache(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")

	build := func() *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		// The generator writes the same bytes every run and touches the file
		// while doing it, which is what a compiler does to files it did not
		// change.
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("size.txt"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	_, first := e2e(t, build(), cacheDir, "r1")
	if count(first, api.CacheMiss) != 1 || count(first, api.CacheSaved) != 1 {
		t.Fatalf("the first run should miss once and save once: miss=%d saved=%d",
			count(first, api.CacheMiss), count(first, api.CacheSaved))
	}

	secondDir, second := e2e(t, build(), cacheDir, "r2")
	if count(second, api.CacheHit) != 1 {
		var why string
		for _, e := range second {
			if e.Type == api.CacheMiss {
				var b api.CacheMissBody
				_ = e.Decode(&b)
				why = b.Reason + "/" + b.Differing
			}
		}
		t.Fatalf("the second run did not hit (%s).\n"+
			"design.md §11 item 3: an unnormalized workspace tar digests differently on every run and "+
			"silently destroys every cache key downstream of a workspace, which is exactly this", why)
	}
	if b, err := os.ReadFile(filepath.Join(secondDir, "ws", "src", "size.txt")); err != nil {
		t.Errorf("the hit did not restore the declared output: %v", err)
	} else if !strings.Contains(string(b), "main.go") {
		t.Errorf("restored output = %q", b)
	}
}

// The negative half. Without it the test above passes for a cache that hits
// unconditionally, which is a wrong build rather than a slow one.
func TestChangingTheContentMissesAndNamesTheComponent(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")

	build := func(body string) *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf '"+body+"' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("size.txt"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	_, _ = e2e(t, build("package main\\n"), cacheDir, "r1")
	_, second := e2e(t, build("package main\\n\\nvar x = 1\\n"), cacheDir, "r2")

	if count(second, api.CacheHit) != 0 {
		t.Fatal("a changed source hit the cache, which is a wrong build")
	}
	var body api.CacheMissBody
	for _, e := range second {
		if e.Type == api.CacheMiss {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode cache.miss: %v", err)
			}
		}
	}
	if body.Differing == "" {
		t.Error("the miss names no differing component, so `cache explain` has nothing to report")
	}
}

// Secrets must never appear in cache keys, events, or logs. This is where
// that bites hardest: a key is derived from a step's inputs and environment,
// and a cache entry outlives the run directory.
func TestNoEnvironmentValueReachesTheCacheOrTheLedger(t *testing.T) {
	const token = "s3cr3t-canary-value-do-not-store" //nolint:gosec // a test fixture, not a credential
	cacheDir := filepath.Join(t.TempDir(), "cache")

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("compile", exec.Command("sh", "-c", "wc -c main.go > size.txt")).
		Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Env("BUILD_TOKEN=" + token).
		Pure().Inputs(artifact.Glob("**/*.go")).CacheEnv("BUILD_TOKEN")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir, _ := e2e(t, p, cacheDir, "r1")

	// The cache root, which persists across runs and is shared by every
	// pipeline on the machine.
	var sawName bool
	err = filepath.WalkDir(cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), token) {
			t.Errorf("the value appears in the cache at %s", path)
		}
		if strings.Contains(string(b), "BUILD_TOKEN") {
			sawName = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk cache: %v", err)
	}
	// The positive half, and it is what makes the negative half mean
	// anything: the walk really does read the file the key lives in, so a
	// value would have been found if one were there.
	if !sawName {
		t.Fatal("the walk never saw BUILD_TOKEN at all, so it proves nothing about the value")
	}

	// The ledger, and every log file.
	ledger, err := os.ReadFile(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if strings.Contains(string(ledger), token) {
		t.Error("the value appears in events.jsonl")
	}
	err = filepath.WalkDir(filepath.Join(runDir, "logs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(b), token) {
			t.Errorf("the value appears in a log file at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk logs: %v", err)
	}
}

// The trust boundary, stated as two assertions rather than as a comment.
func TestOnlyAnAllowlistedEnvironmentVariableInvalidates(t *testing.T) {
	build := func(declared, value string) *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		sb := l.Step("compile", exec.Command("sh", "-c", "true")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Env("VAR="+value).
			Pure().Inputs(artifact.Glob("**/*.go"))
		if declared != "" {
			sb.CacheEnv(declared)
		}
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	t.Run("allowlisted value change misses", func(t *testing.T) {
		cacheDir := filepath.Join(t.TempDir(), "cache")
		_, _ = e2e(t, build("VAR", "one"), cacheDir, "r1")
		_, second := e2e(t, build("VAR", "two"), cacheDir, "r2")
		if count(second, api.CacheHit) != 0 {
			t.Error("an allowlisted variable changed and the step still hit, so the cache served the wrong build")
		}
	})

	t.Run("undeclared value change does not miss", func(t *testing.T) {
		cacheDir := filepath.Join(t.TempDir(), "cache")
		_, _ = e2e(t, build("", "one"), cacheDir, "r1")
		_, second := e2e(t, build("", "two"), cacheDir, "r2")
		if count(second, api.CacheHit) != 1 {
			t.Error("an undeclared variable changed and the step missed; nothing outside CacheEnv enters a key, " +
				"which is what stops every machine-specific variable making every key unique")
		}
	})
}

// A fully cached run is still a run that says what happened, and the fold is
// what every renderer reads.
func TestAFullyCachedRunFoldsToSucceededWithCachedSteps(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	build := func() *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "true")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.go"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}
	_, _ = e2e(t, build(), cacheDir, "r1")
	_, second := e2e(t, build(), cacheDir, "r2")

	st := api.NewRunState()
	for _, e := range second {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", st.Run.Status)
	}
	if st.Steps["compile"].State != api.StateCached || !st.Steps["compile"].Cached {
		t.Errorf("compile = %+v, want cached in both the state and the flag", st.Steps["compile"])
	}

	// The published fixture corpus reads these; a cached run must survive a
	// round trip through JSON like every other event.
	for _, e := range second {
		if _, err := json.Marshal(e); err != nil {
			t.Fatalf("marshal %s: %v", e.Type, err)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails.** Run `go test . -run TestRewritingAFile -v` BEFORE the rest of the plan's tasks are complete and it fails on a missing symbol; run it now, after Task 12, and it should pass. If it does not, the bug is in the wiring, not in the normalization: Task 2's tests would have caught a normalization regression first. Record which of the two is failing before changing anything.

- [ ] **Step 3: Extend the golden scrubber.** In `internal/engine/golden_test.go`, add `"key"` to the scrubbed payload fields with this comment above the list:

```go
			// "key" is a cache key digest, and a cache key includes the
			// executor class and the declared platform, both of which come
			// straight from runtime.GOOS and runtime.GOARCH. A golden
			// generated on darwin/arm64 would fail on the Linux half of the
			// CI matrix, exactly as executor_class and platform already do.
			//
			// The workspace digests in ws.snapshot are deliberately NOT
			// scrubbed. A normalized tar depends on file content, file names
			// and the executable bit alone, so it is identical on every host,
			// and pinning it is the strongest mutation detection this suite
			// has: a regression in the normalization design.md §11 item 3
			// warns about turns those digests nondeterministic, and this
			// golden starts failing on every run rather than silently never
			// hitting.
```

- [ ] **Step 4: Add the cached golden.** In `internal/engine/golden_test.go`:

```go
// TestGoldenCachedRun pins the event stream of a run served entirely from
// cache: cache.hit, the restored workspace, the replayed log markers, and a
// step.finished carrying state cached with no step.started in front of it.
func TestGoldenCachedRun(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	build := func() *senro.Plan {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("cached")
		l := pipe.Workflow("main")
		l.Step("generate", exec.Command("sh", "-c", "printf 'hello\\n' > a.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("read", exec.Command("cat", "a.txt")).
			Needs("generate").WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.txt"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}

	var second []api.Event
	for i, runID := range []string{"r1", "r2"} {
		store, err := storage.Open(cacheDir)
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		runDir := filepath.Join(t.TempDir(), "run")
		rec := sink.Recording()
		if _, err := engine.Run(context.Background(), build(), engine.Options{
			Dir: runDir, Executor: localexec.New(runDir, store.Snapshotter),
			Sink: rec, Storage: store, RunID: runID,
		}); err != nil {
			t.Fatalf("engine.Run %d: %v", i+1, err)
		}
		_ = store.Close()
		second = rec.Events()
	}
	compareOrUpdateGolden(t, second, filepath.Join("testdata", "golden", "cached.jsonl"))
}

func TestGoldenCachedRunFoldsToSucceeded(t *testing.T) {
	events, err := readGolden(t, filepath.Join("testdata", "golden", "cached.jsonl"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	st := api.NewRunState()
	for _, e := range events {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", st.Run.Status)
	}
	if st.Steps["read"].State != api.StateCached {
		t.Errorf("read = %s, want cached", st.Steps["read"].State)
	}
}
```

If `readGolden` does not already exist in the file, add it as a small helper that reads the golden and unmarshals one `api.Event` per line, matching what `compareOrUpdateGolden` writes.

- [ ] **Step 5: Generate the golden and read it.**

```bash
UPDATE_GOLDEN=1 go test ./internal/engine/ -run TestGoldenCachedRun
git diff --stat internal/engine/testdata/golden/cached.jsonl
```

Then open the file and check every line by eye before committing it. Specifically: there is exactly one `cache.hit` and no `cache.miss`; the `read` step has no `step.started`; its `step.finished` carries `"state":"cached"` and `"cached":true`; the `ws.snapshot` digests are real `sha256:` values and not empty; and `seq` is strictly increasing with no gaps. A golden nobody read is a golden that pins whatever the bug produced.

- [ ] **Step 6: Run the golden twice to confirm it is stable.**

```bash
go test ./internal/engine/ -run TestGolden -count=2
```

Two consecutive passes with no `UPDATE_GOLDEN` is the check that the workspace digests really are deterministic. If this fails on the second run, the normalization has regressed and Task 2's tests should be consulted first.

- [ ] **Step 7: Document the surface in `README.md`.** Add a short section covering: declaring a workspace and mounting it, `Pure()` with `Inputs` and `Outputs`, `CacheEnv` and the fact that only names are declared and only digests are stored, the scratch cache, `SENRO_CACHE_DIR`, and the three commands (`senro cache explain`, `senro cache gc`, `senro ws ls`). State plainly that `Pure()` is trusted rather than enforced, and that a scratch cache is never an input to a cache key.

- [ ] **Step 8: Run every gate one last time.**

```bash
go test ./... -race && (cd api && go test ./... -race)
make all
golangci-lint run ./... && (cd api && golangci-lint run ./...)
git status --porcelain
```

- [ ] **Step 9: Commit**

```bash
git add storage_e2e_test.go internal/engine README.md
git commit -m "test(storage): the mtime proof, secret containment and a cached golden

Two runs of a pipeline whose first step rewrites a file with identical
content and a fresh mtime, driven through senro.Run, asserting the second
run's downstream Pure() step hits. That is design.md section 11 item 3's
failure mode as an operator would meet it, and the negative half asserts a
real content change still misses. A canary value in a step's environment is
searched for across the whole cache root, the ledger and every log file, with
the variable NAME asserted present so the search is known to be looking. The
cached golden pins the hit path event for event."
```

---

## Self-Review

### Every §3 and §4 requirement in scope, mapped to a task

| Requirement | Where |
|---|---|
| §3.1 two caches, deliberately separate | `internal/cache` (Tasks 7, 8) and `internal/scratch` (Task 10) are separate packages sharing only the CAS |
| §3.2 `Pure()` opt-in, impure never cached | Task 4 declares it, Task 9 enforces it, `TestAnImpureStepIsNeverCachedAndEmitsNoCacheEvents` |
| §3.3 key from named components | Task 7 |
| §3.3 `executorClass` as an equivalence class | Task 9 reads `Executor.Class`, which already returns `local/<os>/<arch>` rather than a host name |
| §3.3 declared platform in the key | Task 9 reads `DeclaredPlatform`, never `ObservedPlatform` |
| §3.3 env intersected with an allowlist | Task 7's `EnvComponent`, Task 4's `CacheEnv` |
| §3.3 secret identities, never values | Task 7's reserved `Secrets` component, Task 13's containment test |
| §3.3 workspace digests in the key | Task 6's `wsManager.digests`, Task 9 |
| §3.4 declared inputs as globs relative to the workspace | Task 4 declares, Task 8's `Resolve`, Task 6's `inputRoot` |
| §3.4 `.senroignore` semantics | Task 2's `LoadIgnoreFile`, applied in Task 5 |
| §3.5 `senro cache explain` | Task 8's `FormatExplain`, Task 9's records, Task 12's command |
| §3.6 `CAS` interface | Task 1 |
| §3.6 `ActionCache` interface | Task 8 |
| §3.6 `Result` holds exit code, output digests, workspace digests, log refs, timings, run ID | Task 8 |
| §3.6 a hit replays the stored logs marked cached | Task 9's `replayLog`, `TestACacheHitReplaysTheStoredLogs` |
| §3.6 local-directory backend | Task 1 |
| §3.6 GC, LRU by access time against a size budget, plus TTL | Task 11 |
| §4.1 named, versioned directory with a content digest | Tasks 3, 6 |
| §4.1 `ScopeRun` | Task 4 declares all three scopes and refuses the two that are not v0 |
| §4.1 `Exclude` | Task 4, applied in Task 5 |
| §4.2 snapshot every writable workspace on completion | Task 6 |
| §4.2 digest recorded in the step result and the event log | Tasks 6 and 9 |
| §4.2 the workspace digest is an input to the next step's key | Task 9 |
| §4.2 exclude `.git`, `node_modules` | Task 2's `DefaultExcludes`, forced on in Task 5 |
| §4.2 size warning above a threshold | Task 12's `ws ls`, with the reason it is not an engine warning |
| §4.2 `NoSnapshot()` | Tasks 4 and 6 |
| §4.3 local realization as a directory the step receives | Task 5 |
| §4.3 hardlink hazard | Task 5 refuses hardlinking outright, per the v0 spec's correction 8 |
| §4.4 restore by exact key then restore-key prefixes, newest first | Task 10 |
| §4.4 save only on success and only when the key is absent | Task 10 |
| §4.4 a lost race is silent | Task 10's `O_EXCL` |
| §4.4 never an input to an action cache key | Task 10, `TestAScratchCacheDoesNotEnterAnActionCacheKey` |
| §4.5 `senro ws ls` | Task 12 |
| §7.6 workspaces snapshot on failure | Task 6, `TestAFailedStepStillSnapshotsItsWorkspace` |
| §11 item 2 `Pure()` trusted, `hermeticity: "trusted"` on entries | Task 8 |
| §11 item 3 normalized tar plus a separate index | Tasks 2, 3, and 13 |

### Deliberately out of scope, and why

Named so the boundary is not re-litigated mid-implementation.

- **S3 and OCI cache backends.** v1. `cas.Store` and `cache.ActionCache` are interfaces so a second implementation is additive.
- **`senro shell`, `ws pull`, `ws diff`.** v1. The index that `ws diff` needs is already stored and already has a reader in `ws ls`.
- **`ScopePersistent` and `ScopeStep`.** Later. Both are declared in the builder and refused at plan time, so a pipeline asking for one gets a clear message instead of a silent promotion.
- **The container executor's bind-mount realization.** design.md §4.3's container row. The container executor does not exist yet; the seam it needs is `executor.Mount.Path`, which Task 5 populates on every mount, so the container plan binds a directory rather than inventing a mechanism.
- **A stat cache to skip rehashing unchanged files.** design.md §4.2 lists it as a mitigation. Deferred deliberately: a content-digest cache keyed on mtime, size and inode has exactly one failure mode, a stale entry, and its consequence is a wrong workspace digest and therefore a wrong cache hit. It is worth adding once the determinism tests in Task 2 are established as the guard, and not before.
- **`.gitignore` inheritance.** design.md §3.4 asks for it. `.senroignore` is implemented; gitignore's negation, directory scoping and nested-file semantics are a package of their own, and a half-implementation silently mis-includes or mis-excludes files, which moves a digest for a reason nobody can see. `LoadIgnoreFile` refuses a negation pattern by name rather than misreading it, which is the honest version of the same care.
- **A teardown-time workspace sweep for abandoned steps.** Every step snapshots its own workspaces while its sandbox is alive, on every path including failure and cancellation, so the only uncovered case is a step goroutine teardown abandoned entirely. `run.finished` already carries `CleanupAbandoned` for exactly that condition.
- **`senro verify --recheck-pure`.** v1. `Result.Hermeticity` is written now so entries produced under enforcement stay distinguishable.

### Placeholder scan

No `TBD`, no "similar to task N", no "add appropriate error handling". Every code step carries the code it describes. The three places that read like deferral are deliberate and each states its reason inline: `Key.Secrets`, `Key.FuncIdentity` and `Key.ToolVersions` are declared and empty, so that populating them later adds a value to an existing component rather than changing the key's shape and invalidating every entry in the fleet.

### Type and signature consistency across tasks

Checked in both directions, and two mismatches between an Interfaces block and its implementation were found and corrected inline:

- `wsManager.scratchMounts` is `func (m *wsManager) scratchMounts(ctx context.Context, n *plan.Node) ([]executor.Mount, error)` in Task 10, matching its body and its call site in `runAttempt`.
- `runCore.serveFromCache` returns a single `bool` in Task 9, matching its body and its call site in `runStep`.

The rest:

- `cas.Digest` is the one digest type, used by every package below `internal/cas`, and no package invents a second string form.
- `workspace.Snapshot{Digest, Index, Bytes, Files}` (Task 3) is converted once, in `localexec.Snapshot`, into `executor.Snapshot{Digest, Index string; Bytes int64; Files int}` (Task 5), and once more into `engine.wsSnapshot` (Task 6). Each conversion crosses a package boundary that exists for a reason, and each is a single function.
- `cache.FileDigest` and `cache.WorkspaceDigest` are declared in Task 7 and used unchanged by Tasks 8, 9 and 11.
- `cache.Key` is built in exactly one place, `runCore.cacheLookup`, and read by `Lookup`, `Save`, `Previous`, `Forget`, `Explain`, `Record` and `FormatExplain`.
- `finishStep` gains one trailing `cached bool` in Task 9, and both call sites are updated in the same task.
- `localexec.New` gains one parameter in Task 5, and both call sites, `run.go` and the package's own tests, are updated in the same task.
- `plan.MountSpec`, `plan.WorkspaceSpec` and `plan.ScratchSpec` are declared in Task 4 and consumed by Tasks 5, 6 and 10 without a second shape.
- `plan.mountsAtWorkDir` (Task 4), `wsManager.inputRoot` (Task 6) and `sandbox.isWorkDirMount` (Task 5) resolve the same rule. They must agree, and Task 6's step 1 says so explicitly, because if they disagree a `Pure()` step hashes files it never read.

### The four lessons, as structure rather than prose

1. **Nothing ships unwired.** Every task states its production caller. Task 1 is the only one that names a later task instead, and it names Task 3 by file. Tasks 12 and 13 exist partly to give the file index and the whole cache path a reader through the real entry point.
2. **Negative cases are planned, not assumed.** A miss beside every hit, a corrupt CAS entry beside a good one, a failed restore beside a successful one, an unallowlisted variable beside an allowlisted one, a scratch miss beside a scratch hit, an expired pin beside a live one, and a content change beside a touched mtime.
3. **The class, not the instance.** Eight tasks carry a **Class, not instance** paragraph: `ReadTar`'s escape check (2), `Restore`'s replacement semantics (3), the cache-only-declaration rule (4), the read-only mount check (6), the incomplete-entry handling (8), the hit degradation (9), scratch's immutability (10), and the GC's single reference enumeration (11).
4. **The check that can see the failure.** Four tasks carry a **The check that catches it** paragraph (2, 4, 6, 7), and ten tasks contain an explicit mutation step where the implementation is broken on purpose and the specific test that must fail is named (1, 2, 5, 6, 7, 8, 9 twice, 10, 11, 12). Task 2's is three separate mutations, because its three tests fail in different combinations and the asymmetry between them is the argument for keeping all three.

---

## Next

Plan 6 is secrets: the mamori seam, the redactor with its encoding variants, delivery through `Sandbox.PutSecret`, and `secret.resolved` and `secret.redacted`. It has one hard dependency on this plan, which is `cache.Key.Secrets`: the component is declared and empty here, and secrets populate it with the `provider:key:version:digest8` identity form, never a value. Do not begin it until `make all` passes here and Task 13's mtime proof is green.
