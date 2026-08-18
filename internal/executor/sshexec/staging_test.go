package sshexec_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/sshexec"
	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
)

// stagedFile writes a stand-in for the engine's own binary: staging does not
// care what the bytes are, only that the same bytes are addressed by the same
// digest.
func stagedFile(t *testing.T, body string) senroexec.StagedBinary {
	t.Helper()
	path := filepath.Join(t.TempDir(), "senro")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("writing the binary to stage: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// A digest whose bytes are this test's own, so two tests never collide on
	// one remote path and a rerun of this test finds its own file.
	return senroexec.StagedBinary{
		Digest: "sha256:" + strings.Repeat("0", 40) + digestSuffix(t.Name()),
		Path:   path,
		Size:   fi.Size(),
	}
}

// digestSuffix turns a test name into 24 lowercase hex characters, so the
// synthetic digests above are the right shape and are unique per test.
func digestSuffix(name string) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 24)
	h := uint64(1469598103934665603)
	for i := 0; i < len(out); i++ {
		for j := 0; j < len(name); j++ {
			h ^= uint64(name[j]) + uint64(i)
			h *= 1099511628211
		}
		out[i] = hex[h%16]
	}
	return string(out)
}

func TestStagingPutsTheBinaryOnTheHostAtItsOwnDigest(t *testing.T) {
	ex, srv := newExecutor(t)
	bin := stagedFile(t, "#!/bin/sh\necho staged\n")

	got, err := ex.StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	if got.Reused {
		t.Error("the first staging reported reused; nothing was there to reuse")
	}
	if !strings.Contains(got.Path, strings.TrimPrefix(bin.Digest, "sha256:")) {
		t.Errorf("staged at %q, want a path naming the digest", got.Path)
	}

	// The bytes really are there, checked from outside the code under test.
	body, code := probe(t, srv, "cat "+shellQuote(got.Path))
	if code != 0 {
		t.Fatalf("reading the staged binary on the host: exit %d: %s", code, body)
	}
	if strings.TrimRight(body, "\n") != "#!/bin/sh\necho staged" {
		t.Errorf("the staged file holds %q, want the coordinator's own bytes", body)
	}
}

// 0700 on both the directory and the file: this is an executable senro put in
// somebody else's account, and every other user on that host is somebody who
// should not be able to read it, let alone run it.
func TestAStagedBinaryIsPrivateToTheAccountAndExecutable(t *testing.T) {
	ex, srv := newExecutor(t)
	bin := stagedFile(t, "#!/bin/sh\nexit 0\n")

	got, err := ex.StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}

	mode, code := probe(t, srv, "ls -ld "+shellQuote(got.Path)+" | cut -c1-10")
	if code != 0 {
		t.Fatalf("stat on the host: exit %d: %s", code, mode)
	}
	if want := "-rwx------"; strings.TrimSpace(mode) != want {
		t.Errorf("the staged binary is mode %q, want %q", strings.TrimSpace(mode), want)
	}

	dirMode, code := probe(t, srv, "ls -ld "+shellQuote(filepath.Dir(got.Path))+" | cut -c1-10")
	if code != 0 {
		t.Fatalf("stat on the host: exit %d: %s", code, dirMode)
	}
	if want := "drwx------"; strings.TrimSpace(dirMode) != want {
		t.Errorf("the staging directory is mode %q, want %q", strings.TrimSpace(dirMode), want)
	}
}

// The amortization, at this layer: a second request for a digest already on
// the host transfers nothing.
func TestASecondStagingOfTheSameDigestTransfersNothing(t *testing.T) {
	ex, srv := newExecutor(t)
	bin := stagedFile(t, "#!/bin/sh\nexit 0\n")

	first, err := ex.StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary (first): %v", err)
	}
	// The inode, not the mtime: staging publishes with a rename, so a second
	// transfer replaces the file with a different one and the inode is the
	// thing that cannot survive it. BusyBox `ls -l` reports a timestamp only
	// to the minute, which a fast test would pass through unchanged whether
	// or not it re-uploaded.
	const inode = "ls -li %s | awk '{print $1}'"
	before, code := probe(t, srv, fmt.Sprintf(inode, shellQuote(first.Path)))
	if code != 0 {
		t.Fatalf("stat on the host: exit %d: %s", code, before)
	}

	second, err := ex.StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary (second): %v", err)
	}
	if !second.Reused {
		t.Error("the second staging did not report reused")
	}
	if second.Path != first.Path {
		t.Errorf("second staging landed at %q, want the same %q", second.Path, first.Path)
	}
	after, code := probe(t, srv, fmt.Sprintf(inode, shellQuote(first.Path)))
	if code != 0 {
		t.Fatalf("stat on the host: exit %d: %s", code, after)
	}
	if after != before {
		t.Errorf("the file was replaced between stagings: inode %s then %s",
			strings.TrimSpace(before), strings.TrimSpace(after))
	}
}

// And it amortizes across coordinators, not merely across calls on one
// Executor value: the check is a question asked of the host, not a map in
// this process. A second run of the same release on the same host must not
// transfer 40 MiB again.
func TestAFreshExecutorFindsAnAlreadyStagedBinaryWithoutTransferringIt(t *testing.T) {
	srv := sshdtest.Require(t)
	bin := stagedFile(t, "#!/bin/sh\nexit 0\n")

	first, err := newExecutorOn(t, srv).StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary (first executor): %v", err)
	}
	if first.Reused {
		t.Fatal("the first staging reported reused")
	}

	second, err := newExecutorOn(t, srv, sshexec.WithRunID("another-run")).
		StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary (second executor): %v", err)
	}
	if !second.Reused {
		t.Error("a fresh executor re-transferred a binary the host already had")
	}
	if second.Path != first.Path {
		t.Errorf("second executor staged at %q, want %q", second.Path, first.Path)
	}
}

// A file of the right name but the wrong size is a truncated upload from a
// coordinator that died, not a binary to run. It is replaced rather than
// trusted, because the alternative is executing half a binary forever.
func TestATruncatedStagedBinaryIsReplacedRatherThanTrusted(t *testing.T) {
	ex, srv := newExecutor(t)
	bin := stagedFile(t, "#!/bin/sh\necho whole\n")

	got, err := ex.StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	if out, code := probe(t, srv,
		"printf 'x' > "+shellQuote(got.Path)+" && chmod 700 "+shellQuote(got.Path)); code != 0 {
		t.Fatalf("truncating the staged binary: exit %d: %s", code, out)
	}

	again, err := newExecutorOn(t, srv).StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary (after truncation): %v", err)
	}
	if again.Reused {
		t.Error("a truncated file was reused")
	}
	body, code := probe(t, srv, "cat "+shellQuote(got.Path))
	if code != 0 {
		t.Fatalf("reading the staged binary: exit %d: %s", code, body)
	}
	if strings.TrimRight(body, "\n") != "#!/bin/sh\necho whole" {
		t.Errorf("the staged file holds %q after re-staging, want the whole binary", body)
	}
}

// The staged binary outlives the attempt directories, or the amortization is
// a fiction: Close removes an attempt's own tree and the reaper removes it
// later, and neither may take the binary with it.
func TestClosingASandboxDoesNotRemoveTheStagedBinary(t *testing.T) {
	ex, srv := newExecutor(t)
	bin := stagedFile(t, "#!/bin/sh\nexit 0\n")

	staged, err := ex.StageBinary(context.Background(), bin)
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}

	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	if err := sb.Close(context.Background(), false); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if out, code := probe(t, srv, "test -x "+shellQuote(staged.Path)); code != 0 {
		t.Errorf("the staged binary is gone after a sandbox closed: exit %d: %s", code, out)
	}
}

func TestStagingRefusesABinaryWithNoDigest(t *testing.T) {
	ex, _ := newExecutor(t)
	_, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{Path: "/nope"})
	if err == nil || !strings.Contains(err.Error(), "digest") {
		t.Errorf("StageBinary with no digest returned %v, want a refusal naming the digest", err)
	}
}

func TestStagingReportsAMissingCoordinatorSideFileAsInfrastructure(t *testing.T) {
	ex, _ := newExecutor(t)
	_, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{
		Digest: "sha256:" + strings.Repeat("a", 64),
		Path:   filepath.Join(t.TempDir(), "does-not-exist"),
		Size:   1,
	})
	if err == nil {
		t.Fatal("StageBinary of a missing file succeeded")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("StageBinary of a missing file returned %v, want an infrastructure failure", err)
	}
}

// A remote func step has to be told where its workspaces landed on the HOST,
// and only the sandbox that put them there knows.
func TestASandboxReportsWhereEachMountLandedOnTheHost(t *testing.T) {
	ex, srv := newExecutor(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: src, At: "/src"}},
	})

	loc, ok := sb.(senroexec.MountLocator)
	if !ok {
		t.Fatal("the ssh sandbox does not implement MountLocator")
	}
	path, ok := loc.MountPath("src")
	if !ok {
		t.Fatal("MountPath(\"src\") reported no path for a mount this sandbox realized")
	}
	if out, code := probe(t, srv, "cat "+shellQuote(path+"/f.txt")); code != 0 || out != "hi" {
		t.Errorf("reading %s on the host gave (%q, %d), want the mounted file", path, out, code)
	}
	if _, ok := loc.MountPath("nothing-mounted-here"); ok {
		t.Error("MountPath reported a path for a mount that does not exist")
	}
}
