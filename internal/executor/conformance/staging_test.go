package conformance_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// stagedFile writes a stand-in for the engine's own binary and addresses it
// by its REAL digest, so two targets asked to stage the same bytes agree on
// the name without anything here saying what the name should be.
func stagedFile(t *testing.T, body string) senroexec.StagedBinary {
	t.Helper()
	path := filepath.Join(t.TempDir(), "senro")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("writing the binary to stage: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	return senroexec.StagedBinary{
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		Path:   path,
		Size:   fi.Size(),
	}
}

// stagerFor returns the target's BinaryStager, or skips. An executor that
// cannot host a re-entered step says so by not implementing this, and the
// engine refuses up front naming it; the local executor is never asked,
// because a func step runs in the coordinator's own process there.
func stagerFor(t *testing.T, tg target, ex senroexec.Executor) senroexec.BinaryStager {
	t.Helper()
	st, ok := ex.(senroexec.BinaryStager)
	if !ok {
		if tg.name != "local" {
			t.Fatalf("%s implements no BinaryStager, so no func step can run there", tg.name)
		}
		t.Skip("a func step runs in the coordinator's own process on the local executor")
	}
	return st
}

// TestAStagedBinaryLandsUnderItsOwnDigestAndIsExecutable. The name is what
// makes staging amortize across steps, runs and coordinators, and it is a
// convention shared by targets that never see each other's filesystems: it
// has to be the same rule everywhere or a second coordinator re-transfers a
// binary that is already there.
func TestAStagedBinaryLandsUnderItsOwnDigestAndIsExecutable(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			st := stagerFor(t, tg, ex)

			bin := stagedFile(t, "#!/bin/sh\necho staged-and-run\n")
			want, err := senroexec.StagedName(bin.Digest)
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			got, err := st.StageBinary(ctx, bin)
			if err != nil {
				t.Fatalf("StageBinary: %v", err)
			}
			if !strings.HasSuffix(got.Path, want) {
				t.Errorf("staged at %q, want a path ending in %q: the name is the convention that "+
					"lets a second coordinator find a binary already there", got.Path, want)
			}

			// Deliberately NOT run here. Whether an ordinary sandbox can
			// SEE the staged binary is an executor's own business:
			// containerexec binds it into every container it makes, while
			// k8sexec mounts it only into a pod built for a func step (its
			// TestAnOrdinaryStepDoesNotGetTheStagedBinaryInItsPod says so on
			// purpose). That the staged bytes really run is proved
			// end-to-end, on all three, by TestAFuncStepRunsOffTheCoordinator.
			if got.Path == "" {
				t.Error("StageBinary reported no path at all")
			}
		})
	}
}

// TestStagingTheSameBinaryTwiceIsIdempotent. Staging is paid once per target
// for the life of a run, so the second call must return the same path and
// must not corrupt what the first put there.
func TestStagingTheSameBinaryTwiceIsIdempotent(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			st := stagerFor(t, tg, ex)

			bin := stagedFile(t, "#!/bin/sh\necho twice\n")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			first, err := st.StageBinary(ctx, bin)
			if err != nil {
				t.Fatalf("first StageBinary: %v", err)
			}
			second, err := st.StageBinary(ctx, bin)
			if err != nil {
				t.Fatalf("second StageBinary: %v", err)
			}
			if first.Path != second.Path {
				t.Errorf("staged at %q then %q: one digest must name one path", first.Path, second.Path)
			}
			// Reused answers "did senro have to move the binary for this
			// step", not "was there already a copy over there": an executor
			// that moves nothing reports true from the FIRST call
			// (containerexec, whose target is the coordinator's own machine).
			// So the only portable claim is that the answer is not a lie
			// about the first call having been free.
			if first.Reused && !second.Reused {
				t.Errorf("the first staging reported reused and the second did not, which is the "+
					"one order that cannot be true: %+v then %+v", first, second)
			}
		})
	}
}

// TestStagingRefusesADigestItCannotName. StagedName is the shared rule, and
// every stager must apply it BEFORE moving bytes: a malformed digest that
// reached the target would put a file somewhere nothing can find again.
func TestStagingRefusesADigestItCannotName(t *testing.T) {
	bad := []struct{ name, digest string }{
		{"no algorithm prefix", "deadbeef"},
		{"empty hex", "sha256:"},
		{"uppercase hex", "sha256:DEADBEEF"},
		{"not hex at all", "sha256:zzzz"},
		{"empty", ""},
	}
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			st := stagerFor(t, tg, ex)

			path := filepath.Join(t.TempDir(), "senro")
			if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			for _, tc := range bad {
				got, err := st.StageBinary(ctx,
					senroexec.StagedBinary{Digest: tc.digest, Path: path, Size: 10})
				if err == nil {
					t.Errorf("%s: StageBinary(%q) staged at %q and returned no error",
						tc.name, tc.digest, got.Path)
				}
			}
		})
	}
}

// TestStagingReportsAMissingCoordinatorSideFileAsInfrastructure. The engine's
// own binary not being where the provisioner said is senro's bookkeeping
// failure, not the pipeline's.
func TestStagingReportsAMissingCoordinatorSideFileAsInfrastructure(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			st := stagerFor(t, tg, ex)

			bin := stagedFile(t, "#!/bin/sh\n")
			if err := os.Remove(bin.Path); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			if _, err := st.StageBinary(ctx, bin); err == nil {
				t.Error("StageBinary succeeded for a file the coordinator does not have")
			} else if !senroexec.IsInfra(err) {
				t.Errorf("err = %v, want ErrInfra: a binary senro cannot find is senro's own "+
					"bookkeeping failure, not the workload's", err)
			}
		})
	}
}
