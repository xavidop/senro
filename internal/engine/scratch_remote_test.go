package engine_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// remoteExecutor stands in for k8sexec and sshexec: a target that does not
// share the coordinator's filesystem, so the only thing that can say what the
// step left in a mount is ReadMount. It leaves a marker the coordinator's own
// copy never had, which is what makes "the run saved the wrong directory"
// visible rather than merely likely.
type remoteExecutor struct {
	left    string
	readErr error
	reads   atomic.Int64
}

func (r *remoteExecutor) Class(context.Context) (string, error) { return "fake/remote", nil }

func (r *remoteExecutor) DeclaredPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "linux", Arch: "amd64"}, nil
}

func (r *remoteExecutor) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return declared, nil
}

func (r *remoteExecutor) Sandbox(context.Context, executor.SandboxSpec) (executor.Sandbox, error) {
	return &remoteSandbox{owner: r}, nil
}

type remoteSandbox struct{ owner *remoteExecutor }

func (s *remoteSandbox) ObservedPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "linux", Arch: "amd64"}, nil
}
func (s *remoteSandbox) Snapshot(context.Context, string) (executor.Snapshot, error) {
	return executor.Snapshot{}, nil
}
func (s *remoteSandbox) PutSecret(context.Context, string, []byte) (string, error) {
	return "/dev/null", nil
}
func (s *remoteSandbox) Run(context.Context, executor.Cmd, io.Writer, io.Writer) (int, error) {
	return 0, nil
}
func (s *remoteSandbox) Close(context.Context, bool) error { return nil }

// ReadMount is the whole point of the fake: it hands back a directory the
// coordinator never wrote, exactly as a pod's or a host's copy comes back.
func (s *remoteSandbox) ReadMount(_ context.Context, _, dest string) error {
	s.owner.reads.Add(1)
	if s.owner.readErr != nil {
		return s.owner.readErr
	}
	return os.WriteFile(filepath.Join(dest, "marker"), []byte(s.owner.left), 0o644)
}

var _ executor.MountReader = (*remoteSandbox)(nil)

// remoteScratchPlan is one step on a target with no shared filesystem,
// mounting one scratch cache.
func remoteScratchPlan() *plan.Plan {
	return &plan.Plan{
		Version: 1,
		Scratch: []plan.ScratchSpec{{Name: "deps", Key: "deps-v1"}},
		Nodes: []plan.Node{{
			ID: "install", Kind: "exec", Cmd: []string{"true"},
			Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07"},
			Mounts:   []plan.MountSpec{{Scratch: "deps", At: "/m"}},
		}},
	}
}

func runRemoteScratch(t *testing.T, ex *remoteExecutor, cacheRoot string) (string, api.RunStatus) {
	t.Helper()
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	p := remoteScratchPlan()
	runDir := filepath.Join(t.TempDir(), "run")
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Storage: store, Sink: sink.Recording(), RunID: "r1",
		Executors: map[string]executor.Executor{p.Nodes[0].Executor.Key(): ex},
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	return runDir, status
}

// restored materializes what the store holds under key, or reports a miss.
func restored(t *testing.T, cacheRoot, key string) (string, bool) {
	t.Helper()
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	dest := filepath.Join(t.TempDir(), "restored")
	if _, ok, err := store.Scratch.Restore(context.Background(), key, nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	} else if !ok {
		return "", false
	}
	b, err := os.ReadFile(filepath.Join(dest, "marker")) // #nosec G304 -- a path this test named
	if err != nil {
		return "", true
	}
	return string(b), true
}

// The bytes saved under a scratch cache's key must be the bytes the remote
// step left, not the copy the coordinator sent out. Saving the coordinator's
// own directory is the failure the refusal these executors used to carry was
// protecting against, and an entry is written once under its key and never
// rewritten, so it would be the answer every later run got.
func TestAScratchCacheOnARemoteTargetIsSavedFromWhatCameBack(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	ex := &remoteExecutor{left: "left by the remote step"}

	runDir, status := runRemoteScratch(t, ex, cacheRoot)
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}
	if ex.reads.Load() != 1 {
		t.Fatalf("the run read the mount back %d time(s), want exactly 1", ex.reads.Load())
	}

	recs, err := scratch.ReadRecords(filepath.Join(runDir, "cache"))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || !recs[0].Saved || recs[0].Unread {
		t.Fatalf("scratch records = %+v, want one saved entry", recs)
	}

	got, ok := restored(t, cacheRoot, "deps-v1")
	if !ok {
		t.Fatal("nothing was stored under deps-v1")
	}
	if got != "left by the remote step" {
		t.Errorf("the entry holds %q, want what the remote step left: the run saved the "+
			"coordinator's own copy instead", got)
	}
}

// A read-back that fails must save NOTHING. The coordinator still holds the
// copy it sent out, and storing that would put a stale tree under a key that
// can never be rewritten: the run reports the skip instead.
func TestAScratchCacheIsNotSavedWhenTheRemoteCopyDoesNotComeBack(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	ex := &remoteExecutor{left: "unreachable", readErr: errors.New("the pod went away")}

	runDir, status := runRemoteScratch(t, ex, cacheRoot)
	// Best-effort, still: a cache that could not be read back does not fail
	// a step that succeeded.
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded: a scratch cache costs time, never correctness", status)
	}

	recs, err := scratch.ReadRecords(filepath.Join(runDir, "cache"))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].Saved || !recs[0].Unread {
		t.Fatalf("scratch records = %+v, want one entry recorded as unread and not saved", recs)
	}
	if _, ok := restored(t, cacheRoot, "deps-v1"); ok {
		t.Error("the run stored an entry under deps-v1 even though the remote copy never came " +
			"back, so every later run would be served the coordinator's stale copy")
	}
}

// The read-back copies are inputs to the save and nothing else: left behind
// they would double the run directory's size for every remote step.
func TestAReadBackScratchCopyDoesNotSurviveTheRun(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	runDir, _ := runRemoteScratch(t, &remoteExecutor{left: "x"}, cacheRoot)

	entries, err := os.ReadDir(filepath.Join(runDir, "scratch"))
	if err != nil {
		t.Fatalf("read the run's scratch directory: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".senro-read-") {
			t.Errorf("the run directory still holds the read-back copy %q", e.Name())
		}
	}
}

// handoffPlan is the shape the build-time refusal used to forbid: one scratch
// cache mounted by a step on a machine of its own AND by a step on the
// coordinator's filesystem, ordered so they cannot overlap.
func handoffPlan() *plan.Plan {
	return &plan.Plan{
		Version: 1,
		Scratch: []plan.ScratchSpec{{Name: "deps", Key: "deps-v1"}},
		Nodes: []plan.Node{
			{
				ID: "install", Kind: "exec", Cmd: []string{"true"},
				Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07"},
				Mounts:   []plan.MountSpec{{Scratch: "deps", At: "/m"}},
			},
			{
				// Reads what the remote step left and writes proof it saw it.
				ID: "verify", Kind: "exec",
				Cmd:    []string{"sh", "-c", "cp m/marker m/seen"},
				Needs:  []string{"install"},
				Mounts: []plan.MountSpec{{Scratch: "deps", At: "/m"}},
			},
		},
	}
}

// TestARemoteStepHandsItsScratchCacheToALocalStep is the whole hand-off: the
// coordinator step must see what the remote step produced, not the copy that
// was sent out to it.
//
// Without the swap in readScratch this fails at the `cp`, because m/marker
// only ever existed on the far side and the coordinator's directory still
// holds what it sent.
func TestARemoteStepHandsItsScratchCacheToALocalStep(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ex := &remoteExecutor{left: "from-the-pod"}
	p := handoffPlan()
	runDir := filepath.Join(t.TempDir(), "run")
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Storage: store, Sink: sink.Recording(), RunID: "r1",
		Executor:  localexec.New(runDir, store.Snapshotter),
		Executors: map[string]executor.Executor{p.Nodes[0].Executor.Key(): ex},
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("run = %v, want succeeded: the local step could not read what the remote left",
			status)
	}

	// The saved entry must hold BOTH: the remote step's tree and what the
	// coordinator step added on top of it. One lineage, not two.
	dest := filepath.Join(t.TempDir(), "restored")
	if _, ok, err := store.Scratch.Restore(context.Background(), "deps-v1", nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	} else if !ok {
		t.Fatal("nothing was saved for a cache both steps mounted")
	}
	marker, err := os.ReadFile(filepath.Join(dest, "marker")) // #nosec G304 -- a path this test named
	if err != nil {
		t.Fatalf("the remote step's own tree is missing from the entry: %v", err)
	}
	if string(marker) != "from-the-pod" {
		t.Errorf("marker = %q, want from-the-pod", marker)
	}
	seen, err := os.ReadFile(filepath.Join(dest, "seen")) // #nosec G304 -- a path this test named
	if err != nil {
		t.Fatalf("the coordinator step's own write is missing from the entry: %v", err)
	}
	if string(seen) != "from-the-pod" {
		t.Errorf("seen = %q, so the local step did not read the remote step's tree", seen)
	}
}

// A read-back that fails stores nothing, even for a hand-off: the entry would
// be written once and never rewritten, so an incomplete tree saved now is
// what every later run is served.
func TestAFailedReadBackStoresNothingForAHandoff(t *testing.T) {
	cacheRoot := filepath.Join(t.TempDir(), "cache")
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ex := &remoteExecutor{left: "unused", readErr: errors.New("the pod went away")}
	p := handoffPlan()
	// The local step cannot read a marker that never arrived, so it is only
	// asked to succeed here; what is under test is what the run STORED.
	p.Nodes[1].Cmd = []string{"true"}
	runDir := filepath.Join(t.TempDir(), "run")
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Storage: store, Sink: sink.Recording(), RunID: "r1",
		Executor:  localexec.New(runDir, store.Snapshotter),
		Executors: map[string]executor.Executor{p.Nodes[0].Executor.Key(): ex},
	}); err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	if _, ok, err := store.Scratch.Restore(context.Background(), "deps-v1", nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	} else if ok {
		t.Fatal("a hand-off whose read-back failed stored an entry anyway, under an immutable key")
	}
}
