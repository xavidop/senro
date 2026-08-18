// Package mountsnap captures one mounted workspace, the same way for every
// executor that shares the coordinator's filesystem.
//
// It sits below internal/executor because that package is deliberately free
// of the tar and index code (see its Snapshot type): a future executor may
// report a digest computed elsewhere. This package is for the executors that
// compute it here.
package mountsnap

import (
	"context"
	"fmt"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/workspace"
)

// Excluder is what "part of this mount" means, for the snapshot taken here
// and for the tar mountxfer.Send writes alike: one definition, because two
// that disagree by one file are a round trip that is not one.
//
// For a WORKSPACE the default excludes come first and are mandatory: a
// pipeline that forgot .git and node_modules still gets them,
// PreserveSymlinks selects the widened set, and the workspace's own
// .senroignore is appended last, keeping this identical to
// engine.wsManager.excluderFor's answer.
//
// A SCRATCH cache excludes NOTHING. internal/scratch saves and restores the
// directory whole, and node_modules is the usual content rather than
// something to skip: a workspace's defaults applied here would hand a remote
// step a cache with its own contents missing and then store that hollow tree
// under a key nothing can rewrite.
//
// The excluder is usable even when err is non-nil, which is what lets
// mountxfer ignore an unreadable .senroignore: a transfer must not fail for
// it, and Snapshot reports it where it belongs, in the step's own result.
func Excluder(m executor.Mount) (*workspace.Excluder, error) {
	if m.Scratch {
		return workspace.NewExcluder(), nil
	}
	patterns := append(workspace.DefaultExcludesFor(m.PreserveSymlinks), m.Exclude...)
	extra, err := workspace.LoadIgnoreFile(m.Path)
	if err != nil {
		return workspace.NewExcluder(patterns...), err
	}
	return workspace.NewExcluder(append(patterns, extra...)...), nil
}

// Snapshot captures m's coordinator-side directory, over exactly the file
// set Excluder describes.
func Snapshot(ctx context.Context, s *workspace.Snapshotter, m executor.Mount) (executor.Snapshot, error) {
	if s == nil {
		return executor.Snapshot{}, fmt.Errorf("mountsnap: no snapshotter for mount %q", m.Name)
	}
	ex, err := Excluder(m)
	if err != nil {
		return executor.Snapshot{}, err
	}
	snap, err := s.Snapshot(ctx, m.Path, ex)
	if err != nil {
		return executor.Snapshot{}, err
	}
	return executor.Snapshot{
		Digest: string(snap.Digest), Index: string(snap.Index),
		Bytes: snap.Bytes, Files: snap.Files,
	}, nil
}
