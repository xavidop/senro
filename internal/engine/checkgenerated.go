package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// maxDriftPaths is how many changed paths one drift sentence names before
// it counts the rest.
const maxDriftPaths = 10

// checkGenerated compares, for every read-write mount of n that was
// snapshotted, the index of the snapshot the mount was realised from with
// the index of the snapshot the attempt just took. It returns nil when no
// workspace changed, and otherwise one sentence per changed workspace.
//
// snaps already excludes what snapshotMounts skips (a claim-backed or
// step-scoped workspace), and a read-only mount is the read-only breach
// check's business, so only snapshotted read-write mounts are compared.
func (rc *runCore) checkGenerated(
	ctx context.Context, n *plan.Node, mounts []executor.Mount, snaps []wsSnapshot,
) error {
	if rc.ws == nil {
		return nil
	}
	from := make(map[string]executor.Mount, len(mounts))
	for _, mt := range mounts {
		from[mt.Name] = mt
	}
	var sentences []string
	for _, s := range snaps {
		mt, ok := from[s.Name]
		if !ok || mt.RO || cas.Digest(mt.Digest) == s.Digest {
			// Equal body digests are equal trees: snapshots are normalized,
			// so the common up-to-date case reads no index at all.
			continue
		}
		before, err := rc.ws.mountedIndex(ctx, cas.Digest(mt.Digest))
		if err != nil {
			return checkGeneratedInfra(n.ID, s.Name, "read the file index of the snapshot it was mounted from", err)
		}
		if s.Index == "" {
			return checkGeneratedInfra(n.ID, s.Name, "read the file index of the post-step snapshot",
				errors.New("the snapshot carries no index"))
		}
		after, err := rc.ws.snap.LoadIndex(ctx, s.Index)
		if err != nil {
			return checkGeneratedInfra(n.ID, s.Name, "read the file index of the post-step snapshot", err)
		}
		if drift := generatedDrift(before, after); len(drift) > 0 {
			sentences = append(sentences, driftSentence(s.Name, drift))
		}
	}
	if len(sentences) == 0 {
		return nil
	}
	return errors.New(strings.Join(sentences, "; "))
}

// checkGeneratedInfra is the error for a check that could not read an
// index. That is the storage's failure, neither the generator's nor drift,
// so it wraps executor.ErrInfra and says so: the step's result is unknown,
// not wrong.
func checkGeneratedInfra(step, ws, what string, err error) error {
	return fmt.Errorf(
		"engine: %w: step %q: CheckGenerated could not %s for workspace %q, so whether the "+
			"generated files are up to date is unknown (an infrastructure problem, not drift): %w",
		executor.ErrInfra, step, what, ws, err)
}

// generatedDrift is every path that differs between before and after,
// sorted: added, removed, modified, a mode change and a kind change alike,
// since each one means the committed tree is not what the generator makes.
func generatedDrift(before, after workspace.Index) []string {
	changes := workspace.Diff(before, after)
	out := make([]string, 0, len(changes))
	for _, c := range changes {
		out = append(out, c.Path)
	}
	return out
}

// driftSentence is the failure text for one changed workspace, naming at
// most maxDriftPaths paths and counting the rest.
func driftSentence(name string, paths []string) string {
	shown := paths
	var more string
	if len(paths) > maxDriftPaths {
		shown = paths[:maxDriftPaths]
		more = fmt.Sprintf(" and %d more", len(paths)-maxDriftPaths)
	}
	return fmt.Sprintf("generated files are out of date in workspace %s: %d changed: %s%s",
		name, len(paths), strings.Join(shown, ", "), more)
}
