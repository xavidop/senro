package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// resolveRunDir turns a --run value into a run directory: a path that
// exists, a run ID under ./runs, or, given nothing, the newest directory
// under ./runs. It refuses rather than defaulting to a directory that does
// not exist, since a wrong guess turns every later message into one about a
// missing file rather than a missing run.
//
// The result is always absolute: a relative "runs/r7" is correct only until
// something calls os.Chdir or prints the path in an error read later, and
// `cache explain` and `ws ls` both do exactly that.
func resolveRunDir(flag string) (string, error) {
	if flag != "" {
		if fi, err := os.Stat(flag); err == nil && fi.IsDir() {
			return absRunDir(flag)
		}
		if strings.ContainsRune(flag, os.PathSeparator) {
			return "", fmt.Errorf("no run directory at %q", flag)
		}
		p := filepath.Join("runs", flag)
		if fi, err := os.Stat(p); err == nil && fi.IsDir() {
			return absRunDir(p)
		}
		return "", fmt.Errorf("no run %q (looked for %s)", flag, p)
	}

	found, err := runCandidates()
	if err != nil {
		return "", err
	}
	if len(found) == 0 {
		return "", fmt.Errorf("no runs found under ./runs")
	}
	return absRunDir(filepath.Join("runs", found[0].name))
}

// runCandidate is one entry under ./runs: its directory name and the time
// to sort it by. Not yet resolved to an absolute path or read for its
// state, so listing every run costs one os.ReadDir and nothing else.
type runCandidate struct {
	name string
	when time.Time
}

// runCandidates lists every run directory under ./runs, newest first. The
// one shared implementation behind resolveRunDir's "newest, given nothing"
// default and `senro runs`' listing, so the two can never disagree about
// what "newest" means or which entries count as a run.
func runCandidates() ([]runCandidate, error) {
	entries, err := os.ReadDir("runs")
	if err != nil {
		return nil, fmt.Errorf("no runs directory here: name a run with --run, or run from the directory a pipeline ran in")
	}
	var found []runCandidate
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		found = append(found, runCandidate{name: e.Name(), when: info.ModTime()})
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].when.Equal(found[j].when) {
			return found[i].name > found[j].name
		}
		return found[i].when.After(found[j].when)
	})
	return found, nil
}

// absRunDir makes p absolute. filepath.Abs's only failure mode is Getwd
// failing, which happens only when the working directory has been removed
// out from under the process.
func absRunDir(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("resolve %q to an absolute path: %w", p, err)
	}
	return abs, nil
}
