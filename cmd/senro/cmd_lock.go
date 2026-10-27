package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// lockFileName is the lock `senro lock` writes beside a pipeline and
// `senro run` and `senro up` apply when it is there.
const lockFileName = "senro.lock"

// lockEnv, lockWriteEnv and lockCheckEnv are wire names shared with the
// senro package (see lockEnv there), spelled here for the reason funcPkgEnv
// is: each process owns its half of the contract.
const (
	lockEnv      = "SENRO_LOCK"
	lockWriteEnv = "SENRO_LOCK_WRITE"
	lockCheckEnv = "SENRO_LOCK_CHECK"
)

// lockOptions is what parseLockOptions pulls out of senro lock's arguments.
type lockOptions struct {
	pkg   string
	check bool
	// file is --file, defaulting to <pkg>/senro.lock.
	file         string
	pipelineArgs []string
}

// cmdLock implements `senro lock <pkg> [--check] [--file PATH] [--
// pipeline-args...]`: build and launch the pipeline exactly as senro run
// does, with SENRO_LOCK_WRITE (or SENRO_LOCK_CHECK) set to PATH, so the
// pipeline resolves its own images and writes or checks the lock instead of
// running. The child's stdout ("<reference>  <digest>" lines, or the drift
// under --check) is forwarded verbatim, and so is its exit code: 0 written or
// current, non-zero for a failure or drift, 2 for usage errors.
func cmdLock(args []string, stdout, stderr io.Writer) int {
	opts, err := parseLockOptions(args)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro lock:", err)
		return exitUsage
	}
	run := runOptions{pkg: opts.pkg, ui: string(uiNone), pipelineArgs: opts.pipelineArgs}
	return runBuilt("lock", run, false, runHooks{env: lockChildEnv(opts)}, stdout, stderr, false)
}

// parseLockOptions splits senro lock's arguments the way parseRunOptions
// splits senro run's: flags anywhere before "--", one package path, and
// everything after "--" for the pipeline.
func parseLockOptions(args []string) (lockOptions, error) {
	var o lockOptions
	var rest []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			o.pipelineArgs = args[i+1:]
			break
		}
		switch {
		case a == "--check":
			o.check = true
		case a == "--file":
			if i+1 >= len(args) {
				return lockOptions{}, errors.New("--file requires a path")
			}
			o.file = args[i+1]
			i++
		case strings.HasPrefix(a, "--file="):
			o.file = strings.TrimPrefix(a, "--file=")
			if o.file == "" {
				return lockOptions{}, errors.New("--file requires a path")
			}
		case strings.HasPrefix(a, "-"):
			return lockOptions{}, fmt.Errorf("unknown flag %q", a)
		default:
			rest = append(rest, a)
		}
	}
	switch len(rest) {
	case 0:
		return lockOptions{}, errors.New("missing package path (usage: senro lock <pkg> " +
			"[--check] [--file PATH] [-- pipeline-args...])")
	case 1:
		o.pkg = rest[0]
	default:
		return lockOptions{}, fmt.Errorf("unexpected extra arguments: %v", rest[1:])
	}
	if o.file == "" {
		o.file = filepath.Join(o.pkg, lockFileName)
	}
	return o, nil
}

// lockChildEnv is childEnv for senro lock: the lock mode set, and every lock
// variable this process inherited removed, so the child never applies a lock
// while it writes or checks one.
func lockChildEnv(o lockOptions) []string {
	env := []string{funcPkgEnv + "=" + o.pkg}
	for _, e := range os.Environ() {
		name, _, _ := strings.Cut(e, "=")
		switch name {
		case lockEnv, lockWriteEnv, lockCheckEnv, funcPkgEnv:
			continue
		}
		env = append(env, e)
	}
	if o.check {
		return append(env, lockCheckEnv+"="+o.file)
	}
	return append(env, lockWriteEnv+"="+o.file)
}

// existingLock returns <pkg>/senro.lock when that file exists, for childEnv.
func existingLock(pkg string) (string, bool) {
	path := filepath.Join(pkg, lockFileName)
	fi, err := os.Stat(path)
	if err != nil || !fi.Mode().IsRegular() {
		return "", false
	}
	return path, true
}
