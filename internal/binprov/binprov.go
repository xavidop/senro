// Package binprov obtains the binary a remote step will be re-entered as.
// A func step's body is compiled into the pipeline binary, so running one
// off the coordinator means putting THAT BINARY on the target (see
// internal/stepwire); this package gets the copy, internal/executor stages
// it.
//
// Two strategies: identity (target platform equals the coordinator's, ship
// os.Executable(), nothing to build) and an on-demand
// GOOS/GOARCH/CGO_ENABLED=0 cross-build of the package the coordinator was
// built from, cached by package identity and platform (needs a toolchain
// and Options.Pkg).
//
// cgo is checked BEFORE compiling: CGO_ENABLED=0 does not fail on a
// cgo-dependent graph so much as quietly build something else, surfacing as
// a name that will not resolve on host 47 rather than a compiler error.
// internal/cgocheck asks with cgo ENABLED, the only way to see the
// dangerous packages, and is the same detector `senro func check` prints.
//
// Windows is refused by name: a staged binary runs through a POSIX shell
// over ssh, chmodded 0700 and reaped with rm -rf, none of which reaches a
// Windows host.
package binprov

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/xavidop/senro/internal/cgocheck"
	"github.com/xavidop/senro/internal/executor"
)

// Strategy names how a Binary was obtained, reported in the binary.staged
// event: "slow because it compiled" and "slow because the link was slow"
// are different problems.
type Strategy string

const (
	// Identity is the coordinator's own executable, shipped unchanged.
	Identity Strategy = "identity"
	// CrossBuild is a fresh compile for the target platform.
	CrossBuild Strategy = "cross-build"
)

// Binary is one coordinator-side file, ready to be staged on a target.
// Digest makes staging content-addressed and skew fatal: it names the
// target path (senro-<digest>), the child reports it on handshake, and a
// disagreement aborts the step.
type Binary struct {
	Path     string
	Digest   string
	Size     int64
	Platform executor.Platform
	Strategy Strategy
}

// Options configure a Provisioner.
type Options struct {
	// Dir is where cross-built binaries are cached. Left empty, a cross-build
	// is refused rather than written somewhere arbitrary.
	Dir string

	// Pkg is the package the coordinator was built from: a directory or any
	// pattern `go build` accepts. Needed only to cross-build, and there is
	// no way to derive it: the build path is not recorded in the binary.
	Pkg string

	// Self overrides os.Executable, SelfPlatform overrides
	// runtime.GOOS/GOARCH: both for tests that force a cross-build for a
	// platform the test machine can actually execute.
	Self         string
	SelfPlatform executor.Platform
}

// Provisioner hands out one Binary per target platform, building at most once
// for each.
type Provisioner struct {
	opts Options

	mu   sync.Mutex
	done map[executor.Platform]result
}

type result struct {
	bin Binary
	err error
}

// New returns a Provisioner. It contacts nothing and compiles nothing: the
// first For does that, and only if it has to.
func New(opts Options) *Provisioner {
	return &Provisioner{opts: opts, done: map[executor.Platform]result{}}
}

// For returns a binary that runs on target.
//
// Concurrent calls for one platform are serialised, so twelve func steps on
// one architecture compile once. One lock for the whole build rather than
// per platform, deliberately: `go build` already saturates the machine, and
// two racing are slower than either alone.
func (p *Provisioner) For(ctx context.Context, target executor.Platform) (Binary, error) {
	if target.OS == "" || target.Arch == "" {
		return Binary{}, fmt.Errorf(
			"binprov: cannot provision a binary for %q: the target's platform is not fully known", target)
	}
	if target.OS == "windows" {
		return Binary{}, fmt.Errorf(
			"binprov: %s is not supported: senro stages a step binary through a POSIX shell, at a "+
				"path it chmods 0700 and reaps with rm -rf, and none of that reaches a Windows host. "+
				"Run func steps for this target on the coordinator instead, or use an Exec step",
			target)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.done[target]; ok {
		return r.bin, r.err
	}
	bin, err := p.provision(ctx, target)
	p.done[target] = result{bin: bin, err: err}
	return bin, err
}

func (p *Provisioner) provision(ctx context.Context, target executor.Platform) (Binary, error) {
	if target == p.self() {
		return p.identity(target)
	}
	return p.crossBuild(ctx, target)
}

// self is the coordinator's own platform.
func (p *Provisioner) self() executor.Platform {
	if p.opts.SelfPlatform.OS != "" {
		return p.opts.SelfPlatform
	}
	return executor.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
}

// selfPath is the coordinator's own executable.
func (p *Provisioner) selfPath() (string, error) {
	if p.opts.Self != "" {
		return p.opts.Self, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("binprov: locating this binary to stage it on a remote target: %w", err)
	}
	return exe, nil
}

func (p *Provisioner) identity(target executor.Platform) (Binary, error) {
	exe, err := p.selfPath()
	if err != nil {
		return Binary{}, err
	}
	fi, err := os.Stat(exe)
	if err != nil {
		return Binary{}, fmt.Errorf("binprov: reading %s to stage it on a remote target: %w", exe, err)
	}
	digest, err := Digest(exe)
	if err != nil {
		return Binary{}, err
	}
	return Binary{
		Path: exe, Digest: digest, Size: fi.Size(),
		Platform: target, Strategy: Identity,
	}, nil
}

// crossBuild compiles the coordinator's own package for target, or reuses the
// compile a previous run already cached.
func (p *Provisioner) crossBuild(ctx context.Context, target executor.Platform) (Binary, error) {
	if p.opts.Pkg == "" {
		return Binary{}, fmt.Errorf(
			"binprov: this step runs a func on a %s target and this coordinator is %s, so the "+
				"binary has to be cross-compiled, and senro was not told which package to compile. "+
				"Pass senro.WithFuncBuild(\"./ci\") naming the package this program was built from, "+
				"or set SENRO_FUNC_PKG to it. `senro run` sets it for you",
			target, p.self())
	}
	if p.opts.Dir == "" {
		return Binary{}, errors.New(
			"binprov: no cache directory was configured for cross-built binaries")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		return Binary{}, fmt.Errorf(
			"binprov: this step runs a func on a %s target and this coordinator is %s, so the "+
				"binary has to be cross-compiled, and there is no Go toolchain on PATH to "+
				"cross-compile it with. Install one on the coordinator, or run this step on a "+
				"target of the coordinator's own platform, where senro ships this binary as it is",
			target, p.self())
	}

	// The key is the coordinator's own bytes, the package, and the target
	// platform. The coordinator's digest stands in for the source, the
	// toolchain and the flags at once: it moves whenever any of them does,
	// stricter than a source-tree digest and costing one hash of a file
	// hashed anyway. The package MUST be in the key: a coordinator can be
	// asked to build something other than what it was built from (this
	// package's tests do), and leaving it out let one cached build answer
	// for a different package.
	self, err := p.selfPath()
	if err != nil {
		return Binary{}, err
	}
	selfDigest, err := Digest(self)
	if err != nil {
		return Binary{}, err
	}
	// Absolute: the build runs in the package's directory, and a relative
	// -o would land beside the caller's source rather than in the cache.
	root, err := filepath.Abs(p.opts.Dir)
	if err != nil {
		return Binary{}, fmt.Errorf("binprov: resolving the cross-build cache directory: %w", err)
	}
	key := sha256.Sum256([]byte(selfDigest + "\x00" + p.opts.Pkg))
	dir := filepath.Join(root,
		fmt.Sprintf("%s-%s-%s", target.OS, target.Arch, hex.EncodeToString(key[:8])))
	out := filepath.Join(dir, "senro")

	if fi, err := os.Stat(out); err == nil && fi.Mode().IsRegular() {
		digest, err := Digest(out)
		if err != nil {
			return Binary{}, err
		}
		return Binary{
			Path: out, Digest: digest, Size: fi.Size(),
			Platform: target, Strategy: CrossBuild,
		}, nil
	}

	if err := p.checkCgo(ctx); err != nil {
		return Binary{}, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Binary{}, fmt.Errorf("binprov: preparing the cross-build cache: %w", err)
	}

	// Built to a temp name and renamed over, so a second coordinator sees
	// no file or a whole one, never a half-written binary it would run.
	tmp, err := os.CreateTemp(dir, ".senro-*")
	if err != nil {
		return Binary{}, fmt.Errorf("binprov: preparing the cross-build cache: %w", err)
	}
	tmpName := tmp.Name()
	_ = tmp.Close()
	defer func() { _ = os.Remove(tmpName) }()

	if err := p.build(ctx, goBin, target, tmpName); err != nil {
		return Binary{}, err
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		return Binary{}, fmt.Errorf("binprov: %w", err)
	}
	if err := os.Rename(tmpName, out); err != nil {
		return Binary{}, fmt.Errorf("binprov: publishing the cross-built binary: %w", err)
	}

	fi, err := os.Stat(out)
	if err != nil {
		return Binary{}, fmt.Errorf("binprov: %w", err)
	}
	digest, err := Digest(out)
	if err != nil {
		return Binary{}, err
	}
	return Binary{
		Path: out, Digest: digest, Size: fi.Size(),
		Platform: target, Strategy: CrossBuild,
	}, nil
}

// build is the compile itself.
//
// netgo and osusergo plus a static external link, so glibc/musl skew stops
// being a category of bug: a binary resolving hostnames through the host's
// libc fails on Alpine with a missing shared object. CGO_ENABLED=0 already
// forces the pure-Go paths; the tags say so explicitly, and
// -extldflags=-static keeps the intent true if an external link ever
// appears.
func (p *Provisioner) build(ctx context.Context, goBin string, target executor.Platform, out string) error {
	dir, pattern := p.resolvePkg()
	cmd := exec.CommandContext(ctx, goBin, "build",
		"-tags", "netgo,osusergo",
		"-ldflags", "-extldflags=-static",
		"-o", out,
		pattern,
	)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GOOS="+target.OS,
		"GOARCH="+target.Arch,
		"CGO_ENABLED=0",
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	cmd.Stdout = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"binprov: cross-compiling %s for %s: %w%s",
			p.opts.Pkg, target, err, detail(stderr.String()))
	}
	return nil
}

// checkCgo refuses a graph that cannot honestly be built with
// CGO_ENABLED=0, naming the offending import path and its chain. The report
// comes from internal/cgocheck, the same detector `senro func check`
// prints, so refusal and report can never disagree.
func (p *Provisioner) checkCgo(ctx context.Context) error {
	dir, pattern := p.resolvePkg()
	if dir == "" {
		dir = "."
	}
	offenders, err := cgocheck.Check(ctx, dir, pattern)
	if err != nil {
		return fmt.Errorf("binprov: checking %s for cgo before cross-compiling it: %w", p.opts.Pkg, err)
	}
	if len(offenders) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b,
		"binprov: %s cannot be cross-compiled for another platform: %d package(s) in its "+
			"dependency graph compile a cgo file, and a cross-compile is built with "+
			"CGO_ENABLED=0, which cannot link their C dependency for the target",
		p.opts.Pkg, len(offenders))
	for _, o := range offenders {
		fmt.Fprintf(&b, "\n  %s (%s)\n    via: %s",
			o.ImportPath, strings.Join(o.CgoFiles, ", "), strings.Join(o.Chain, " -> "))
	}
	b.WriteString("\n`senro func check` reports the whole graph. " +
		"Common causes: os/user (build with -tags osusergo), net (-tags netgo), " +
		"and any package wrapping a C library")
	return errors.New(b.String())
}

// resolvePkg splits Options.Pkg into the directory to run the toolchain in
// and the pattern to name to it. A directory on disk becomes (dir, "."):
// `go build` refuses a path outside the coordinator's module as a pattern.
// One resolution, shared by the cgo check and the build, so the two can
// never ask about different packages.
func (p *Provisioner) resolvePkg() (dir, pattern string) {
	if fi, err := os.Stat(p.opts.Pkg); err == nil && fi.IsDir() {
		return p.opts.Pkg, "."
	}
	return "", p.opts.Pkg
}

// Digest is sha256 of a file's bytes, in senro's "sha256:<hex>" form. Here
// rather than in the engine because three callers need the same answer: the
// engine (cache identity), this package (staged path, skew check), and the
// step child reporting what it actually is.
func Digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("binprov: reading %s to digest it: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("binprov: hashing %s: %w", path, err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// SelfDigest is Digest of this process's own executable.
func SelfDigest() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("binprov: locating this binary to digest it: %w", err)
	}
	return Digest(exe)
}

// detail renders a compiler's own output under an error, or nothing when it
// said nothing.
func detail(s string) string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return ""
	}
	return ":\n" + s
}
