package binprov_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/executor"
)

func hostPlatform() executor.Platform {
	return executor.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
}

// --- 1. Identity: the target platform equals the coordinator's ---

func TestATargetOnTheCoordinatorsOwnPlatformShipsThisBinaryUnchanged(t *testing.T) {
	p := binprov.New(binprov.Options{Dir: t.TempDir()})

	b, err := p.For(context.Background(), hostPlatform())
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if b.Strategy != binprov.Identity {
		t.Errorf("strategy = %q, want %q", b.Strategy, binprov.Identity)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	if b.Path != exe {
		t.Errorf("path = %q, want this process's own executable %q", b.Path, exe)
	}
	want, err := binprov.Digest(exe)
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	if b.Digest != want {
		t.Errorf("digest = %q, want %q", b.Digest, want)
	}
	if b.Platform != hostPlatform() {
		t.Errorf("platform = %v, want %v", b.Platform, hostPlatform())
	}
	if b.Size <= 0 {
		t.Errorf("size = %d, want the executable's own size", b.Size)
	}
}

// Identity costs nothing, and "nothing" has to include not needing a Go
// toolchain or a package to build: a Linux coordinator driving Linux hosts is
// the common case and must work on a machine with no compiler on it.
func TestIdentityNeedsNeitherAToolchainNorAPackage(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	p := binprov.New(binprov.Options{Dir: t.TempDir()}) // no Pkg

	if _, err := p.For(context.Background(), hostPlatform()); err != nil {
		t.Fatalf("For on the coordinator's own platform: %v", err)
	}
}

// --- 2. On-demand cross-build ---

// forceCrossBuild makes every target a cross-build by telling the provisioner
// the coordinator is a platform nothing will ask for, so the result is a
// binary this machine can actually execute and assert about.
func forceCrossBuild(opts binprov.Options) binprov.Options {
	opts.SelfPlatform = executor.Platform{OS: "plan9", Arch: "mips"}
	return opts
}

// probeModule is a module whose main package reports, at run time, which
// build tags and cgo setting it was compiled with. That is how the build
// flags are checked: not by inspecting the command line the provisioner
// assembled, which would only prove it says what it says, but by running what
// it produced.
func probeModule(t *testing.T) string {
	t.Helper()
	return writeModule(t, map[string]string{
		"go.mod": "module example.com/probe\n\ngo 1.26\n",
		"main.go": "package main\n\nimport \"fmt\"\n\n" +
			"var tags []string\n\n" +
			"func main() { fmt.Println(tags) }\n",
		"netgo.go":     "//go:build netgo\n\npackage main\n\nfunc init() { tags = append(tags, \"netgo\") }\n",
		"osusergo.go":  "//go:build osusergo\n\npackage main\n\nfunc init() { tags = append(tags, \"osusergo\") }\n",
		"nocgo.go":     "//go:build !cgo\n\npackage main\n\nfunc init() { tags = append(tags, \"nocgo\") }\n",
		"withcgo.go":   "//go:build cgo\n\npackage main\n\nfunc init() { tags = append(tags, \"cgo\") }\n",
		"untagged.go":  "//go:build !netgo\n\npackage main\n\nfunc init() { tags = append(tags, \"no-netgo\") }\n",
		"untagged2.go": "//go:build !osusergo\n\npackage main\n\nfunc init() { tags = append(tags, \"no-osusergo\") }\n",
	})
}

func TestACrossBuildUsesNetgoOsusergoAndCgoDisabled(t *testing.T) {
	dir := probeModule(t)
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: t.TempDir(), Pkg: dir}))

	b, err := p.For(context.Background(), hostPlatform())
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if b.Strategy != binprov.CrossBuild {
		t.Fatalf("strategy = %q, want %q", b.Strategy, binprov.CrossBuild)
	}

	out, err := exec.Command(b.Path).Output()
	if err != nil {
		t.Fatalf("running the cross-built binary: %v", err)
	}
	got := string(out)
	for _, want := range []string{"netgo", "osusergo", "nocgo"} {
		if !strings.Contains(got, want) {
			t.Errorf("the cross-built binary reports %q, which does not include %q", strings.TrimSpace(got), want)
		}
	}
	for _, unwanted := range []string{"no-netgo", "no-osusergo", " cgo"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("the cross-built binary reports %q, which should not include %q", strings.TrimSpace(got), unwanted)
		}
	}
}

func TestACrossBuiltBinaryIsDigestedFromItsOwnBytes(t *testing.T) {
	dir := probeModule(t)
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: t.TempDir(), Pkg: dir}))

	b, err := p.For(context.Background(), hostPlatform())
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	want, err := binprov.Digest(b.Path)
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	if b.Digest != want {
		t.Errorf("digest = %q, want the file's own %q", b.Digest, want)
	}
	fi, err := os.Stat(b.Path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if b.Size != fi.Size() {
		t.Errorf("size = %d, want %d", b.Size, fi.Size())
	}
	if fi.Mode().Perm()&0o100 == 0 {
		t.Errorf("the cross-built binary is mode %v, which is not executable", fi.Mode().Perm())
	}
}

// The build is cached by platform, so a run with twelve func steps on two
// architectures compiles twice, not twenty-four times.
func TestASecondRequestForTheSamePlatformDoesNotBuildAgain(t *testing.T) {
	dir := probeModule(t)
	cache := t.TempDir()
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: cache, Pkg: dir}))

	first, err := p.For(context.Background(), hostPlatform())
	if err != nil {
		t.Fatalf("For (first): %v", err)
	}
	before, err := os.Stat(first.Path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	// A brand new provisioner, so this is the ON-DISK cache being reused,
	// not an in-memory memo that would evaporate between runs.
	second, err := binprov.New(forceCrossBuild(binprov.Options{Dir: cache, Pkg: dir})).
		For(context.Background(), hostPlatform())
	if err != nil {
		t.Fatalf("For (second): %v", err)
	}
	if second.Path != first.Path || second.Digest != first.Digest {
		t.Fatalf("second request produced %q/%q, want the cached %q/%q",
			second.Path, second.Digest, first.Path, first.Digest)
	}
	after, err := os.Stat(second.Path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("the binary was rebuilt: mtime moved from %v to %v", before.ModTime(), after.ModTime())
	}
}

// Two packages must not share one cached build. A key that was only the
// coordinator's digest and the platform let a build of package A answer a
// request for package B, and the symptom was the cgo refusal below never
// firing: a clean module had been built first, and the tainted one was served
// its binary.
func TestTwoPackagesDoNotShareOneCachedBuild(t *testing.T) {
	cache := t.TempDir()
	first := binprov.New(forceCrossBuild(binprov.Options{Dir: cache, Pkg: probeModule(t)}))
	if _, err := first.For(context.Background(), hostPlatform()); err != nil {
		t.Fatalf("For (first package): %v", err)
	}

	other := writeModule(t, map[string]string{
		"go.mod":  "module example.com/other\n\ngo 1.26\n",
		"main.go": "package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(\"other\") }\n",
	})
	second, err := binprov.New(forceCrossBuild(binprov.Options{Dir: cache, Pkg: other})).
		For(context.Background(), hostPlatform())
	if err != nil {
		t.Fatalf("For (second package): %v", err)
	}
	out, err := exec.Command(second.Path).Output()
	if err != nil {
		t.Fatalf("running the second package's binary: %v", err)
	}
	if strings.TrimSpace(string(out)) != "other" {
		t.Errorf("the second package's binary printed %q; it was served the first package's build", out)
	}
}

// --- 3. Refusals ---

func TestAWindowsTargetIsRefusedRatherThanHalfSupported(t *testing.T) {
	p := binprov.New(binprov.Options{Dir: t.TempDir(), Pkg: "."})

	_, err := p.For(context.Background(), executor.Platform{OS: "windows", Arch: "amd64"})
	if err == nil {
		t.Fatal("For(windows) succeeded; want a refusal")
	}
	if !strings.Contains(err.Error(), "windows") || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("For(windows) = %v, want a refusal naming windows and saying it is not supported", err)
	}
}

func TestACrossBuildWithNoGoToolchainSaysSoAndNamesTheAlternative(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: t.TempDir(), Pkg: "."}))

	_, err := p.For(context.Background(), hostPlatform())
	if err == nil {
		t.Fatal("For with no go on PATH succeeded; want a refusal")
	}
	for _, want := range []string{"Go toolchain", "cross-compile"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestACrossBuildWithNoPackageNamesBothWaysToSupplyOne(t *testing.T) {
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: t.TempDir()}))

	_, err := p.For(context.Background(), hostPlatform())
	if err == nil {
		t.Fatal("For with no package succeeded; want a refusal")
	}
	for _, want := range []string{"WithFuncBuild", "SENRO_FUNC_PKG"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// The cgo detector already exists and already produces the offending import
// path and the chain that pulled it in. A cross-build refuses on its report
// rather than running a second analysis of its own.
func TestACgoTaintedGraphIsRefusedWithTheImportPathAndTheChain(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/tainted\n\ngo 1.26\n",
		"main.go": "package main\n\nimport _ \"example.com/tainted/inner\"\n\nfunc main() {}\n",
		"inner/inner.go": "package inner\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func Free() { C.free(nil) }\n",
	})
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: t.TempDir(), Pkg: dir}))

	_, err := p.For(context.Background(), hostPlatform())
	if err == nil {
		t.Fatal("For on a cgo-tainted module succeeded; want a refusal")
	}
	msg := err.Error()
	if !strings.Contains(msg, "example.com/tainted/inner") {
		t.Errorf("error %q does not name the offending import path", msg)
	}
	if !strings.Contains(msg, "example.com/tainted ->") {
		t.Errorf("error %q does not carry the chain that pulled it in", msg)
	}
	if !strings.Contains(msg, "senro func check") {
		t.Errorf("error %q does not point at the command that reports the whole graph", msg)
	}
}

func TestABuildFailureCarriesTheCompilersOwnOutput(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/broken\n\ngo 1.26\n",
		"main.go": "package main\n\nfunc main() { thisIsNotDefined() }\n",
	})
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: t.TempDir(), Pkg: dir}))

	_, err := p.For(context.Background(), hostPlatform())
	if err == nil {
		t.Fatal("For on a module that does not compile succeeded; want a refusal")
	}
	if !strings.Contains(err.Error(), "thisIsNotDefined") {
		t.Errorf("error %q does not carry the compiler's own message", err)
	}
}

// A build that failed must leave nothing behind that a later run would find
// and mistake for a usable binary.
func TestAFailedBuildLeavesNoBinaryInTheCache(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/broken2\n\ngo 1.26\n",
		"main.go": "package main\n\nfunc main() { alsoNotDefined() }\n",
	})
	cache := t.TempDir()
	p := binprov.New(forceCrossBuild(binprov.Options{Dir: cache, Pkg: dir}))

	if _, err := p.For(context.Background(), hostPlatform()); err == nil {
		t.Fatal("For succeeded; want a refusal")
	}
	var found []string
	err := filepath.WalkDir(cache, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the cache: %v", err)
	}
	if len(found) != 0 {
		t.Errorf("a failed build left %v in the cache", found)
	}
}

// --- Digest ---

func TestDigestIsSha256OfTheFilesBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := binprov.Digest(path)
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	const want = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Errorf("Digest = %q, want %q", got, want)
	}
}

func TestDigestOfAMissingFileIsAnError(t *testing.T) {
	if _, err := binprov.Digest(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("Digest of a missing file succeeded; want an error")
	}
}

func writeModule(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("WriteFile %s: %v", path, err)
		}
	}
	return dir
}
