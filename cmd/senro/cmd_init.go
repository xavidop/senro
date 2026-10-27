package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/xavidop/senro/kit/all"
)

// initReplaceEnv names a directory to point the senro module at in a go.mod
// senro init writes. It is undocumented on purpose: it exists so this
// repository's tests can build a generated pipeline against the working tree
// instead of a published release.
const initReplaceEnv = "SENRO_INIT_REPLACE"

// initOptions is a parsed `senro init` command line.
type initOptions struct {
	dir, out              string
	force, dryRun, noTidy bool
}

// parseInitOptions parses `senro init [DIR] [--out ci] [--force] [--dry-run]
// [--no-tidy]`. DIR defaults to "." and --out to "ci".
func parseInitOptions(args []string) (initOptions, error) {
	o := initOptions{dir: ".", out: "ci"}
	var rest []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--out":
			if i+1 >= len(args) || args[i+1] == "" {
				return initOptions{}, errors.New("--out requires a directory")
			}
			o.out = args[i+1]
			i++
		case strings.HasPrefix(a, "--out="):
			o.out = strings.TrimPrefix(a, "--out=")
			if o.out == "" {
				return initOptions{}, errors.New("--out requires a directory")
			}
		case a == "--force":
			o.force = true
		case a == "--dry-run":
			o.dryRun = true
		case a == "--no-tidy":
			o.noTidy = true
		case strings.HasPrefix(a, "-") && a != "-":
			return initOptions{}, fmt.Errorf("unknown flag %q (usage: senro init [DIR] [--out ci] [--force] "+
				"[--dry-run] [--no-tidy])", a)
		default:
			rest = append(rest, a)
		}
	}
	switch len(rest) {
	case 0:
	case 1:
		o.dir = rest[0]
	default:
		return initOptions{}, fmt.Errorf("unexpected extra arguments: %v", rest[1:])
	}
	return o, nil
}

// cmdInit implements `senro init`: detect which kits apply under DIR, write
// <out>/main.go with one step (or one expansion) per kit, and make the
// package buildable.
//
// Exit codes: 0 written (or printed, with --dry-run), 1 a file could not be
// read or written, 2 usage errors, including nothing detected and an
// existing <out>/main.go without --force. A failing `go get` or
// `go mod tidy` is a warning, not a failure: the file is already written.
func cmdInit(args []string, stdout, stderr io.Writer) int {
	opts, err := parseInitOptions(args)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init:", err)
		return exitUsage
	}
	// Absolute before anything else, so detection and the kits' lockfile
	// searches stop at the repository and never climb past it.
	dir, err := filepath.Abs(opts.dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init:", err)
		return exitUsage
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		_, _ = fmt.Fprintf(stderr, "senro init: %s is not a directory\n", opts.dir)
		return exitUsage
	}

	ctx := context.Background()
	matches, err := all.Detect(ctx, dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init: detect:", err)
		return exitRunFailed
	}
	if len(matches) == 0 {
		_, _ = fmt.Fprintf(stderr, "senro init: no kit applies to %s; it looked for:\n"+
			"  gotest      a go.mod\n"+
			"  pytest      pytest configured in pyproject.toml, pytest.ini, tox.ini or setup.cfg\n"+
			"  eslint      a package.json with an ESLint config or eslint as a dependency\n"+
			"  playwright  a playwright.config file\n",
			opts.dir)
		return exitUsage
	}

	name := moduleSegment(filepath.Base(dir))
	src, err := renderInit(name, matches)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init:", err)
		return exitRunFailed
	}
	if opts.dryRun {
		_, _ = stdout.Write(src)
		return exitSuccess
	}

	outDir := opts.out
	if !filepath.IsAbs(outDir) {
		outDir = filepath.Join(dir, outDir)
	}
	mainGo := filepath.Join(outDir, "main.go")
	shownMain := filepath.Join(opts.out, "main.go")
	if _, err := os.Stat(mainGo); err == nil && !opts.force {
		_, _ = fmt.Fprintf(stderr, "senro init: %s already exists; pass --force to replace it, "+
			"or --dry-run to print what would be written\n", shownMain)
		return exitUsage
	}

	_, _ = fmt.Fprintln(stderr, "senro init: detected")
	for _, m := range matches {
		_, _ = fmt.Fprintln(stderr, "  "+matchLine(m))
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init:", err)
		return exitRunFailed
	}
	if err := os.WriteFile(mainGo, src, 0o644); err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init:", err)
		return exitRunFailed
	}
	written := []string{shownMain}

	wroteMod := !fileExists(filepath.Join(dir, "go.mod")) && !fileExists(filepath.Join(outDir, "go.mod"))
	if err := initModule(ctx, dir, outDir, cliVersion(), opts.noTidy, stderr); err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init:", err)
		return exitRunFailed
	}
	if wroteMod {
		written = append(written, filepath.Join(opts.out, "go.mod"))
	}

	for _, w := range written {
		_, _ = fmt.Fprintln(stderr, "Wrote", w)
	}
	run := "senro run ./" + filepath.ToSlash(filepath.Clean(opts.out))
	if filepath.IsAbs(opts.out) {
		run = "senro run " + opts.out
	}
	if cwd, err := os.Getwd(); err != nil || cwd != dir {
		run = "cd " + opts.dir + " && " + run
	}
	_, _ = fmt.Fprintln(stderr, "Next:", run)
	return exitSuccess
}

// initModule makes the package in out buildable, step 4 of senro init. When
// dir already holds a go.mod the pipeline joins that module; when out holds
// one it is used as is; otherwise out/go.mod is written, naming the
// toolchain's Go version and, when SENRO_INIT_REPLACE is set, a replace of
// the senro module. It then runs `go get github.com/xavidop/senro@version`
// and `go mod tidy` in the module's directory, or with noTidy prints them
// instead.
//
// Only writing out/go.mod can fail it: a command that fails, or a missing go binary, is a warning on stderr, since
// the pipeline file is already written and the module can be fixed by hand.
func initModule(ctx context.Context, dir, out, version string, noTidy bool, stderr io.Writer) error {
	modDir := out
	switch {
	case fileExists(filepath.Join(dir, "go.mod")):
		modDir = dir
	case fileExists(filepath.Join(out, "go.mod")):
	default:
		var b strings.Builder
		fmt.Fprintf(&b, "module %s/%s\n", moduleSegment(filepath.Base(dir)), moduleSegment(filepath.Base(out)))
		if v := goDirective(runtime.Version()); v != "" {
			fmt.Fprintf(&b, "\ngo %s\n", v)
		}
		if r := os.Getenv(initReplaceEnv); r != "" {
			fmt.Fprintf(&b, "\nreplace %s => %s\n", senroModule, r)
		}
		if err := os.WriteFile(filepath.Join(out, "go.mod"), []byte(b.String()), 0o644); err != nil {
			return err
		}
	}

	cmds := [][]string{
		{"get", senroModule + "@" + version},
		{"mod", "tidy"},
	}
	printCmds := func() {
		_, _ = fmt.Fprintf(stderr, "Run these in %s before senro run:\n", modDir)
		for _, c := range cmds {
			_, _ = fmt.Fprintln(stderr, "  go "+strings.Join(c, " "))
		}
	}
	if noTidy {
		printCmds()
		return nil
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro init: warning: no Go toolchain found on PATH; the module needs "+
			"these commands before senro run")
		printCmds()
		return nil
	}
	for _, c := range cmds {
		cmd := exec.CommandContext(ctx, goBin, c...)
		cmd.Dir = modDir
		cmd.Stdout = stderr
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			_, _ = fmt.Fprintf(stderr, "senro init: warning: go %s in %s: %v; the module needs it before senro run\n",
				strings.Join(c, " "), modDir, err)
			return nil
		}
	}
	return nil
}

// cliVersion is the senro module version this binary was built from, which is
// the version a generated pipeline should require, or "latest" for a
// development build that has no version of its own.
func cliVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "latest"
	}
	v := info.Main.Version
	if v == "" || v == "(devel)" || strings.Contains(v, "+dirty") {
		return "latest"
	}
	return v
}

// goDirective turns a runtime.Version such as "go1.26.9" into the go.mod
// directive "1.26", or "" when the version is not a release.
func goDirective(v string) string {
	i := strings.Index(v, "go1.")
	if i < 0 {
		return ""
	}
	parts := strings.SplitN(v[i+len("go"):], ".", 3)
	if len(parts) < 2 {
		return ""
	}
	minor := parts[1]
	if j := strings.IndexFunc(minor, func(r rune) bool { return r < '0' || r > '9' }); j >= 0 {
		minor = minor[:j]
	}
	if minor == "" {
		return ""
	}
	return parts[0] + "." + minor
}

// moduleSegment makes a directory name usable as a module path element and
// a pipeline name: anything outside letters, digits, ".", "_" and "-"
// becomes "-", and an empty or dot-only name becomes "pipeline".
func moduleSegment(name string) string {
	s := idSegment(name)
	if strings.Trim(s, ".") == "" {
		return "pipeline"
	}
	return s
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}
