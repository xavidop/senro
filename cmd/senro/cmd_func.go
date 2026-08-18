package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/xavidop/senro/internal/cgocheck"
)

// funcUsage documents cmdFunc's one subcommand today. Spelled as a group
// ("func check") because other func surfaces will land under the same noun,
// and moving a command later breaks scripts and muscle memory.
//
// A Func step on an ssh host, in a container or in a pod whose platform
// differs from the coordinator's is cross-compiled with CGO_ENABLED=0 (see
// internal/binprov), and a cgo package in the graph is what breaks that. A
// container image and a pod are both linux, so a macOS coordinator
// cross-compiles for every one of those. A finding is therefore a warning
// about where a pipeline can run, not about whether it runs at all.
const funcUsage = `Usage:
  senro func check [--dir DIR] [packages...]
      Report every cgo-dependent package in a module's dependency graph, with
      the import chain that pulled each one in. Exit 1 when any is found.

      A Func step on an ssh host, in a container or in a pod of another
      platform is cross-compiled with CGO_ENABLED=0, and a cross-compiled
      binary cannot link a C library for a platform it is not building on, so
      every package reported here has to leave the graph before those steps can
      run there. A container image and a pod are both linux: a macOS
      coordinator cross-compiles for every one of them. Steps on the
      coordinator, and steps on a target that matches the coordinator's own
      platform, ship this binary as it is and are unaffected.
`

// cmdFunc implements `senro func check`.
func cmdFunc(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, funcUsage)
		return exitUsage
	}
	switch args[0] {
	case "check":
		return cmdFuncCheck(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "senro: unknown func subcommand %q\n\n%s", args[0], funcUsage)
		return exitUsage
	}
}

// cmdFuncCheck implements `senro func check [--dir DIR] [packages...]`,
// against the exit-code contract in exitcode.go: exitUsage when
// cgocheck.Check itself errors, since nothing about the module's cgo status
// was determined; exitRunFailed when the check completes and finds
// cgo-tainted packages, reusing the value that already means "not clean"
// rather than fragmenting the contract; exitSuccess when the graph is
// clean.
func cmdFuncCheck(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("senro func check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "module directory to analyse")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	found, err := cgocheck.Check(context.Background(), *dir, fs.Args()...)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "senro: %v\n", err)
		return exitUsage
	}
	if len(found) == 0 {
		_, _ = fmt.Fprintf(stdout, "no cgo in the dependency graph of %s\n", *dir)
		return exitSuccess
	}
	_, _ = fmt.Fprintf(stdout,
		"%d cgo-dependent package(s) in %s.\n\n"+
			"A senro func step cross-compiled for another platform is built with CGO_ENABLED=0,\n"+
			"which cannot link a cgo package's C dependency for the target platform, so every\n"+
			"package below has to leave the graph before these steps can run on an ssh host, in\n"+
			"a container or in a pod of another platform. Steps on the coordinator, and steps on a\n"+
			"target that matches the coordinator's platform, ship this binary as it is and are\n"+
			"unaffected.\n\n",
		len(found), *dir)
	for _, o := range found {
		_, _ = fmt.Fprintf(stdout, "  %s\n    files: %s\n    via:   %s\n\n",
			o.ImportPath, strings.Join(o.CgoFiles, ", "), strings.Join(o.Chain, " -> "))
	}
	_, _ = fmt.Fprint(stdout,
		"Common causes: os/user (build with -tags osusergo), net (-tags netgo),\n"+
			"and any package wrapping a C library.\n")
	return exitRunFailed
}
