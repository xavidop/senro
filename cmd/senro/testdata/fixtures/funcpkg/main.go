// Command funcpkg prints the package `senro run` says it was built from, and
// exits with a distinct, checkable code.
//
// That variable is the one thing a pipeline binary cannot work out for
// itself: a Go program records nothing about where its own source is, and a
// func step on a remote host of another platform has to be cross-compiled
// from it. `senro run` knows, having just built it, and this fixture is how
// a test can see that it actually said so.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("SENRO_FUNC_PKG=" + os.Getenv("SENRO_FUNC_PKG"))
	os.Exit(43)
}
