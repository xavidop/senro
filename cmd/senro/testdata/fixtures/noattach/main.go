// Command noattach never calls attach.Listen: it prints its own arguments
// (proving `senro run ... -- args` forwarding) and exits with a distinct,
// checkable code. It stands in for a plain `./pipeline` with no flags
// (plain streaming output, no socket, no TUI), and for `senro run` it
// exercises the "no attach server ever registered" fallback: relay this
// process's own stdout/stderr and propagate its exit code directly, since
// there is no run status to derive one from.
package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println(strings.Join(os.Args[1:], "|"))
	os.Exit(42)
}
