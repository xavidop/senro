//go:build !(js && wasm)

// This file exists so `go build ./...` and `go vet ./...` on an ordinary
// host still have a buildable package here: every other file is behind
// //go:build js && wasm, and a package whose files are all excluded is an
// error for ./..., not a skip. The alternatives are worse: an underscore
// directory hides the client from vet and editors, and a separate module
// defeats a program whose whole point is importing this one's api package.
// Built for anything but js/wasm, it says what it is and exits.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr,
		"senro browser UI client: this program is only meaningful compiled to WebAssembly. "+
			"Build it with `make wasm`, which runs GOOS=js GOARCH=wasm go build against this package "+
			"and puts the result where internal/webui embeds it from.")
	os.Exit(2)
}
