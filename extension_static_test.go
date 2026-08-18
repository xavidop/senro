package senro_test

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// extensionPackages are the two worked examples that stand in for code
// somebody else wrote: a notifier built on notify.Renderer and an event
// source built on trigger.Provider. See examples/extensions.
const extensionPackages = "./examples/extensions/..."

// TestAnExtensionImportsOnlySenrosPublicSurface: the worked examples live in
// this module, so the compiler would never object to them importing
// internal/, and they could document an extension point nobody outside this
// repository could actually use. Refusing any DIRECT import under internal/
// asks the same question as "would this compile in somebody else's module".
// Transitive imports are deliberately not checked (notify itself uses
// internal/sink, and may, because it is senro); what matters is the import
// line an extension has to write. The toolchain is asked rather than the
// source grepped: a resolved import graph is a fact, a grep is a guess.
func TestAnExtensionImportsOnlySenrosPublicSurface(t *testing.T) {
	const internalPrefix = "github.com/xavidop/senro/internal/"

	// Imports, TestImports and XTestImports together: an extension whose own
	// tests reached into internal/ could not be run outside this repository,
	// and plain .Imports would never see it.
	const format = `{{.ImportPath}}{{range .Imports}} {{.}}{{end}}` +
		`{{range .TestImports}} {{.}}{{end}}{{range .XTestImports}} {{.}}{{end}}`

	cmd := exec.Command("go", "list", "-f", format, extensionPackages)
	cmd.Env = append(os.Environ(), "GOWORK=off")

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", extensionPackages, err, stderr.String())
	}

	var packages int
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		packages++
		self := fields[0]
		for _, imp := range fields[1:] {
			if strings.HasPrefix(imp, internalPrefix) {
				t.Errorf("%s imports %q: an extension that needs anything under internal/ is not "+
					"an extension anybody outside this repository could write, so the extension "+
					"point it demonstrates does not actually exist", self, imp)
			}
		}
	}
	if packages < 2 {
		t.Fatalf("go list %s found %d packages, want at least the notifier and the event source: "+
			"a check that silently stops checking anything is worse than no check",
			extensionPackages, packages)
	}
}
