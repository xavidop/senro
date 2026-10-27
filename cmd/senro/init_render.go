package main

import (
	"bytes"
	"fmt"
	"go/format"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/all"
)

const senroModule = "github.com/xavidop/senro"

// fanOutGraph is, for a kit that renders an Expand when it matches more than
// once, the unit graph's import path and the call that builds it. A kit
// missing here renders one Add per match.
var fanOutGraph = map[string]struct{ importPath, call string }{
	"gotest": {senroModule + "/unit/gowork", "gowork.Modules()"},
	"pytest": {senroModule + "/unit/pyproject", "pyproject.Packages()"},
}

// initTemplate is the generated main.go before go/format. It is the
// quickstart's shape with the detected kits in place of hand-written steps.
var initTemplate = template.Must(template.New("init").Parse(`// senro init wrote this file from the checks it found below; edit it freely.
//
{{range .Found}}//	{{.}}
{{end}}package main

import (
	"context"
	"log"
	"os"

{{range .Imports}}	{{printf "%q" .}}
{{end}})

func main() {
	ctx := context.Background()

	p := senro.New({{printf "%q" .Name}})

	verify := p.Workflow("verify")
{{range .Steps}}	{{.}}
{{end}}
	// Opens a unix socket a second terminal can attach to while this runs.
	att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket})
	if err != nil {
		log.Fatal(err)
	}
	defer att.Close()

	// Run builds p first, so a duplicate id or a bad option surfaces here,
	// before anything executes.
	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
`))

// matchLine is how one match is shown to a person, in the generated file's
// doc comment and in senro init's summary: kit, directory, reason.
func matchLine(m kit.Match) string {
	return fmt.Sprintf("%-10s %-10s %s", m.Kit, m.Dir, m.Reason)
}

// renderInit renders the pipeline senro init writes for matches, formatted
// with go/format. name is the pipeline's name. The output depends only on the
// set of matches, not their order: kits are rendered in all.Names order and
// each kit's matches in kit.SortMatches order.
func renderInit(name string, matches []kit.Match) ([]byte, error) {
	ms := append([]kit.Match(nil), matches...)
	kit.SortMatches(ms)
	byKit := map[string][]kit.Match{}
	for _, m := range ms {
		byKit[m.Kit] = append(byKit[m.Kit], m)
	}

	for k := range byKit {
		if !slices.Contains(all.Names(), k) {
			return nil, fmt.Errorf("unknown kit %q", k)
		}
	}

	imports := map[string]bool{senroModule: true, senroModule + "/attach": true}
	used := map[string]bool{}
	var steps, found []string
	for _, k := range all.Names() {
		km := byKit[k]
		if len(km) == 0 {
			continue
		}
		imports[senroModule+"/kit/"+k] = true
		for _, m := range km {
			found = append(found, matchLine(m))
		}
		if g, ok := fanOutGraph[k]; ok && len(km) > 1 {
			imports[g.importPath] = true
			steps = append(steps, fmt.Sprintf("verify.Expand(%q, %s).Template(%s.Template())", uniqueID(k, used), g.call, k))
			continue
		}
		for _, m := range km {
			id := k
			if len(km) > 1 && m.Dir != "." {
				id = k + "-" + idSegment(m.Dir)
			}
			opts := ""
			if m.Dir != "." {
				opts = fmt.Sprintf("%s.Dir(%s)", k, strconv.Quote(m.Dir))
			}
			steps = append(steps, fmt.Sprintf("verify.Add(%q, %s.Step(%s))", uniqueID(id, used), k, opts))
		}
	}
	importList := make([]string, 0, len(imports))
	for p := range imports {
		importList = append(importList, p)
	}
	sort.Strings(importList)

	var buf bytes.Buffer
	err := initTemplate.Execute(&buf, struct {
		Name                  string
		Found, Imports, Steps []string
	}{name, found, importList, steps})
	if err != nil {
		return nil, err
	}
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated pipeline: %w", err)
	}
	return out, nil
}

// idSegment is the last path segment of dir with anything outside letters,
// digits, ".", "_" and "-" replaced by "-", so a directory name can never
// put a character in a step id that the id grammar reserves.
func idSegment(dir string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		}
		return '-'
	}, path.Base(dir))
}

// uniqueID returns id, or id-2, id-3 and so on when an earlier step already
// took it, and records the one it returns.
func uniqueID(id string, used map[string]bool) string {
	got := id
	for n := 2; used[got]; n++ {
		got = id + "-" + strconv.Itoa(n)
	}
	used[got] = true
	return got
}
