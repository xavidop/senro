package webui

import (
	"os"
	"regexp"
	"sort"
	"testing"
)

// This file checks that the page the client draws into and the client that
// draws into it agree about what exists.
//
// The failure it guards against is completely silent: view.go resolves
// every element once with document.getElementById, and every helper there
// tolerates null on purpose (a partial page must not take the client
// down), so an id typo produces a client that runs perfectly and simply
// never draws that part of the page. Both directions are checked: an id
// looked up but never defined is the silent-blank case; an id defined and
// never used is dead markup.

// idsIn extracts the ids a file mentions under the given pattern.
func idsIn(t *testing.T, path string, re *regexp.Regexp) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var out []string
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(string(src), -1) {
		if seen[m[1]] {
			continue
		}
		seen[m[1]] = true
		out = append(out, m[1])
	}
	sort.Strings(out)
	return out
}

func TestThePageAndTheClientAgreeOnEveryElement(t *testing.T) {
	// byID("run-name") in view.go's constructor.
	looked := idsIn(t, "client/view.go", regexp.MustCompile(`byID\("([^"]+)"\)`))
	// id="run-name" in the served markup.
	defined := idsIn(t, "assets/index.html", regexp.MustCompile(`\bid="([^"]+)"`))
	// #run-actions in the stylesheet. Styling is the other legitimate reason
	// for an element to carry an id, so an id the CSS targets is in use even
	// if no Go code ever resolves it.
	styled := idsIn(t, "assets/app.css", regexp.MustCompile(`#([A-Za-z][\w-]*)\s*[,{]`))

	if len(looked) == 0 {
		t.Fatal("found no byID lookups in client/view.go: this test's pattern has stopped " +
			"matching, which makes it a test that always passes")
	}
	if len(defined) == 0 {
		t.Fatal("found no ids in assets/index.html: this test's pattern has stopped matching")
	}

	set := func(ids []string) map[string]bool {
		m := make(map[string]bool, len(ids))
		for _, id := range ids {
			m[id] = true
		}
		return m
	}
	definedSet, lookedSet, styledSet := set(defined), set(looked), set(styled)

	for _, id := range looked {
		if !definedSet[id] {
			t.Errorf("client/view.go looks up element %q and assets/index.html does not define it: "+
				"getElementById returns null, every helper in view.go silently tolerates null, "+
				"and that part of the page would simply never draw", id)
		}
	}
	for _, id := range defined {
		if !lookedSet[id] && !styledSet[id] {
			t.Errorf("assets/index.html defines element %q and nothing uses it: no client code "+
				"looks it up and no rule in app.css targets it. Either wire it up or remove it", id)
		}
	}
	for _, id := range styled {
		if !definedSet[id] {
			t.Errorf("app.css styles #%s and assets/index.html defines no such element", id)
		}
	}
}
