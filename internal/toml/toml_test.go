package toml_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/toml"
)

// realCargoManifest is the shape a Cargo workspace member actually has, with
// every form this reader has to survive: a dotted key, an inline table, a
// multi-line array, a quoted table header with dots and parentheses inside
// it, and a multi-line string holding text that looks like a table header.
const realCargoManifest = `# a comment
[package]
name = "web"
version.workspace = true
edition = "2021"
description = """
A crate.
[dependencies] in here is prose, not a table header.
"""
keywords = [
    "one",
    "two",  # trailing comma next
]

[dependencies]
core = { path = "../core", version = "0.1.0" }
serde = "1.0"
tracing.workspace = true

[dev-dependencies]
testutil = { path = "../testutil" }

[target.'cfg(unix)'.dependencies]
nix = { version = "0.29", features = ["fs"] }

[[bin]]
name = "web"
path = "src/main.rs"

[[bin]]
name = "webctl"
path = "src/ctl.rs"
`

func TestParseReadsARealCargoManifest(t *testing.T) {
	tbl, err := toml.Parse([]byte(realCargoManifest))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := tbl.Str("package", "name"); got != "web" {
		t.Errorf("package.name = %q, want web", got)
	}
	if got := tbl.Str("package", "edition"); got != "2021" {
		t.Errorf("package.edition = %q, want 2021", got)
	}
	// A dotted key inside a table is a nested table, not a key with a dot in
	// it: `version.workspace = true` is `[package.version] workspace = true`.
	if !tbl.Bool("package", "version", "workspace") {
		t.Error("package.version.workspace is not true")
	}
	if !tbl.Bool("dependencies", "tracing", "workspace") {
		t.Error("dependencies.tracing.workspace is not true")
	}
	if got := tbl.Str("dependencies", "core", "path"); got != "../core" {
		t.Errorf("dependencies.core.path = %q, want ../core", got)
	}
	if got := tbl.Str("dev-dependencies", "testutil", "path"); got != "../testutil" {
		t.Errorf("dev-dependencies.testutil.path = %q", got)
	}
	// A dependency declared as a bare version string is a string, not a
	// table, and asking it for a path must not panic or invent one.
	if got := tbl.Str("dependencies", "serde", "path"); got != "" {
		t.Errorf("dependencies.serde.path = %q, want nothing", got)
	}
	if got := tbl.StrList("package", "keywords"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Errorf("package.keywords = %v", got)
	}
}

// TestParseHandlesAQuotedTableHeader: a target-specific dependency table's
// middle segment is a quoted string holding dots, quotes and parentheses, and
// splitting the header on "." without honouring the quoting would shred it.
func TestParseHandlesAQuotedTableHeader(t *testing.T) {
	tbl, err := toml.Parse([]byte(realCargoManifest))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	deps := tbl.Sub("target", "cfg(unix)", "dependencies")
	if deps == nil {
		t.Fatalf("no target.'cfg(unix)'.dependencies table; got keys %v", tbl.Sub("target").Keys())
	}
	if got := deps.Str("nix", "version"); got != "0.29" {
		t.Errorf("nix.version = %q", got)
	}
}

// TestParseDoesNotReadInsideAMultiLineString is the mis-parse that would
// matter: "[dependencies]" appears inside package.description, and a reader
// that treated it as a table header would file the keys after it in the wrong
// place and quietly lose a dependency edge.
func TestParseDoesNotReadInsideAMultiLineString(t *testing.T) {
	tbl, err := toml.Parse([]byte(realCargoManifest))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	desc := tbl.Str("package", "description")
	if !strings.Contains(desc, "[dependencies] in here is prose") {
		t.Fatalf("package.description = %q, want the whole multi-line body", desc)
	}
	if strings.HasPrefix(desc, "\n") {
		t.Errorf("description = %q, want the newline right after \"\"\" trimmed", desc)
	}
	// And the real [dependencies] table is still where it belongs.
	if tbl.Sub("dependencies") == nil {
		t.Error("no dependencies table")
	}
}

func TestParseArrayOfTables(t *testing.T) {
	tbl, err := toml.Parse([]byte(realCargoManifest))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	bins := tbl.SubList("bin")
	if len(bins) != 2 {
		t.Fatalf("[[bin]] produced %d tables, want 2", len(bins))
	}
	if bins[0].Str("name") != "web" || bins[1].Str("name") != "webctl" {
		t.Errorf("[[bin]] names = %q, %q", bins[0].Str("name"), bins[1].Str("name"))
	}
}

// TestParseReadsARealPyproject covers the other manifest this reader exists
// for, including a literal string, a nested tool table and an array of
// requirement strings.
func TestParseReadsARealPyproject(t *testing.T) {
	const src = `[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "bird-feeder"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["tqdm>=4,<5", 'seeds']

[tool.uv.workspace]
members = ["packages/*"]
exclude = ["packages/seeds"]

[tool.uv.sources]
seeds = { workspace = true }
`
	tbl, err := toml.Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := tbl.Str("project", "name"); got != "bird-feeder" {
		t.Errorf("project.name = %q", got)
	}
	deps := tbl.StrList("project", "dependencies")
	// A comma inside a requirement string is not an array separator.
	if len(deps) != 2 || deps[0] != "tqdm>=4,<5" || deps[1] != "seeds" {
		t.Errorf("project.dependencies = %#v", deps)
	}
	if got := tbl.StrList("tool", "uv", "workspace", "members"); len(got) != 1 || got[0] != "packages/*" {
		t.Errorf("tool.uv.workspace.members = %v", got)
	}
	if !tbl.Bool("tool", "uv", "sources", "seeds", "workspace") {
		t.Error("tool.uv.sources.seeds.workspace is not true")
	}
}

func TestParseRejectsMalformedInput(t *testing.T) {
	for _, tc := range []struct{ name, src string }{
		{"unterminated table header", "[package\nname = \"x\"\n"},
		{"no value", "[package]\nname =\n"},
		{"unterminated string", "[package]\nname = \"x\n"},
		{"unterminated array", "members = [\"a\",\n"},
		{"unterminated inline table", "dep = { path = \"../a\"\n"},
		{"junk after a value", "name = \"a\" \"b\"\n"},
		{"bare key with no equals", "[package]\nname\n"},
		{"unterminated multi-line string", "d = \"\"\"\nhello\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := toml.Parse([]byte(tc.src)); err == nil {
				t.Fatalf("Parse(%q) returned no error; a mis-parse is a missing edge", tc.src)
			}
		})
	}
}

// TestParseErrorNamesTheLine, because the caller reports it against a file a
// person then has to open.
func TestParseErrorNamesTheLine(t *testing.T) {
	_, err := toml.Parse([]byte("[package]\nname = \"a\"\n\nbroken\n"))
	if err == nil {
		t.Fatal("no error")
	}
	if !strings.Contains(err.Error(), "line 4") {
		t.Errorf("error %q does not name line 4", err)
	}
}

// TestAccessorsAreTotal: every accessor answers for a path that is not there,
// or is there with the wrong type, without panicking. A graph reads manifests
// it did not write.
func TestAccessorsAreTotal(t *testing.T) {
	tbl, err := toml.Parse([]byte("a = 1\n[b]\nc = \"d\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.Str("nope", "deeper"); got != "" {
		t.Errorf("Str of a missing path = %q", got)
	}
	if got := tbl.Str("a"); got != "" {
		t.Errorf("Str of an integer = %q", got)
	}
	if got := tbl.Sub("b", "c"); got != nil {
		t.Errorf("Sub of a string = %v", got)
	}
	if got := tbl.StrList("b", "c"); got != nil {
		t.Errorf("StrList of a string = %v", got)
	}
	if got := tbl.SubList("b"); got != nil {
		t.Errorf("SubList of a table = %v", got)
	}
	if tbl.Bool("b", "c") {
		t.Error("Bool of a string is true")
	}
	var nilTable toml.Table
	if got := nilTable.Str("a"); got != "" {
		t.Errorf("Str on a nil Table = %q", got)
	}
	if got := nilTable.Keys(); len(got) != 0 {
		t.Errorf("Keys on a nil Table = %v", got)
	}
}

func TestKeysAreSorted(t *testing.T) {
	tbl, err := toml.Parse([]byte("[t]\nz = 1\na = 2\nm = 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	got := tbl.Sub("t").Keys()
	want := []string{"a", "m", "z"}
	if len(got) != len(want) {
		t.Fatalf("Keys = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("Keys = %v, want %v", got, want)
		}
	}
}

func TestParseEscapesInABasicString(t *testing.T) {
	tbl, err := toml.Parse([]byte(`p = "a\tb\nc\\d\"eA"` + "\nq = 'a\\tb'\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tbl.Str("p"), "a\tb\nc\\d\"eA"; got != want {
		t.Errorf("p = %q, want %q", got, want)
	}
	// A literal string escapes nothing, which is what makes it the right way
	// to write a Windows path.
	if got, want := tbl.Str("q"), `a\tb`; got != want {
		t.Errorf("q = %q, want %q", got, want)
	}
}
