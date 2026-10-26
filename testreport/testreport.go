// Package testreport is how a pipeline author tells senro that a step runs
// tests, and in what format it reports them, so the run can show one result
// per test instead of one exit code per step.
//
// The step DECLARES its format, and senro never guesses it from the command
// line. "go test -json ./..." and "gotestsum -- ./..." both speak Go's JSON
// event stream, and "make check" might speak anything; sniffing argv would
// mean a wrong guess turns a green step red or silently reports no tests at
// all. A declaration is wrong in the open, at Build, where a typo in the
// format is refused instead of misread.
//
// Select is argv with a placeholder rather than a Go func because it has to
// survive in plan.json: "senro rerun --test" reads the plan, not the
// pipeline's source, and a closure cannot be written to disk. The
// placeholder stands for the chosen test names, joined by Join, and is
// substituted into the step's own command to re-run just those tests.
//
// This package imports nothing from senro, on purpose: the engine imports it
// and the pipeline author imports it, and a leaf cannot take part in a cycle.
package testreport

// The report formats a step can declare. gotest and tap are read from the
// step's stdout as it runs; junit is read from a file after it exits.
const (
	FormatGoTest = "gotest"
	FormatTAP    = "tap"
	FormatJUnit  = "junit"
)

// Placeholder is the token a Select argument carries where the chosen test
// names go. A literal string in the plan, not a template engine, so a
// reader of plan.json sees exactly what will be substituted.
const Placeholder = "{tests}"

// Spec is how one step reports its tests. Build one with GoTestJSON, TAP or
// JUnitXML and refine it with Selecting and JoinedBy; the zero Spec is not
// a valid declaration, and plan.Validate refuses it at Build.
type Spec struct {
	// Format is one of the Format constants.
	Format string

	// Path is the report file, for FormatJUnit only. Streaming formats
	// leave it empty because their report is the step's stdout.
	Path string

	// Select is the argv appended to the step's command to run only some
	// tests, with Placeholder where their names go. Empty means the format
	// has no way to select, and a per-test re-run is refused.
	Select []string

	// Join separates the chosen names where they replace Placeholder.
	Join string
}

// GoTestJSON declares that the step prints Go's "go test -json" event
// stream. The step's command must already pass -json (or be a wrapper that
// does); senro reads the stream and does not add the flag for you, because
// it does not edit your argv.
//
// Select is -run with an anchored alternation, "^(A|B)$": go test's -run is
// an unanchored regular expression, so without the anchors a re-run of
// TestFoo would also run TestFooBar.
func GoTestJSON() Spec {
	return Spec{
		Format: FormatGoTest,
		Select: []string{"-run", "^(" + Placeholder + ")$"},
		Join:   "|",
	}
}

// TAP declares that the step prints the Test Anything Protocol on stdout.
// It sets no Select: TAP says nothing about how a runner picks tests, so
// the author supplies it with Selecting for the runner they use.
func TAP() Spec {
	return Spec{Format: FormatTAP}
}

// JUnitXML declares that the step writes a JUnit XML report to path, read
// after the step exits. The path is resolved against the step's working
// directory. Only a step that runs on the coordinator may declare it, since
// that is the only filesystem senro can read the file from.
func JUnitXML(path string) Spec {
	return Spec{Format: FormatJUnit, Path: path}
}

// Selecting returns a copy of s whose Select is args, replacing any it had.
// At least one argument must contain Placeholder; plan.Validate refuses the
// declaration otherwise, since a selection that names no test selects
// nothing.
func (s Spec) Selecting(args ...string) Spec {
	s.Select = append([]string(nil), args...)
	return s
}

// JoinedBy returns a copy of s whose Join is sep, replacing the previous
// one. For a runner that wants "-k 'a or b'" rather than go test's "a|b".
func (s Spec) JoinedBy(sep string) Spec {
	s.Join = sep
	return s
}
