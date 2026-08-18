package gradle

import (
	"fmt"
	"strings"
)

// inertBlocks are the settings blocks whose body cannot add a project to this
// build. Their contents are skipped rather than read, because a reader strict
// enough to be trusted about `include` would otherwise refuse on every real
// repository: pluginManagement, dependencyResolutionManagement and a
// develocity block are what a modern settings file is mostly made of.
//
// includeBuild is still picked out of them, because pluginManagement is where
// a convention-plugin build is usually included from and this graph has to
// know which directories belong to a build it is not reading.
var inertBlocks = map[string]bool{
	"pluginManagement":               true,
	"dependencyResolutionManagement": true,
	"plugins":                        true,
	"buildscript":                    true,
	"buildCache":                     true,
	"gradleEnterprise":               true,
	"develocity":                     true,
	"toolchainManagement":            true,
	"sourceControl":                  true,
	"caches":                         true,
}

// settings is everything this graph reads out of settings.gradle(.kts).
type settings struct {
	// file is the name of the settings file, for an error message.
	file string
	// includes are the project paths as the file wrote them.
	includes []string
	// dirs are the projectDir reassignments, keyed by normalised project path.
	dirs map[string]string
	// builds are the directories of composite builds this graph does not read.
	builds []string
}

// readSettings reads a settings script, or refuses.
//
// The whitelist is the design. Every statement has to be one of the handful
// this reader understands; anything else stops it. The alternative, reading
// what it recognises and ignoring the rest, produces a project list that is
// SHORTER than the build's and looks exactly like a correct one, and a short
// project list is an affected set that skips the project a change broke.
func readSettings(file, src string) (*settings, error) {
	s := &settings{file: file, dirs: map[string]string{}}
	for _, st := range parse(lex(src)) {
		if err := s.top(st); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *settings) top(st stmt) error {
	if len(st.toks) == 0 {
		return nil
	}
	head := st.toks[0]
	if isPunct(head, "@") {
		return nil // a Kotlin file annotation configures the script, not the build
	}
	if head.kind != tokWord {
		return s.refuse(st, "")
	}
	switch head.text {
	case "include":
		paths, ok := literalArgs(st.toks[1:])
		if !ok || st.hasBody {
			return s.refuse(st, "an include whose argument is not a plain string literal names a "+
				"project only Gradle can work out, and this reader will not guess at one")
		}
		s.includes = append(s.includes, paths...)
	case "includeBuild":
		dir, ok := stripParens(st.toks[1:])
		if !ok {
			return s.refuse(st, "")
		}
		s.builds = append(s.builds, dir)
	case "includeFlat":
		return s.refuse(st, "includeFlat puts a project in a SIBLING of the root directory, "+
			"outside the tree senro was pointed at, so it has no root-relative directory and no "+
			"changed path could ever be attributed to it")
	case "rootProject":
		// rootProject.name is inert. rootProject.buildFileName is not: it
		// changes which file this graph would have to read the edges out of.
		if len(st.toks) < 4 || !isPunct(st.toks[1], ".") || !isWord(st.toks[2], "name") ||
			!isPunct(st.toks[3], "=") {
			return s.refuse(st, "")
		}
	case "project":
		if !s.projectDirStmt(st) {
			return s.refuse(st, "a projectDir this reader cannot compute would put a project's "+
				"whole subtree on the wrong unit, or on none")
		}
	case "enableFeaturePreview", "import", "package":
		// None of the three can add a project.
	default:
		if st.hasBody && inertBlocks[head.text] {
			return s.inert(st.body)
		}
		return s.refuse(st, "")
	}
	return nil
}

// inert walks a block that cannot add a project, looking only for the
// includeBuild that pluginManagement so often holds.
func (s *settings) inert(body []stmt) error {
	for _, st := range body {
		if len(st.toks) > 0 && isWord(st.toks[0], "includeBuild") {
			if err := s.top(st); err != nil {
				return err
			}
			continue
		}
		if len(st.toks) > 0 && (isWord(st.toks[0], "include") || isWord(st.toks[0], "includeFlat")) {
			return s.refuse(st, "an include in a block this reader treats as unable to add "+
				"projects means the two disagree, and the disagreement is this reader's fault")
		}
		if st.hasBody {
			if err := s.inert(st.body); err != nil {
				return err
			}
		}
	}
	return nil
}

// projectDirStmt reads `project(':a:b').projectDir = file('somewhere')`.
func (s *settings) projectDirStmt(st stmt) bool {
	t := st.toks
	if len(t) < 8 || !isPunct(t[1], "(") || t[2].kind != tokString || t[2].interp ||
		!isPunct(t[3], ")") || !isPunct(t[4], ".") || !isWord(t[5], "projectDir") ||
		!isPunct(t[6], "=") {
		return false
	}
	dir, ok := dirExpr(t[7:])
	if !ok {
		return false
	}
	s.dirs[normPath(t[2].text)] = dir
	return true
}

// stripParens reads the single directory argument of an includeBuild, with or
// without parentheses and with or without the file() call around it.
func stripParens(toks []token) (string, bool) {
	if len(toks) >= 2 && isPunct(toks[0], "(") {
		toks, _ = callArgs(toks, 0)
	}
	return dirExpr(toks)
}

func (s *settings) refuse(st stmt, why string) error {
	if why == "" {
		why = "this reader knows an include, an includeBuild, a rootProject.name, a projectDir " +
			"assignment and a short list of blocks that cannot add a project, and nothing else"
	}
	return fmt.Errorf("%s line %d: %w: cannot read %q. %s. Reading on and reporting the projects "+
		"it did understand would be a project list shorter than the build's, and a short project "+
		"list is an affected set that skips what the change broke. Rewrite the includes as string "+
		"literals, or fan out over unit/glob and run every project without an affected set",
		s.file, st.line, ErrNotDeclarative, render(st), why)
}

// normPath is a Gradle project path in its canonical form. `include 'a:b'` and
// `include ':a:b'` name the same project.
func normPath(p string) string {
	segs := splitPath(p)
	if len(segs) == 0 {
		return ":"
	}
	return ":" + strings.Join(segs, ":")
}

func splitPath(p string) []string {
	var out []string
	for _, s := range strings.Split(p, ":") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// parentPath is the path of the project one level up. The parent of a
// top-level project is the root, ":".
func parentPath(p string) string {
	segs := splitPath(p)
	if len(segs) <= 1 {
		return ":"
	}
	return ":" + strings.Join(segs[:len(segs)-1], ":")
}

// accessorName renders one project name the way Gradle's type-safe accessors
// do: "-" and "_" are word separators and what follows one is capitalised, so
// :libs:data-store is reached as projects.libs.dataStore.
//
// Gradle refuses to generate accessors at all for a name outside
// [a-zA-Z]([A-Za-z0-9\-_])*, which is why a dot is not a separator here: a
// project called "other.thing" has no accessor to map back from.
func accessorName(name string) string {
	parts := strings.FieldsFunc(name, func(r rune) bool { return r == '-' || r == '_' })
	if len(parts) == 0 {
		return name
	}
	var b strings.Builder
	b.WriteString(parts[0])
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]) + p[1:])
	}
	return b.String()
}
