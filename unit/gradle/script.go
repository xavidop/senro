package gradle

import (
	"strings"
)

// This file is the reader: enough of Groovy and Kotlin to tell a string
// literal from a comment and a statement from the next one, and no more.
// Deliberately not a parser: everything it cannot read it hands back to the
// caller, which either refuses (settings.gradle, where an unread statement
// means a short project list) or over-approximates (a build script, where
// an unread dependency means one project depending on all of them).

type tokKind int

const (
	tokWord tokKind = iota
	tokString
	tokPunct
	tokEOL
)

// token is one lexical item. A tokString's text is the DECODED content of the
// literal, with an interpolation left as written so interp can flag it.
type token struct {
	kind   tokKind
	text   string
	interp bool
	line   int
}

func isPunct(t token, s string) bool { return t.kind == tokPunct && t.text == s }
func isWord(t token, s string) bool  { return t.kind == tokWord && t.text == s }

func identStart(c rune) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func identPart(c rune) bool { return identStart(c) || (c >= '0' && c <= '9') }

// lex turns a build or settings script into tokens, dropping comments and
// decoding string literals.
//
// The one Groovy form it does not know is the slashy string, /like this/,
// which no settings file and almost no build script writes and which cannot be
// told from division without a real parser. A slashy string containing a
// comment marker would confuse this; the consequence in a settings file is a
// refusal, which is the safe direction.
func lex(src string) []token {
	rs := []rune(src)
	n := len(rs)
	out := make([]token, 0, n/4+8)
	line := 1
	for i := 0; i < n; {
		c := rs[i]
		switch {
		case c == '\n':
			out = append(out, token{kind: tokEOL, line: line})
			line++
			i++
		case c == ' ' || c == '\t' || c == '\r' || c == '\f':
			i++
		case c == '/' && i+1 < n && rs[i+1] == '/':
			for i < n && rs[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && rs[i+1] == '*':
			i += 2
			for i < n && (rs[i] != '*' || i+1 >= n || rs[i+1] != '/') {
				if rs[i] == '\n' {
					line++
				}
				i++
			}
			i = min(i+2, n)
		case c == '\'' || c == '"':
			var t token
			t, i, line = lexString(rs, i, line)
			out = append(out, t)
		case c == '`':
			// Kotlin quotes an identifier that is otherwise a keyword like
			// this: plugins { `java-library` }.
			j := i + 1
			for j < n && rs[j] != '`' {
				j++
			}
			out = append(out, token{kind: tokWord, text: string(rs[i+1 : min(j, n)]), line: line})
			i = min(j+1, n)
		case identStart(c):
			j := i
			for j < n && identPart(rs[j]) {
				j++
			}
			out = append(out, token{kind: tokWord, text: string(rs[i:j]), line: line})
			i = j
		case c >= '0' && c <= '9':
			j := i
			for j < n && identPart(rs[j]) {
				j++
			}
			out = append(out, token{kind: tokWord, text: string(rs[i:j]), line: line})
			i = j
		default:
			out = append(out, token{kind: tokPunct, text: string(c), line: line})
			i++
		}
	}
	return out
}

// lexString reads one literal, single, double or triple quoted, and reports
// where it ended and what line that was.
func lexString(rs []rune, i, line int) (token, int, int) {
	n := len(rs)
	q := rs[i]
	start := line
	quote := 1
	if i+2 < n && rs[i+1] == q && rs[i+2] == q {
		quote = 3
	}
	var b strings.Builder
	interp := false
	j := i + quote
	for j < n {
		c := rs[j]
		if c == '\\' && j+1 < n {
			switch rs[j+1] {
			case 'n':
				b.WriteRune('\n')
			case 't':
				b.WriteRune('\t')
			case 'r':
				b.WriteRune('\r')
			default:
				b.WriteRune(rs[j+1])
			}
			j += 2
			continue
		}
		if c == q {
			if quote == 1 {
				j++
				break
			}
			if j+2 < n && rs[j+1] == q && rs[j+2] == q {
				j += 3
				break
			}
		}
		if c == '$' && q == '"' {
			// Only a double-quoted string interpolates, in both languages. A
			// Groovy single-quoted string holding a $ is a literal $.
			interp = true
		}
		if c == '\n' {
			line++
		}
		b.WriteRune(c)
		j++
	}
	return token{kind: tokString, text: b.String(), interp: interp, line: start}, j, line
}

// stmt is one statement, with its block body if it opened one. `include ':a'`
// is a stmt with three tokens and no body; `pluginManagement { ... }` is one
// token and a body.
type stmt struct {
	toks    []token
	body    []stmt
	hasBody bool
	line    int
}

// parse groups tokens into statements. A statement ends at a newline, unless
// the newline is inside brackets or the line plainly continues onto the next
// one, which is how a real settings file writes a long include list:
//
//	include ':libs:core',
//	        ':libs:store'
func parse(toks []token) []stmt {
	i := 0
	return parseBlock(toks, &i)
}

func parseBlock(toks []token, i *int) []stmt {
	var out []stmt
	var cur stmt
	depth := 0 // ( and [ only: a brace at depth 0 opens a block
	add := func(t token) {
		if len(cur.toks) == 0 {
			cur.line = t.line
		}
		cur.toks = append(cur.toks, t)
	}
	flush := func() {
		if len(cur.toks) > 0 || cur.hasBody {
			out = append(out, cur)
		}
		cur = stmt{}
	}
	for *i < len(toks) {
		t := toks[*i]
		switch {
		case t.kind == tokEOL:
			*i++
			if depth == 0 && len(cur.toks) > 0 && !continues(cur.toks, toks, *i) {
				flush()
			}
		case isPunct(t, ";"):
			*i++
			if depth == 0 {
				flush()
			}
		case isPunct(t, "(") || isPunct(t, "["):
			depth++
			add(t)
			*i++
		case isPunct(t, ")") || isPunct(t, "]"):
			depth--
			add(t)
			*i++
		case isPunct(t, "{") && depth == 0:
			*i++
			cur.body = parseBlock(toks, i)
			cur.hasBody = true
			if cur.line == 0 {
				cur.line = t.line
			}
			flush()
		case isPunct(t, "}") && depth == 0:
			*i++
			flush()
			return out
		default:
			add(t)
			*i++
		}
	}
	flush()
	return out
}

// continues reports whether a statement runs past this newline: it ends on an
// operator or a comma, or the next line starts with one.
func continues(cur []token, toks []token, next int) bool {
	last := cur[len(cur)-1]
	if last.kind == tokPunct && strings.Contains(",.=+-*/%&|^<>?:", last.text) {
		return true
	}
	for k := next; k < len(toks); k++ {
		if toks[k].kind == tokEOL {
			continue
		}
		return isPunct(toks[k], ".") || isPunct(toks[k], ",")
	}
	return false
}

// render writes a statement back out for an error message, short enough to
// read in one line and long enough to find in the file.
func render(s stmt) string {
	var b strings.Builder
	for i, t := range s.toks {
		switch t.kind {
		case tokString:
			b.WriteString("'" + t.text + "'")
		case tokPunct:
			b.WriteString(t.text)
		default:
			if i > 0 && !isPunct(s.toks[i-1], "(") && !isPunct(s.toks[i-1], ".") {
				b.WriteString(" ")
			}
			b.WriteString(t.text)
		}
	}
	out := strings.TrimSpace(b.String())
	if s.hasBody {
		out += " { ... }"
	}
	if r := []rune(out); len(r) > 72 {
		out = string(r[:69]) + "..."
	}
	return out
}

// literalArgs reads an argument list of nothing but plain string literals,
// with or without the parentheses Groovy makes optional and Kotlin does not.
// An interpolated literal is not a literal: what it names depends on a value
// this reader does not have.
func literalArgs(toks []token) ([]string, bool) {
	if len(toks) >= 2 && isPunct(toks[0], "(") && isPunct(toks[len(toks)-1], ")") {
		toks = toks[1 : len(toks)-1]
	}
	var out []string
	want := true
	for _, t := range toks {
		if want {
			if t.kind != tokString || t.interp {
				return nil, false
			}
			out = append(out, t.text)
			want = false
			continue
		}
		if !isPunct(t, ",") {
			return nil, false
		}
		want = true
	}
	return out, len(out) > 0
}

// rootPrefixes are the interpolations whose value this graph knows exactly:
// every one of them is the directory it was pointed at.
var rootPrefixes = []string{
	"${rootDir}", "$rootDir",
	"${settingsDir}", "$settingsDir",
	"${rootProject.projectDir}", "${rootProject.rootDir}",
}

// projPrefixes are the interpolations that mean the directory of the project
// whose script is being read.
var projPrefixes = []string{"${projectDir}", "$projectDir", "${project.projectDir}"}

// pathLiteral reads a string literal as a path, resolving the two
// interpolations it can. It reports whether the path is relative to the root
// rather than to whatever directory the caller is reading from.
func pathLiteral(t token) (p string, fromRoot, ok bool) {
	s := t.text
	if t.interp {
		for _, pre := range rootPrefixes {
			if strings.HasPrefix(s, pre) {
				return clean(strings.TrimPrefix(s[len(pre):], "/")), true, !strings.Contains(s[len(pre):], "$")
			}
		}
		for _, pre := range projPrefixes {
			if strings.HasPrefix(s, pre) {
				return clean(strings.TrimPrefix(s[len(pre):], "/")), false, !strings.Contains(s[len(pre):], "$")
			}
		}
		return "", false, false
	}
	return clean(s), false, true
}

func clean(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\\", "/"))
	return strings.TrimPrefix(s, "./")
}

// dirExpr reads the right-hand side of a projectDir assignment: a bare string
// or a file(...) call around one. Anything else, new File(x) and
// rootDir.resolve(y) among them, is a value this reader cannot compute, and
// guessing at a project's directory puts its whole subtree on the wrong unit.
func dirExpr(toks []token) (string, bool) {
	switch {
	case len(toks) == 1 && toks[0].kind == tokString:
		p, _, ok := pathLiteral(toks[0])
		return p, ok
	case len(toks) == 4 && isWord(toks[0], "file") && isPunct(toks[1], "(") &&
		toks[2].kind == tokString && isPunct(toks[3], ")"):
		p, _, ok := pathLiteral(toks[2])
		return p, ok
	}
	return "", false
}

// callArgs returns the tokens between the parenthesis at toks[at] and its
// match, and the index just past the closing parenthesis.
func callArgs(toks []token, at int) ([]token, int) {
	depth := 0
	for i := at; i < len(toks); i++ {
		switch {
		case isPunct(toks[i], "(") || isPunct(toks[i], "["):
			depth++
		case isPunct(toks[i], ")") || isPunct(toks[i], "]"):
			depth--
			if depth == 0 {
				return toks[at+1 : i], i + 1
			}
		}
	}
	return nil, len(toks)
}

// namedArg finds `key: value` or `key = value` in an argument list, which is
// how both DSLs write project(path: ':lib') and apply(from = "x.gradle").
func namedArg(args []token, key string) (token, bool) {
	for i := 0; i+2 < len(args); i++ {
		if isWord(args[i], key) && (isPunct(args[i+1], ":") || isPunct(args[i+1], "=")) {
			return args[i+2], true
		}
	}
	return token{}, false
}

// dropEOL strips the line breaks. A build script is SCANNED rather than parsed
// into statements, so where its lines end says nothing this reader needs, and
// keeping them would make an argument list spread over three lines look like
// one it could not read.
func dropEOL(toks []token) []token {
	out := make([]token, 0, len(toks))
	for _, t := range toks {
		if t.kind != tokEOL {
			out = append(out, t)
		}
	}
	return out
}

// scanResult is what one build script says about the projects it reads.
type scanResult struct {
	// paths are the project paths it names, in canonical form.
	paths []string
	// applies are the script plugins it pulls in by a readable path.
	applies []applyRef
	// dynamic reports a project reference whose target this reader could not
	// work out. The caller turns that into "this project depends on every
	// project", or into a refusal where that would cover the repository.
	dynamic bool
}

type applyRef struct {
	path string
	// fromRoot distinguishes "$rootDir/gradle/java.gradle" from
	// "gradle/java.gradle", which Gradle resolves against the project.
	fromRoot bool
}

// scan reads a build script for the projects it depends on.
//
// It scans the WHOLE script rather than only a dependencies block. A
// `project(':lib')` outside one is a coupling too, evaluationDependsOn and a
// task reading another project's output among them, and tracking which block
// the reader is inside would mean understanding blocks, which is the parser
// this package is not.
func scan(toks []token, byAccessor map[string]string) scanResult {
	var r scanResult
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.kind != tokWord {
			continue
		}
		// A word after a dot is a member of something else: rootProject.project
		// and foo.projects are not these.
		if i > 0 && isPunct(toks[i-1], ".") {
			continue
		}
		switch t.text {
		case "project", "evaluationDependsOn":
			if i+1 >= len(toks) || !isPunct(toks[i+1], "(") {
				continue // `project.name`, the ubiquitous Groovy property
			}
			args, next := callArgs(toks, i+1)
			i = next - 1
			if p, ok := projectArg(args); ok {
				r.paths = append(r.paths, p)
			} else {
				r.dynamic = true
			}
		case "projects":
			if i+1 >= len(toks) || !isPunct(toks[i+1], ".") {
				continue
			}
			chain, next := accessorChain(toks, i+1)
			i = next - 1
			if p, ok := resolveAccessor(chain, byAccessor); ok {
				r.paths = append(r.paths, p)
			} else {
				// Either a project this reader does not have, or a variable
				// that happens to be called projects. Both run more.
				r.dynamic = true
			}
		case "apply":
			// `apply plugin: 'java'` is not this; only a from: is.
			win := toks[i+1 : min(i+8, len(toks))]
			v, ok := namedArg(win, "from")
			if !ok {
				continue
			}
			if v.kind != tokString {
				r.dynamic = true
				continue
			}
			p, fromRoot, ok := pathLiteral(v)
			if !ok {
				r.dynamic = true
				continue
			}
			r.applies = append(r.applies, applyRef{path: p, fromRoot: fromRoot})
		}
	}
	return r
}

// projectArg reads the argument of a project(...) call, positionally or under
// the `path` key both DSLs allow: project(path: ':lib', configuration: 'x')
// names :lib and not :x, and a reader that took every literal in the call
// would invent the second.
func projectArg(args []token) (string, bool) {
	if len(args) > 0 && args[0].kind == tokString && !args[0].interp &&
		(len(args) == 1 || isPunct(args[1], ",")) {
		return normPath(args[0].text), true
	}
	if v, ok := namedArg(args, "path"); ok && v.kind == tokString && !v.interp {
		return normPath(v.text), true
	}
	return "", false
}

// accessorChain reads the dotted identifiers after `projects`, starting at the
// dot, and reports where it stopped.
func accessorChain(toks []token, at int) ([]string, int) {
	var chain []string
	i := at
	for i+1 < len(toks) && isPunct(toks[i], ".") && toks[i+1].kind == tokWord {
		chain = append(chain, toks[i+1].text)
		i += 2
	}
	return chain, i
}

// resolveAccessor takes the longest prefix of the chain that is a project, so
// `projects.libs.core.name` resolves to :libs:core and not to :libs.
func resolveAccessor(chain []string, byAccessor map[string]string) (string, bool) {
	for n := len(chain); n > 0; n-- {
		if p, ok := byAccessor[strings.Join(chain[:n], ".")]; ok {
			return p, true
		}
	}
	return "", false
}
