// Package toml reads the slice of TOML a package manifest is written in
// (Cargo.toml, pyproject.toml), so a unit graph needs no toolchain present.
// NOT a general TOML implementation: it exists because the root module
// carries no third-party dependency.
//
// It reads tables, arrays of tables, bare/quoted/dotted keys, all four
// string forms with escapes, booleans, integers, floats, arrays and inline
// tables: every construct a real manifest uses. Dates stay literal text (no
// graph reads one, and a wrong date is worse than an unparsed one);
// integers and floats are parsed only so their presence does not derail the
// keys after them.
//
// Every malformed input is an error, never a guess: a manifest half-read is
// a dependency edge missing, which is a unit left out of an affected set,
// which is a green build for a tree that does not build.
package toml

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Table is a parsed TOML table. Values are string, bool, int64, float64,
// []any or Table; an array of tables is an []any of Tables.
//
// Read it through the accessors, not by indexing: they are total, answer
// for absent or wrongly-typed paths, and work on a nil Table, which lets a
// caller chain Sub without checking at every hop.
type Table map[string]any

// Parse reads a whole TOML document.
func Parse(data []byte) (Table, error) {
	p := &parser{src: data}
	root := Table{}
	cur := root
	for {
		p.skipSpace(true)
		if p.eof() {
			return root, nil
		}
		if p.peek() == '[' {
			t, err := p.tableHeader(root)
			if err != nil {
				return nil, err
			}
			cur = t
			continue
		}
		if err := p.keyValue(cur); err != nil {
			return nil, err
		}
		if err := p.endOfLine(); err != nil {
			return nil, err
		}
	}
}

// Sub is the table at keys, or nil when there is nothing there or what is
// there is not a table.
func (t Table) Sub(keys ...string) Table {
	v, ok := t.lookup(keys)
	if !ok {
		return nil
	}
	sub, _ := v.(Table)
	return sub
}

// SubList is the array of tables at keys ([[bin]] and friends), or nil. An
// element that is not a table is dropped, so the result is always usable.
func (t Table) SubList(keys ...string) []Table {
	v, ok := t.lookup(keys)
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]Table, 0, len(arr))
	for _, e := range arr {
		if sub, ok := e.(Table); ok {
			out = append(out, sub)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Str is the string at keys, or "" when it is absent or is not a string.
func (t Table) Str(keys ...string) string {
	v, ok := t.lookup(keys)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// StrList is the array of strings at keys, or nil. A non-string element is
// dropped rather than failing the read: no key any graph reads mixes types.
func (t Table) StrList(keys ...string) []string {
	v, ok := t.lookup(keys)
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Bool is the boolean at keys, and false for anything else, absent included.
func (t Table) Bool(keys ...string) bool {
	v, ok := t.lookup(keys)
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

// Keys are this table's own keys, sorted: a unit graph's every output has
// to be deterministic.
func (t Table) Keys() []string {
	out := make([]string, 0, len(t))
	for k := range t {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (t Table) lookup(keys []string) (any, bool) {
	if len(keys) == 0 {
		return nil, false
	}
	cur := t
	for i, k := range keys {
		if cur == nil {
			return nil, false
		}
		v, ok := cur[k]
		if !ok {
			return nil, false
		}
		if i == len(keys)-1 {
			return v, true
		}
		sub, ok := v.(Table)
		if !ok {
			return nil, false
		}
		cur = sub
	}
	return nil, false
}

type parser struct {
	src []byte
	pos int
}

func (p *parser) eof() bool  { return p.pos >= len(p.src) }
func (p *parser) peek() byte { return p.src[p.pos] }

func (p *parser) has(s string) bool {
	return p.pos+len(s) <= len(p.src) && string(p.src[p.pos:p.pos+len(s)]) == s
}

// line is the 1-based line the current position sits on, computed only when
// an error is being built.
func (p *parser) line() int {
	n := 1
	for i := 0; i < p.pos && i < len(p.src); i++ {
		if p.src[i] == '\n' {
			n++
		}
	}
	return n
}

func (p *parser) errf(format string, args ...any) error {
	return fmt.Errorf("toml: line %d: %s", p.line(), fmt.Sprintf(format, args...))
}

// skipSpace skips blanks and comments. With newlines, it skips those too,
// which is what separates "between statements" from "inside one".
func (p *parser) skipSpace(newlines bool) {
	for !p.eof() {
		switch c := p.peek(); {
		case c == ' ' || c == '\t' || c == '\r':
			p.pos++
		case newlines && c == '\n':
			p.pos++
		case c == '#':
			for !p.eof() && p.peek() != '\n' {
				p.pos++
			}
		default:
			return
		}
	}
}

// endOfLine requires that nothing but a comment follows a value: skipping
// to the next newline would let `name = "a" "b"`'s second half vanish.
func (p *parser) endOfLine() error {
	p.skipSpace(false)
	if p.eof() {
		return nil
	}
	if p.peek() == '\n' {
		p.pos++
		return nil
	}
	return p.errf("unexpected %q after a value", string(p.peek()))
}

// tableHeader reads [a.b] or [[a.b]] and returns the table to file the
// following keys into.
func (p *parser) tableHeader(root Table) (Table, error) {
	p.pos++ // '['
	array := false
	if !p.eof() && p.peek() == '[' {
		array = true
		p.pos++
	}
	keys, err := p.keyPath()
	if err != nil {
		return nil, err
	}
	p.skipSpace(false)
	if p.eof() || p.peek() != ']' {
		return nil, p.errf("unterminated table header")
	}
	p.pos++
	if array {
		if p.eof() || p.peek() != ']' {
			return nil, p.errf("unterminated array-of-tables header")
		}
		p.pos++
	}
	t, err := p.navigate(root, keys, array)
	if err != nil {
		return nil, err
	}
	if err := p.endOfLine(); err != nil {
		return nil, err
	}
	return t, nil
}

// navigate walks (and creates) the table a header names. An intermediate
// key holding an array of tables descends into its LAST element, which is
// what makes [[a]] followed by [a.b] mean what TOML says.
func (p *parser) navigate(root Table, keys []string, array bool) (Table, error) {
	cur := root
	for i, k := range keys {
		last := i == len(keys)-1
		switch v := cur[k].(type) {
		case Table:
			if last && array {
				return nil, p.errf("[[%s]] redefines a table", strings.Join(keys, "."))
			}
			cur = v
		case []any:
			if len(v) == 0 {
				return nil, p.errf("empty array of tables %q", k)
			}
			if last && array {
				next := Table{}
				cur[k] = append(v, next)
				return next, nil
			}
			sub, ok := v[len(v)-1].(Table)
			if !ok {
				return nil, p.errf("%q is an array of values, not of tables", k)
			}
			cur = sub
		case nil:
			next := Table{}
			if last && array {
				cur[k] = []any{next}
			} else {
				cur[k] = next
			}
			cur = next
		default:
			return nil, p.errf("%q is a value, not a table", k)
		}
	}
	return cur, nil
}

// keyValue reads one `key = value` and files it into t, creating the nested
// tables a dotted key names.
func (p *parser) keyValue(t Table) error {
	keys, err := p.keyPath()
	if err != nil {
		return err
	}
	p.skipSpace(false)
	if p.eof() || p.peek() != '=' {
		return p.errf("expected \"=\" after key %q", strings.Join(keys, "."))
	}
	p.pos++
	p.skipSpace(false)
	v, err := p.value()
	if err != nil {
		return err
	}
	dst := t
	for _, k := range keys[:len(keys)-1] {
		sub, ok := dst[k].(Table)
		if !ok {
			if dst[k] != nil {
				return p.errf("%q is a value, not a table", k)
			}
			sub = Table{}
			dst[k] = sub
		}
		dst = sub
	}
	dst[keys[len(keys)-1]] = v
	return nil
}

// keyPath reads a dotted key, honouring quoting: the middle segment of
// [target.'cfg(unix)'.dependencies] holds dots and parentheses of its own.
func (p *parser) keyPath() ([]string, error) {
	var out []string
	for {
		p.skipSpace(false)
		if p.eof() {
			return nil, p.errf("unterminated key")
		}
		var k string
		switch p.peek() {
		case '"', '\'':
			s, err := p.stringValue()
			if err != nil {
				return nil, err
			}
			k = s
		default:
			start := p.pos
			for !p.eof() && isBareKey(p.peek()) {
				p.pos++
			}
			if p.pos == start {
				return nil, p.errf("expected a key, found %q", string(p.peek()))
			}
			k = string(p.src[start:p.pos])
		}
		out = append(out, k)
		p.skipSpace(false)
		if p.eof() || p.peek() != '.' {
			return out, nil
		}
		p.pos++
	}
}

func isBareKey(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

func (p *parser) value() (any, error) {
	if p.eof() {
		return nil, p.errf("expected a value")
	}
	switch c := p.peek(); {
	case c == '"' || c == '\'':
		return p.stringValue()
	case c == '[':
		return p.arrayValue()
	case c == '{':
		return p.inlineTable()
	case p.has("true"):
		p.pos += len("true")
		return true, nil
	case p.has("false"):
		p.pos += len("false")
		return false, nil
	default:
		return p.scalar()
	}
}

// scalar reads a number or a date, which are the values this package keeps
// only so the keys after them still parse. A date is kept as its text.
func (p *parser) scalar() (any, error) {
	start := p.pos
	for !p.eof() {
		c := p.peek()
		if c == ',' || c == ']' || c == '}' || c == '\n' || c == '#' {
			break
		}
		p.pos++
	}
	raw := strings.TrimSpace(string(p.src[start:p.pos]))
	if raw == "" {
		p.pos = start
		return nil, p.errf("expected a value")
	}
	clean := strings.ReplaceAll(raw, "_", "")
	if n, err := strconv.ParseInt(clean, 0, 64); err == nil {
		return n, nil
	}
	if f, err := strconv.ParseFloat(clean, 64); err == nil {
		return f, nil
	}
	return raw, nil
}

func (p *parser) arrayValue() (any, error) {
	p.pos++ // '['
	out := []any{}
	for {
		p.skipSpace(true)
		if p.eof() {
			return nil, p.errf("unterminated array")
		}
		if p.peek() == ']' {
			p.pos++
			return out, nil
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		p.skipSpace(true)
		if p.eof() {
			return nil, p.errf("unterminated array")
		}
		switch p.peek() {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return out, nil
		default:
			return nil, p.errf("expected \",\" or \"]\" in an array, found %q", string(p.peek()))
		}
	}
}

func (p *parser) inlineTable() (any, error) {
	p.pos++ // '{'
	t := Table{}
	for {
		// Newlines are tolerated inside an inline table (TOML 1.0 forbids
		// them): not ambiguous, and refusing would fail a build over
		// formatting.
		p.skipSpace(true)
		if p.eof() {
			return nil, p.errf("unterminated inline table")
		}
		if p.peek() == '}' {
			p.pos++
			return t, nil
		}
		if err := p.keyValue(t); err != nil {
			return nil, err
		}
		p.skipSpace(true)
		if p.eof() {
			return nil, p.errf("unterminated inline table")
		}
		switch p.peek() {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return t, nil
		default:
			return nil, p.errf("expected \",\" or \"}\" in an inline table, found %q", string(p.peek()))
		}
	}
}

// stringValue reads all four string forms. The multi-line ones matter: a
// description holding "[dependencies]" would otherwise be read as a table
// header, filing every key after it in the wrong table.
func (p *parser) stringValue() (string, error) {
	switch {
	case p.has(`"""`):
		return p.multiLine(`"""`, true)
	case p.has("'''"):
		return p.multiLine("'''", false)
	case p.peek() == '"':
		return p.basicString()
	default:
		return p.literalString()
	}
}

func (p *parser) literalString() (string, error) {
	p.pos++ // '\''
	start := p.pos
	for !p.eof() && p.peek() != '\'' {
		if p.peek() == '\n' {
			return "", p.errf("unterminated literal string")
		}
		p.pos++
	}
	if p.eof() {
		return "", p.errf("unterminated literal string")
	}
	s := string(p.src[start:p.pos])
	p.pos++
	return s, nil
}

func (p *parser) basicString() (string, error) {
	p.pos++ // '"'
	var b strings.Builder
	for {
		if p.eof() || p.peek() == '\n' {
			return "", p.errf("unterminated string")
		}
		c := p.peek()
		if c == '"' {
			p.pos++
			return b.String(), nil
		}
		if c == '\\' {
			if err := p.escape(&b); err != nil {
				return "", err
			}
			continue
		}
		b.WriteByte(c)
		p.pos++
	}
}

// multiLine reads a triple-quoted string, basic or literal. The newline
// right after the opening delimiter is dropped, as TOML says.
func (p *parser) multiLine(delim string, escapes bool) (string, error) {
	p.pos += len(delim)
	if !p.eof() && p.peek() == '\r' {
		p.pos++
	}
	if !p.eof() && p.peek() == '\n' {
		p.pos++
	}
	var b strings.Builder
	for {
		if p.eof() {
			return "", p.errf("unterminated multi-line string")
		}
		if p.has(delim) {
			p.pos += len(delim)
			return b.String(), nil
		}
		if escapes && p.peek() == '\\' {
			// A backslash at the end of a line swallows the newline and the
			// whitespace after it.
			if j := p.pos + 1; j < len(p.src) && trailingNewline(p.src[j:]) {
				p.pos++
				p.skipSpace(true)
				continue
			}
			if err := p.escape(&b); err != nil {
				return "", err
			}
			continue
		}
		b.WriteByte(p.peek())
		p.pos++
	}
}

// trailingNewline reports whether only blanks separate the position from the
// end of its line.
func trailingNewline(rest []byte) bool {
	for _, c := range rest {
		switch c {
		case ' ', '\t', '\r':
		case '\n':
			return true
		default:
			return false
		}
	}
	return false
}

func (p *parser) escape(b *strings.Builder) error {
	p.pos++ // '\\'
	if p.eof() {
		return p.errf("a string ends in a backslash")
	}
	c := p.peek()
	p.pos++
	switch c {
	case 'b':
		b.WriteByte('\b')
	case 't':
		b.WriteByte('\t')
	case 'n':
		b.WriteByte('\n')
	case 'f':
		b.WriteByte('\f')
	case 'r':
		b.WriteByte('\r')
	case '"':
		b.WriteByte('"')
	case '\\':
		b.WriteByte('\\')
	case 'u', 'U':
		n := 4
		if c == 'U' {
			n = 8
		}
		if p.pos+n > len(p.src) {
			return p.errf("truncated \\%c escape", c)
		}
		v, err := strconv.ParseUint(string(p.src[p.pos:p.pos+n]), 16, 32)
		if err != nil {
			return p.errf("bad \\%c escape", c)
		}
		p.pos += n
		r := rune(v)
		if !utf8.ValidRune(r) {
			return p.errf("\\%c escape is not a character", c)
		}
		b.WriteRune(r)
	default:
		return p.errf("unknown escape \\%c", c)
	}
	return nil
}
