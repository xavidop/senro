package redact

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
)

// Variants returns v and every encoding of it this package recognises:
// base64 (std and URL, padded and unpadded), URL-encoded,
// JSON-string-escaped, and shell-quoted.
//
// Both spellings of each ambiguous family are registered: URL escaping as a
// query component ("+" for space) and as a path component ("%20"); JSON
// escaping with and without SetEscapeHTML; shell quoting as a single-quoted
// body (only "'" special) and a double-quoted body (backslash, quote, dollar
// and backtick escaped). A tool logs whichever form its own library picked.
//
// Entries may repeat and may equal the raw value; New deduplicates. Order is
// fixed so that Len is a stable number a test can assert.
func Variants(v []byte) [][]byte {
	if len(v) == 0 {
		return nil
	}
	s := string(v)
	out := [][]byte{
		v,
		[]byte(base64.StdEncoding.EncodeToString(v)),
		[]byte(base64.RawStdEncoding.EncodeToString(v)),
		[]byte(base64.URLEncoding.EncodeToString(v)),
		[]byte(base64.RawURLEncoding.EncodeToString(v)),
		[]byte(url.QueryEscape(s)),
		[]byte(url.PathEscape(s)),
		jsonBody(s, true),
		jsonBody(s, false),
		[]byte(strings.ReplaceAll(s, `'`, `'\''`)),
		[]byte(doubleQuoteEscaper.Replace(s)),
	}
	kept := out[:0]
	for _, form := range out {
		if len(form) >= MinLength {
			kept = append(kept, form)
		}
	}
	return kept
}

// doubleQuoteEscaper produces the body of a double-quoted shell word.
// strings.Replacer never rescans its own output, so the backslash pair
// coming first cannot double-escape what the later pairs insert.
var doubleQuoteEscaper = strings.NewReplacer(
	`\`, `\\`,
	`"`, `\"`,
	"`", "\\`",
	`$`, `\$`,
)

// jsonBody is s encoded as a JSON string with the surrounding quotes
// removed: the form a value takes inside someone else's JSON output.
// Encoding cannot fail for a Go string; the nil it would return is filtered
// by Variants' MinLength pass rather than panicking.
func jsonBody(s string, escapeHTML bool) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(escapeHTML)
	if err := enc.Encode(s); err != nil {
		return nil
	}
	// Encode appends a newline and wraps the value in quotes.
	b := bytes.TrimRight(buf.Bytes(), "\n")
	if len(b) < 2 {
		return nil
	}
	return b[1 : len(b)-1]
}
