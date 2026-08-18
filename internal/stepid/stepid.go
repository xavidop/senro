// Package stepid owns senro's step identifier grammar.
//
//	stepID   := segment ("/" segment)*         "deploy/discover/apply-cm4"
//	expanded := stepID "[" k=v ("," k=v)* "]"  keys sorted
//	address  := (stepID|expanded) ["@" N]      CLI surface only, N >= 1
//
// The attempt suffix is an addressing form for the CLI and never appears in an
// event's step field, where attempt is its own routing field.
package stepid

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// Format builds an expanded child ID. Keys are sorted so that an expander
// returning map iteration order still produces a stable, reproducible ID.
func Format(base string, keys map[string]string) string {
	if len(keys) == 0 {
		return base
	}
	pairs := make([]string, 0, len(keys))
	for k, v := range keys {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return base + "[" + strings.Join(pairs, ",") + "]"
}

// Keys reverses Format, splitting an expanded ID into its base and key set.
// A bare ID with no bracket group is a base and no keys, with ok true.
//
// ok is false for anything off the grammar and must not be ignored: its
// caller reads a unit back out of a child's ID to record how long that
// unit's step took, and guessing would credit one unit's time to another.
//
// It unescapes nothing because Format escapes nothing: an expansion refuses
// a unit whose ID contains a delimiter (senro's unitIDNeedsEscaping), so a
// value here can never contain "[", "]", "=" or ",".
func Keys(id string) (base string, keys map[string]string, ok bool) {
	open := strings.IndexByte(id, '[')
	if open < 0 {
		if strings.ContainsAny(id, "]=,") {
			return "", nil, false
		}
		return id, nil, true
	}
	if open == 0 || !strings.HasSuffix(id, "]") {
		return "", nil, false
	}
	base = id[:open]
	inner := id[open+1 : len(id)-1]
	if inner == "" || strings.ContainsAny(base, "]=,") || strings.ContainsAny(inner, "[]") {
		return "", nil, false
	}
	keys = make(map[string]string)
	for _, pair := range strings.Split(inner, ",") {
		eq := strings.IndexByte(pair, '=')
		if eq <= 0 || eq == len(pair)-1 {
			return "", nil, false
		}
		k := pair[:eq]
		if _, dup := keys[k]; dup {
			return "", nil, false
		}
		keys[k] = pair[eq+1:]
	}
	return base, keys, true
}

// ParseAddress splits a CLI address into its step ID and attempt number.
// An absent @N yields attempt 0, meaning "unspecified".
func ParseAddress(s string) (string, int, error) {
	// Search after the bracketed key set, so a value containing @ is safe.
	search := s
	if i := strings.LastIndex(s, "]"); i >= 0 {
		search = s[i:]
	}
	at := strings.LastIndex(search, "@")
	if at < 0 {
		return s, 0, nil
	}
	cut := len(s) - len(search) + at
	id, rest := s[:cut], s[cut+1:]
	n, err := strconv.Atoi(rest)
	if err != nil {
		return "", 0, fmt.Errorf("stepid: bad attempt in %q: %w", s, err)
	}
	if n < 1 {
		return "", 0, fmt.Errorf("stepid: attempt must be >= 1 in %q", s)
	}
	return id, n, nil
}

// Encode makes a step ID safe as a single filesystem path segment while
// keeping it readable, which matters when reading a run's logs from disk.
func Encode(id string) string { return url.PathEscape(id) }

// Decode reverses Encode.
func Decode(s string) (string, error) { return url.PathUnescape(s) }
