package secrets

import "strings"

// sourceIdentity reduces a mamori source tag to what is safe to record in
// an event and a cache key.
//
// mamori's ref grammar puts the fragment BEFORE the query
// (scheme://path[#key][?opts], or scheme:path for opaque schemes), so
// net/url cannot parse it and the cuts are done here. Userinfo is removed
// because "vault://user:pass@host/kv" carries a credential into
// events.jsonl and a cache entry that outlives the run; the query because
// ?opts are decoding directives that say nothing about WHICH secret this
// is. A tag mamori would reject is not rejected here: mamori.Load already
// failed on a malformed ref long before senro sees the struct.
func sourceIdentity(tag string) string {
	if tag == "" {
		return ""
	}
	s := tag
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok {
		// The opaque form has no authority, so there is no userinfo to strip.
		return s
	}
	authority := rest
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		authority = rest[:i]
	}
	if at := strings.LastIndexByte(authority, '@'); at >= 0 {
		rest = rest[at+1:]
	}
	return scheme + "://" + rest
}
