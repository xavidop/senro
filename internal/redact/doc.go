// Package redact removes secret values from a byte stream.
//
// mamori keeps secrets out of logs written inside the Go program; senro's
// exposure is a child process's stdout (`go test -v` echoing an env var,
// `curl -v` printing a token). secret.String cannot protect a byte a
// subprocess wrote, so this package does.
//
// # The guarantee
//
// No complete occurrence of any registered pattern appears in the output.
// Deliberately not the stronger "no secret byte survives": when registered
// patterns overlap in the input, replacing one can leave a fragment of the
// other. See TestOverlappingPatternsLeaveAFragment and the README's secrets
// section.
//
// # What is not covered
//
// Named deliberately: a redactor believed to cover more than it does is
// worse than none.
//
//   - Any hashing of the value. Not a leak, and not recoverable anyway.
//   - Compression or encryption. A step that gzips its own log, or writes an
//     archive into a workspace, defeats this entirely.
//   - Hex, base32, ROT13, case changes, and any encoding not listed above.
//   - A value printed in pieces with other content between them, e.g.
//     `echo "${T:0:8}"; echo "${T:8}"`. A value split across two WRITE
//     chunks is covered (the rolling buffer spans that boundary); a CONTENT
//     boundary is not.
//   - A value shorter than MinLength. New reports these through Skipped and
//     the engine refuses to run rather than proceed unprotected.
//   - Anything outside this process: ps(1), /proc/<pid>/environ, shell
//     history, auditd. That is internal/engine/guard.go's run-start refusal,
//     not this package.
//   - A value a step writes into a file that becomes a workspace snapshot or
//     a declared output. Those bytes go to the CAS unread by this package.
//
// # Why Aho-Corasick and not bytes.Replace
//
// A child's output arrives in whatever chunks the pipe hands over, so
// per-chunk bytes.Replace misses a split value, and nondeterministically.
// Aho-Corasick streams through Writer, which holds back only the automaton's
// current depth: the longest suffix consumed so far that is a prefix of some
// pattern, bounded by the longest registered pattern (Set.max, not
// len(secret): a base64 variant runs longer than the secret it encodes).
//
// # Concurrency
//
// A Set is immutable once New returns and safe for concurrent use. A Writer
// holds per-stream state behind its own mutex, because a backgrounded orphan
// can write while the engine flushes (see localexec's waitDelay).
package redact
