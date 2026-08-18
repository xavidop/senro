package redact

import "sort"

// Placeholder replaces every matched occurrence. Byte-for-byte mamori's own
// secret.Redacted, so both paths look identical in a log;
// TestPlaceholderMatchesMamori pins that.
const Placeholder = "[REDACTED]"

// MinLength is the shortest value worth registering: a secret whose value is
// "true" or "1" would redact half a log. New reports skips through Skipped,
// which the engine's run-start check turns into a refusal.
const MinLength = 6

// rootState is the automaton's start state and is always index 0.
const rootState int32 = 0

// Value is one secret to register. Label names it for Match, so a caller
// can say which secret matched without printing the value.
type Value struct {
	Label string
	Value []byte
}

// node is one state of the Aho-Corasick automaton.
//
// next is a map, not a dense [256]int32: a few thousand states with dense
// tables would cost megabytes for transitions almost never taken. The root,
// hit on almost every byte, gets its own dense table on Set, so scanning a
// secret-free log costs one array index per byte.
type node struct {
	next  map[byte]int32
	fail  int32
	depth int32
	// match is the length of the longest pattern ending at this state, 0 when
	// none does; failure links propagate it during construction.
	match int32
	label string
}

// Set is an immutable set of secret values and their encodings, compiled
// into one automaton. The nil *Set is the no-secrets case; every method
// treats it as an identity, so callers hold one unconditionally.
type Set struct {
	nodes   []node
	root    [256]int32
	max     int
	pats    int
	skipped []string
}

// Len is how many distinct patterns are registered, counting encodings.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return s.pats
}

// Skipped names the secrets New refused to register because their value was
// shorter than MinLength. Sorted, so a caller's error message is stable.
func (s *Set) Skipped() []string {
	if s == nil {
		return nil
	}
	return s.skipped
}

// step is the automaton's transition function: follow the goto edge if one
// exists, else failure links, falling back to the root's dense table. It
// terminates because failure links strictly decrease depth.
func (s *Set) step(state int32, c byte) int32 {
	for {
		if state == rootState {
			return s.root[c]
		}
		if n, ok := s.nodes[state].next[c]; ok {
			return n
		}
		state = s.nodes[state].fail
	}
}

// build compiles pats into an automaton, or returns nil only when there is
// nothing to report at all: no pattern to register AND nothing skipped.
//
// pats empty with skipped non-empty is real: every secret was shorter than
// MinLength. Keying nil on len(pats) alone would discard skipped, so the
// run-start refusal (which reads Skipped() off this *Set) could never fire
// and the run would start unprotected. See
// TestNewReportsSkippedEvenWhenNothingElseWasRegistrable.
func build(pats []Value, skipped []string) *Set {
	if len(pats) == 0 && len(skipped) == 0 {
		return nil
	}
	sort.Strings(skipped)
	s := &Set{nodes: []node{{next: map[byte]int32{}}}, skipped: skipped}

	for _, p := range pats {
		cur := rootState
		for _, c := range p.Value {
			nxt, ok := s.nodes[cur].next[c]
			if !ok {
				s.nodes = append(s.nodes, node{
					next:  map[byte]int32{},
					depth: s.nodes[cur].depth + 1,
				})
				nxt = int32(len(s.nodes) - 1)
				s.nodes[cur].next[c] = nxt
			}
			cur = nxt
		}
		if s.nodes[cur].match == 0 {
			s.nodes[cur].match = int32(len(p.Value))
			s.nodes[cur].label = p.Label
		}
		if len(p.Value) > s.max {
			s.max = len(p.Value)
		}
		s.pats++
	}

	// The root's dense table must be complete before the breadth-first pass,
	// which calls step, which consults it.
	queue := make([]int32, 0, len(s.nodes))
	for c := 0; c < 256; c++ {
		n, ok := s.nodes[rootState].next[byte(c)]
		if !ok {
			s.root[c] = rootState
			continue
		}
		s.root[c] = n
		s.nodes[n].fail = rootState
		queue = append(queue, n)
	}

	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		// Propagate the failure state's match. Correct only breadth-first:
		// fail[u] has smaller depth, so it has already been through this.
		if f := s.nodes[u].fail; s.nodes[f].match > s.nodes[u].match {
			s.nodes[u].match = s.nodes[f].match
			s.nodes[u].label = s.nodes[f].label
		}
		for c, v := range s.nodes[u].next {
			s.nodes[v].fail = s.step(s.nodes[u].fail, c)
			queue = append(queue, v)
		}
	}
	return s
}

// New compiles vals and every encoding of them into one automaton, returning
// nil only when there is nothing to say: no vals, or every Value empty. A
// value shorter than MinLength is skipped entirely, encodings included:
// protecting the base64 of a four-byte secret while the four bytes stay
// exposed would be worse than the refusal built on Skipped. See build's doc
// for why skipped survives even when nothing is registrable.
func New(vals ...Value) *Set {
	var pats []Value
	var skipped []string
	seen := make(map[string]bool)
	for _, v := range vals {
		if len(v.Value) == 0 {
			continue
		}
		if len(v.Value) < MinLength {
			skipped = append(skipped, v.Label)
			continue
		}
		for _, form := range Variants(v.Value) {
			if len(form) < MinLength || seen[string(form)] {
				continue
			}
			seen[string(form)] = true
			pats = append(pats, Value{Label: v.Label, Value: form})
		}
	}
	return build(pats, skipped)
}

// Redact replaces every complete occurrence of a registered pattern with
// Placeholder and reports how many replacements it made.
//
// When nothing matched it returns b itself, not a copy: the no-secret case
// costs one scan and zero allocations.
//
// After a replacement the automaton restarts from the root, which bounds the
// package's guarantee: an occurrence beginning inside a replaced span is not
// detected, but is not present in the output either, because part of it was
// replaced.
func (s *Set) Redact(b []byte) ([]byte, int) {
	if s == nil || len(b) == 0 {
		return b, 0
	}
	var out []byte
	state := rootState
	last := 0
	n := 0
	for i := 0; i < len(b); i++ {
		state = s.step(state, b[i])
		L := int(s.nodes[state].match)
		if L == 0 {
			continue
		}
		// start cannot precede last: the automaton was restarted at last, so
		// it has consumed at most i-last+1 bytes and L is bounded by that.
		start := i - L + 1
		if out == nil {
			out = make([]byte, 0, len(b)+len(Placeholder))
		}
		out = append(out, b[last:start]...)
		out = append(out, Placeholder...)
		last = i + 1
		state = rootState
		n++
	}
	if n == 0 {
		return b, 0
	}
	return append(out, b[last:]...), n
}

// Match reports whether b contains a complete occurrence of any registered
// pattern, and which secret it belongs to, so the guard's error can name the
// secret and never the value.
func (s *Set) Match(b []byte) (string, bool) {
	if s == nil {
		return "", false
	}
	state := rootState
	for i := 0; i < len(b); i++ {
		state = s.step(state, b[i])
		if s.nodes[state].match > 0 {
			return s.nodes[state].label, true
		}
	}
	return "", false
}

// MatchString is Match over a string. The conversion allocates: fine for the
// guard's one pass over a plan, which is why the streaming path uses Writer.
func (s *Set) MatchString(str string) (string, bool) { return s.Match([]byte(str)) }
