# senro v0 Secrets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pipeline declares its credentials as a typed struct, `mamori` resolves them once on the coordinator before the first step runs, each step that asked for one receives it as a file whose path arrives in the environment, and no byte of any value reaches the event log, a log file, a cache key, an attached client, or a process argument list. A step that would leak a value through a channel senro cannot protect is refused before the run starts, not redacted after the fact.

**Architecture:** One immutable pattern set per run, built from the resolved values and every encoding of them senro recognises, sits at exactly two chokepoints and nowhere else. The first is `runCore.append`, which is the single function through which every event in this engine reaches both the on-disk ledger and `Sink.Emit`; redacting there means the hub, the TUI, `events.jsonl` and every future sink are covered by one call. The second is the `io.Writer` chain in `runAttempt`, upstream of the log-marker writer, so the bytes that land in a log file are already redacted and the byte offsets recorded in `step.log.appended` describe the redacted file rather than the raw stream the child produced. The attach server serves scrollback by opening those files (`Server.handleLogs`), so a redacted file is a redacted response with no second mechanism. Delivery is `Sandbox.PutSecret`, which already exists and which nothing has ever called; this plan calls it, moves its output off the run directory onto tmpfs, and deletes it when the sandbox closes.

**Tech Stack:** Go 1.26, darwin and linux. One new root-module dependency, `github.com/xavidop/mamori` v1.12.1, imported in production code as `github.com/xavidop/mamori/secret` only (a leaf package) and imported whole (`mamori`, `mamori/mamoritest`) in tests. `api` stays standard-library only. Nothing here binds a socket.

**Spec:** `docs/design.md` §1 in full, §6.11, §10's v0 line ("`mamori`-backed secrets, file delivery, and redaction with encoding variants"), and §12's worked example, which pins the builder spelling `SecretEnv("NPM_TOKEN", "NPMToken")`.

---

## The three properties this plan is really about

**1. Redaction happens before the hub.** §6.11: "Secrets are redacted before the hub, so a client never receives values regardless of authorization. Redaction is not an authorization mechanism, but it is the backstop." A filter on the way out to a particular client is too late by construction, because `events.jsonl` and the per-step log files are written before any client is consulted, and a `FileSource` post-mortem reader opens those same files with no server in the loop at all. So this plan puts redaction on the two write paths and nowhere near a connection. Task 5 proves it for events, Task 6 for logs, and both assert against the bytes on disk rather than against what a subscriber received.

**2. Encoding variants, because a naive redactor is theatre.** §1.5 requires base64 (standard and URL, padded and unpadded), URL encoding, JSON string escaping and shell quoting. Task 2 builds all of them, Task 1 builds the matcher that catches a value split across two write chunks, and Task 10's documentation states plainly which transformations are covered and which are not. A redactor that claims more than it delivers is worse than none, so the uncovered list is written down as carefully as the covered one.

**3. Secrets never reach a cache key.** Plan 5 established the mechanism: only names declared with `CacheEnv` enter the key, each as `NAME=` plus 8 hex of the value's sha256, never the value. This plan does not weaken it. Secrets travel to a step through `Sandbox.PutSecret` and the environment carries only a filesystem path, so the delivered environment can never contain a value for `EnvComponent` to digest. `Key.Secrets`, already declared and always empty, is populated in Task 9 with identities only. And because a path under the attempt directory changes every run, Task 9 also refuses a pipeline that names the same variable in both `CacheEnv` and `SecretEnv`, which is the composition defect these two features would otherwise produce silently.

---

## Global Constraints

These bind every task in this plan.

- **`api/go.mod` must keep zero `require` entries, stdlib only.** The root module may take dependencies.
- **The event log is the single source of truth.** `Seq` is monotonic: a gap is survivable, a regression or duplicate is not.
- **`Sink.Emit` must never block and never fail.** Redaction sits on that path, so it must not make `Emit` slow or fallible.
- **Secret values never in cache keys, events, or logs.**
- **`plan_digest` must not move for a semantically unchanged pipeline.** Four golden fixtures pin it plus `TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest`.
- **`cache.KeyVersion` is currently 2.** Bumping it invalidates every entry; do so only deliberately and say why.
- **No TCP binding in v0, unix sockets only.**
- **Go 1.26, darwin and linux. Windows is a documented exclusion.**
- **`golangci-lint run ./...` clean in both modules; `make all` includes a `GOWORK=off` check.**
- **Test isolation is enforced by `TestEveryTestPackageThatCanReachTheDefaultCacheRootHasIsolation`.**
- **No em dashes anywhere in the plan.** Restructure the sentence instead.

And five rules this plan adds, each the scar of a defect the first five plans shipped:

- **Nothing ships unwired.** Five times this project shipped code with no production caller. Every task below either wires its capability to a production caller in the same task, with a test that exercises it through the real entry point, or names in its own text the exact task and file that does. Tasks 1, 2 and 3 are the only tasks that use the second form, they build one package between them, and they name Tasks 5 and 6.
- **Every behaviour gets its negative case planned, and a proof that could pass vacuously gets a guard.** Plan 5's secret canary asserts the search saw the variable name before concluding the value is absent. Every search-for-absence in this plan carries the same guard, under the heading **The canary**.
- **Fix the class, not the instance.** Three fixes in earlier plans closed the reported case and left the general defect. Where a task closes a category rather than one occurrence, its text says so under **Class, not instance**.
- **Watch every test fail before it passes.** All thirteen tasks in plan 5 found a bug in their own brief this way. Every task below is written TDD-first for that reason.
- **Beware composition.** Plan 5's two Criticals were both cases where each piece was correct alone and the combination was broken. Where two tasks touch the same state, a test exercises them together, under **Composition**.

### Six design decisions this plan makes, written down because they are load-bearing

**1. A command argument is refused, not redacted.**

`api.StepStartedBody.Cmd` records the real argv in `events.jsonl`. That is intentional published-API behaviour: a reader of the ledger has to know what ran. Plan 5's fix for the same field hashed argv in the *cache key*, not in the *event*, so a secret passed as a command argument still lands permanently in the ledger.

Redacting it would be the obvious fix and it is the wrong primary answer, because the ledger is not where the leak matters most. §1.4 says it directly, about the ssh executor: "Never as a command argument, visible in `ps`, in remote shell history, and in auditd `execve` records." No redactor in this process can reach any of those three. A run that redacts argv in the event log and then hands the same argv to `execve` has cleaned up the record of a leak it still committed.

So this plan does all three things the brief lists, in this order of authority:

- **Refuse (Task 8).** At run start, after resolution and before `run.started`, the engine scans every node's `Cmd` and every value in its `Env`, including handler nodes, for any registered secret value in any encoding, and returns an error rather than starting the run. The error names the step, the argument index or the variable name, and the secret's field name, and never the value.
- **Redact anyway (Task 5).** Every event payload passes through the redactor in `runCore.append`. This is not argv-specific; it is the class fix, and it covers the payload fields the refusal cannot see because they are built at run time from a child process's behaviour: `StepFinishedBody.Error`, `StepRetriedBody.Reason`, and any field a future event type adds.
- **Document (Task 10).** The README gains a table of channels with "safe" and "unsafe" stated plainly, and the refusal's error message points at it.

The refusal deliberately does **not** cover `Node.WorkDir`. A working directory is not an OS-visible credential channel the way argv and the environment block are, refusing it would be over-reach, and leaving it uncovered gives Task 5 a real, uncontrived leak to prove the backstop against.

**2. The plan file stores a field reference, not a source URI, and this is a deviation from §1.2.**

§1.2 says "`plan.json` stores the `source:` URI". `(*Pipeline).Build` cannot honour that: it has no access to the resolved config struct, because §12's own worked example hands the struct to `Run`, not to `New`. The alternative, enriching the plan after `Build` returns, would make `plan.Digest()` differ between the value a caller inspected and the value the engine reports in `plan.resolved`, for exactly those pipelines that declare a secret. That is a worse violation of a harder constraint.

So `plan.Node.Secrets[i].Name` holds the Go struct field name, which is a compile-time constant and therefore stable and semantic, and `plan.Node.Secrets[i].Source` is declared with `omitempty`, left empty in v0, and documented as the field a future builder fills once it learns the config type. The resolved source URI is recorded where it is genuinely available: in the `secret.resolved` event (`api.SecretResolvedBody.Source`, already published) and in the cache key's `Secrets` component (Task 9). The real invariant from §1.1, that a plan can be serialized, stored and re-run and must never contain values, is untouched.

**3. `SecretEnv` puts a path in the environment, never a value.**

§1.4's local row: "tmpfs file, mode `0600`, path in `SENRO_SECRET_<NAME>`". §12's comment on `SecretEnv("NPM_TOKEN", "NPMToken")`: "tmpfs file + env pointing at it". Both say the same thing, so this plan implements both spellings of it. Every secret a step declares gets a file and a `SENRO_SECRET_<NAME>` variable holding its path; `SecretEnv(envName, field)` additionally sets `envName` to the same path. The value is in the file, the path is in the environment, and `Node.Env` never grows an entry at all, so nothing about Plan 5's cache-key contract changes.

The cost is that a tool expecting `NPM_TOKEN` to *be* the token gets a path instead and the step has to read the file (`$(cat "$NPM_TOKEN")`). That is the design's explicit trade and Task 10 documents it, because the alternative puts the value in the environment block, which is readable through `/proc/<pid>/environ` for the whole life of the process.

**4. The redactor's guarantee is stated exactly, and it is not "no secret bytes survive".**

The matcher is Aho-Corasick over a rolling buffer. When a pattern's last byte arrives, the matched span is replaced with `[REDACTED]` and the automaton restarts from the root. The guarantee that follows is: **no complete occurrence of any registered pattern appears in the output.** A *fragment* can survive when two registered patterns overlap in the input, for instance when one secret's value is a substring of another's. Task 1 tests that case and pins the surviving-fragment behaviour rather than pretending it does not exist, and Task 10 documents it. Claiming the stronger property would be the exact failure mode the brief warns about.

**5. A value shorter than six bytes is refused, not silently unprotected.**

§1.5: "Skip values shorter than a threshold (default 6 bytes), or one secret whose value is `true` redacts half your logs." Skipping is right; skipping *silently* is not, because the pipeline author then believes a value is protected that is not. Task 4's run-start check refuses a run whose resolved config carries a non-empty secret shorter than `redact.MinLength`, with an error saying why. An empty value is a different case and is simply not registered, since an optional secret nobody references is not an error.

**6. `cache.KeyVersion` stays at 2.**

Task 9 populates `Key.Secrets`, which is already a declared component and is currently the empty string for every step. A step that declares no secret keeps `Secrets: ""` and therefore keeps its exact current digest. A step that declares one is a step using a builder method that did not exist before this plan, so no previously-saved entry can be reachable under a key that moved. Nothing is invalidated, so nothing needs to be. Task 9 step 1 pins a zero-secrets key digest to a literal recorded from unmodified code, which is the check that would fail if this reasoning were wrong.

---

## What already exists

`api` declares `secret.resolved` and `secret.redacted` in `v0Types`, with `SecretResolvedBody{Name, Source, Version}` and `SecretRedactedBody{Count}`. Nothing emits either.

`internal/executor` declares `SecretRef{Name, Source}` and `SandboxSpec.Secrets`, and `Sandbox.PutSecret(ctx, name, v) (path, error)`. `localexec` implements `PutSecret` by writing `<sandbox>/.secrets/<name>` at 0600 inside a 0700 directory. Nothing calls it, the sandbox directory is inside the run directory, and `sandbox.Close` deletes nothing.

`internal/cache` declares `Key.Secrets` with the doc "Empty in this build, and declared now so the secrets subsystem populates an existing component rather than changing the key's shape", and `internal/engine/cache.go` sets it to `""` with a matching comment. `EnvComponent` already digests values rather than storing them.

`internal/engine`'s `runCore.append` is the single path from any event to both `ledger.Append` and `sink.Emit`, under `emitMu`. `runAttempt` builds `stdout`/`stderr` as `io.MultiWriter(&logMarker{...}, tail)` and hands them to `Sandbox.Run`. `internal/attachsrv`'s `handleLogs` serves scrollback by opening the file `eventlog.LogSet.Path` names.

`senro.go`'s `StepBuilder` has `Env`, `CacheEnv`, `Mount`, `Pure`, `Inputs`, `Outputs`, `NoSnapshot`. `run.go` has `WithAttach`, `WithDir`, `WithRunID`, `WithLocalClass`, `WithCacheDir`. There is no `WithSecrets` and no `SecretEnv`.

**No package in this repository has ever held a secret value.**

---

## File Structure

```
internal/redact/set.go            Value, Set, New, Redact, Match, the Aho-Corasick automaton
internal/redact/variants.go       Variants: base64 x4, URL x2, JSON x2, shell x2
internal/redact/writer.go         Writer: streaming redaction with prefix holdback, Flush, Redacted

internal/secrets/secrets.go       Secret, Set, Identity, Value, Names, RedactValues
internal/secrets/reveal.go        FromConfig: the ONE place Reveal is ever called
internal/secrets/source.go        sourceIdentity: a source URI with any userinfo stripped

internal/engine/engine.go         Options.Secrets, rc.redact, rc.secrets, secret.resolved
internal/engine/engine.go         runCore.append redacts every payload (Task 5)
internal/engine/attempt.go        the redacting writer chain, secret delivery, secret.redacted
internal/engine/guard.go          checkSecretChannels: refuse argv and env values (Task 8)
internal/engine/cache.go          Key.Secrets, and replay through the redactor

internal/executor/localexec/secretdir.go   tmpfs-preferring secret root, removal on Close

internal/plan/plan.go             Node.Secrets []SecretSpec
internal/plan/validate.go         secret shape rules, and the CacheEnv/SecretEnv refusal

senro.go                          StepBuilder.SecretEnv
run.go                            WithSecrets

secrets_e2e_test.go               the end-to-end composition test (Task 10)
reveal_static_test.go             exactly one Reveal in non-test source (Task 10)
README.md                         the channel table and the covered/uncovered list (Task 10)
```

`internal/redact` imports nothing from this repo and nothing outside the standard library. Its own test imports `github.com/xavidop/mamori/secret` for exactly one assertion, that senro's placeholder and mamori's are the same string; the package itself does not. `internal/secrets` imports `internal/redact` and `github.com/xavidop/mamori/secret`. `internal/engine` imports both. `internal/executor/localexec` imports neither: it receives values as `[]byte` through the `Sandbox.PutSecret` interface it already has, so the executor package never learns what a secret is.

---

### Task 1: The matcher

**Files:**
- Create `internal/redact/set.go`, `internal/redact/set_test.go`
- Create `internal/redact/doc.go`

**Interfaces:**
- Consumes: nothing. This is the base of the plan.
- Produces:
  ```go
  package redact

  const Placeholder = "[REDACTED]"
  const MinLength = 6

  type Value struct {
      Label string // names the secret for Match; never printed alongside the value
      Value []byte
  }

  type Set struct{ /* unexported */ }

  func New(vals ...Value) *Set
  func (s *Set) Len() int
  func (s *Set) Skipped() []string
  func (s *Set) Redact(b []byte) ([]byte, int)
  func (s *Set) Match(b []byte) (label string, ok bool)
  func (s *Set) MatchString(str string) (label string, ok bool)
  ```

**Wiring.** Tasks 1, 2 and 3 build one package with no production caller between them. **Task 5 is the first task that calls `Redact`, in `internal/engine/engine.go`'s `runCore.append`. Task 6 is the first that calls `Writer`, in `internal/engine/attempt.go`. Task 8 is the first that calls `Match`, in `internal/engine/guard.go`.** All three are in this plan. If this plan is ever abandoned partway, `internal/redact` must be reverted rather than left in the tree with no reader.

**The guarantee.** No complete occurrence of any registered pattern appears in the output of `Redact` or of `Writer`. Fragments can survive when two registered patterns overlap; see Step 8.

- [ ] **Step 1: Write the failing test for a nil `*Set`**

The nil receiver is the no-secrets fast path and it must be free, so it is the first thing pinned. Create `internal/redact/set_test.go`:

```go
package redact_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestANilSetRedactsNothingAndAllocatesNothing pins the fast path a pipeline
// with no secrets takes on every single event and every single log chunk. A
// nil *Set is what New returns when there is nothing to register, and every
// method on it has to behave as an identity rather than panic, because the
// engine holds one unconditionally and must not branch on it at each call
// site.
func TestANilSetRedactsNothingAndAllocatesNothing(t *testing.T) {
	var s *redact.Set

	in := []byte("nothing secret here")
	out, n := s.Redact(in)
	if n != 0 {
		t.Errorf("Redact on a nil set reported %d replacements, want 0", n)
	}
	if &out[0] != &in[0] {
		t.Error("Redact on a nil set returned a copy; it must return the input slice itself")
	}
	if label, ok := s.Match(in); ok {
		t.Errorf("Match on a nil set reported a match labelled %q", label)
	}
	if got := s.Len(); got != 0 {
		t.Errorf("Len on a nil set = %d, want 0", got)
	}
	if got := s.Skipped(); got != nil {
		t.Errorf("Skipped on a nil set = %v, want nil", got)
	}
}

// TestNewReturnsNilWhenThereIsNothingToRegister is the other half: the engine
// gets a nil *Set from New rather than an empty non-nil one, so the fast path
// above is the one a secret-free run actually takes.
func TestNewReturnsNilWhenThereIsNothingToRegister(t *testing.T) {
	if s := redact.New(); s != nil {
		t.Error("New() with no values returned a non-nil set")
	}
	if s := redact.New(redact.Value{Label: "empty", Value: nil}); s != nil {
		t.Error("New() with an empty value returned a non-nil set")
	}
}

// TestNewSkipsAndReportsAValueShorterThanMinLength covers design.md section
// 1.5's threshold: "Skip values shorter than a threshold (default 6 bytes), or
// one secret whose value is true redacts half your logs." Skipping is right;
// skipping silently is not, because the author then believes a value is
// protected that is not. Skipped is what Task 4 turns into a refusal.
func TestNewSkipsAndReportsAValueShorterThanMinLength(t *testing.T) {
	s := redact.New(
		redact.Value{Label: "pin", Value: []byte("1234")},
		redact.Value{Label: "token", Value: []byte("abcdef0123456789")},
	)
	if s == nil {
		t.Fatal("New returned nil despite one registrable value")
	}
	skipped := s.Skipped()
	if len(skipped) != 1 || skipped[0] != "pin" {
		t.Fatalf("Skipped() = %v, want [pin]", skipped)
	}
	out, n := s.Redact([]byte("pin 1234 token abcdef0123456789"))
	if n != 1 {
		t.Fatalf("Redact reported %d replacements, want 1 (the long value only)", n)
	}
	if !bytes.Contains(out, []byte("1234")) {
		t.Error("the short value was redacted; MinLength must have skipped it")
	}
	if bytes.Contains(out, []byte("abcdef0123456789")) {
		t.Error("the long value survived redaction")
	}
	if !strings.Contains(string(out), redact.Placeholder) {
		t.Error("no placeholder in the output")
	}
}
```

Run it and watch it fail to compile. That is the expected failure at this step: the package does not exist.

```bash
go test ./internal/redact/
```

- [ ] **Step 2: Write the package doc and the automaton's data**

Create `internal/redact/doc.go`:

```go
// Package redact removes secret values from a byte stream.
//
// It exists because design.md section 1.3 draws a line mamori cannot cross:
// mamori guarantees no secret value reaches a log from inside the Go program,
// and senro's exposure is a CHILD PROCESS's stdout. `go test -v` echoing an
// environment variable, `curl -v` printing a URL with a token, a Helm error
// quoting the values file. secret.String cannot protect a byte that a
// subprocess wrote, so this package does.
//
// # The guarantee
//
// No complete occurrence of any registered pattern appears in the output.
// That is the exact claim, and it is deliberately not the stronger "no secret
// byte survives": when two registered patterns overlap in the input, for
// instance when one secret's value is a substring of another's, replacing the
// first can leave a fragment of the second in place. See the tests named
// TestOverlappingPatternsLeaveAFragment for the pinned behaviour, and the
// README's secrets section for the user-facing statement of it.
//
// # Why Aho-Corasick and not bytes.Replace
//
// design.md section 1.5: "Aho-Corasick over a rolling buffer with
// max(len(secret))-1 bytes of lookback, so a value split across two write
// chunks is still caught. Per-chunk bytes.Replace misses these, and misses
// them nondeterministically, which is worse than missing them consistently."
// A child process's output arrives in whatever chunks the pipe hands over, so
// the same step run twice can split the same token in two different places.
//
// # Concurrency
//
// A Set is immutable once New returns and is safe for concurrent use by any
// number of goroutines. A Writer holds per-stream state and is guarded by its
// own mutex, because a step that backgrounds work can leave an orphan writing
// to a stream while the engine is flushing it (see localexec's waitDelay).
package redact
```

Create `internal/redact/set.go`:

```go
package redact

import "sort"

// Placeholder replaces every matched occurrence. It is byte-for-byte
// mamori's own secret.Redacted, so a value that mamori stringified inside the
// coordinator and a value this package caught on a child's stdout look
// identical to whoever reads the log. TestPlaceholderMatchesMamori pins that.
const Placeholder = "[REDACTED]"

// MinLength is the shortest value worth registering, from design.md section
// 1.5. A secret whose value is "true" or "1" would otherwise redact half a
// log. New reports anything it skipped for this reason through Skipped, and
// Task 4's run-start check turns that into a refusal rather than a silent
// hole.
const MinLength = 6

// rootState is the automaton's start state and is always index 0.
const rootState int32 = 0

// Value is one secret to register. Label names it for Match, so a caller that
// found a secret in a command argument can say WHICH secret without ever
// printing the value.
type Value struct {
	Label string
	Value []byte
}

// node is one state of the Aho-Corasick automaton.
//
// next is a map rather than a dense [256]int32 because a pattern set of a
// handful of secrets and their encodings produces a few thousand states, and
// a dense table at each one would cost megabytes for transitions that are
// almost never taken. The state that IS taken on almost every byte is the
// root, which gets its own dense table on Set instead: a byte that starts no
// pattern costs one array index and no map lookup at all, which is the whole
// cost of scanning a log that contains no secret.
type node struct {
	next  map[byte]int32
	fail  int32
	depth int32
	// match is the length of the longest pattern ending at this state, and 0
	// when none does. Failure links propagate it during construction, so a
	// state reached by a longer pattern also reports a shorter one that ends
	// at the same position.
	match int32
	label string
}

// Set is an immutable set of secret values and their encodings, compiled into
// one automaton. The nil *Set is the no-secrets case and every method treats
// it as an identity, so a caller holds one unconditionally and never branches.
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
// exists, otherwise follow failure links until one does, and fall back to the
// root's dense table. It terminates because each failure link strictly
// decreases depth and the root always answers.
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

// build compiles pats into an automaton, or returns nil when pats is empty.
func build(pats []Value, skipped []string) *Set {
	if len(pats) == 0 {
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

	// The root's dense table must be complete before the breadth-first pass
	// below, because step consults it for every failure that unwinds to the
	// root, and that pass calls step.
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
		// Propagate the failure state's match upward. Breadth-first order is
		// what makes this correct: fail[u] has strictly smaller depth, so it
		// has already been through this same step.
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
```

- [ ] **Step 3: Write `New` against the raw value only**

Encodings arrive in Task 2. `New` registers only `v.Value` for now, which is enough to make Step 1's tests pass and keeps the two concerns separately testable.

Append to `internal/redact/set.go`:

```go
// New compiles vals and every encoding of them into one automaton, and
// returns nil when nothing was registrable. A value shorter than MinLength is
// skipped entirely, including its encodings: registering the base64 of a
// four-byte secret while leaving the four bytes themselves exposed would
// protect the derived form and not the primary one, which is worse than
// consistent silence and much worse than the refusal Task 4 builds on top of
// Skipped.
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
```

And a placeholder `Variants` in the same file for now, replaced wholesale in Task 2:

```go
// Variants returns v and every encoding of it this package recognises. Task 2
// replaces this body with the full set from design.md section 1.5.
func Variants(v []byte) [][]byte {
	if len(v) == 0 {
		return nil
	}
	return [][]byte{v}
}
```

- [ ] **Step 4: Write `Redact` and `Match`**

Append to `internal/redact/set.go`:

```go
// Redact replaces every complete occurrence of a registered pattern with
// Placeholder and reports how many replacements it made.
//
// When nothing matched it returns b ITSELF, not a copy, so the no-secret case
// costs one scan and zero allocations. A caller can compare the returned
// slice's identity, and the engine relies on the count instead.
//
// After a replacement the automaton restarts from the root. That is what
// bounds the guarantee stated in this package's doc: an occurrence beginning
// inside a span that was just replaced is not detected, but such an
// occurrence is not present in the output either, because part of it was
// replaced. An occurrence beginning after the replaced span is detected
// normally.
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
// pattern, and which secret it belongs to. It is the guard's primitive (Task
// 8): a caller that finds a secret in a command argument names the secret in
// its error and never the value.
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

// MatchString is Match over a string. The conversion allocates, which is fine
// for the guard's one pass over a plan and is why the streaming path uses
// Writer instead.
func (s *Set) MatchString(str string) (string, bool) { return s.Match([]byte(str)) }
```

Run Step 1's tests. They must now pass.

```bash
go test ./internal/redact/ -run 'NilSet|NewReturnsNil|MinLength' -v
```

- [ ] **Step 5: Write the failing test for a split value and for `Match`**

Append to `internal/redact/set_test.go`:

```go
// TestRedactCatchesAValueWhereverItSits covers the ordinary cases and the
// boundaries: at the very start of the buffer, at the very end, twice in one
// buffer, and adjacent to itself.
func TestRedactCatchesAValueWhereverItSits(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"alone", secret, redact.Placeholder},
		{"at the start", secret + " trailing", redact.Placeholder + " trailing"},
		{"at the end", "leading " + secret, "leading " + redact.Placeholder},
		{"in the middle", "a " + secret + " b", "a " + redact.Placeholder + " b"},
		{"twice", secret + "|" + secret, redact.Placeholder + "|" + redact.Placeholder},
		{"back to back", secret + secret, redact.Placeholder + redact.Placeholder},
		{"absent", "nothing to see", "nothing to see"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, _ := s.Redact([]byte(tc.in))
			if string(out) != tc.want {
				t.Errorf("Redact(%q) = %q, want %q", tc.in, out, tc.want)
			}
			if bytes.Contains(out, []byte(secret)) {
				t.Errorf("the value survived in %q", out)
			}
		})
	}
}

// TestMatchNamesTheSecretItFound is the guard's primitive. The label comes
// back so an error message can name a field without printing a value.
func TestMatchNamesTheSecretItFound(t *testing.T) {
	s := redact.New(
		redact.Value{Label: "NPMToken", Value: []byte("npm-aaaaaaaaaa")},
		redact.Value{Label: "RegistryToken", Value: []byte("reg-bbbbbbbbbb")},
	)
	label, ok := s.MatchString("--auth=reg-bbbbbbbbbb")
	if !ok {
		t.Fatal("Match missed a value that is plainly present")
	}
	if label != "RegistryToken" {
		t.Errorf("Match labelled it %q, want RegistryToken", label)
	}
	if _, ok := s.MatchString("--auth=harmless"); ok {
		t.Error("Match reported a match in a string containing no value")
	}
}

// TestOverlappingPatternsLeaveAFragment pins the exact boundary of this
// package's guarantee, so nobody later reads the doc as a stronger promise
// than it makes. One secret is a substring of another. Replacing the shorter
// one first destroys the longer one's completeness, which is the guarantee,
// and leaves the longer one's tail visible, which is not covered.
func TestOverlappingPatternsLeaveAFragment(t *testing.T) {
	s := redact.New(
		redact.Value{Label: "short", Value: []byte("abcdef")},
		redact.Value{Label: "long", Value: []byte("abcdefghij")},
	)
	out, n := s.Redact([]byte("abcdefghij"))
	if n != 1 {
		t.Fatalf("reported %d replacements, want 1", n)
	}
	if bytes.Contains(out, []byte("abcdef")) {
		t.Error("the shorter value survived complete")
	}
	if bytes.Contains(out, []byte("abcdefghij")) {
		t.Error("the longer value survived complete")
	}
	if string(out) != redact.Placeholder+"ghij" {
		t.Errorf("Redact = %q; the documented behaviour is %q, and the docs "+
			"must change with this assertion if the algorithm changes",
			out, redact.Placeholder+"ghij")
	}
}

// TestPlaceholderMatchesMamori keeps senro's placeholder identical to the one
// mamori's secret.String prints for itself, so a reader of a log cannot tell
// which side caught a value and does not have to learn two spellings.
func TestPlaceholderMatchesMamori(t *testing.T) {
	if redact.Placeholder != secret.Redacted {
		t.Errorf("redact.Placeholder = %q, mamori secret.Redacted = %q; keep them identical",
			redact.Placeholder, secret.Redacted)
	}
}
```

Add the import to the test file's block:

```go
	"github.com/xavidop/mamori/secret"
```

- [ ] **Step 6: Add the mamori dependency**

```bash
go get github.com/xavidop/mamori@v1.12.1
go mod tidy
make modcheck
```

`mamori` is the root module's second declared third-party dependency after the charm stack and `klauspost/compress`. Production code imports only `github.com/xavidop/mamori/secret`, which is a leaf package with no dependencies of its own, so a senro binary that never resolves a secret does not link the validator, fsnotify, mapstructure, toml or yaml. Tests import `github.com/xavidop/mamori` and `github.com/xavidop/mamori/mamoritest`, which is why those appear as indirect requires in `go.mod`.

`api/go.mod` is not touched and must still have zero `require` entries. Confirm:

```bash
grep -c require api/go.mod || echo "zero requires: correct"
```

- [ ] **Step 7: Run the tests and confirm they pass**

```bash
go test ./internal/redact/ -v
go vet ./internal/redact/
golangci-lint run ./internal/redact/...
```

**The check that catches it.** `TestOverlappingPatternsLeaveAFragment` asserts the exact output string, not merely "the secret is gone". An implementation that replaced the whole buffer with the placeholder, or that silently switched to a leftmost-longest rule, would pass a containment-only assertion and fail this one. That is deliberate: the surviving fragment is documented behaviour, and documented behaviour that no test pins is a comment.

---

### Task 2: The encoding variants

**Files:**
- Create `internal/redact/variants.go`, `internal/redact/variants_test.go`
- Modify `internal/redact/set.go` (delete the placeholder `Variants`)

**Interfaces:**
- Consumes: Task 1's `Set` and `New`.
- Produces:
  ```go
  package redact
  func Variants(v []byte) [][]byte
  ```
  Unchanged signature; Task 1 declared it deliberately so this task is a body swap and not an API change.

**Wiring.** `New` already calls `Variants`, so this task is live the moment it lands and every test in Task 1 exercises it. Its production caller is Task 4's `secrets.Set.RedactValues` feeding Task 5's and Task 6's chokepoints.

**Class, not instance.** design.md §1.5 names five families ("base64 (std and URL, padded and unpadded), URL-encoded, JSON-string-escaped, shell-quoted"), and the temptation is to implement the one that bit somebody. This task implements every named family plus both members of the two that have two spellings each (`url.QueryEscape` versus `url.PathEscape`, and JSON with and without HTML escaping), because a redactor that covers four of six is the theatre the brief warns about. The uncovered families are listed in Step 4 and documented in Task 10.

- [ ] **Step 1: Write the failing table test**

Create `internal/redact/variants_test.go`:

```go
package redact_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestEveryNamedEncodingIsCaught walks design.md section 1.5's list one entry
// at a time. Each case ENCODES the value with the standard library and then
// asserts the encoded form does not survive, so a bug in senro's encoder is
// caught by disagreeing with the library rather than by agreeing with itself.
func TestEveryNamedEncodingIsCaught(t *testing.T) {
	// Deliberately awkward: a slash and a plus land differently in the two
	// base64 alphabets, the space and the ampersand exercise both URL
	// escapers, the double quote and backslash exercise JSON and shell, and
	// the dollar and backtick exercise double-quoted shell.
	raw := []byte(`tok/en+val ue&"x\y$z` + "`q`")
	s := redact.New(redact.Value{Label: "tok", Value: raw})

	jsonBody := func(v []byte) string {
		b, err := json.Marshal(string(v))
		if err != nil {
			t.Fatalf("marshalling the fixture: %v", err)
		}
		return string(b[1 : len(b)-1])
	}

	cases := []struct {
		name    string
		encoded string
	}{
		{"raw", string(raw)},
		{"base64 std padded", base64.StdEncoding.EncodeToString(raw)},
		{"base64 std unpadded", base64.RawStdEncoding.EncodeToString(raw)},
		{"base64 url padded", base64.URLEncoding.EncodeToString(raw)},
		{"base64 url unpadded", base64.RawURLEncoding.EncodeToString(raw)},
		{"url query escaped", url.QueryEscape(string(raw))},
		{"url path escaped", url.PathEscape(string(raw))},
		{"json string escaped", jsonBody(raw)},
		{"shell single quoted", strings.ReplaceAll(string(raw), `'`, `'\''`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line := "output: " + tc.encoded + " done"
			out, n := s.Redact([]byte(line))
			if n == 0 {
				t.Fatalf("no replacement in %q; this encoding is not registered", line)
			}
			if bytes.Contains(out, []byte(tc.encoded)) {
				t.Errorf("the %s form survived: %q", tc.name, out)
			}
			// The canary: prove the scan was looking at the right buffer.
			// Without this a bug that returned an empty slice would pass the
			// containment check above for every case in the table.
			if !bytes.Contains(out, []byte("output: ")) || !bytes.Contains(out, []byte(" done")) {
				t.Fatalf("the surrounding text is gone from %q; this assertion "+
					"was not actually looking at the redacted line", out)
			}
		})
	}
}

// TestShellDoubleQuoteEscapingIsCaught is separated out because there is no
// standard-library encoder to disagree with, so the expected form is written
// by hand: inside a double-quoted shell word, a backslash, a double quote, a
// dollar and a backtick each acquire a leading backslash.
func TestShellDoubleQuoteEscapingIsCaught(t *testing.T) {
	raw := []byte(`a"b\c$d` + "`e`")
	s := redact.New(redact.Value{Label: "tok", Value: raw})
	encoded := `a\"b\\c\$d` + "\\`e\\`"

	out, n := s.Redact([]byte("echo " + encoded))
	if n == 0 {
		t.Fatalf("the double-quoted shell form is not registered: %q", encoded)
	}
	if bytes.Contains(out, []byte(encoded)) {
		t.Errorf("it survived: %q", out)
	}
	if !bytes.Contains(out, []byte("echo ")) {
		t.Fatalf("the surrounding text is gone from %q", out)
	}
}

// TestVariantsAreDeduplicated keeps the automaton small for the ordinary case.
// A plain alphanumeric token is byte-identical under url escaping, JSON
// escaping and both shell forms, so five of the entries collapse into one and
// only the four base64 forms remain distinct.
func TestVariantsAreDeduplicated(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("abcdefghijkl")})
	if got := s.Len(); got > 6 {
		t.Errorf("Len() = %d for a plain alphanumeric token; the encodings that "+
			"cannot differ from the raw form are not being deduplicated", got)
	}
	if got := s.Len(); got < 2 {
		t.Errorf("Len() = %d; the base64 forms must still be registered", got)
	}
}

// TestAnEncodingShorterThanMinLengthIsNotRegistered is the negative case for
// the length filter inside Variants. Encodings only grow, so this can be
// reached only by a future encoder that shrinks its input, and the filter is
// the guard that stops such an encoder from registering a two-byte pattern
// that would redact everything.
func TestAnEncodingShorterThanMinLengthIsNotRegistered(t *testing.T) {
	for _, v := range redact.Variants([]byte("abcdefgh")) {
		if len(v) > 0 && len(v) < redact.MinLength {
			t.Errorf("Variants produced %q, shorter than MinLength=%d", v, redact.MinLength)
		}
	}
}
```

Run it. Every case except `raw` must fail, because Task 1's placeholder `Variants` returns only the raw value.

```bash
go test ./internal/redact/ -run Encoding -v
```

- [ ] **Step 2: Write `variants.go`**

Create `internal/redact/variants.go`:

```go
package redact

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
)

// Variants returns v and every encoding of it this package recognises, from
// design.md section 1.5: "Register encodings, not just the raw value: base64
// (std and URL, padded and unpadded), URL-encoded, JSON-string-escaped,
// shell-quoted. Tools log base64 of tokens constantly."
//
// Two of the five families have two spellings each and both are registered:
//
//   - URL escaping differs between a query component (a space becomes "+")
//     and a path component (a space becomes "%20"), and a tool that logs a
//     request line uses whichever its own client library picked.
//   - JSON string escaping differs between encoding/json's default, which
//     escapes "<", ">" and "&" as \u00xx, and an Encoder with SetEscapeHTML
//     disabled, which does not. senro's own events use the former; a step
//     printing JSON may use either.
//
// Shell quoting likewise has two forms that matter: the body of a
// single-quoted word, where only "'" is special, and the body of a
// double-quoted word, where a backslash, a double quote, a dollar and a
// backtick each acquire a leading backslash.
//
// Entries may repeat and may equal the raw value; New deduplicates. Order is
// fixed and deterministic so that Len is a stable number a test can assert.
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

// doubleQuoteEscaper produces the body of a double-quoted shell word. The
// backslash pair comes first because strings.Replacer matches the input
// left to right and never rescans what it has already written, so escaping
// backslashes first cannot double-escape the backslashes the later pairs
// insert.
var doubleQuoteEscaper = strings.NewReplacer(
	`\`, `\\`,
	`"`, `\"`,
	"`", "\\`",
	`$`, `\$`,
)

// jsonBody is s encoded as a JSON string with the surrounding quotes removed,
// which is the form a value takes when it is embedded in someone else's JSON
// output. escapeHTML selects between encoding/json's two behaviours; see
// Variants' doc for why both are registered.
//
// A failure to encode is impossible for a Go string (every one of them is a
// valid JSON string once invalid UTF-8 is replaced), and the empty result it
// would produce is filtered by Variants' MinLength pass rather than panicking.
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
```

- [ ] **Step 3: Delete the placeholder `Variants` from `set.go`**

Remove the stub added in Task 1 Step 3. Two definitions of `Variants` will not compile, so this is not a step that can be forgotten.

- [ ] **Step 4: Record what is NOT covered**

Append to `internal/redact/doc.go`, inside the package comment, after the guarantee section:

```go
// # What is not covered
//
// Naming these is as important as implementing the ones above, because a
// redactor that is believed to cover more than it does is worse than none.
//
//   - Any hashing of the value. Not a leak, and not recoverable anyway.
//   - Compression or encryption. A step that gzips its own log defeats this
//     entirely, as does a step that writes an archive into a workspace.
//   - Hex, base32, ROT13, case changes, and any encoding not listed above.
//   - A value printed in pieces with other content between them, for example
//     `echo "${T:0:8}"; echo "${T:8}"`, where a newline lands in the middle.
//     design.md section 1.5's "split across two write chunks" is covered,
//     because that is a WRITE boundary and the rolling buffer spans it; a
//     CONTENT boundary is a different thing and is not covered.
//   - A value shorter than MinLength. New reports these through Skipped and
//     the engine refuses to run rather than proceed unprotected.
//   - Anything outside this process: the ps(1) table, /proc/<pid>/environ,
//     shell history, auditd execve records. That is what the run-start
//     refusal in internal/engine/guard.go exists for, not this package.
//   - A value a step writes into a file that becomes a workspace snapshot or
//     a declared output. Those bytes go to the CAS unread by this package.
```

- [ ] **Step 5: Run and confirm**

```bash
go test ./internal/redact/ -v
golangci-lint run ./internal/redact/...
```

**The canary.** Every case in `TestEveryNamedEncodingIsCaught` asserts that the text surrounding the encoded value is still present after redaction. Without it, an implementation that returned an empty slice would satisfy "the encoded form does not survive" for all nine cases and the table would be vacuously green.

---

### Task 3: The streaming writer

**Files:**
- Create `internal/redact/writer.go`, `internal/redact/writer_test.go`

**Interfaces:**
- Consumes: Task 1's `Set` and `step`, Task 2's `Variants`.
- Produces:
  ```go
  package redact
  type Writer struct{ /* unexported */ }
  func (s *Set) Writer(w io.Writer) *Writer
  func (w *Writer) Write(p []byte) (int, error)
  func (w *Writer) Flush() error
  func (w *Writer) Redacted() int
  ```

**Wiring.** Named callers: Task 6 (`internal/engine/attempt.go`, wrapping each attempt's stdout and stderr) and Task 9 (`internal/engine/cache.go`'s `replayLog`).

**The two properties that make this usable at all.**

`Write` must return `len(p)` on success even though it hands a different number of bytes downstream, because the caller is `io.MultiWriter` inside `io.Copy` inside `os/exec`, and `io.Copy` treats `n < len(p)` with a nil error as `io.ErrShortWrite` and aborts the copy. A redactor that shortened its return value would truncate every step's log the first time it fired.

`Write` must hold back only the bytes that could still become part of a match, not `max(len(secret))-1` bytes unconditionally. The automaton's current state carries exactly that number: its `depth` is the length of the longest suffix of everything consumed so far that is a prefix of some pattern. For a log line containing nothing secret-shaped, that is zero, so the line is emitted immediately and the TUI shows it with no added latency. Holding back a fixed window instead would stall the last few dozen bytes of every line until the next write arrived, which for a step that prints a prompt and waits is forever.

- [ ] **Step 1: Write the failing test for a value split across writes**

Create `internal/redact/writer_test.go`:

```go
package redact_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestAValueSplitAcrossWritesIsStillCaught is design.md section 1.5's whole
// reason for a rolling buffer: "a value split across two write chunks is
// still caught. Per-chunk bytes.Replace misses these, and misses them
// nondeterministically, which is worse than missing them consistently."
//
// A child's output arrives in whatever chunks the pipe hands over, so this is
// the ordinary case rather than an adversarial one. The split is walked
// across every position inside the value so that no single lucky offset can
// make the test pass.
func TestAValueSplitAcrossWritesIsStillCaught(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})
	const line = "prefix " + secret + " suffix"

	for cut := 1; cut < len(line); cut++ {
		var got bytes.Buffer
		w := s.Writer(&got)
		if n, err := w.Write([]byte(line[:cut])); err != nil || n != cut {
			t.Fatalf("cut %d: first Write = (%d, %v), want (%d, nil)", cut, n, err, cut)
		}
		rest := line[cut:]
		if n, err := w.Write([]byte(rest)); err != nil || n != len(rest) {
			t.Fatalf("cut %d: second Write = (%d, %v), want (%d, nil)", cut, n, err, len(rest))
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("cut %d: Flush: %v", cut, err)
		}
		if strings.Contains(got.String(), secret) {
			t.Fatalf("cut %d: the value survived: %q", cut, got.String())
		}
		if want := "prefix " + redact.Placeholder + " suffix"; got.String() != want {
			t.Fatalf("cut %d: got %q, want %q", cut, got.String(), want)
		}
		if w.Redacted() != 1 {
			t.Fatalf("cut %d: Redacted() = %d, want 1", cut, w.Redacted())
		}
	}
}

// TestWriteAlwaysReportsTheFullInputLength is the property that keeps
// io.MultiWriter and io.Copy working. io.Copy treats a short write with a nil
// error as io.ErrShortWrite and aborts, so a redactor that reported the
// number of bytes it actually passed downstream would truncate every log the
// first time it fired.
func TestWriteAlwaysReportsTheFullInputLength(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})

	var got bytes.Buffer
	w := s.Writer(&got)
	// io.Copy is the real caller shape, through a MultiWriter, exactly as
	// internal/engine/attempt.go will wire it in Task 6.
	src := strings.NewReader(strings.Repeat("x", 100) + secret + strings.Repeat("y", 100))
	n, err := io.Copy(io.MultiWriter(w), src)
	if err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if want := int64(200 + len(secret)); n != want {
		t.Errorf("io.Copy reported %d bytes, want %d", n, want)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if strings.Contains(got.String(), secret) {
		t.Error("the value survived")
	}
	if want := strings.Repeat("x", 100) + redact.Placeholder + strings.Repeat("y", 100); got.String() != want {
		t.Errorf("got %q, want %q", got.String(), want)
	}
}

// TestNothingIsHeldBackWhenNothingCouldMatch is the latency property. A line
// whose bytes cannot begin any registered pattern must be downstream before
// Flush is ever called, or a step that prints a prompt and waits for input
// shows nothing until it exits.
func TestNothingIsHeldBackWhenNothingCouldMatch(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("zzzzzzzzzzzz")})
	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("waiting for input: ")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "waiting for input: " {
		t.Errorf("downstream saw %q before Flush; nothing here can start the "+
			"pattern, so nothing should have been held back", got.String())
	}
}

// TestOnlyThePartialPrefixIsHeldBack is the other half: a trailing byte
// sequence that COULD begin a match is held, and released verbatim by Flush
// when the stream ends without completing one.
func TestOnlyThePartialPrefixIsHeldBack(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("zzzzzzzzzzzz")})
	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("tail: zzz")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "tail: " {
		t.Errorf("downstream saw %q, want %q: the three trailing z's could still "+
			"become a match and must be held", got.String(), "tail: ")
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got.String() != "tail: zzz" {
		t.Errorf("after Flush downstream saw %q, want %q", got.String(), "tail: zzz")
	}
}

// TestANilSetWriterIsAPassthrough keeps the no-secrets case free: the engine
// wraps every stream unconditionally, and a run with no secrets must not pay
// for a scan, a buffer or a mutex.
func TestANilSetWriterIsAPassthrough(t *testing.T) {
	var s *redact.Set
	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("plain output")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "plain output" {
		t.Errorf("got %q, want the input verbatim, before any Flush", got.String())
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if w.Redacted() != 0 {
		t.Errorf("Redacted() = %d on a nil set, want 0", w.Redacted())
	}
}

// TestADownstreamErrorPropagates. A log writer whose file has been closed is
// the real case (see eventlog.LogWriter's closed guard), and swallowing it
// here would hide a run whose output stopped being recorded.
func TestADownstreamErrorPropagates(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("abcdefghijkl")})
	boom := errors.New("boom")
	w := s.Writer(errWriter{boom})
	n, err := w.Write([]byte("some plain output that flushes straight through"))
	if !errors.Is(err, boom) {
		t.Errorf("Write err = %v, want boom", err)
	}
	if n != len("some plain output that flushes straight through") {
		t.Errorf("Write n = %d; even on an error the count must be the input length, "+
			"or io.Copy reports ErrShortWrite instead of the real cause", n)
	}
}

type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

// TestConcurrentWriteAndFlushIsRaceFree covers the orphan case. localexec's
// waitDelay lets a backgrounded child keep writing for up to five seconds
// after Run returns, which is exactly when the engine calls Flush.
func TestConcurrentWriteAndFlushIsRaceFree(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("abcdefghijkl")})
	w := s.Writer(io.Discard)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, _ = w.Write([]byte("abcdefghijkl and some more text"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			_ = w.Flush()
		}
	}()
	wg.Wait()
}
```

Run it and watch it fail to compile: `Writer` does not exist.

```bash
go test ./internal/redact/ -run Writer -race
```

- [ ] **Step 2: Write `writer.go`**

Create `internal/redact/writer.go`:

```go
package redact

import (
	"io"
	"sync"
)

// Writer redacts a stream on its way to w.
//
// One Writer per stream, because the rolling-buffer state is per stream and
// two streams interleaved through one buffer would splice a match out of
// bytes that were never adjacent. The engine builds two per attempt, one for
// stdout and one for stderr, and sums their Redacted counts.
type Writer struct {
	set *Set
	w   io.Writer

	mu    sync.Mutex
	state int32
	// pend holds the bytes not yet passed downstream: exactly the current
	// state's depth, which is the longest suffix of what has been consumed
	// that is a prefix of some pattern. Zero in the ordinary case, so a line
	// with nothing secret-shaped in it reaches the log with no delay.
	pend []byte
	// out is a reusable scratch buffer, so a chatty step does not allocate
	// once per write.
	out []byte
	n   int
}

// Writer wraps w. A nil *Set returns a Writer that passes bytes straight
// through with no scan, no buffer and no lock, which is what a run with no
// secrets gets.
func (s *Set) Writer(w io.Writer) *Writer { return &Writer{set: s, w: w} }

// Write scans p, replaces every complete occurrence of a registered pattern,
// and passes the result downstream.
//
// It always reports len(p) as the number of bytes consumed, including on a
// downstream error. io.Copy treats a short write with a nil error as
// io.ErrShortWrite and aborts the copy, so reporting the number of bytes
// actually written downstream (which differs from len(p) by design every time
// a replacement fires) would truncate the step's log at the first secret.
// Reporting len(p) alongside a real error keeps the error the reported cause
// rather than ErrShortWrite.
func (w *Writer) Write(p []byte) (int, error) {
	if w.set == nil {
		return w.w.Write(p)
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	s := w.set
	w.out = w.out[:0]
	for _, c := range p {
		w.pend = append(w.pend, c)
		w.state = s.step(w.state, c)

		if L := int(s.nodes[w.state].match); L > 0 {
			// The match ends at the byte just appended, so it starts L bytes
			// back. keep cannot be negative: pend is trimmed to the state's
			// depth after every non-matching byte, depth grows by at most one
			// per byte, and match is bounded by depth.
			keep := len(w.pend) - L
			w.out = append(w.out, w.pend[:keep]...)
			w.out = append(w.out, Placeholder...)
			w.pend = w.pend[:0]
			w.state = rootState
			w.n++
			continue
		}

		// Emit everything except the suffix that could still become a match.
		if d := int(s.nodes[w.state].depth); len(w.pend) > d {
			w.out = append(w.out, w.pend[:len(w.pend)-d]...)
			// copy semantics: append into pend[:0] from a later region of the
			// same backing array is a memmove and is safe for the overlap.
			w.pend = append(w.pend[:0], w.pend[len(w.pend)-d:]...)
		}
	}

	if len(w.out) > 0 {
		if _, err := w.w.Write(w.out); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

// Flush passes any held-back partial prefix downstream verbatim and resets
// the automaton. A partial prefix is by definition not a complete occurrence
// of anything, so emitting it is correct rather than a concession.
//
// The engine calls this once per stream at the end of an attempt, before the
// step's step.finished is emitted, so every step.log.appended marker for the
// attempt is already in the ledger by then. It is idempotent.
func (w *Writer) Flush() error {
	if w.set == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state = rootState
	if len(w.pend) == 0 {
		return nil
	}
	_, err := w.w.Write(w.pend)
	w.pend = w.pend[:0]
	return err
}

// Redacted is how many replacements this stream has made, which is what a
// secret.redacted event reports (design.md section 1.5).
func (w *Writer) Redacted() int {
	if w.set == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

var _ io.Writer = (*Writer)(nil)
```

- [ ] **Step 3: Run the tests, including under the race detector**

```bash
go test ./internal/redact/ -race -v
golangci-lint run ./internal/redact/...
```

**Composition.** `TestAValueSplitAcrossWritesIsStillCaught` walks the split point across every position in the line rather than picking one. The two pieces of this task, the automaton from Task 1 and the holdback here, are each correct in isolation for a value that arrives whole; the interaction between the `depth`-based holdback and a match that ends exactly at a chunk boundary is where a bug would live, and only sweeping the boundary finds it.

**The check that catches it.** `TestWriteAlwaysReportsTheFullInputLength` drives the writer through `io.Copy(io.MultiWriter(w), src)`, which is the literal call shape `os/exec` uses. A unit test that called `w.Write` directly and checked only the output bytes would pass against an implementation that returns the downstream count, and every step's log would then be truncated at its first secret in production.

---

### Task 4: Resolution, the one `Reveal` seam, and `secret.resolved`

**Files:**
- Create `internal/secrets/secrets.go`, `internal/secrets/secrets_test.go`
- Create `internal/secrets/reveal.go`, `internal/secrets/reveal_test.go`
- Create `internal/secrets/source.go`, `internal/secrets/source_test.go`
- Modify `run.go` (add `WithSecrets`, resolve in `RunPlan`, around line 33 to line 260)
- Modify `internal/engine/engine.go` (add `Options.Secrets`, `runCore.redact`, `runCore.secrets`, the MinLength refusal, the `secret.resolved` emits)
- Test `run_test.go`, `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `redact.New`, `redact.Value`, `redact.MinLength`, `redact.Placeholder` (Tasks 1 to 3).
- Produces:
  ```go
  package secrets

  type Secret struct {
      Name    string // the config field a step references, "Outer.Inner" when nested
      Source  string // the mamori source: URI with any userinfo and query removed
      Version string // always "" in v0; see the doc
      // value is unexported
  }
  func (s Secret) String() string
  func (s Secret) LogValue() slog.Value

  type Identity struct {
      Name    string
      Source  string
      Version string
  }

  type Set struct{ /* unexported */ }
  func FromConfig(cfg any) (*Set, error)
  func (s *Set) Len() int
  func (s *Set) Has(name string) bool
  func (s *Set) Names() []string
  func (s *Set) Identities() []Identity
  func (s *Set) Identity(name string) (Identity, bool)
  func (s *Set) Value(name string) ([]byte, bool)
  func (s *Set) RedactValues() []redact.Value

  package engine
  // Options gains: Secrets *secrets.Set
  // runCore gains: redact *redact.Set, secrets *secrets.Set, redactedPayloads atomic.Int64

  package senro
  func WithSecrets(cfg any) Option
  ```

**Wiring.** `senro.RunPlan` calls `secrets.FromConfig` and hands the result to `engine.Options.Secrets`; `engine.Run` builds the run's `*redact.Set` from it before any event is emitted and emits one `secret.resolved` per resolved secret. The test that proves it runs `senro.Run` with a struct that `mamori.Load` actually produced, and reads `events.jsonl` off disk.

**Class, not instance.** `revealValue` in `reveal.go` is the only function in senro that takes a value out of its wrapper, which is design.md §1.3's own instruction: "That's the only place in the codebase where `Reveal()` should appear, which makes the audit trivial, since it's one grep." Task 10 mechanises that grep. The recognition rule is duck-typed (`interface{ Sensitive() bool }` plus one of the two `Reveal` shapes) rather than a comparison against `secret.String` by name, so a user's own wrapper type is covered by the same rule and a future mamori type does not need a code change here.

- [ ] **Step 1: Write the failing test for the walk**

Create `internal/secrets/reveal_test.go`:

```go
package secrets_test

import (
	"strings"
	"testing"

	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/internal/secrets"
)

type flatConfig struct {
	NPMToken      secret.String `source:"aws-sm://ci/npm#token"`
	KubeConfig    secret.Bytes  `source:"vault://kv/ci/kubeconfig#raw"`
	DeployEnv     string        `source:"env:DEPLOY_ENV" default:"staging"`
	MaxParallel   int           `source:"env:CI_PARALLEL"`
	NeverResolved secret.String `source:"aws-sm://ci/unused#token"`
}

// TestFromConfigTakesSecretsAndLeavesConfigurationAlone is the recognition
// rule. A secret.String and a secret.Bytes are secrets; a plain string and a
// plain int with source tags are configuration and must NOT be registered,
// or a DeployEnv of "staging" would redact the word "staging" out of every
// log in the run.
func TestFromConfigTakesSecretsAndLeavesConfigurationAlone(t *testing.T) {
	cfg := flatConfig{
		NPMToken:    secret.NewString("npm-token-aaaaaaaa"),
		KubeConfig:  secret.NewBytes([]byte("kube-config-bbbbbbbb")),
		DeployEnv:   "staging",
		MaxParallel: 12,
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	names := set.Names()
	want := []string{"KubeConfig", "NPMToken"}
	if len(names) != len(want) {
		t.Fatalf("Names() = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("Names() = %v, want %v (sorted)", names, want)
		}
	}

	v, ok := set.Value("NPMToken")
	if !ok || string(v) != "npm-token-aaaaaaaa" {
		t.Errorf("Value(NPMToken) = (%q, %v)", v, ok)
	}
	v, ok = set.Value("KubeConfig")
	if !ok || string(v) != "kube-config-bbbbbbbb" {
		t.Errorf("Value(KubeConfig) = (%q, %v)", v, ok)
	}
	if _, ok := set.Value("DeployEnv"); ok {
		t.Error("DeployEnv, a plain string, was registered as a secret")
	}
	// An unset secret is not an error and is not registered: an optional
	// credential nobody referenced must not make the run refuse to start.
	if _, ok := set.Value("NeverResolved"); ok {
		t.Error("an empty secret.String was registered")
	}
}

// TestIdentitiesCarryNoValue is the containment assertion for the one struct
// that is going to be marshalled into events.jsonl and into a cache entry.
func TestIdentitiesCarryNoValue(t *testing.T) {
	cfg := flatConfig{NPMToken: secret.NewString("npm-token-aaaaaaaa")}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	ids := set.Identities()
	if len(ids) != 1 {
		t.Fatalf("Identities() = %v, want one entry", ids)
	}
	if ids[0].Name != "NPMToken" {
		t.Errorf("Name = %q, want NPMToken", ids[0].Name)
	}
	if ids[0].Source != "aws-sm://ci/npm#token" {
		t.Errorf("Source = %q, want the tag verbatim", ids[0].Source)
	}
	// The canary: the assertion below can only mean anything if the struct
	// it is scanning actually has content.
	rendered := ids[0].Name + "|" + ids[0].Source + "|" + ids[0].Version
	if !strings.Contains(rendered, "NPMToken") {
		t.Fatal("the rendered identity is empty; the check below proves nothing")
	}
	if strings.Contains(rendered, "npm-token-aaaaaaaa") {
		t.Errorf("an Identity carries the value: %q", rendered)
	}
}

// TestIdentityDoesNotPrintAValue covers the accidental %v, the slog line and
// the panic dump. secret.String already protects itself; the types senro's
// own code passes around have to as well.
func TestIdentityDoesNotPrintAValue(t *testing.T) {
	cfg := flatConfig{NPMToken: secret.NewString("npm-token-aaaaaaaa")}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	// Names is the only accessor that hands back a Secret-shaped string, so
	// the rendering is checked through the type's own String method.
	for _, id := range set.Identities() {
		if strings.Contains(id.String(), "npm-token") {
			t.Errorf("Identity.String() leaked the value: %q", id.String())
		}
	}
}

type nestedConfig struct {
	Registry struct {
		Token secret.String `source:"aws-sm://ci/ghcr#token"`
	}
	Embedded
	Ptr  *inner
	Nil  *inner
	Slack secret.String `source:"aws-sm://ci/slack#webhook_url"`
}

type Embedded struct {
	EmbeddedToken secret.String `source:"aws-sm://ci/embedded#token"`
}

type inner struct {
	InnerToken secret.String `source:"aws-sm://ci/inner#token"`
}

// TestFromConfigWalksNestedStructs matters because mamori itself recurses
// into an untagged nested struct (its decode.go does), so a config grouped
// into sub-structs would otherwise be half covered: mamori would populate the
// nested secrets and senro would neither redact nor deliver them.
//
// An embedded field's secrets keep their bare names, matching Go's own field
// promotion. A named nested struct's secrets are qualified with a dot, which
// is what a step's SecretEnv reference has to spell (Task 7).
func TestFromConfigWalksNestedStructs(t *testing.T) {
	cfg := nestedConfig{Ptr: &inner{InnerToken: secret.NewString("inner-cccccccc")}}
	cfg.Registry.Token = secret.NewString("ghcr-dddddddd")
	cfg.EmbeddedToken = secret.NewString("embedded-eeeeeeee")
	cfg.Slack = secret.NewString("slack-ffffffff")

	set, err := secrets.FromConfig(&cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	for _, want := range []string{"Registry.Token", "EmbeddedToken", "Ptr.InnerToken", "Slack"} {
		if !set.Has(want) {
			t.Errorf("secret %q was not found; Names() = %v", want, set.Names())
		}
	}
	if set.Len() != 4 {
		t.Errorf("Len() = %d, want 4 (a nil *inner contributes nothing)", set.Len())
	}
}

type unexportedConfig struct {
	token secret.String `source:"aws-sm://ci/npm#token"` //nolint:unused // the point of the test
}

// TestFromConfigRefusesAnUnexportedSecret is the loud failure in place of a
// silent hole. reflect cannot read an unexported field's value, so such a
// secret can be neither delivered nor redacted, and skipping it quietly would
// leave the author believing a credential is protected that is not.
func TestFromConfigRefusesAnUnexportedSecret(t *testing.T) {
	_, err := secrets.FromConfig(unexportedConfig{})
	if err == nil {
		t.Fatal("FromConfig accepted an unexported field carrying a source tag")
	}
	if !strings.Contains(err.Error(), "token") || !strings.Contains(err.Error(), "unexported") {
		t.Errorf("error %q must name the field and say why", err)
	}
}

// TestFromConfigRefusesANonStruct and its nil sibling. WithSecrets takes what
// mamori.Load returned, and anything else is a mistake worth naming at the
// call site rather than a silent empty set.
func TestFromConfigRefusesANonStruct(t *testing.T) {
	if _, err := secrets.FromConfig("a string"); err == nil {
		t.Error("FromConfig accepted a string")
	}
	if _, err := secrets.FromConfig((*flatConfig)(nil)); err == nil {
		t.Error("FromConfig accepted a nil pointer")
	}
}

// TestANilSetIsUsable keeps the engine's call sites branch-free: a run with
// no WithSecrets holds a nil *Set and calls the same methods.
func TestANilSetIsUsable(t *testing.T) {
	var s *secrets.Set
	if s.Len() != 0 || s.Names() != nil || s.Identities() != nil || s.RedactValues() != nil {
		t.Error("a nil *Set is not behaving as an empty one")
	}
	if _, ok := s.Value("anything"); ok {
		t.Error("a nil *Set returned a value")
	}
	if s.Has("anything") {
		t.Error("a nil *Set reported Has")
	}
}
```

Run and watch it fail to compile.

```bash
go test ./internal/secrets/
```

- [ ] **Step 2: Write `source.go` and its test**

Create `internal/secrets/source.go`:

```go
package secrets

import "strings"

// sourceIdentity reduces a mamori source tag to what is safe to record in an
// event and in a cache key.
//
// mamori's ref grammar (its ref.go) is:
//
//	scheme://path[#key][?opts]   for hierarchical schemes (aws-sm, vault, file)
//	scheme:path[#key][?opts]     for opaque schemes (env, exec)
//
// Note that the fragment comes BEFORE the query, which is the reverse of a
// standard URL, so net/url cannot parse this and the two cuts are done here.
//
// Two parts are removed. The userinfo, because "vault://user:pass@host/kv" is
// a source URI with a credential inside it and this string is written to
// events.jsonl and into a cache entry that outlives the run. And the query,
// because mamori's ?opts are decoding directives (?decode=base64) that say
// nothing about WHICH secret this is, while a provider is free to accept an
// option that carries one.
//
// A tag mamori would reject is not rejected here: this function's job is to
// produce a safe identity string, and mamori.Load has already failed on a
// malformed ref long before senro sees the struct.
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
```

Create `internal/secrets/source_test.go`:

```go
package secrets

import "testing"

// TestSourceIdentity is a whitebox test because sourceIdentity is the exact
// function whose output reaches events.jsonl and a cache key, and its safety
// argument is about bytes rather than about the Set's behaviour.
func TestSourceIdentity(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"aws-sm://ci/npm#token", "aws-sm://ci/npm#token"},
		{"env:DEPLOY_ENV", "env:DEPLOY_ENV"},
		{"file:///etc/senro/token", "file:///etc/senro/token"},
		{"aws-sm://ci/npm#token?decode=base64", "aws-sm://ci/npm#token"},
		{"vault://user:hunter2@vault.internal/kv/ci#raw", "vault://vault.internal/kv/ci#raw"},
		{"vault://token@vault.internal/kv/ci", "vault://vault.internal/kv/ci"},
		{"vault://user:pw@host", "vault://host"},
	}
	for _, tc := range cases {
		if got := sourceIdentity(tc.in); got != tc.want {
			t.Errorf("sourceIdentity(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestSourceIdentityNeverKeepsAUserinfoSecret is the negative case stated as
// the property rather than as a table row, so a future grammar change that
// broke the cut is caught by intent rather than by one example.
func TestSourceIdentityNeverKeepsAUserinfoSecret(t *testing.T) {
	for _, in := range []string{
		"vault://u:hunter2@h/p",
		"vault://u:hunter2@h/p#k",
		"vault://u:hunter2@h/p#k?decode=hex",
		"postgres://u:hunter2@h:5432/db#password",
	} {
		got := sourceIdentity(in)
		if got == "" {
			t.Fatalf("sourceIdentity(%q) returned empty; the assertion below proves nothing", in)
		}
		if contains(got, "hunter2") {
			t.Errorf("sourceIdentity(%q) = %q kept the password", in, got)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
```

- [ ] **Step 3: Write `secrets.go`**

Create `internal/secrets/secrets.go`:

```go
// Package secrets holds a run's resolved credentials.
//
// Resolution is central and happens once, on the coordinator, at run start
// (design.md section 1.1). This package does not resolve anything: mamori
// does that, before senro.Run is ever called, and this package receives the
// struct mamori.Load returned and takes the values out of it. That division
// is the whole point of section 1.2 ("senro does not define a provider
// interface"), and it is why there is no provider registry, no version
// pinning and no watch loop anywhere in senro.
//
// A value is unexported inside a Secret and leaves this package through
// exactly one accessor, Value, so every route to a plaintext credential is
// one grep away.
package secrets

import (
	"log/slog"
	"sort"

	"github.com/xavidop/senro/internal/redact"
)

// Secret is one resolved credential and the identity of where it came from.
type Secret struct {
	// Name is the Go struct field a step references with SecretEnv. A field
	// inside a named nested struct is qualified with a dot ("Registry.Token");
	// a field promoted from an embedded struct keeps its bare name, matching
	// Go's own promotion.
	Name string
	// Source is the mamori source: tag with any userinfo and any query
	// removed (see sourceIdentity). It is identity, never content, and it is
	// what reaches a secret.resolved event and a cache key's Secrets
	// component.
	Source string
	// Version is the provider's version for this value. Always "" in v0:
	// mamori surfaces a Value.Version to a provider, not to Load's caller, so
	// senro has no way to read one. Declared because design.md section 1.6
	// keys on it and api.SecretResolvedBody already publishes the field.
	Version string

	value []byte
}

// String renders a Secret for a %v, a %s and a panic dump without its value.
func (s Secret) String() string { return s.Name + "=" + redact.Placeholder }

// LogValue keeps a Secret out of a structured log line, the same way mamori's
// own secret.String does.
func (s Secret) LogValue() slog.Value {
	return slog.GroupValue(slog.String("name", s.Name), slog.String("source", s.Source))
}

// Identity is a Secret with no value at all, which is the form that leaves
// this package for an event payload or a cache key. Constructing one is what
// makes "no value here" a property of the type rather than of a caller's
// discipline.
type Identity struct {
	Name    string `json:"name"`
	Source  string `json:"source,omitempty"`
	Version string `json:"version,omitempty"`
}

// String renders an Identity, which by construction has nothing to hide.
func (i Identity) String() string { return i.Name + " " + i.Source }

// Set is a run's resolved secrets. The nil *Set is a run with none, and every
// method treats it as empty so the engine's call sites never branch.
type Set struct {
	order  []*Secret
	byName map[string]*Secret
}

func (s *Set) add(sec *Secret) {
	if s.byName == nil {
		s.byName = make(map[string]*Secret)
	}
	if _, dup := s.byName[sec.Name]; dup {
		return
	}
	s.byName[sec.Name] = sec
	s.order = append(s.order, sec)
}

// Len is how many secrets resolved to a non-empty value.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.order)
}

// Has reports whether name resolved to a value, which is what the engine's
// reference check asks before a step declares it needs one.
func (s *Set) Has(name string) bool {
	if s == nil {
		return false
	}
	_, ok := s.byName[name]
	return ok
}

// Names is every resolved secret's name, sorted, so an error message that
// lists them is stable.
func (s *Set) Names() []string {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.order))
	for _, sec := range s.order {
		out = append(out, sec.Name)
	}
	sort.Strings(out)
	return out
}

// Identities is every secret's identity, sorted by name. This is what the
// engine emits as secret.resolved and what Task 9 folds into a cache key.
func (s *Set) Identities() []Identity {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	out := make([]Identity, 0, len(s.order))
	for _, sec := range s.order {
		out = append(out, Identity{Name: sec.Name, Source: sec.Source, Version: sec.Version})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Identity looks one secret's identity up by name. Its caller is the cache
// key builder (Task 9), which needs the source and the version alongside a
// digest of the value and must never hold the two in one struct.
func (s *Set) Identity(name string) (Identity, bool) {
	if s == nil {
		return Identity{}, false
	}
	sec, ok := s.byName[name]
	if !ok {
		return Identity{}, false
	}
	return Identity{Name: sec.Name, Source: sec.Source, Version: sec.Version}, true
}

// Value is the one accessor that hands back a plaintext credential. Its only
// caller is the engine's delivery path (Task 7), which passes the bytes
// straight to Sandbox.PutSecret and keeps no copy.
//
// It returns a copy, so a caller that mutates or retains the slice cannot
// reach back into this Set.
func (s *Set) Value(name string) ([]byte, bool) {
	if s == nil {
		return nil, false
	}
	sec, ok := s.byName[name]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), sec.value...), true
}

// RedactValues is the seed for the run's redactor: every value, labelled by
// its own name so redact.Set.Match can say WHICH secret it found without
// printing it. This is design.md section 1.3's "seed the stream redactor",
// and it is called once, in engine.Run, before the first event.
func (s *Set) RedactValues() []redact.Value {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	out := make([]redact.Value, 0, len(s.order))
	for _, sec := range s.order {
		out = append(out, redact.Value{Label: sec.Name, Value: sec.value})
	}
	return out
}
```

- [ ] **Step 4: Write `reveal.go`, the one seam**

Create `internal/secrets/reveal.go`:

```go
package secrets

import (
	"fmt"
	"reflect"

	"github.com/xavidop/mamori/secret"
)

// maxDepth bounds the walk. A config struct nested deeper than this is a
// mistake worth naming rather than a shape to support, and the bound removes
// any question about what a pathological type could do to run start.
const maxDepth = 8

// revealer and byteRevealer are the two shapes a self-redacting value takes.
// sensitive is mamori's marker, and a field must satisfy it AND one of the
// revealers to count, which is what keeps a plain `DeployEnv string` with a
// source tag out of the redactor. A DeployEnv of "staging" registered as a
// secret would redact the word "staging" out of every log in the run.
type (
	revealer     interface{ Reveal() string }
	byteRevealer interface{ Reveal() []byte }
	sensitive    interface{ Sensitive() bool }
)

// These four lines are the whole of senro's PRODUCTION coupling to mamori,
// and they are deliberately compile-time rather than behavioural: if a future
// mamori changes Reveal's receiver or its signature, this file stops
// compiling instead of silently reclassifying every credential in every
// pipeline as ordinary configuration. Reveal has a VALUE receiver on both
// types, which is what lets the walk below call it on a non-addressable field
// of a struct that mamori.Load returned by value.
var (
	_ revealer     = secret.String{}
	_ sensitive    = secret.String{}
	_ byteRevealer = secret.Bytes{}
	_ sensitive    = secret.Bytes{}
)

// FromConfig walks the struct mamori.Load returned and collects every secret
// in it.
//
// This is design.md section 1.3's seam, in full: "the seam between the two is
// exactly one call: walk the resolved config struct, Reveal() each
// secret.String, and seed the stream redactor. That's the only place in the
// codebase where Reveal() should appear, which makes the audit trivial, since
// it's one grep." reveal_static_test.go at the repository root is that grep,
// mechanised, and it fails the build if a second call site appears anywhere.
//
// cfg may be a struct or a pointer to one. Anything else is refused by name,
// because WithSecrets(someString) is a mistake worth reporting at the call
// site rather than a run that starts with an empty set and delivers nothing.
func FromConfig(cfg any) (*Set, error) {
	if cfg == nil {
		return nil, fmt.Errorf("secrets: WithSecrets was given nil; pass the struct mamori.Load returned")
	}
	rv := reflect.ValueOf(cfg)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("secrets: WithSecrets was given a nil %s", rv.Type())
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"secrets: WithSecrets takes the struct mamori.Load returned; got %s", rv.Type())
	}
	s := &Set{}
	if err := s.walk(rv, "", 0); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Set) walk(rv reflect.Value, prefix string, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("secrets: config struct nests more than %d levels deep at %q", maxDepth, prefix)
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		name := prefix + f.Name
		tag := f.Tag.Get("source")

		if !f.IsExported() {
			// reflect cannot read this field's value, so a secret here can be
			// neither delivered nor redacted. Refusing by name beats skipping
			// silently: the author believes it is covered, and it is not.
			if tag != "" {
				return fmt.Errorf(
					"secrets: config field %q is unexported but declares source:%q; "+
						"senro cannot read it, so it can be neither delivered nor redacted; "+
						"export the field", name, tag)
			}
			continue
		}

		fv := rv.Field(i)
		if v, ok := revealValue(fv); ok {
			if len(v) == 0 {
				// An optional secret nobody set. Not an error: a step that
				// references it gets a named refusal at run start (Task 7),
				// and a step that does not is unaffected.
				continue
			}
			s.add(&Secret{Name: name, Source: sourceIdentity(tag), value: v})
			continue
		}

		// Not a secret. Recurse, because mamori itself recurses into an
		// untagged nested struct (its decode.go), so a config grouped into
		// sub-structs would otherwise be populated by mamori and invisible to
		// senro: half covered is the worst of the three options.
		//
		// An embedded field keeps the current prefix, matching Go's own field
		// promotion, so cfg.EmbeddedToken is referenced as "EmbeddedToken".
		sub := prefix
		if !f.Anonymous {
			sub = name + "."
		}
		switch fv.Kind() {
		case reflect.Struct:
			if err := s.walk(fv, sub, depth+1); err != nil {
				return err
			}
		case reflect.Pointer:
			if !fv.IsNil() && fv.Elem().Kind() == reflect.Struct {
				if err := s.walk(fv.Elem(), sub, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// revealValue is THE ONE PLACE in senro where a secret value is taken out of
// its wrapper. Nothing else in this repository may call Reveal; see
// FromConfig's doc and reveal_static_test.go.
func revealValue(fv reflect.Value) ([]byte, bool) {
	if !fv.CanInterface() {
		return nil, false
	}
	iv := fv.Interface()
	sens, ok := iv.(sensitive)
	if !ok || !sens.Sensitive() {
		return nil, false
	}
	switch r := iv.(type) {
	case revealer:
		return []byte(r.Reveal()), true
	case byteRevealer:
		return append([]byte(nil), r.Reveal()...), true
	}
	return nil, false
}
```

Run the package's tests. They must now pass.

```bash
go test ./internal/secrets/ -v
```

- [ ] **Step 5: Write the failing test for `WithSecrets` through `senro.Run`**

This is the wiring proof, and it drives the real path: `mamori.Load` with `mamoritest`'s in-memory provider, `senro.Run`, then `events.jsonl` read off disk.

Append to `run_test.go`:

```go
// TestRunEmitsSecretResolvedForEveryResolvedSecret drives the whole seam:
// mamori resolves a struct through an in-memory provider, WithSecrets hands
// that struct to Run, and the run's own ledger on disk carries one
// secret.resolved per secret, with the identity and no value.
//
// mamoritest rather than a real provider because the cloud provider
// submodules in the mamori repository are not separately tagged and cannot be
// fetched from outside it. The env: and file: schemes are the other
// credential-free options; the in-memory provider is preferred here because
// it exercises WithProvider, which is the option design.md section 1.2's own
// example uses.
func TestRunEmitsSecretResolvedForEveryResolvedSecret(t *testing.T) {
	type config struct {
		NPMToken  secret.String `source:"fake://ci/npm#token"`
		DeployEnv string        `source:"fake://ci/env#name"`
	}

	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", "npm-token-aaaaaaaaaa")
	p.Set("ci/env#name", "staging")

	ctx := context.Background()
	cfg, err := mamori.Load[config](ctx, mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	pipe := senro.New("p")
	wf := pipe.Workflow("w")
	wf.Step("noop", exec.Command("true"))

	if err := senro.Run(ctx, pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	// The canary: without this, every assertion below would pass against an
	// empty file.
	if !bytes.Contains(raw, []byte(`"secret.resolved"`)) {
		t.Fatalf("no secret.resolved event in the ledger; the checks below prove nothing")
	}
	if !bytes.Contains(raw, []byte(`"NPMToken"`)) {
		t.Error("secret.resolved does not name the secret")
	}
	if !bytes.Contains(raw, []byte(`"fake://ci/npm#token"`)) {
		t.Error("secret.resolved does not carry the source URI")
	}
	if bytes.Contains(raw, []byte("npm-token-aaaaaaaaaa")) {
		t.Error("the ledger contains the secret VALUE")
	}
	if bytes.Contains(raw, []byte(`"DeployEnv"`)) {
		t.Error("DeployEnv, a plain string, was reported as a secret")
	}
}

// TestRunRefusesASecretTooShortToRedact is design decision 5. Skipping a
// short value silently would leave the author believing it is protected.
func TestRunRefusesASecretTooShortToRedact(t *testing.T) {
	type config struct {
		PIN secret.String `source:"fake://ci/pin#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/pin#v", "1234")

	ctx := context.Background()
	cfg, err := mamori.Load[config](ctx, mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("noop", exec.Command("true"))

	err = senro.Run(ctx, pipe,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a secret shorter than redact.MinLength")
	}
	if !strings.Contains(err.Error(), "PIN") {
		t.Errorf("the error must name the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "1234") {
		t.Errorf("the error contains the value: %q", err)
	}
}

// TestWithSecretsRejectsANonStruct proves the error reaches the Run caller
// rather than being swallowed into an empty set.
func TestWithSecretsRejectsANonStruct(t *testing.T) {
	pipe := senro.New("p")
	pipe.Workflow("w").Step("noop", exec.Command("true"))
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()),
		senro.WithSecrets("not a struct"))
	if err == nil {
		t.Fatal("Run accepted WithSecrets(\"not a struct\")")
	}
}
```

Add to `run_test.go`'s import block:

```go
	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
```

- [ ] **Step 6: Add `WithSecrets` to `run.go`**

In `runConfig`, add two fields:

```go
	secrets     any
	hasSecrets  bool
```

`hasSecrets` rather than a nil check on `secrets`, because `WithSecrets(nil)` is a mistake that must be reported and an `any` holding a nil pointer is not `== nil` anyway.

Add the option, next to `WithCacheDir`:

```go
// WithSecrets hands Run the resolved configuration struct mamori.Load
// returned, exactly as design.md section 12's worked example does:
//
//	cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(awssm.New()))
//	if err != nil { return err }
//	senro.Run(ctx, pipeline(cfg), senro.WithSecrets(cfg))
//
// Load, not Watch. design.md section 1.2: "A run lasts minutes and reads each
// value once; nothing rotates underneath it." senro therefore takes a
// snapshot of the struct, not a subscription to it, and a value that rotates
// mid-run is a step that fails on use rather than a re-delivery path.
//
// What senro does with it, once:
//
//   - Every field that is self-redacting (mamori's secret.String and
//     secret.Bytes, or any type with the same Sensitive/Reveal shape) has its
//     value read exactly once, in internal/secrets/reveal.go, which is the
//     only place in senro that reads one. A plain string or int field with a
//     source tag is configuration, not a credential, and is left alone.
//   - Those values seed the run's redactor, which sits in front of the event
//     ledger and every log file (design.md section 1.5 and section 6.11).
//   - Their identities, the field name and the source URI with any userinfo
//     removed, are emitted as one secret.resolved event each. A value is
//     never in an event.
//   - A step that declared SecretEnv receives its value as a file, with the
//     path in the environment. The value never enters a command argument,
//     an environment variable, a cache key, or plan.json.
//
// Run refuses to start if a resolved value is shorter than six bytes, because
// a value that short cannot be redacted without redacting unrelated output
// (design.md section 1.5), and refuses to start if any step would put a
// resolved value into a command argument or an environment variable, because
// those are visible in ps(1) and /proc/<pid>/environ, where no redactor can
// reach. See the secrets section of the README for the full channel list.
//
// Passing anything that is not a struct or a pointer to one is an error Run
// returns rather than an empty set it silently proceeds with.
func WithSecrets(cfg any) Option {
	return func(c *runConfig) { c.secrets, c.hasSecrets = cfg, true }
}
```

In `RunPlan`, after the storage is opened and before `engine.Run`:

```go
	var secretSet *secrets.Set
	if cfg.hasSecrets {
		secretSet, err = secrets.FromConfig(cfg.secrets)
		if err != nil {
			return fmt.Errorf("senro: %w", err)
		}
	}
```

and add `Secrets: secretSet` to the `engine.Options` literal. Add `"github.com/xavidop/senro/internal/secrets"` to the import block.

- [ ] **Step 7: Wire the engine**

In `internal/engine/engine.go`, add to `Options`:

```go
	// Secrets is the run's resolved credentials (design.md section 1.2),
	// resolved once on the coordinator before the first step. A nil Secrets
	// is a run with none: no redactor is built, no secret.resolved is
	// emitted, no delivery happens, and every call site here treats the nil
	// set as empty rather than branching on it.
	//
	// This engine never resolves anything itself. senro.Run's WithSecrets
	// takes the struct mamori.Load returned and internal/secrets walks it;
	// see design.md section 1.2 for why senro defines no provider interface.
	Secrets *secrets.Set
```

Add to `runCore`:

```go
	// redact is this run's pattern set, built from Options.Secrets before the
	// first event is emitted. nil for a run with no secrets, which is the
	// free path: append's redaction is one nil check.
	//
	// It is immutable from the moment Run assigns it, which is why append
	// reads it with no lock and why the scan happens OUTSIDE emitMu: Sink.Emit
	// must never block, and holding the ledger's critical section across a
	// scan of a payload would put every other emitter behind it.
	redact *redact.Set

	// secrets is the same set the redactor was built from, kept so the
	// delivery path (attempt.go) can look up a value by name.
	secrets *secrets.Set

	// redactedPayloads counts replacements made in event payloads. Not
	// emitted as a secret.redacted event: that event is step-scoped and
	// reports log-stream redactions (design.md section 1.5's own example
	// carries a "step" field), and emitting an event from inside the emit
	// path would recurse. Read by internal tests.
	redactedPayloads atomic.Int64
```

In `Run`, immediately after the `opts.Storage == nil && planNeedsStorage(p)` check and BEFORE `eventlog.Open`, so a refused run creates nothing on disk:

```go
	// The redactor is built before anything is opened or emitted, because
	// every event this run produces has to pass through it, including the
	// very first one. design.md section 6.11: redaction runs before the hub,
	// so a client never receives values regardless of its permissions.
	red := redact.New(opts.Secrets.RedactValues()...)
	if skipped := red.Skipped(); len(skipped) > 0 {
		return "", fmt.Errorf(
			"engine: secret(s) %s resolved to a value shorter than %d bytes; senro cannot "+
				"redact a value that short without redacting unrelated output, so it refuses "+
				"to run rather than deliver a credential it cannot protect (design.md 1.5). "+
				"Use a longer credential, or stop declaring this field as a secret",
			strings.Join(skipped, ", "), redact.MinLength)
	}
```

Assign it on the `runCore` literal:

```go
	rc := &runCore{
		ledger: ledger, sink: opts.Sink, runID: opts.RunID, cancel: cancel,
		oc: newOutcomes(len(p.Nodes)), ws: ws,
		redact: red, secrets: opts.Secrets,
	}
```

And emit the identities, immediately after the `plan.resolved` emit and before the `step.created` loop, because resolution is a run-level fact that happened before any step existed:

```go
	// One secret.resolved per resolved credential: identity only, never a
	// value (api.SecretResolvedBody's own doc says so). design.md section 1.2
	// notes that mamori's middleware.Audit gives this event essentially for
	// free and "logs access without payloads, which is exactly the semantics
	// the event stream needs".
	for _, id := range opts.Secrets.Identities() {
		rc.emit(api.Event{
			Type: api.SecretResolved, Run: opts.RunID,
			Payload: mustMarshal(api.SecretResolvedBody{
				Name: id.Name, Source: id.Source, Version: id.Version,
			}),
		})
	}
```

Add `"strings"`, `"sync/atomic"`, `"github.com/xavidop/senro/internal/redact"` and `"github.com/xavidop/senro/internal/secrets"` to the import block.

- [ ] **Step 8: Run everything and confirm the golden fixtures did not move**

```bash
go test ./internal/secrets/ ./internal/engine/ . -race
go test ./internal/engine/ -run Golden -v
make all
```

The four golden fixtures declare no secrets, so `Options.Secrets` is nil, `Identities()` returns nil, no `secret.resolved` is emitted, and the event sequence is byte-identical. If any golden moved, the emit was placed on an unconditional path and must be corrected rather than regenerated.

**The canary.** `TestRunEmitsSecretResolvedForEveryResolvedSecret` asserts `"secret.resolved"` is present in the ledger with `t.Fatalf` before it asserts the value is absent. Without that guard, a run that emitted nothing at all, or wrote to a different directory, would pass every containment check in the test.

---

### Task 5: Redaction on the event path

**Files:**
- Modify `internal/engine/engine.go` (`runCore.append`, around line 450)
- Create `internal/engine/redact_internal_test.go`
- Test `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `redact.Set.Redact` (Task 1), `runCore.redact` (Task 4).
- Produces:
  ```go
  package engine
  func (rc *runCore) redactPayload(p json.RawMessage) json.RawMessage
  ```

**Wiring.** `runCore.append` is the production caller, and it is the only function in this engine through which an event reaches either `ledger.Append` or `sink.Emit`. There is no second path and the package's own doc comments say so twice. Redacting here therefore covers `events.jsonl`, the attach hub, the TUI, the plain renderer, the `RunState` fold, and every sink a later phase adds, with one call.

**Class, not instance.** The reported hole is `api.StepStartedBody.Cmd`. The fix is not "redact Cmd": it is "redact every payload", because `StepFinishedBody.Error`, `StepRetriedBody.Reason` and `CacheMissBody.Differing` are all built at run time from text senro did not author, and a future event type will be too. A field-by-field fix would close one of four and leave the class open, which is the defect three earlier plans shipped.

- [ ] **Step 1: Write the failing internal test**

Create `internal/engine/redact_internal_test.go`:

```go
package engine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestRedactPayloadReplacesAValueInAnyField is the class check: the same one
// call covers every payload type, so the test asserts against three unrelated
// shapes rather than against the one field that was reported.
func TestRedactPayloadReplacesAValueInAnyField(t *testing.T) {
	rc := &runCore{redact: redact.New(redact.Value{Label: "tok", Value: []byte("s3cr3t-value-here")})}

	for _, in := range []string{
		`{"cmd":["curl","--header","Authorization: s3cr3t-value-here"]}`,
		`{"state":"failed","error":"chdir /tmp/s3cr3t-value-here: no such file or directory"}`,
		`{"attempt":2,"reason":"exit status 1: s3cr3t-value-here rejected"}`,
	} {
		out := rc.redactPayload(json.RawMessage(in))
		if strings.Contains(string(out), "s3cr3t-value-here") {
			t.Errorf("the value survived in %s", out)
		}
		if !strings.Contains(string(out), redact.Placeholder) {
			t.Errorf("no placeholder in %s; this assertion was not looking at redacted output", out)
		}
		if !json.Valid(out) {
			t.Errorf("the redacted payload is not valid JSON: %s", out)
		}
	}
}

// TestRedactPayloadIsAnIdentityWhenThereIsNothingToDo pins the free path. The
// returned value must be the SAME slice, not a copy, because this runs on
// every event of every run and a pipeline with no secrets must pay nothing.
func TestRedactPayloadIsAnIdentityWhenThereIsNothingToDo(t *testing.T) {
	in := json.RawMessage(`{"state":"succeeded"}`)

	rc := &runCore{}
	if out := rc.redactPayload(in); &out[0] != &in[0] {
		t.Error("a nil redactor copied the payload")
	}
	rc = &runCore{redact: redact.New(redact.Value{Label: "tok", Value: []byte("absent-value-xyz")})}
	if out := rc.redactPayload(in); &out[0] != &in[0] {
		t.Error("a payload with no match was copied")
	}
	if rc.redactedPayloads.Load() != 0 {
		t.Errorf("redactedPayloads = %d with nothing redacted", rc.redactedPayloads.Load())
	}
}

// TestRedactPayloadDropsABodyItCouldNotKeepValid is the negative case for the
// one way this could produce output no reader can decode: a replacement that
// spans a JSON structural boundary. Vanishingly unlikely for a value of six
// bytes or more, and the answer must still be a body a fold can skip rather
// than a line that breaks every downstream parser of events.jsonl.
func TestRedactPayloadDropsABodyItCouldNotKeepValid(t *testing.T) {
	// The value straddles the closing quote and brace of the payload.
	rc := &runCore{redact: redact.New(redact.Value{Label: "tok", Value: []byte(`xxxxxx"}`)})}
	out := rc.redactPayload(json.RawMessage(`{"a":"yyxxxxxx"}`))
	if !json.Valid(out) {
		t.Fatalf("redactPayload produced invalid JSON: %s", out)
	}
	if strings.Contains(string(out), `xxxxxx"}`) {
		t.Errorf("the value survived: %s", out)
	}
	if string(out) != `{"redacted":true}` {
		t.Errorf("out = %s, want the documented fallback body", out)
	}
	if rc.redactedPayloads.Load() != 1 {
		t.Errorf("redactedPayloads = %d, want 1", rc.redactedPayloads.Load())
	}
}
```

Run and watch it fail: `redactPayload` does not exist.

```bash
go test ./internal/engine/ -run RedactPayload
```

- [ ] **Step 2: Write `redactPayload` and call it from `append`**

Add to `internal/engine/engine.go`, next to `append`:

```go
// redactPayload removes every registered secret value from one event's body.
//
// This is design.md section 6.11's requirement, implemented where it can
// actually hold: "Secrets are redacted before the hub, so a client never
// receives values regardless of authorization. Redaction is not an
// authorization mechanism, but it is the backstop." A filter on the way out
// to a particular client would be too late by construction, because
// events.jsonl is written first and a FileSource post-mortem reader opens it
// with no server in the loop at all.
//
// It runs OUTSIDE emitMu, deliberately. Sink.Emit must never block, and the
// ledger's critical section already serialises every emitter in the run; a
// scan held inside it would put each one behind the others' payload scans.
// rc.redact is immutable from the moment Run assigns it, so no lock is
// needed to read it.
//
// Cost when there is nothing to do is one nil check, and when there are
// secrets but no match it is one pass over a payload that is typically a few
// hundred bytes, returning the caller's own slice. Neither is a cost a
// no-secrets pipeline pays at all.
//
// A replacement can in principle straddle a JSON structural boundary and
// leave a body that no longer parses. Placeholder contains no quote and no
// backslash, so a replacement CONTAINED in a string literal is always still
// valid, and only a match that consumed a quote or a brace can break it,
// which needs a secret value that itself contains JSON punctuation. The
// answer is a body a fold can skip rather than a line that breaks every
// parser of events.jsonl downstream: the routing fields (seq, type, step,
// attempt) are outside Payload and are untouched either way.
func (rc *runCore) redactPayload(p json.RawMessage) json.RawMessage {
	if rc.redact == nil || len(p) == 0 {
		return p
	}
	out, n := rc.redact.Redact(p)
	if n == 0 {
		return p
	}
	rc.redactedPayloads.Add(int64(n))
	if !json.Valid(out) {
		return json.RawMessage(`{"redacted":true}`)
	}
	return out
}
```

And in `append`, on the line after `e.Run = rc.runID`:

```go
	e.Payload = rc.redactPayload(e.Payload)
```

- [ ] **Step 3: Write the failing integration test through `senro.Run`**

The channel is `Node.WorkDir`, which the run-start guard in Task 8 deliberately does not scan and which reaches an event payload through text senro did not author: `os/exec` reports a failed chdir as `chdir <dir>: no such file or directory`, and the engine records that in `StepFinishedBody.Error`.

Append to `internal/engine/engine_test.go`:

```go
// TestAValueInARuntimeErrorIsRedactedInTheLedger proves the backstop against
// a payload field the run-start guard cannot see. WorkDir is not a channel
// the guard refuses (see design decision 1: argv and the environment block
// are OS-visible, a working directory is not), and os/exec puts the directory
// verbatim into the error it returns, which the engine records in
// step.finished's error field.
//
// This is the general case the class fix exists for: every payload string
// built at run time from something senro did not author.
func TestAValueInARuntimeErrorIsRedactedInTheLedger(t *testing.T) {
	const value = "s3cr3t-value-here"

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("chdir", exec.Command("true")).
		WorkDir(filepath.Join(t.TempDir(), "missing-"+value))

	// The run fails, which is the point: the failure is what carries the
	// value into an event.
	runErr := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if runErr == nil {
		t.Fatal("the run succeeded; it was supposed to fail on a missing working directory")
	}

	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	// The canary, twice over: the ledger must contain the step's finished
	// event AND evidence that redaction ran, or "the value is absent" is a
	// statement about an empty file.
	if !bytes.Contains(raw, []byte(`"step.finished"`)) {
		t.Fatalf("no step.finished in the ledger; the checks below prove nothing")
	}
	if !bytes.Contains(raw, []byte(redact.Placeholder)) {
		t.Fatalf("no placeholder in the ledger; redaction did not run on this path")
	}
	if bytes.Contains(raw, []byte(value)) {
		t.Error("the ledger contains the secret value")
	}
	// And the run's OWN error, which senro.Run hands the caller, must not
	// carry it either: RunError renders step names, never step error text,
	// and this pins that.
	if strings.Contains(runErr.Error(), value) {
		t.Errorf("the error senro.Run returned contains the value: %q", runErr)
	}
}
```

- [ ] **Step 4: Run everything**

```bash
go test ./internal/engine/ . -race
make all
```

**Composition.** The last assertion in the integration test crosses this task with plan 4's `RunError`. `RunError.Error` renders step names rather than step error text, deliberately ("a step's own error text or command line can carry values that must not be repeated here", says its own doc), and this pins that decision against the case it was written for. Each piece is correct alone; the combination is what a user actually sees on stderr.

---

### Task 6: Redaction on the log path, and `secret.redacted`

**Files:**
- Modify `internal/engine/attempt.go` (`runAttempt`, the writer chain and the flush)
- Test `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `redact.Set.Writer`, `Writer.Flush`, `Writer.Redacted` (Task 3), `runCore.redact` (Task 4).
- Produces: no new exported surface. `api.SecretRedacted` and `api.SecretRedactedBody` already exist and are emitted for the first time here.

**Wiring.** `runAttempt` is the production caller and there is no other place a step's output reaches a file. The attach server serves scrollback by opening exactly those files (`attachsrv.Server.handleLogs` calls `os.Open` on `eventlog.LogSet.Path`), and `source.FileSource` reads them for a post-mortem, so a redacted file is a redacted response through both, with no second mechanism to keep in sync.

**The composition trap this task has to get right.** `logMarker` records the byte offset and length of each write into `step.log.appended`, and a client range-requests those bytes back from the file. Redaction changes the byte count. Putting the redactor UPSTREAM of `logMarker` is what keeps the two consistent: the marker then describes the redacted bytes that actually landed on disk. Putting it downstream, or making `logMarker` report the pre-redaction length, would produce offsets that point past the end of the file. Step 3's test asserts every marker's range lies inside the finished file and that the last one ends exactly at its size.

- [ ] **Step 1: Write the failing test**

The leak is a file the step reads, which is design.md §1.3's own example ("a Helm error quoting the values file") and is a channel no guard covers, ever.

Append to `internal/engine/engine_test.go`:

```go
// TestAValueOnAStepsStdoutNeverReachesTheLogFile is design.md section 1.3's
// exact scenario: "senro's exposure is elsewhere: a secret shows up on a
// child process's stdout. go test -v echoing an env var, curl -v printing a
// URL with a token, a Helm error quoting the values file. mamori cannot see
// that stream, and secret.String cannot protect a byte that a subprocess
// wrote."
//
// The value arrives through a file the step reads, so nothing about this case
// is reachable by a plan-time check: it has to be the redactor or nothing.
func TestAValueOnAStepsStdoutNeverReachesTheLogFile(t *testing.T) {
	const value = "s3cr3t-value-here"

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "values.yaml"),
		[]byte("token: "+value+"\nother: fine\n"), 0o600); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").Step("show", exec.Command("cat", "values.yaml")).WorkDir(work)

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logPath := eventlog.NewLogSet(dir).Path("show", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading %s: %v", logPath, err)
	}
	// The canary: the step really did run and really did print the file, so
	// "the value is absent" is not a statement about an empty log.
	if !bytes.Contains(body, []byte("other: fine")) {
		t.Fatalf("the log does not contain the step's output at all: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("no placeholder in the log; redaction did not run on this path")
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the log file contains the secret value")
	}
}

// TestSecretRedactedReportsTheCountForTheAttempt covers design.md section
// 1.5's "Emit {"type":"secret.redacted","step":"...","count":3} so the UI can
// show redaction is live."
func TestSecretRedactedReportsTheCountForTheAttempt(t *testing.T) {
	const value = "s3cr3t-value-here"

	work := t.TempDir()
	// Three occurrences, two on stdout and one on stderr, so the assertion
	// also proves the two streams' counts are summed into one event rather
	// than reported as two.
	script := "cat a.txt; cat a.txt; cat b.txt 1>&2"
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte(value+"\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "b.txt"), []byte(value+"\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("leak", exec.Command("sh", "-c", script)).WorkDir(work)
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// engine.Run with a recording sink, the established pattern in this
	// package for observing an event stream (see golden_test.go). senro.Run
	// has no WithSink option in this build, and the events this test asserts
	// on are engine-emitted, so this is the real entry point for them.
	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var count int
	var seen int
	for _, e := range rec.Events() {
		if e.Type != api.SecretRedacted {
			continue
		}
		seen++
		if e.Step != "leak" {
			t.Errorf("secret.redacted has step %q, want leak", e.Step)
		}
		var b api.SecretRedactedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decoding secret.redacted: %v", err)
		}
		count += b.Count
	}
	if seen != 1 {
		t.Fatalf("got %d secret.redacted events for one attempt, want exactly 1", seen)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3 (two on stdout, one on stderr)", count)
	}
}

// TestNoSecretRedactedWhenNothingWasRedacted is the negative case. A run that
// emitted the event with a zero count would tell a UI that redaction fired
// when it did not, which is the same class of lie as not redacting.
func TestNoSecretRedactedWhenNothingWasRedacted(t *testing.T) {
	pipe := senro.New("p")
	pipe.Workflow("w").Step("clean", exec.Command("echo", "nothing to see"))
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST",
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var stepFinished bool
	for _, e := range rec.Events() {
		if e.Type == api.StepFinished {
			stepFinished = true
		}
		if e.Type == api.SecretRedacted {
			t.Errorf("secret.redacted emitted for a run with no secrets: %+v", e)
		}
	}
	if !stepFinished {
		t.Fatal("no step.finished; the assertion above proves nothing")
	}
}

// TestLogMarkersDescribeTheRedactedFile is the composition check between this
// task and the attach server's range requests. Redaction changes byte counts,
// and a step.log.appended offset that points past the end of the file is a
// scrollback fetch that returns garbage or nothing.
func TestLogMarkersDescribeTheRedactedFile(t *testing.T) {
	const value = "s3cr3t-value-here"

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "v.txt"),
		[]byte("a "+value+" b\nc "+value+" d\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("show", exec.Command("cat", "v.txt")).WorkDir(work)
	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	rec := sink.Recording()
	dir := t.TempDir()
	if _, err := engine.Run(t.Context(), pl, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: rec,
		MaxParallel: 1, RunID: "01TEST", Secrets: set,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	st, err := os.Stat(eventlog.NewLogSet(dir).Path("show", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	var markers int
	var end int64
	for _, e := range rec.Events() {
		if e.Type != api.StepLogAppended || e.Step != "show" {
			continue
		}
		var b api.StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if b.Stream != api.StreamStdout {
			continue
		}
		markers++
		if b.Offset+b.Len > st.Size() {
			t.Errorf("marker [%d,%d) reaches past the %d-byte log file",
				b.Offset, b.Offset+b.Len, st.Size())
		}
		if b.Offset > end {
			t.Errorf("marker at offset %d leaves a gap after %d", b.Offset, end)
		}
		end = b.Offset + b.Len
	}
	if markers == 0 {
		t.Fatal("no step.log.appended markers; the checks above prove nothing")
	}
	if end != st.Size() {
		t.Errorf("the markers cover %d bytes but the file is %d; the redactor's "+
			"output and the marker's accounting disagree", end, st.Size())
	}
}
```

These tests and Task 5's need these additions to `internal/engine/engine_test.go`'s import block, on top of what it already imports (`senro`, `api`, `exec`, `engine`, `localexec`, `sink`, `storage`):

```go
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/secrets"
```

Run and watch the redaction assertions fail while the marker assertions pass. That order matters: it proves the marker test is not vacuously green before redaction exists.

```bash
go test ./internal/engine/ -run 'Stdout|SecretRedacted|LogMarkers' -v
```

- [ ] **Step 2: Wrap the writer chain in `runAttempt`**

In `internal/engine/attempt.go`, replace the `stdout`/`stderr` construction:

```go
	tail := &tailBuffer{}
	// The redactor sits UPSTREAM of logMarker and of the tail buffer, so
	// every consumer downstream of it sees redacted bytes and there is
	// exactly one place that can be wrong.
	//
	// Upstream of logMarker specifically, because logMarker records the byte
	// offset and length of each write and a client range-requests those bytes
	// back out of the file (attachsrv.Server.handleLogs). Redaction changes
	// byte counts, so the marker has to describe what landed on disk, not
	// what the child produced. See TestLogMarkersDescribeTheRedactedFile.
	//
	// Upstream of the tail buffer because a retry predicate reads it
	// (retry.Attempt.LogTail), and a log_match pattern must not be able to
	// match a value in memory that the same bytes on disk no longer contain.
	// The consequence is that a retry predicate cannot match against a
	// secret; that is the correct trade and Task 10 documents it.
	//
	// One Writer per stream, never one shared: the rolling-buffer state is
	// per stream, and interleaving stdout and stderr through one buffer would
	// splice a match out of bytes that were never adjacent.
	stdoutRW := rc.redact.Writer(io.MultiWriter(
		&logMarker{rc: rc, w: stdoutW, step: n.ID, attempt: attempt, stream: api.StreamStdout}, tail))
	stderrRW := rc.redact.Writer(io.MultiWriter(
		&logMarker{rc: rc, w: stderrW, step: n.ID, attempt: attempt, stream: api.StreamStderr}, tail))
```

`stdoutRW` and `stderrRW` are then what `sb.Run` receives, in place of the `stdout` and `stderr` locals this block used to define. `*redact.Writer` satisfies `io.Writer`, so no conversion is needed and none should be written: `unconvert` in `golangci-lint` flags it.

- [ ] **Step 3: Flush, and emit `secret.redacted`, immediately after `sb.Run`**

Directly after the `exit, runErr := sb.Run(...)` line and before the snapshot block:

```go
	// Flush both streams before anything else happens, so every
	// step.log.appended marker for this attempt is in the ledger before its
	// step.finished, and so the held-back tail of a partial match reaches the
	// file rather than being dropped when the writers close.
	//
	// Explicit here rather than deferred, even though Flush is idempotent:
	// the deferred writer closes below would otherwise run first (defers
	// unwind last-registered-first) and a flush into a closed LogWriter
	// returns ErrClosed with the bytes lost.
	//
	// A backgrounded child can still be writing at this moment: localexec's
	// waitDelay lets an orphan hold the pipe for up to five seconds past
	// Run's return. redact.Writer is mutex-guarded for exactly that race.
	if err := stdoutRW.Flush(); err != nil && runErr == nil {
		runErr = err
	}
	if err := stderrRW.Flush(); err != nil && runErr == nil {
		runErr = err
	}
	// One event per attempt, carrying both streams' counts, rather than one
	// per stream: api.SecretRedactedBody has only a Count, so two events
	// would be indistinguishable to a reader, and design.md section 1.5's own
	// example shows a single step-scoped event.
	if c := stdoutRW.Redacted() + stderrRW.Redacted(); c > 0 {
		rc.emit(api.Event{
			Type: api.SecretRedacted, Step: n.ID, Attempt: attempt,
			Payload: mustMarshal(api.SecretRedactedBody{Count: c}),
		})
	}
```

- [ ] **Step 4: Run everything**

```bash
go test ./internal/engine/ . -race
go test ./internal/engine/ -run Golden -v
make all
```

The golden fixtures have no secrets, so `rc.redact` is nil, `Writer` is a passthrough, `Redacted()` is 0, and no `secret.redacted` is emitted. Nothing in the pinned streams moves.

**The check that catches it.** `TestLogMarkersDescribeTheRedactedFile` compares the sum of the markers against `os.Stat` of the finished file. An implementation that put the redactor downstream of `logMarker`, or that reported the pre-redaction length, produces markers that overrun the file by exactly the number of bytes redaction removed, and only a check against the file's real size sees it. A test that merely asserted "the value is not in the log" would pass against that broken wiring while every scrollback fetch in the TUI returned truncated output.

---

### Task 7: Delivery, as a file on tmpfs with the path in the environment

**Files:**
- Modify `internal/plan/plan.go` (add `SecretSpec`, `Node.Secrets`, `SecretEnvVar`, sort in `Digest`)
- Modify `internal/plan/validate.go` (add `validateSecrets`, called from `nodeShape`)
- Modify `senro.go` (add `StepBuilder.SecretEnv`, `secretEnv`, the `toNode` lowering)
- Create `internal/executor/localexec/secretdir.go`, `internal/executor/localexec/secretdir_test.go`
- Modify `internal/executor/localexec/localexec.go` (`sandbox.secretDir`, `PutSecret`, `Close`)
- Create `internal/engine/guard.go`, `internal/engine/guard_test.go`
- Modify `internal/engine/engine.go` (call `checkSecretRefs`), `internal/engine/attempt.go` (deliver)
- Test `internal/plan/plan_test.go`, `senro_test.go`, `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `secrets.Set.Value`, `secrets.Set.Names`, `secrets.Set.Has` (Task 4); `redact` through Task 6's already-wired log path.
- Produces:
  ```go
  package plan
  type SecretSpec struct {
      Name   string `json:"name"`
      Env    string `json:"env,omitempty"`
      Source string `json:"source,omitempty"`
  }
  // Node gains: Secrets []SecretSpec `json:"secrets,omitempty"`
  func SecretEnvVar(name string) string

  package senro
  func (s *StepBuilder) SecretEnv(envName, field string) *StepBuilder

  package engine
  func checkSecretRefs(p *plan.Plan, set *secrets.Set) error
  ```

**Wiring.** `senro.SecretEnv` lowers into `plan.Node.Secrets`, `engine.Run` refuses a plan referencing an unresolved field before any step starts, and `runAttempt` calls `Sandbox.PutSecret` (which has existed unwired since phase 9) and puts the returned paths into `Cmd.Env`. The proof runs `senro.Run` on a pipeline whose step reads the file.

**Composition.** This task crosses Task 6 twice. A step that reads its own secret file and prints it must land `[REDACTED]` in the log, which is Task 6's redactor acting on Task 7's delivery. And the secret file must be somewhere a workspace snapshot can never reach, which is why it goes to tmpfs rather than under the sandbox directory: `localexec.Sandbox.Snapshot` walks the MOUNT's path, not the sandbox's, but a step whose workspace is mounted at the sandbox root would erase that distinction and put a credential in the CAS forever. Step 6's test asserts against the whole run directory and the whole cache root.

- [ ] **Step 1: Record the plan digest literal, before anything in this task changes**

The pin in `TestSecretsDoNotMoveTheDigestOfAPlanWithoutThem` (Step 2) is only meaningful if the value comes from the tree BEFORE `Node.Secrets` exists. Write a throwaway test on the current tree, run it, and paste what it prints into that constant:

```bash
cat > /tmp/digest_pin_test.go <<'EOF'
package plan_test

import (
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

func TestRecordTheDigest(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	t.Log(p.Digest())
}
EOF
cp /tmp/digest_pin_test.go internal/plan/digest_pin_test.go
go test ./internal/plan/ -run RecordTheDigest -v
rm internal/plan/digest_pin_test.go
```

Paste the logged value into the `want` constant. If this step is skipped, the pin proves nothing.

- [ ] **Step 2: Write the failing plan and validation tests**

Append to `internal/plan/plan_test.go`:

```go
// TestSecretsDoNotMoveTheDigestOfAPlanWithoutThem is the guard on the four
// golden fixtures and on TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest.
// Node.Secrets is omitempty, so a node that declares none must marshal, and
// therefore digest, exactly as it did before the field existed.
func TestSecretsDoNotMoveTheDigestOfAPlanWithoutThem(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Contains(b, []byte("secrets")) {
		t.Errorf("a plan with no secrets serialized a secrets key: %s", b)
	}
	// The literal is recorded by running this test once against the tree
	// BEFORE Node.Secrets is added, and pasted here. It must not change.
	const want = "PASTE THE DIGEST RECORDED IN STEP 0"
	if got := p.Digest(); got != want {
		t.Errorf("Digest() = %q, want %q; adding Node.Secrets moved the digest of a "+
			"plan that declares none, which invalidates every golden fixture", got, want)
	}
}

// TestReorderingSecretsDoesNotChangeTheDigest. A node's secret set is
// unordered, exactly like its Mounts, Inputs, Outputs and CacheEnv, so
// declaring the same two in the other order is the same timetable.
func TestReorderingSecretsDoesNotChangeTheDigest(t *testing.T) {
	mk := func(secs ...plan.SecretSpec) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Secrets: secs},
		}}
	}
	one := mk(
		plan.SecretSpec{Name: "NPMToken", Env: "NPM_TOKEN"},
		plan.SecretSpec{Name: "RegistryToken", Env: "REG_TOKEN"},
	)
	two := mk(
		plan.SecretSpec{Name: "RegistryToken", Env: "REG_TOKEN"},
		plan.SecretSpec{Name: "NPMToken", Env: "NPM_TOKEN"},
	)
	if one.Digest() != two.Digest() {
		t.Errorf("reordering a node's secrets changed the digest: %s vs %s",
			one.Digest(), two.Digest())
	}
	// And a DIFFERENT set must not collide with either.
	three := mk(plan.SecretSpec{Name: "NPMToken", Env: "NPM_TOKEN"})
	if three.Digest() == one.Digest() {
		t.Error("dropping a secret did not change the digest")
	}
}

// TestSecretEnvVar pins the transformation the delivered environment and the
// on-disk file name both derive from.
func TestSecretEnvVar(t *testing.T) {
	cases := []struct{ in, want string }{
		{"NPMToken", "SENRO_SECRET_NPMTOKEN"},
		{"npm_token", "SENRO_SECRET_NPM_TOKEN"},
		{"Registry.Token", "SENRO_SECRET_REGISTRY_TOKEN"},
		{"a-b", "SENRO_SECRET_A_B"},
	}
	for _, tc := range cases {
		if got := plan.SecretEnvVar(tc.in); got != tc.want {
			t.Errorf("SecretEnvVar(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestValidateRefusesABrokenSecretDeclaration covers every rule at once, and
// each case is a way a node could silently deliver the wrong thing.
func TestValidateRefusesABrokenSecretDeclaration(t *testing.T) {
	mk := func(secs []plan.SecretSpec, env, cacheEnv []string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"echo"},
			Secrets: secs, Env: env, CacheEnv: cacheEnv,
		}}}
	}
	cases := []struct {
		name string
		p    *plan.Plan
		want string
	}{
		{
			"an empty name",
			mk([]plan.SecretSpec{{Env: "TOK"}}, nil, nil),
			"empty",
		},
		{
			"an env name containing =",
			mk([]plan.SecretSpec{{Name: "T", Env: "A=B"}}, nil, nil),
			`"="`,
		},
		{
			"two secrets on one variable",
			mk([]plan.SecretSpec{{Name: "A", Env: "TOK"}, {Name: "B", Env: "TOK"}}, nil, nil),
			"TOK",
		},
		{
			"a variable the step already sets",
			mk([]plan.SecretSpec{{Name: "A", Env: "TOK"}}, []string{"TOK=plain"}, nil),
			"TOK",
		},
		{
			"two names that collide as SENRO_SECRET_ variables",
			mk([]plan.SecretSpec{{Name: "a.b", Env: "X"}, {Name: "a_b", Env: "Y"}}, nil, nil),
			"SENRO_SECRET_A_B",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestValidateAcceptsAHandlerWithASecret is the positive case that keeps the
// rule from being over-broad. An OnFailure handler that posts to Slack needs
// a webhook URL, and design.md section 7.3's own model is that a handler
// inherits its parent's environment and collects evidence, so refusing it a
// credential would make the whole notify-on-failure story impossible.
func TestValidateAcceptsAHandlerWithASecret(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec", Cmd: []string{"post"},
			Secrets: []plan.SecretSpec{{Name: "Slack", Env: "SLACK_URL"}},
		}},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("Validate refused a handler declaring a secret: %v", err)
	}
}
```

- [ ] **Step 3: Add `SecretSpec`, `Node.Secrets` and `SecretEnvVar`**

In `internal/plan/plan.go`, add to `Node`, after `CacheEnv`:

```go
	// Secrets are the credentials this node needs, by REFERENCE. A value is
	// never here and never in a plan file: design.md section 1.1 requires
	// that a plan can be serialized, stored and re-run, which is only true
	// if it carries no credential.
	//
	// omitempty: a node that declares none marshals, and therefore digests,
	// exactly as it did before this field existed, which is what keeps the
	// four golden fixtures and every plan built without a secret at the same
	// plan_digest.
	Secrets []SecretSpec `json:"secrets,omitempty"`
```

And the type, next to `MountSpec`:

```go
// SecretSpec is one secret a node needs, named by the field of the
// configuration struct senro.WithSecrets was given.
type SecretSpec struct {
	// Name is that field. A field inside a NAMED nested struct is qualified
	// with a dot ("Registry.Token"); a field promoted from an EMBEDDED struct
	// keeps its bare name, matching Go's own promotion. See
	// internal/secrets.FromConfig.
	Name string `json:"name"`
	// Env is the environment variable that receives the FILE PATH the value
	// was written to, never the value (design.md section 1.4's local row,
	// "path in SENRO_SECRET_<NAME>", and section 12's SecretEnv comment,
	// "tmpfs file + env pointing at it"). Empty means the step gets only the
	// uniform SecretEnvVar(Name) variable and no alias.
	Env string `json:"env,omitempty"`
	// Source is the resolved mamori source: URI. ALWAYS EMPTY IN v0, and
	// declared now for the same reason cache.Key.Secrets and
	// api.SecretResolvedBody.Version were declared before anything filled
	// them: filling it later is additive rather than a schema change.
	//
	// It cannot be filled today because (*Pipeline).Build has no access to
	// the configuration struct (section 12's own example hands it to Run, not
	// to New), and enriching the plan after Build returned would make
	// plan.Digest() differ between the value a caller inspected and the value
	// plan.resolved reports, for exactly the pipelines that declare a secret.
	// The resolved URI is recorded where it genuinely is available instead:
	// in the secret.resolved event and in the cache key's secrets component.
	Source string `json:"source,omitempty"`
}

// SecretEnvVar is the uniform environment variable every delivered secret
// gets, from design.md section 1.4's local row: "path in
// SENRO_SECRET_<NAME>".
//
// The name is uppercased and every byte outside [A-Z0-9_] becomes "_",
// because a configuration field can be called "Registry.Token" and an
// environment variable name cannot contain a dot. Two different field names
// can therefore map to one variable ("a.b" and "a_b" both become
// "SENRO_SECRET_A_B"); Validate refuses a node whose secrets collide that
// way rather than letting one silently overwrite the other.
func SecretEnvVar(name string) string {
	var b strings.Builder
	b.WriteString("SENRO_SECRET_")
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte(c - 'a' + 'A')
		case (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
```

Add `"strings"` to the import block.

In `Plan.Digest`, alongside the existing `n.CacheEnv = sortedCopy(n.CacheEnv)`:

```go
		// A node's secret set is unordered, exactly like its Mounts, Inputs,
		// Outputs and CacheEnv, so declaring the same two in the other order
		// is the same timetable.
		n.Secrets = append([]SecretSpec(nil), n.Secrets...)
		sort.Slice(n.Secrets, func(a, b int) bool {
			if n.Secrets[a].Name != n.Secrets[b].Name {
				return n.Secrets[a].Name < n.Secrets[b].Name
			}
			return n.Secrets[a].Env < n.Secrets[b].Env
		})
```

- [ ] **Step 4: Add `validateSecrets`**

In `internal/plan/validate.go`, add the call as the last statement of `nodeShape`, before its `return nil`:

```go
	return validateSecrets(n)
```

`nodeShape` rather than a separate loop, deliberately: its own doc says "Top-level nodes and handler nodes are both run through this exact function, on purpose", and a handler that declares a secret must be checked by the same rules as a step that does. That is the class fix, and `TestValidateAcceptsAHandlerWithASecret` is the positive case that keeps it from being an outright ban.

```go
// validateSecrets checks a node's secret declarations. Every rule here closes
// a way the node could deliver something other than what it declared.
func validateSecrets(n Node) error {
	if len(n.Secrets) == 0 {
		return nil
	}
	declared := make(map[string]bool, len(n.Env))
	for _, kv := range n.Env {
		if name, _, ok := strings.Cut(kv, "="); ok {
			declared[name] = true
		}
	}
	seenEnv := make(map[string]bool, len(n.Secrets)*2)
	seenVar := make(map[string]string, len(n.Secrets))
	for _, s := range n.Secrets {
		if s.Name == "" {
			return fmt.Errorf("plan: step %q declares a secret with an empty name", n.ID)
		}
		v := SecretEnvVar(s.Name)
		if prev, dup := seenVar[v]; dup {
			return fmt.Errorf(
				"plan: step %q declares secrets %q and %q, which both deliver to %s; "+
					"rename one of the configuration fields", n.ID, prev, s.Name, v)
		}
		seenVar[v] = s.Name
		if declared[v] {
			return fmt.Errorf(
				"plan: step %q sets %s with Env and also declares secret %q, which "+
					"delivers to the same variable", n.ID, v, s.Name)
		}
		if s.Env == "" {
			continue
		}
		if strings.Contains(s.Env, "=") {
			return fmt.Errorf(
				"plan: step %q delivers secret %q to a variable named %q, which contains \"=\"",
				n.ID, s.Name, s.Env)
		}
		if seenEnv[s.Env] {
			return fmt.Errorf(
				"plan: step %q delivers two secrets to the variable %q; one would silently "+
					"overwrite the other", n.ID, s.Env)
		}
		if declared[s.Env] {
			return fmt.Errorf(
				"plan: step %q sets %q with Env and also delivers a secret to it", n.ID, s.Env)
		}
		seenEnv[s.Env] = true
	}
	return nil
}
```

Run the plan tests.

```bash
go test ./internal/plan/ -v
```

- [ ] **Step 5: Add `SecretEnv` to the builder**

In `senro.go`, add to `StepBuilder`:

```go
	secretEnvs []secretEnv
```

and the type, next to `Mount`:

```go
// secretEnv is one SecretEnv declaration: which configuration field, and
// which environment variable receives its file's path.
type secretEnv struct {
	env   string
	field string
}
```

The method, after `Env`:

```go
// SecretEnv delivers a resolved secret to this step as a FILE, and puts that
// file's PATH into the named environment variable:
//
//	setup.Step("install", exec.Command("pnpm", "install")).
//		SecretEnv("NPM_TOKEN", "NPMToken")
//
// field names a field of the struct handed to senro.WithSecrets. A field
// inside a named nested struct is spelled with a dot ("Registry.Token"); a
// field promoted from an embedded struct keeps its bare name.
//
// # The variable holds a path, not a value
//
// This is the single most important thing to know about this method, and it
// is design.md section 1.4's decision rather than an implementation
// convenience: a value in an environment variable is readable through
// /proc/<pid>/environ for the whole life of the process, by anything running
// as the same user, and senro's own redactor cannot reach there. So the value
// goes to a file at mode 0600 in a tmpfs-preferring directory, and the
// variable holds its path. A step reads it:
//
//	SecretEnv("NPM_TOKEN", "NPMToken")   // NPM_TOKEN=/run/user/1000/senro-secret-xyz/NPMToken
//	// in the step:  npm config set //registry.npmjs.org/:_authToken="$(cat "$NPM_TOKEN")"
//
// Every declared secret ALSO arrives under the uniform name
// SENRO_SECRET_<NAME>, where <NAME> is the field name uppercased with every
// character outside A-Z, 0-9 and _ replaced by _, so a step can read it
// without the pipeline having chosen an alias. SecretEnv's own variable is
// the ergonomic second name for the same path.
//
// # What senro refuses
//
// A run whose plan puts a resolved value into a command argument or into an
// environment variable is refused before the first step starts, because both
// are visible outside this process (ps(1), /proc/<pid>/environ, shell
// history, auditd execve records) where redaction cannot follow. See the
// secrets section of the README for the full list of safe and unsafe
// channels.
//
// # Caching
//
// The secret's IDENTITY (its source URI, and a digest of its value salted
// with that URI) enters the step's cache key, so changing the credential
// invalidates the step, as design.md section 1.6 requires. The value never
// does. Naming the same variable in both SecretEnv and CacheEnv is refused at
// build time: a SecretEnv variable holds a per-attempt path, so folding it
// into the key would change the key on every run and the step would never
// hit.
func (s *StepBuilder) SecretEnv(envName, field string) *StepBuilder {
	switch {
	case envName == "":
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares a SecretEnv with an empty variable name (for secret %q)",
			s.id, field))
	case strings.Contains(envName, "="):
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares a SecretEnv named %q, which contains \"=\"; SecretEnv takes "+
				"a variable name and a configuration field name as two arguments", s.id, envName))
	case field == "":
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares SecretEnv(%q, \"\") with no configuration field to read",
			s.id, envName))
	default:
		s.secretEnvs = append(s.secretEnvs, secretEnv{env: envName, field: field})
	}
	return s
}
```

And in `toNode`, next to the `CacheEnv` line:

```go
	for _, se := range sb.secretEnvs {
		n.Secrets = append(n.Secrets, plan.SecretSpec{Name: se.field, Env: se.env})
	}
```

- [ ] **Step 6: Move the secret file off the run directory**

Create `internal/executor/localexec/secretdir.go`:

```go
package localexec

import (
	"os"
	"runtime"
)

// secretRoot is the directory tree secret files are created under.
//
// design.md section 1.4's local row asks for tmpfs, "/dev/shm or
// $XDG_RUNTIME_DIR", and the reason is concrete: PutSecret previously wrote
// under the SANDBOX directory, which lives inside the RUN directory, next to
// events.jsonl and the log files. That is the directory a user tars up and
// attaches to a bug report.
//
// The order is deliberate:
//
//   - $XDG_RUNTIME_DIR when it is set and is a directory. Per-user, 0700 by
//     convention, and tmpfs-backed on every system that sets it.
//   - /dev/shm on linux, which is tmpfs by definition. Its own mode is 1777,
//     so isolation comes from the 0700 directory created inside it.
//   - os.TempDir() otherwise, which is the darwin case: there is no /dev/shm
//     and $XDG_RUNTIME_DIR is normally unset, leaving $TMPDIR (a per-user
//     /var/folders/... directory). It is DISK backed. senro does not claim to
//     shred, and the README says so plainly rather than implying a tmpfs
//     guarantee this executor cannot make on darwin.
func secretRoot() string {
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		if fi, err := os.Stat(d); err == nil && fi.IsDir() {
			return d
		}
	}
	if runtime.GOOS == "linux" {
		if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
			return "/dev/shm"
		}
	}
	return os.TempDir()
}

// secretFileName reduces name to a single safe path element. A configuration
// field can be called "Registry.Token", and a name containing a separator
// would otherwise write outside the secret directory.
//
// Two names cannot collide here without also colliding under
// plan.SecretEnvVar, which is strictly coarser (it uppercases, and it maps
// "-" to "_" where this keeps it), and plan.Validate already refuses a node
// whose secrets collide there. So this needs no collision handling of its
// own; it needs only to be safe.
func secretFileName(name string) string {
	b := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-':
			b = append(b, c)
		default:
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "secret"
	}
	return string(b)
}
```

In `internal/executor/localexec/localexec.go`, add to `sandbox`:

```go
	// secretDir is created by the first PutSecret and removed by Close. Empty
	// for a step that declares no secret, which is the common case and costs
	// nothing.
	secretDir string
```

Replace `PutSecret`:

```go
// PutSecret writes v to a file outside the run directory and returns its path.
//
// The file is 0600 inside a 0700 directory under secretRoot, which prefers
// tmpfs (design.md section 1.4). It gates other OS users but not sibling
// steps: every step in a run executes as the same user, so the local executor
// provides no isolation between steps. Use a container or Kubernetes executor
// where steps must not see each other's secrets.
//
// Not under the sandbox directory, which is where this used to write. The
// sandbox directory is inside the RUN directory, alongside events.jsonl and
// the log files, and a run directory is a thing people archive and share.
func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	if s.secretDir == "" {
		d, err := os.MkdirTemp(secretRoot(), "senro-secret-")
		if err != nil {
			return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
		}
		// MkdirTemp already creates at 0700; setting it explicitly means the
		// mode does not depend on a umask or on a future change to MkdirTemp.
		if err := os.Chmod(d, 0o700); err != nil {
			_ = os.RemoveAll(d)
			return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
		}
		s.secretDir = d
	}
	p := filepath.Join(s.secretDir, secretFileName(name))
	if err := os.WriteFile(p, v, 0o600); err != nil {
		return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return p, nil
}
```

Replace `Close`:

```go
func (s *sandbox) Close(_ context.Context, keep bool) error {
	// Secret files are removed on EVERY path, including keep. keep exists so
	// a debugging shell can inspect the filesystem state of a failed step
	// (design.md section 7.6), and a kept sandbox holding a plaintext
	// credential is that credential on disk for as long as the operator takes
	// to look. Re-running the step re-delivers the value, so nothing is lost
	// that cannot be recovered.
	//
	// Removal, not shredding. On tmpfs the unlink frees the pages. On the
	// darwin fallback the bytes may persist in free space, and senro does not
	// claim otherwise; see secretRoot and the README.
	if s.secretDir != "" {
		dir := s.secretDir
		s.secretDir = ""
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("localexec: %w: removing the secret directory: %w",
				senroexec.ErrInfra, err)
		}
	}
	if keep {
		return nil
	}
	return nil // the run directory is the run's artifact; a later plan reaps it
}
```

Create `internal/executor/localexec/secretdir_test.go`:

```go
package localexec_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
)

// TestPutSecretWritesOutsideTheRunDirectory is the whole point of moving it.
// A run directory is what a user tars up and attaches to a bug report.
func TestPutSecretWritesOutsideTheRunDirectory(t *testing.T) {
	root := t.TempDir()
	ex := localexec.New(root, nil)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}

	p, err := sb.PutSecret(context.Background(), "Registry.Token", []byte("value-aaaaaaaa"))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	if strings.HasPrefix(p, root) {
		t.Errorf("PutSecret wrote %q inside the run directory %q", p, root)
	}
	if filepath.Base(p) != "Registry_Token" {
		t.Errorf("file name %q; a dot in a field name must not become a path separator",
			filepath.Base(p))
	}

	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("file mode %v, want 0600", fi.Mode().Perm())
	}
	di, err := os.Stat(filepath.Dir(p))
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if di.Mode().Perm() != 0o700 {
		t.Errorf("directory mode %v, want 0700", di.Mode().Perm())
	}
	body, err := os.ReadFile(p)
	if err != nil || string(body) != "value-aaaaaaaa" {
		t.Fatalf("the file does not hold the value: (%q, %v)", body, err)
	}

	if err := sb.Close(context.Background(), false); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(p)); !os.IsNotExist(err) {
		t.Errorf("the secret directory survived Close: %v", err)
	}
}

// TestCloseRemovesSecretsEvenWhenKeepingTheSandbox is the negative case for
// the debugging path. keep preserves the workspace state, not the credential.
func TestCloseRemovesSecretsEvenWhenKeepingTheSandbox(t *testing.T) {
	ex := localexec.New(t.TempDir(), nil)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	p, err := sb.PutSecret(context.Background(), "Tok", []byte("value-aaaaaaaa"))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	if err := sb.Close(context.Background(), true); err != nil {
		t.Fatalf("Close(keep=true): %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Errorf("the secret file survived Close(keep=true): %v", err)
	}
}

// TestCloseWithNoSecretsCostsNothing. A sandbox that never delivered one must
// not create, or try to remove, a directory.
func TestCloseWithNoSecretsCostsNothing(t *testing.T) {
	ex := localexec.New(t.TempDir(), nil)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if err := sb.Close(context.Background(), false); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
```

- [ ] **Step 7: Deliver in `runAttempt`, and refuse an unresolved reference at run start**

Create `internal/engine/guard.go`:

```go
package engine

import (
	"fmt"
	"strings"

	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
)

// checkSecretRefs refuses a plan that names a configuration field the
// resolved set does not have, before any step runs.
//
// Fail fast rather than fail at delivery: a 40-step pipeline whose LAST step
// references a typo'd field would otherwise run for twenty minutes and then
// fail on a mistake visible at second zero. It walks handler nodes too, for
// the same reason plan.nodeShape does: a handler that cannot get its
// credential is exactly as broken as a step that cannot.
func checkSecretRefs(p *plan.Plan, set *secrets.Set) error {
	var walk func(n *plan.Node, owner string) error
	walk = func(n *plan.Node, owner string) error {
		for _, s := range n.Secrets {
			if set.Has(s.Name) {
				continue
			}
			available := "none were resolved"
			if names := set.Names(); len(names) > 0 {
				available = "resolved: " + strings.Join(names, ", ")
			}
			return fmt.Errorf(
				"engine: %s %q needs secret %q, which the struct passed to senro.WithSecrets "+
					"does not provide (%s)", owner, n.ID, s.Name, available)
		}
		for _, list := range [][]plan.Node{n.OnFailure, n.Always} {
			for i := range list {
				if err := walk(&list[i], "handler"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := range p.Nodes {
		if err := walk(&p.Nodes[i], "step"); err != nil {
			return err
		}
	}
	return nil
}
```

Call it in `engine.Run`, immediately after the redactor's `Skipped` refusal added in Task 4:

```go
	if err := checkSecretRefs(p, opts.Secrets); err != nil {
		return "", err
	}
```

In `internal/engine/attempt.go`, replace the `sb, err := opts.Executor.Sandbox(...)` block and the `sb.Run(...)` call:

```go
	// SandboxSpec.Secrets carries identities only, so an executor that
	// provisions secrets itself (a future Kubernetes executor delegating to
	// IRSA) has what it needs without the coordinator ever pushing a value.
	// Source is empty in v0; see plan.SecretSpec.Source.
	var specSecrets []executor.SecretRef
	for _, sec := range n.Secrets {
		specSecrets = append(specSecrets, executor.SecretRef{Name: sec.Name, Source: sec.Source})
	}

	// Node.Env verbatim: see engine.go's runStep doc for why nothing else is
	// added here.
	sb, err := opts.Executor.Sandbox(attemptCtx, executor.SandboxSpec{
		StepID: n.ID, Attempt: attempt, Env: n.Env, WorkDir: n.WorkDir,
		Secrets: specSecrets,
		Mounts:  append(append([]executor.Mount(nil), mounts...), scratchMounts...),
	})
```

and, after the `defer sb.Close(...)` line:

```go
	// Secret delivery: one FILE per declared secret, with its PATH in the
	// environment (design.md section 1.4). Nothing here ever puts a VALUE in
	// the environment, which is what keeps two separate promises at once: the
	// cache key's env component (which digests only CacheEnv-declared names)
	// can never reach a credential, and /proc/<pid>/environ holds paths.
	//
	// Built as a copy of n.Env rather than appended to it, so the plan's own
	// slice is never mutated and a retry's next attempt starts from the
	// declared environment rather than from the previous attempt's paths.
	cmdEnv := n.Env
	if len(n.Secrets) > 0 {
		cmdEnv = append([]string(nil), n.Env...)
		for _, sec := range n.Secrets {
			v, ok := rc.secrets.Value(sec.Name)
			if !ok {
				// checkSecretRefs already refused this at run start, so
				// reaching here means a *plan.Plan assembled by hand rather
				// than through Run. Failing the step keeps the invariant
				// that a step never runs believing it has a credential it
				// does not.
				return attemptResult{state: api.StateFailed, err: fmt.Errorf(
					"engine: step %q needs secret %q, which was not resolved", n.ID, sec.Name)}
			}
			path, err := sb.PutSecret(attemptCtx, sec.Name, v)
			if err != nil {
				return attemptResult{state: api.StateFailed, err: err}
			}
			cmdEnv = append(cmdEnv, plan.SecretEnvVar(sec.Name)+"="+path)
			if sec.Env != "" {
				cmdEnv = append(cmdEnv, sec.Env+"="+path)
			}
		}
	}
```

and change the `sb.Run` call's `Env`:

```go
	exit, runErr := sb.Run(attemptCtx,
		executor.Cmd{Args: n.Cmd, Env: cmdEnv, Dir: cmdDir}, stdoutRW, stderrRW)
```

- [ ] **Step 8: Write the integration test through `senro.Run`**

Append to `senro_test.go`:

```go
// TestASecretIsDeliveredAsAFileAndItsValueNeverLandsAnywhere is this plan's
// central end-to-end claim, driven through senro.Run.
//
// The step reads its own credential through BOTH names, the SecretEnv alias
// and the uniform SENRO_SECRET_ one, and prints it, which is the leak
// design.md section 1.3 says senro alone can defend against. Afterwards
// nothing under the run directory or the cache root contains the value, and
// the file itself is gone.
func TestASecretIsDeliveredAsAFileAndItsValueNeverLandsAnywhere(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		NPMToken secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	runDir, cacheDir := t.TempDir(), t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("use", exec.Command("sh", "-c",
			`test -f "$NPM_TOKEN" || { echo "alias is not a file"; exit 1; }
			 test "$NPM_TOKEN" = "$SENRO_SECRET_NPMTOKEN" || { echo "names disagree"; exit 1; }
			 echo "path is $NPM_TOKEN"
			 cat "$NPM_TOKEN"`)).
		SecretEnv("NPM_TOKEN", "NPMToken")

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logPath := eventlog.NewLogSet(runDir).Path("use", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	// The canary: the step ran, both names agreed, and the redactor fired.
	// Without all three, the absence checks below prove nothing.
	if !bytes.Contains(body, []byte("path is ")) {
		t.Fatalf("the step's output is missing from the log: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("the log has no placeholder, so the value was never printed or never redacted: %q", body)
	}

	// And now the sweep. Every file under the run directory and under the
	// cache root, not just the ones this test knows the names of.
	for _, root := range []string{runDir, cacheDir} {
		found := scanTreeFor(t, root, value)
		if found != "" {
			t.Errorf("the value appears in %s", found)
		}
	}

	// The delivered file is gone. Its path was printed, so it can be read
	// back out of the log even though the log no longer holds the value.
	path := secretPathFromLog(t, body)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the secret file %q survived the run: %v", path, err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Errorf("the secret directory %q survived the run", filepath.Dir(path))
	}
}

// scanTreeFor reports the first file under root whose bytes contain want, or
// "" if none does. It fails the test outright if the tree has no files at
// all, so a caller's "the value is absent" assertion cannot be a statement
// about an empty directory.
func scanTreeFor(t *testing.T, root, want string) string {
	t.Helper()
	var files int
	var hit string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files++
		b, err := os.ReadFile(p)
		if err != nil {
			return nil // an unreadable file is not evidence either way
		}
		if hit == "" && bytes.Contains(b, []byte(want)) {
			hit = p
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if files == 0 {
		t.Fatalf("%s contains no files; a scan of it proves nothing", root)
	}
	return hit
}

// secretPathFromLog pulls the delivered file's path back out of the step's
// own output line, "path is <p>".
func secretPathFromLog(t *testing.T, body []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(body), "\n") {
		if p, ok := strings.CutPrefix(line, "path is "); ok {
			return strings.TrimSpace(p)
		}
	}
	t.Fatalf("no \"path is\" line in %q", body)
	return ""
}

// TestRunRefusesAStepNamingASecretThatDoesNotExist is the fail-fast case. A
// typo in a field name must be an error at second zero, not at minute twenty.
func TestRunRefusesAStepNamingASecretThatDoesNotExist(t *testing.T) {
	type config struct {
		NPMToken secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", "s3cr3t-token-value-aaaa")
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	runDir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("use", exec.Command("true")).
		SecretEnv("NPM_TOKEN", "NpmToken") // wrong case

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a step naming a secret that does not exist")
	}
	if !strings.Contains(err.Error(), "NpmToken") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name both the missing field and what IS available; got %q", err)
	}
	// Nothing ran, so there is no ledger at all.
	if _, statErr := os.Stat(filepath.Join(runDir, "events.jsonl")); !os.IsNotExist(statErr) {
		t.Error("a refused run still opened a ledger")
	}
}

// TestSecretEnvRefusesAMalformedDeclaration is the builder's own negative
// case, reported by Build rather than at run time.
func TestSecretEnvRefusesAMalformedDeclaration(t *testing.T) {
	for _, tc := range []struct{ name, env, field string }{
		{"empty variable", "", "Tok"},
		{"variable with =", "A=B", "Tok"},
		{"empty field", "TOK", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe := senro.New("p")
			pipe.Workflow("w").Step("s", exec.Command("true")).SecretEnv(tc.env, tc.field)
			if _, err := pipe.Build(); err == nil {
				t.Fatalf("Build accepted SecretEnv(%q, %q)", tc.env, tc.field)
			}
		})
	}
}
```

`senro_test.go` needs `"io/fs"`, `"github.com/xavidop/senro/internal/eventlog"` and `"github.com/xavidop/senro/internal/redact"` added, alongside the mamori imports.

- [ ] **Step 9: Run everything**

```bash
go test ./... -race
go test ./internal/engine/ -run Golden -v
make all
golangci-lint run ./...
```

**The canary.** `scanTreeFor` fails the test when the tree it walked contained no files, which is the guard the brief asks for: without it, a sweep of a directory that a bug left empty, or that the run wrote to a different path, reports "the value is absent" and means nothing.

---

### Task 8: Refuse a value in argv or in the environment block

**Files:**
- Modify `internal/engine/guard.go` (add `checkSecretChannels`)
- Modify `internal/engine/engine.go` (call it)
- Create/extend `internal/engine/guard_test.go`
- Test `senro_test.go`

**Interfaces:**
- Consumes: `redact.Set.Match`, `redact.Set.MatchString` (Task 1).
- Produces:
  ```go
  package engine
  func checkSecretChannels(p *plan.Plan, red *redact.Set) error
  ```

**Wiring.** Called from `engine.Run`, next to `checkSecretRefs`, before `eventlog.Open` and therefore before any event, any directory and any step. Both `senro.Run` and `senro.RunPlan` go through it.

**Why refuse rather than redact.** design.md §1.4, about ssh: "Never as a command argument, visible in `ps`, in remote shell history, and in auditd `execve` records." None of those three is reachable from inside this process, so redacting the ledger would clean up the *record* of a leak that still happened. The environment block is the same class: `/proc/<pid>/environ` is readable by anything running as the same user for the life of the process. A refusal is the only response that addresses the actual exposure, and Task 5's payload redaction stays in place underneath it as the backstop for text senro did not author.

**Class, not instance.** One scan, driven by the same automaton the redactor uses, covering `Cmd` and every `Env` value, on every node including handlers at any depth. Using the redactor's own pattern set rather than a second literal comparison means "what senro refuses" and "what senro redacts" can never drift apart, and it means a base64 of a token in argv is refused exactly as the raw token is.

- [ ] **Step 1: Write the failing tests**

Create `internal/engine/guard_test.go`:

```go
package engine

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
)

func guardSet() *redact.Set {
	return redact.New(redact.Value{Label: "NPMToken", Value: []byte("s3cr3t-token-value")})
}

// TestCheckSecretChannelsRefusesAValueInArgv is the reported hole:
// api.StepStartedBody.Cmd records the real argv in events.jsonl, and a secret
// passed as an argument lands there permanently. It also lands in ps(1) and
// in auditd, which is why this refuses rather than redacts.
func TestCheckSecretChannelsRefusesAValueInArgv(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "publish", Kind: "exec",
		Cmd: []string{"npm", "publish", "--token=s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in argv was accepted")
	}
	if !strings.Contains(err.Error(), "publish") {
		t.Errorf("the error must name the step; got %q", err)
	}
	if !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the secret; got %q", err)
	}
	if !strings.Contains(err.Error(), "argument 2") {
		t.Errorf("the error must name the position; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInAnEnvironmentValue.
// /proc/<pid>/environ is readable by anything running as the same user for
// the whole life of the process, and no redactor in this process can reach
// it.
func TestCheckSecretChannelsRefusesAValueInAnEnvironmentValue(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"},
		Env: []string{"CI=1", "NPM_TOKEN=s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in an environment value was accepted")
	}
	if !strings.Contains(err.Error(), "NPM_TOKEN") {
		t.Errorf("the error must name the variable; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAnEncodedValue is what makes this a class fix
// rather than a literal comparison. A step that base64s a token into an
// argument has leaked it exactly as much as one that passes it raw, and the
// scan uses the redactor's own automaton so the two can never disagree about
// what counts.
func TestCheckSecretChannelsRefusesAnEncodedValue(t *testing.T) {
	// base64.StdEncoding of "s3cr3t-token-value".
	const encoded = "czNjcjN0LXRva2VuLXZhbHVl"
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "curl", Kind: "exec",
		Cmd: []string{"curl", "-H", "Authorization: Basic " + encoded},
	}}}
	if err := checkSecretChannels(p, guardSet()); err == nil {
		t.Fatal("a base64-encoded secret in argv was accepted")
	}
}

// TestCheckSecretChannelsWalksHandlers. An OnFailure handler is a step that
// runs on the same host with the same exposure, and a scan that stopped at
// the top level would leave the notify-on-failure path, which is exactly
// where somebody reaches for a webhook URL, completely uncovered.
func TestCheckSecretChannelsWalksHandlers(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm", "upgrade"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec",
			Cmd: []string{"curl", "-d", "token=s3cr3t-token-value"},
		}},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in a handler's argv was accepted")
	}
	if !strings.Contains(err.Error(), "notify") {
		t.Errorf("the error must name the handler; got %q", err)
	}
}

// TestCheckSecretChannelsRefusesArgvZero. A program NAME can be a secret too,
// for instance a path under a directory named after a token, and cmd[0] is
// the one argument cache.CommandComponent stores in the clear.
func TestCheckSecretChannelsRefusesArgvZero(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "run", Kind: "exec", Cmd: []string{"/opt/s3cr3t-token-value/bin/tool"},
	}}}
	if err := checkSecretChannels(p, guardSet()); err == nil {
		t.Fatal("a secret in cmd[0] was accepted")
	}
}

// TestCheckSecretChannelsAcceptsACleanPlan is the positive case, and the one
// that would catch a scan so eager it refuses every run.
func TestCheckSecretChannelsAcceptsACleanPlan(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec",
		Cmd:     []string{"pnpm", "install", "--frozen-lockfile"},
		Env:     []string{"CI=1", "PNPM_HOME=/pnpm-store"},
		Secrets: []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
		OnFailure: []plan.Node{{
			ID: "logs", Kind: "exec", Cmd: []string{"cat", "npm-debug.log"},
		}},
	}}}
	if err := checkSecretChannels(p, guardSet()); err != nil {
		t.Errorf("a clean plan was refused: %v", err)
	}
}

// TestCheckSecretChannelsDoesNotScanWorkDir pins design decision 1's
// deliberate exclusion. A working directory is not an OS-visible credential
// channel the way argv and the environment block are, refusing it would be
// over-reach, and Task 5's backstop is what covers it in the ledger.
func TestCheckSecretChannelsDoesNotScanWorkDir(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"},
		WorkDir: "/build/s3cr3t-token-value",
	}}}
	if err := checkSecretChannels(p, guardSet()); err != nil {
		t.Errorf("WorkDir is deliberately not scanned; got %v", err)
	}
}

// TestCheckSecretChannelsIsFreeWithNoSecrets. Every run pays for this scan,
// so the nil case must short-circuit before touching a single node.
func TestCheckSecretChannelsIsFreeWithNoSecrets(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo", "s3cr3t-token-value"},
	}}}
	if err := checkSecretChannels(p, nil); err != nil {
		t.Errorf("a nil redactor refused a plan: %v", err)
	}
}
```

Run and watch it fail to compile.

```bash
go test ./internal/engine/ -run CheckSecretChannels
```

- [ ] **Step 2: Write `checkSecretChannels`**

Append to `internal/engine/guard.go`:

```go
// checkSecretChannels refuses a plan that would put a resolved secret value
// into a command argument or into an environment variable's value, before any
// step runs.
//
// Redaction is not an option for either. design.md section 1.4 says it
// directly, about the ssh executor: a command argument is "visible in ps, in
// remote shell history, and in auditd execve records", and an environment
// block is readable through /proc/<pid>/environ by anything running as the
// same user for the life of the process. None of those three is reachable
// from inside this process, so redacting the ledger would clean up the RECORD
// of a leak that still happened. A refusal is the only answer that addresses
// the exposure, and it is a mistake the author can fix in one line.
//
// The scan uses the run's own redactor, so "what senro refuses" and "what
// senro redacts" are the same set by construction: a base64 of a token in
// argv is refused exactly as the raw token is, and a future encoding added to
// redact.Variants is covered here on the same commit.
//
// WorkDir is deliberately NOT scanned. A working directory is not an
// OS-visible credential channel the way argv and the environment block are,
// refusing it would be over-reach, and Task 5's payload redaction is what
// keeps it out of the ledger. See TestCheckSecretChannelsDoesNotScanWorkDir.
//
// Nothing here ever prints a value: the error names the step, the position or
// the variable, and the secret's field name.
func checkSecretChannels(p *plan.Plan, red *redact.Set) error {
	if red == nil {
		return nil
	}
	var walk func(n *plan.Node, owner string) error
	walk = func(n *plan.Node, owner string) error {
		for i, arg := range n.Cmd {
			label, hit := red.MatchString(arg)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in command argument %d; a command "+
					"argument is visible in ps(1), in shell history and in auditd execve records, "+
					"where senro cannot redact it, so senro refuses to run rather than leak it. "+
					"Deliver it as a file instead: SecretEnv(\"VAR\", %q), then read \"$VAR\" as a "+
					"path in the step", owner, n.ID, label, i, label)
		}
		for _, kv := range n.Env {
			name, value, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			label, hit := red.MatchString(value)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in environment variable %q; an "+
					"environment block is readable through /proc/<pid>/environ for the life of "+
					"the process, where senro cannot redact it, so senro refuses to run rather "+
					"than leak it. Deliver it as a file instead: SecretEnv(%q, %q), which puts "+
					"the file's PATH in that variable", owner, n.ID, label, name, name, label)
		}
		for _, list := range [][]plan.Node{n.OnFailure, n.Always} {
			for i := range list {
				if err := walk(&list[i], "handler"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := range p.Nodes {
		if err := walk(&p.Nodes[i], "step"); err != nil {
			return err
		}
	}
	return nil
}
```

Add `"github.com/xavidop/senro/internal/redact"` to `guard.go`'s import block.

Call it in `engine.Run`, immediately after `checkSecretRefs`:

```go
	if err := checkSecretChannels(p, red); err != nil {
		return "", err
	}
```

- [ ] **Step 3: Write the end-to-end refusal test**

Append to `senro_test.go`:

```go
// TestRunRefusesASecretPassedAsACommandArgument is the guard through the real
// entry point. The pipeline is the mistake somebody actually makes: reaching
// for Reveal in their own code and interpolating the result.
func TestRunRefusesASecretPassedAsACommandArgument(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		NPMToken secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	runDir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("publish", exec.Command("npm", "publish", "--token="+cfg.NPMToken.Reveal()))

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a secret in a command argument")
	}
	if !strings.Contains(err.Error(), "publish") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), value) {
		t.Errorf("the error contains the value: %q", err)
	}
	// Nothing ran and nothing was written, so there is no record of the
	// argument anywhere: that is the difference between refusing and
	// redacting.
	if _, statErr := os.Stat(filepath.Join(runDir, "events.jsonl")); !os.IsNotExist(statErr) {
		t.Error("a refused run wrote a ledger, which would carry the argument")
	}
}
```

This test is the one place outside `internal/secrets/reveal.go` that calls `Reveal`, and it is a test file, so Task 10's static check excludes it by construction. That exclusion is deliberate and Task 10's own doc says why: the check is about production code, and a test that proves the guard works has to be able to construct the mistake.

- [ ] **Step 4: Run everything**

```bash
go test ./... -race
make all
```

**The canary.** `TestRunRefusesASecretPassedAsACommandArgument` asserts that `events.jsonl` does not exist, not merely that the value is absent from it. A guard placed after `eventlog.Open` would leave a directory and a `run.started` behind, and only an existence check sees the difference between "refused before anything happened" and "refused after the run had begun".

---

### Task 9: The cache key's `Secrets` component, and the `CacheEnv` collision

**Files:**
- Modify `internal/cache/key.go` (add `SecretIdentity`, `SecretDigest`, `SecretsComponent`)
- Modify `internal/cache/key_test.go`
- Modify `internal/secrets/secrets.go` (add `Set.Identity`)
- Modify `internal/engine/cache.go` (populate `Key.Secrets`, redact `replayLog`)
- Modify `internal/plan/validate.go` (extend `validateSecrets` with the `CacheEnv` rule)
- Test `internal/plan/plan_test.go`, `internal/engine/cache_test.go`, `senro_test.go`

**Interfaces:**
- Consumes: `secrets.Set.Value`, `secrets.Set.Identity` (Task 4); `redact.Set.Writer` (Task 3); `plan.Node.Secrets`, `plan.SecretEnvVar` (Task 7).
- Produces:
  ```go
  package cache
  type SecretIdentity struct {
      Name    string
      Source  string
      Version string
      Digest8 string
  }
  func SecretDigest(source string, value []byte) string
  func SecretsComponent(secs []SecretIdentity) string

  package secrets
  func (s *Set) Identity(name string) (Identity, bool)
  ```

**Wiring.** `internal/engine/cache.go`'s `cacheLookup` is the production caller: the `Secrets: ""` line it has carried since plan 5, with the comment "populated by the secrets subsystem; identities only, never values", becomes the call. The proof runs the same pipeline twice through `senro.Run` with the same secret (hit), then a third time with a different one (miss, explained by the `secrets` component).

**`cache.KeyVersion` stays at 2.** A step that declares no secret produces `Secrets: ""` exactly as it does today, so its digest does not move and no existing entry is invalidated. A step that declares one uses a builder method that did not exist before this plan, so no saved entry is reachable under a key that moved. Step 1 records the pin that would fail if this reasoning were wrong.

**Composition.** Three crossings, each with its own test:
- Plan 5's cache key plus Task 7's delivery. `SecretEnv` puts a per-attempt path in the environment; naming that same variable in `CacheEnv` would fold a path that changes every run into the key, and the step would never hit. Each feature is correct alone. Step 4 refuses the combination at build time.
- Task 6's log redaction plus plan 5's log replay. A cached log's bytes were redacted when they were written, so a replay is safe for the secrets that existed then. It is not safe for a secret that became one afterwards, so `replayLog` writes through this run's redactor too.
- Task 7's delivery plus plan 5's workspace snapshots. A `Pure()` step with a secret can write the value into a file that becomes a snapshot or a declared output, which no redactor can reach. Step 6's test proves the delivered file itself never lands there, and Task 10 documents the case that remains the author's responsibility.

- [ ] **Step 1: Record the key digest pin BEFORE changing anything**

Add to `internal/cache/key_test.go`, and run it on the unmodified tree to get the literal:

```go
// TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas is the check behind this
// plan's decision not to bump cache.KeyVersion. Populating Key.Secrets must
// leave every step that declares none at exactly the digest it had, or every
// cache entry in every developer's and every CI machine's cache root is
// silently orphaned.
//
// The literal is recorded by running this test against the tree BEFORE
// SecretsComponent is wired into internal/engine/cache.go, and pasted here.
// If it has to change, KeyVersion has to change with it, deliberately, with a
// note in its doc saying why.
func TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas(t *testing.T) {
	k := cache.Key{
		Command:          cache.CommandComponent("exec", []string{"go", "test", "./..."}, "/repo"),
		Env:              cache.EnvComponent([]string{"CI=1"}, []string{"CI"}),
		Secrets:          "",
		ExecutorClass:    "local/linux/amd64",
		Platform:         "linux/amd64",
		InputDigests:     cache.InputsComponent(nil),
		WorkspaceDigests: cache.WorkspacesComponent(nil),
		MountShape:       cache.MountShapeComponent(nil),
		StepShape:        cache.StepShapeComponent(false, nil),
		Version:          cache.KeyVersion,
	}
	const want = "PASTE THE DIGEST RECORDED BELOW"
	if got := string(k.Digest()); got != want {
		t.Errorf("Digest() = %q, want %q", got, want)
	}
}
```

```bash
go test ./internal/cache/ -run AKeyWithNoSecrets -v
```

It fails and prints the real digest. Paste that into `want` and confirm it passes, all before any other change in this task. If this step is skipped, the pin proves nothing.

- [ ] **Step 2: Write the failing tests for the component**

Append to `internal/cache/key_test.go`:

```go
// TestSecretsComponentCarriesNoValue is the containment assertion for the one
// piece of a cache key derived from a credential. A cache entry outlives the
// run that wrote it and is shared by every run on the machine, so this is the
// longest-lived artifact in the whole system that touches a secret at all.
func TestSecretsComponentCarriesNoValue(t *testing.T) {
	const value = "s3cr3t-token-value"
	const source = "aws-sm://ci/npm#token"
	got := cache.SecretsComponent([]cache.SecretIdentity{{
		Name: "NPMToken", Source: source, Version: "v7",
		Digest8: cache.SecretDigest(source, []byte(value)),
	}})
	// The canary: an empty component would satisfy every absence check below.
	if !strings.Contains(got, "NPMToken") || !strings.Contains(got, source) {
		t.Fatalf("the component does not name the secret at all: %q", got)
	}
	if strings.Contains(got, value) {
		t.Errorf("the component contains the value: %q", got)
	}
}

// TestSecretDigestChangesWithTheValueAndNotWithAnythingElse is design.md
// section 1.6's requirement: "a changed secret invalidates dependent steps".
func TestSecretDigestChangesWithTheValueAndNotWithAnythingElse(t *testing.T) {
	const source = "aws-sm://ci/npm#token"
	a := cache.SecretDigest(source, []byte("value-one-aaaaaa"))
	b := cache.SecretDigest(source, []byte("value-two-bbbbbb"))
	if a == b {
		t.Error("two different values produced the same digest")
	}
	if a != cache.SecretDigest(source, []byte("value-one-aaaaaa")) {
		t.Error("the digest is not stable for one value")
	}
	if len(a) != 8 {
		t.Errorf("digest %q is %d hex digits, want 8", a, len(a))
	}
}

// TestSecretDigestIsSaltedBySource is the refinement of section 1.6 this plan
// makes deliberately. An unsalted 32-bit digest of a low-entropy value is
// invertible by anyone holding the cache directory, and a cache entry
// outlives the run. Salting costs nothing and keeps the property section 1.6
// actually wants.
func TestSecretDigestIsSaltedBySource(t *testing.T) {
	const value = "s3cr3t-token-value"
	if cache.SecretDigest("aws-sm://a#k", []byte(value)) ==
		cache.SecretDigest("aws-sm://b#k", []byte(value)) {
		t.Error("the same value under two sources produced the same digest; the salt is not applied")
	}
}

// TestSecretsComponentIsOrderIndependentAndUnambiguous. Same grammar as
// InputsComponent and WorkspacesComponent, and the same reason: a name and a
// source are free-form text, and a delimiter-joined encoding of one set can
// collide with a different set.
func TestSecretsComponentIsOrderIndependentAndUnambiguous(t *testing.T) {
	one := []cache.SecretIdentity{
		{Name: "A", Source: "s://a", Digest8: "11111111"},
		{Name: "B", Source: "s://b", Digest8: "22222222"},
	}
	two := []cache.SecretIdentity{one[1], one[0]}
	if cache.SecretsComponent(one) != cache.SecretsComponent(two) {
		t.Error("reordering changed the component")
	}
	if cache.SecretsComponent(nil) != "" {
		t.Error("no secrets must produce the empty component, or every existing key moves")
	}
	// A name that contains the record decoration must not be able to imitate
	// a second record.
	tricky := cache.SecretsComponent([]cache.SecretIdentity{
		{Name: "A B\n2:CD", Source: "s://a", Digest8: "11111111"},
	})
	plain := cache.SecretsComponent([]cache.SecretIdentity{
		{Name: "A", Source: "s://a", Digest8: "11111111"},
		{Name: "CD", Source: "s://a", Digest8: "11111111"},
	})
	if tricky == plain {
		t.Error("a crafted name collided with a different secret set")
	}
}
```

- [ ] **Step 3: Write the component**

Append to `internal/cache/key.go`:

```go
// SecretIdentity is one secret's contribution to a cache key. A VALUE is
// never here: only a name, the source it came from, the provider's version of
// it, and a digest that stands in for the value.
type SecretIdentity struct {
	Name    string
	Source  string
	Version string
	Digest8 string
}

// secretUnitSep separates a SecretIdentity's three sub-fields inside one
// framed record. 0x1f cannot appear in a source URI (a URI forbids raw
// control characters and requires percent-encoding), in a provider version
// string, or in a hex digest, so the split is unambiguous without a second
// layer of length framing. Same argument MountShapeComponent's mode+"@"+at
// join relies on.
const secretUnitSep = "\x1f"

// SecretDigest is the eight hex digits that stand in for a secret's value in
// a cache key.
//
// design.md section 1.6: "Key on the source: URI plus the resolved version
// plus an 8-byte digest of the value, so a changed secret invalidates
// dependent steps without the key becoming a plaintext oracle."
//
// The digest is SALTED WITH THE SOURCE URI, which is a deliberate refinement
// of that sentence rather than a departure from it. An unsalted 32-bit digest
// of a LOW-ENTROPY value, a four-digit PIN or a short password, is invertible
// by anyone holding the cache directory, and a cache entry persists across
// runs and outlives the run directory. Salting costs nothing, keeps exactly
// the property section 1.6 wants (a changed value changes the key, an
// unchanged one does not), and removes the precomputation. The source is
// already in the same component in the clear, so the salt hides nothing that
// was not already visible.
//
// Eight digits, matching EnvComponent's own choice: enough to distinguish two
// values in practice, short enough to print in `senro cache explain`.
func SecretDigest(source string, value []byte) string {
	b := make([]byte, 0, len(source)+1+len(value))
	b = append(b, source...)
	b = append(b, 0)
	b = append(b, value...)
	return cas.FromBytes(b).Short()
}

// SecretsComponent renders a step's secret identities as a length-framed,
// sorted encoding of (name, identity) pairs, the same grammar
// InputsComponent, WorkspacesComponent and MountShapeComponent use, so
// `senro cache explain` needs no new decoder.
//
// An empty set produces the empty string, which is what every key in every
// existing cache already carries for this component. That is what lets this
// component be populated without bumping KeyVersion; see
// TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas.
func SecretsComponent(secs []SecretIdentity) string {
	if len(secs) == 0 {
		return ""
	}
	sorted := make([]SecretIdentity, len(secs))
	copy(sorted, secs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Name != sorted[j].Name {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Source < sorted[j].Source
	})
	var b strings.Builder
	for _, s := range sorted {
		writeFramed(&b, s.Name)
		b.WriteByte(' ')
		writeFramed(&b, s.Source+secretUnitSep+s.Version+secretUnitSep+s.Digest8)
		b.WriteByte('\n')
	}
	return b.String()
}
```

- [ ] **Step 4: Add the `CacheEnv` refusal**

In `internal/plan/validate.go`, inside `validateSecrets`, after the `seenVar` and `seenEnv` maps are populated (so at the end of the function, before its `return nil`):

```go
	// The composition refusal. A SecretEnv variable holds a per-ATTEMPT file
	// path, which changes on every run by construction, so folding it into
	// the cache key would move the key every time and the step would never
	// hit. Each feature is correct alone: plan 5's CacheEnv digests declared
	// names, and this plan's SecretEnv delivers a path. The combination is
	// the defect, and it is silent, because a cache that never hits looks
	// exactly like a cache that is working on the first run.
	//
	// A secret's own identity is already in the key's secrets component
	// (cache.SecretsComponent), which is what actually invalidates a step
	// when the credential changes. So the fix is to delete the CacheEnv
	// entry, not to add a mechanism.
	for _, ce := range n.CacheEnv {
		if name, ok := seenVar[ce]; ok {
			return fmt.Errorf(
				"plan: step %q names %s in both CacheEnv and its secret declarations (secret %q); "+
					"that variable holds a per-attempt FILE PATH, not a value, so folding it into "+
					"the cache key would change the key on every run and the step would never hit. "+
					"The secret's own identity is already in the key, so remove it from CacheEnv",
				n.ID, ce, name)
		}
		if seenEnv[ce] {
			return fmt.Errorf(
				"plan: step %q names %q in both CacheEnv and SecretEnv; that variable holds a "+
					"per-attempt FILE PATH, not a value, so folding it into the cache key would "+
					"change the key on every run and the step would never hit. The secret's own "+
					"identity is already in the key, so remove it from CacheEnv", n.ID, ce)
		}
	}
```

Append to `internal/plan/plan_test.go`:

```go
// TestValidateRefusesCacheEnvOnASecretVariable is the composition refusal.
// Both spellings, the SecretEnv alias and the uniform SENRO_SECRET_ name.
func TestValidateRefusesCacheEnvOnASecretVariable(t *testing.T) {
	for _, name := range []string{"NPM_TOKEN", "SENRO_SECRET_NPMTOKEN"} {
		t.Run(name, func(t *testing.T) {
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
				ID: "a", Kind: "exec", Cmd: []string{"echo"},
				Secrets:  []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
				CacheEnv: []string{name},
			}}}
			err := p.Validate()
			if err == nil {
				t.Fatalf("Validate accepted CacheEnv(%q) on a secret variable", name)
			}
			if !strings.Contains(err.Error(), "never hit") {
				t.Errorf("the error must say what actually goes wrong; got %q", err)
			}
		})
	}
}

// TestValidateAcceptsCacheEnvOnAnUnrelatedVariable keeps the rule from being
// over-broad: a step with a secret can still declare an ordinary CacheEnv.
func TestValidateAcceptsCacheEnvOnAnUnrelatedVariable(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo"},
		Env:      []string{"CI=1"},
		Secrets:  []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
		CacheEnv: []string{"CI"},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("Validate refused an unrelated CacheEnv: %v", err)
	}
}
```

- [ ] **Step 5: Populate the key**

`secrets.Set.Identity` was written in Task 4 (`internal/secrets/secrets.go`) and has had no caller until now; this is it.

In `internal/engine/cache.go`'s `cacheLookup`, add the preparation above the `cache.Key` literal and replace the `Secrets: ""` line:

```go
	// Identities only, never values (design.md section 1.6). A step that
	// declares no secret produces the empty component here, exactly as it did
	// before this was populated, which is why cache.KeyVersion does not move.
	//
	// The value is read only to digest it, and the digest is salted with the
	// source so a cache directory is not a rainbow table for a low-entropy
	// credential. See cache.SecretDigest.
	var secretIDs []cache.SecretIdentity
	for _, sec := range n.Secrets {
		id, ok := rc.secrets.Identity(sec.Name)
		if !ok {
			return cacheDecision{}, fmt.Errorf(
				"engine: step %q needs secret %q, which was not resolved", n.ID, sec.Name)
		}
		v, _ := rc.secrets.Value(sec.Name)
		secretIDs = append(secretIDs, cache.SecretIdentity{
			Name: id.Name, Source: id.Source, Version: id.Version,
			Digest8: cache.SecretDigest(id.Source, v),
		})
	}
```

and, in the `cache.Key` literal:

```go
		Secrets:          cache.SecretsComponent(secretIDs),
```

- [ ] **Step 6: Redact a replayed log**

In `internal/engine/cache.go`'s `replayLog`, replace the copy:

```go
	// Through logMarker, exactly as a live step's output goes, so a client
	// range-requesting the replayed log reads the same shape of marker it
	// would for a step that ran.
	//
	// And through THIS RUN's redactor, not merely trusting that the stored
	// bytes were redacted when they were written. They were, for the secrets
	// that existed then. A value that became a secret afterwards, or a
	// credential rotated into a config that a previous run did not have, is
	// in those bytes and would be replayed into this run's log file and out
	// to every attached client. Redacting on the way out closes the class
	// rather than the instance.
	m := &logMarker{rc: rc, w: w, step: n.ID, attempt: 1, stream: l.Stream}
	rw := rc.redact.Writer(m)
	if _, err := io.Copy(rw, body); err != nil {
		return err
	}
	return rw.Flush()
```

- [ ] **Step 7: Write the cache integration tests**

Append to `internal/engine/cache_test.go`:

```go
// TestTheSameSecretHitsAndAChangedOneMisses is design.md section 1.6's whole
// requirement, driven through senro.Run twice against one cache root.
func TestTheSameSecretHitsAndAChangedOneMisses(t *testing.T) {
	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("input"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	build := func(value string) any {
		type config struct {
			Tok secret.String `source:"fake://ci/tok#v"`
		}
		p := mamoritest.NewProvider("fake")
		p.Set("ci/tok#v", value)
		cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
		if err != nil {
			t.Fatalf("mamori.Load: %v", err)
		}
		return cfg
	}
	pipeline := func() *senro.Pipeline {
		pipe := senro.New("p")
		pipe.Workflow("w").
			Step("pure", exec.Command("cat", "in.txt")).
			WorkDir(work).
			Pure().
			Inputs(artifact.File("in.txt")).
			SecretEnv("TOK", "Tok")
		return pipe
	}
	runOnce := func(value string) []api.Event {
		dir := t.TempDir()
		if err := senro.Run(context.Background(), pipeline(),
			senro.WithDir(dir), senro.WithCacheDir(cacheDir),
			senro.WithSecrets(build(value))); err != nil {
			t.Fatalf("Run: %v", err)
		}
		return readLedger(t, dir)
	}

	const first = "value-one-aaaaaaaa"
	_ = runOnce(first)

	second := runOnce(first)
	if !hasEvent(second, api.CacheHit) {
		t.Error("the second run with the SAME secret did not hit")
	}

	third := runOnce("value-two-bbbbbbbb")
	if hasEvent(third, api.CacheHit) {
		t.Error("a run with a CHANGED secret hit; a changed credential must invalidate the step")
	}
	miss := findEvent(t, third, api.CacheMiss)
	var body api.CacheMissBody
	if err := miss.Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(body.Differing, "secrets") {
		t.Errorf("cache.miss blames %q; it must name the secrets component", body.Differing)
	}
	if strings.Contains(body.Differing, "value-one") || strings.Contains(body.Differing, "value-two") {
		t.Errorf("cache.miss carries a secret value: %q", body.Differing)
	}
}

// TestACachedLogIsRedactedAgainstTheCURRENTRunsSecrets is the class fix for
// replay. The first run's bytes were written when nothing was a secret, so
// they hold the value in the clear inside the CAS; the second run knows it is
// a secret and must not put it back into a log file or out to a client.
func TestACachedLogIsRedactedAgainstTheCURRENTRunsSecrets(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("leak: "+value+"\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	pipeline := func() *senro.Pipeline {
		pipe := senro.New("p")
		pipe.Workflow("w").
			Step("pure", exec.Command("cat", "in.txt")).
			WorkDir(work).Pure().Inputs(artifact.File("in.txt"))
		return pipe
	}

	// Run one: no secrets at all, so the log the cache stores holds the value.
	firstDir := t.TempDir()
	if err := senro.Run(context.Background(), pipeline(),
		senro.WithDir(firstDir), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Run two: the same plan, but now the value is a declared secret.
	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	secondDir := t.TempDir()
	if err := senro.Run(context.Background(), pipeline(),
		senro.WithDir(secondDir), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if !hasEvent(readLedger(t, secondDir), api.CacheHit) {
		t.Fatal("the second run did not hit, so nothing was replayed and this test proves nothing")
	}

	body, err := os.ReadFile(eventlog.NewLogSet(secondDir).Path("pure", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the replayed log: %v", err)
	}
	if !bytes.Contains(body, []byte("leak: ")) {
		t.Fatalf("the replayed log has no content: %q", body)
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the replayed log carries a value that IS a secret in this run")
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Error("the replayed log was not redacted")
	}
}
```

`readLedger`, `hasEvent` and `findEvent` are small helpers; if `internal/engine`'s tests do not already have them, add them next to these tests: `readLedger` decodes `events.jsonl` line by line into `[]api.Event` and `t.Fatal`s on an empty file, and `findEvent` `t.Fatal`s when the type is absent, so a missing event is never read as a passing assertion.

- [ ] **Step 8: Write the containment sweep over the cache root**

Append to `senro_test.go`:

```go
// TestNoSecretValueReachesTheCacheRoot is the longest-lived containment
// claim in this plan. A run directory is one run's record; a cache root is
// shared by every run on the machine and outlives all of them.
func TestNoSecretValueReachesTheCacheRoot(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("input"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("pure", exec.Command("sh", "-c", `cat in.txt; cat "$TOK"`)).
		WorkDir(work).
		Pure().
		Inputs(artifact.File("in.txt")).
		SecretEnv("TOK", "Tok")

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The canary: the cache root must actually hold this step's entry, or a
	// sweep of it says nothing. The key digest is not a value, so searching
	// for the step's own id is the honest way to prove the entry exists.
	if scanTreeForAny(t, cacheDir, "pure") == "" {
		t.Fatal("the cache root has no trace of the step; the sweep below proves nothing")
	}
	if found := scanTreeFor(t, cacheDir, value); found != "" {
		t.Errorf("the secret value appears in the cache root, in %s", found)
	}
}
```

`scanTreeForAny` is `scanTreeFor` under a name that reads as a presence check rather than an absence check; add it as a two-line wrapper.

- [ ] **Step 9: Run everything**

```bash
go test ./... -race
go test ./internal/engine/ -run Golden -v
go test ./internal/cache/ -run AKeyWithNoSecrets -v
make all
golangci-lint run ./...
```

**The check that catches it.** `TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas` is the only thing standing between this task and silently orphaning every cache entry on every machine. It is written and pinned in Step 1, before anything else in the task, precisely so its value comes from the old code rather than from the new.

---

### Task 10: The documentation, the one-`Reveal` check, and the end-to-end proof

**Files:**
- Create `reveal_static_test.go`
- Create `secrets_e2e_test.go`
- Modify `README.md` (a Secrets section)
- Modify `docs/design.md` (a short note recording the two deviations)

**Interfaces:**
- Consumes: everything above.
- Produces: no new Go API.

**Wiring.** The static check and the end-to-end test are the wiring: they run in `go test ./...` and fail the build.

- [ ] **Step 1: Write the one-`Reveal` static check**

Create `reveal_static_test.go` at the repository root, following the shape of `cachedir_isolation_static_test.go`:

```go
package senro_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRevealAppearsInExactlyOnePlaceInProductionCode mechanises design.md
// section 1.3's own audit instruction:
//
//	"the seam between the two is exactly one call: walk the resolved config
//	struct, Reveal() each secret.String, and seed the stream redactor. That's
//	the only place in the codebase where Reveal() should appear, which makes
//	the audit trivial, since it's one grep."
//
// A grep a human has to remember to run is a grep that stops being run. This
// is the same grep, in CI, failing the build.
//
// Scope, and why each exclusion is deliberate:
//
//   - _test.go files are excluded. A test that proves the argv guard works has
//     to be able to CONSTRUCT the mistake (see
//     TestRunRefusesASecretPassedAsACommandArgument), and a test that seeds a
//     fixture has to read a value. The check is about production code, which
//     is what ships.
//   - The api module is excluded: it is a separate module that cannot import
//     mamori and has no secrets in it at all.
//   - internal/secrets/reveal.go is the one permitted file.
//
// The check matches the bare identifier rather than ".Reveal(", so a call
// made through reflect.Value.MethodByName("Reveal") is caught too. That is
// not hypothetical: it is the obvious way somebody would route around a
// check that only looked for the dot form.
func TestRevealAppearsInExactlyOnePlaceInProductionCode(t *testing.T) {
	const allowed = "internal/secrets/reveal.go"
	root := moduleRoot(t)

	var offenders []string
	var sawAllowed bool
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "site", "testdata":
				return fs.SkipDir
			}
			if rel, relErr := filepath.Rel(root, p); relErr == nil && rel == "api" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		b, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		if !strings.Contains(string(b), "Reveal") {
			return nil
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		if filepath.ToSlash(rel) == allowed {
			sawAllowed = true
			return nil
		}
		offenders = append(offenders, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	// The canary. If the permitted file stopped containing the seam, this
	// check would report "zero offenders" for a tree in which nothing reveals
	// anything at all, which is a different bug and must not read as a pass.
	if !sawAllowed {
		t.Fatalf("%s does not mention Reveal; the seam moved or was deleted, and this "+
			"check would now pass over any tree at all", allowed)
	}
	if len(offenders) > 0 {
		t.Errorf("Reveal appears outside %s, in: %s\n\n"+
			"design.md section 1.3 puts the seam in exactly one place so the audit is one "+
			"grep. Route the value through internal/secrets instead of reading it here.",
			allowed, strings.Join(offenders, ", "))
	}
}
```

`moduleRoot` already exists in `cachedir_isolation_static_test.go` in this same package, so it is reused rather than redefined.

- [ ] **Step 2: Write the end-to-end composition test**

Create `secrets_e2e_test.go` at the repository root. This is the one test that exercises every task in this plan at once, on the pipeline shape §12's worked example describes.

```go
package senro_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/redact"
)

// TestSecretsEndToEnd runs the shape design.md section 12's worked example
// describes, with every piece of this plan live at once, and then proves the
// containment claim over every byte the run left behind.
//
// The pipeline: one Pure step that reads its credential from the file
// SecretEnv delivered, prints it (the leak section 1.3 says only senro can
// defend against), writes a derived artifact, and is cached. Then the same
// pipeline again, hitting the cache.
//
// What this test crosses that no single task's test does:
//
//   - Delivery (7) with redaction (6): the value the step printed is
//     [REDACTED] in the log file on disk.
//   - Delivery (7) with the cache key (9): the second run hits, so the
//     per-attempt file path did NOT reach the key.
//   - The cache (9) with redaction (6): the replayed log is redacted too.
//   - Everything with the workspace snapshot path (plan 5): the secret
//     directory is not under the run directory, so no snapshot and no
//     declared output can reach it.
func TestSecretsEndToEnd(t *testing.T) {
	const value = "s3cr3t-registry-token-aaaa"

	type Config struct {
		RegistryToken secret.String `source:"fake://ci/ghcr#token"`
		Registry      string        `source:"fake://ci/ghcr#host"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/ghcr#token", value)
	pr.Set("ci/ghcr#host", "ghcr.io/acme")

	ctx := context.Background()
	cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	if cfg.Registry != "ghcr.io/acme" {
		t.Fatalf("the fixture did not load: Registry = %q", cfg.Registry)
	}

	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	build := func() *senro.Pipeline {
		p := senro.New("monorepo")
		p.Workflow("publish").
			Step("login", exec.Command("sh", "-c",
				`echo "registry is $REGISTRY"
				 echo "credential file is $REGISTRY_TOKEN"
				 cat "$REGISTRY_TOKEN"
				 echo
				 cat "$SENRO_SECRET_REGISTRYTOKEN" > receipt.txt`)).
			WorkDir(work).
			Env("REGISTRY", cfg.Registry).
			SecretEnv("REGISTRY_TOKEN", "RegistryToken").
			CacheEnv("REGISTRY").
			Pure().
			Inputs(artifact.File("Dockerfile"))
		return p
	}

	firstDir := t.TempDir()
	if err := senro.Run(ctx, build(),
		senro.WithDir(firstDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// 1. The step really ran and really printed its credential.
	logPath := eventlog.NewLogSet(firstDir).Path("login", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	if !bytes.Contains(body, []byte("registry is ghcr.io/acme")) {
		t.Fatalf("the step's own output is missing: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("nothing was redacted, so either the value was never printed or the "+
			"redactor never ran: %q", body)
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the log file holds the credential")
	}

	// 2. The step wrote the value into a file it controls, which senro
	//    deliberately does NOT protect. Proving it is there is what makes the
	//    sweep below a real statement about senro's own artifacts rather than
	//    an accident of the pipeline not producing the value anywhere.
	receipt, err := os.ReadFile(filepath.Join(work, "receipt.txt"))
	if err != nil {
		t.Fatalf("reading the step's own output file: %v", err)
	}
	if !bytes.Contains(receipt, []byte(value)) {
		t.Fatalf("the step did not write the value where it was told to; the rest of "+
			"this test is not measuring what it claims")
	}

	// 3. Nothing senro itself wrote holds the value.
	if found := scanTreeFor(t, firstDir, value); found != "" {
		t.Errorf("the value appears under the run directory, in %s", found)
	}
	if found := scanTreeFor(t, cacheDir, value); found != "" {
		t.Errorf("the value appears under the cache root, in %s", found)
	}

	// 4. The second run hits, which is only possible if the per-attempt
	//    secret file's PATH stayed out of the cache key.
	secondDir := t.TempDir()
	if err := senro.Run(ctx, build(),
		senro.WithDir(secondDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	events := readLedgerAt(t, secondDir)
	if !hasEventType(events, api.CacheHit) {
		t.Error("the second run missed; a per-attempt path reached the cache key")
	}

	// 5. The replayed log is redacted too.
	replayed, err := os.ReadFile(eventlog.NewLogSet(secondDir).Path("login", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the replayed log: %v", err)
	}
	if !bytes.Contains(replayed, []byte("registry is ghcr.io/acme")) {
		t.Fatalf("the replayed log has no content: %q", replayed)
	}
	if bytes.Contains(replayed, []byte(value)) {
		t.Error("the replayed log holds the credential")
	}

	// 6. And the run's own record names the secret without its value.
	raw, err := os.ReadFile(filepath.Join(firstDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"secret.resolved"`)) {
		t.Error("no secret.resolved event")
	}
	if !bytes.Contains(raw, []byte(`"secret.redacted"`)) {
		t.Error("no secret.redacted event, so the UI has no way to show redaction is live")
	}
	if !strings.Contains(string(raw), `"RegistryToken"`) {
		t.Error("the ledger does not name the secret")
	}
}
```

`readLedgerAt` and `hasEventType` are the root package's copies of the helpers Task 9 added to `internal/engine`; write them next to this test rather than exporting anything, and make `readLedgerAt` `t.Fatal` on an empty ledger.

- [ ] **Step 3: Write the README section**

Add a `## Secrets` section to `README.md`. The table is the part that matters most, because a user deciding how to pass a credential reads exactly one thing.

```markdown
## Secrets

Declare your credentials as a typed struct and let [mamori](https://github.com/xavidop/mamori)
resolve them, once, before the run starts:

```go
type Config struct {
	RegistryToken secret.String `source:"aws-sm://ci/ghcr#token"`
	Registry      string        `source:"env:REGISTRY" default:"ghcr.io/acme"`
}

cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(awssm.New()))
if err != nil {
	return err
}

senro.Run(ctx, pipeline(cfg), senro.WithSecrets(cfg))
```

A step asks for one by field name, and receives a **file**:

```go
setup.Step("install", exec.Command("pnpm", "install")).
	SecretEnv("NPM_TOKEN", "NPMToken")
```

```sh
# inside the step:
npm config set //registry.npmjs.org/:_authToken="$(cat "$NPM_TOKEN")"
```

`NPM_TOKEN` holds the file's **path**, not the token. Every declared secret also
arrives under the uniform name `SENRO_SECRET_<NAME>`, where `<NAME>` is the field
name uppercased with everything outside `A-Z0-9_` replaced by `_`.

### Which channels are safe

| Channel | Safe? | Why |
|---|---|---|
| The file at `$SENRO_SECRET_<NAME>` | **Yes** | Mode 0600 in a 0700 directory on tmpfs where available, deleted when the step's sandbox closes |
| `SecretEnv("VAR", "Field")` | **Yes** | `VAR` holds the file's path, never the value |
| A step's stdout and stderr | **Redacted** | Every registered value and its encodings are replaced with `[REDACTED]` before the bytes reach a log file, the event stream or an attached client |
| Any event payload | **Redacted** | Same redactor, applied to every event before it reaches `events.jsonl` or any sink |
| A command argument | **Refused** | Visible in `ps(1)`, in shell history and in auditd `execve` records, where no redactor can reach. senro refuses to start the run |
| An environment variable holding a **value** | **Refused** | Readable through `/proc/<pid>/environ` for the life of the process. senro refuses to start the run |
| A file the step itself writes | **Your responsibility** | senro never reads a step's files. A value written into a workspace goes into a snapshot and into the content store, and stays there |
| A declared output (`Outputs(...)`) | **Your responsibility** | Same: the file's bytes are stored as the step produced them |
| A cache key | **Never contains a value** | Only the source URI, the provider version, and eight hex digits of a source-salted digest |
| `plan.json` | **Never contains a value** | A plan stores a field reference, so a plan can be serialized, stored and re-run safely |

### What the redactor covers

Registered for every secret, and matched across write boundaries so a value
split by the pipe is still caught:

- the raw value
- base64: standard and URL alphabets, padded and unpadded
- URL escaping: query form (`+` for a space) and path form (`%20`)
- JSON string escaping, with and without HTML escaping of `<`, `>` and `&`
- shell quoting: the body of a single-quoted word and of a double-quoted word

### What it does not cover

Stated as carefully as the list above, because a redactor believed to cover
more than it does is worse than none:

- **Any hashing, compression or encryption.** A step that gzips its own log
  defeats redaction entirely.
- **Hex, base32, and any encoding not listed above.**
- **A value printed in pieces with other content between them**, for example
  `echo "${T:0:8}"; echo "${T:8}"`. A value split across two *write* calls is
  caught; a value split by *content* is not.
- **Values shorter than six bytes.** senro refuses to start a run whose config
  carries one, rather than deliver a credential it cannot protect: redacting a
  four-byte value would redact unrelated output.
- **Two secrets that overlap.** If one value is a substring of another,
  replacing the first can leave a fragment of the second. No *complete*
  occurrence of a registered value ever survives, which is the exact
  guarantee; no stronger one is claimed.
- **Anything outside the senro process:** `ps(1)`, `/proc/<pid>/environ`, shell
  history, auditd. That is what the refusals in the table above are for.
- **Shredding.** Secret files are unlinked, not overwritten. On tmpfs
  (`$XDG_RUNTIME_DIR`, or `/dev/shm` on Linux) that frees the pages. On macOS
  there is no tmpfs available to this executor, so the file lands in `$TMPDIR`
  and its bytes may persist in free space after the unlink.

### Isolation between steps

The local executor gives none: every step in a run executes as the same user
under one run root, so a step can read another step's secret file. Use a
container or Kubernetes executor where steps must not see each other's
credentials.

### One `Reveal`

`Reveal()` appears in exactly one file in senro's production code,
`internal/secrets/reveal.go`, and a test fails the build if a second appears.
Adopt the same rule in your pipeline repository: mamori's own `go vet`
analyzer is the tool for it where it is available to you.
```

- [ ] **Step 4: Record the deviations in `docs/design.md`**

Two statements in §1 are not implemented literally, and a reader of the design must not have to discover that from the code. Add to `docs/design.md` §1.2, after the paragraph beginning "Steps reference fields by name":

```markdown
> **As implemented (v0):** `plan.json` stores the *field reference*, not the `source:` URI.
> `(*Pipeline).Build` has no access to the resolved config struct, since the worked example in
> §12 hands it to `Run` rather than to `New`, and enriching the plan after `Build` returned would
> make `plan.Digest()` differ between the value a caller inspected and the value `plan.resolved`
> reports. The resolved URI is recorded in the `secret.resolved` event and in the cache key's
> `secrets` component instead. `plan.SecretSpec.Source` is declared and left empty so a future
> builder that does know the config type can fill it additively.
```

And to §1.6, after the sentence about the 8-byte digest:

```markdown
> **As implemented (v0):** the digest is salted with the `source:` URI,
> `sha256(source + NUL + value)`, first eight hex digits. An unsalted 32-bit digest of a
> low-entropy value is invertible by anyone holding the cache directory, and a cache entry
> outlives the run that wrote it. The salt costs nothing and keeps exactly the property this
> section asks for.
```

- [ ] **Step 5: Run everything, one last time**

```bash
go test ./... -race
cd api && go test ./... && cd ..
make all
golangci-lint run ./...
cd api && golangci-lint run ./... && cd ..
grep -c '^require\|^	[a-z]' api/go.mod
```

`api/go.mod` must still declare nothing.

**The canary.** `TestRevealAppearsInExactlyOnePlaceInProductionCode` fails when the permitted file *stops* containing `Reveal`, not only when another file starts containing it. Without that half, deleting or renaming the seam would leave a check that reports zero offenders over a tree in which the whole mechanism has been removed.

---

## Self-review

**Every §1 requirement in scope maps to a task.**

| design.md §1 | Task |
|---|---|
| 1.1 resolution central, once, at run start, never at plan time | 4 (`FromConfig` in `RunPlan`), 7 (`plan.SecretSpec` holds a reference) |
| 1.1 uniform step-side contract | 7 (`SENRO_SECRET_<NAME>` for every declared secret) |
| 1.2 consume mamori, `Load` not `Watch`, typed struct | 4 |
| 1.2 `secret.String` recognised, `source:` tag read | 4 |
| 1.2 `secret.resolved` event, identity only | 4 |
| 1.2 core has no cloud SDKs, bounds shipped binary size | 4 step 6 (production imports only `mamori/secret`) |
| 1.3 senro owns subprocess-output redaction | 6 |
| 1.3 exactly one `Reveal()` call site | 4 (the seam), 10 (the mechanised grep) |
| 1.3 the `go vet` analyzer | 10 step 3, recommended in the README. Not wired into senro's CI; see the open question below |
| 1.4 local delivery: tmpfs file, 0600, path in `SENRO_SECRET_<NAME>`, `exec.Cmd.Env` only | 7 |
| 1.4 never `os.Setenv` | already true; `localexec.Run` sets `cmd.Env` explicitly |
| 1.4 never a command argument | 8 (refused, not merely avoided) |
| 1.4 container, k8s, ssh rows | out of scope: v0 is the local executor |
| 1.4 a step outliving its credential, plan-time TTL warning | out of scope: mamori's `Load` surfaces no `NotAfter` to its caller |
| 1.5 one redactor in front of every stream sink | 5 (events), 6 (logs), 9 (replay) |
| 1.5 register encodings, all named families | 2 |
| 1.5 Aho-Corasick over a rolling buffer, split values caught | 1, 3 |
| 1.5 skip values below a threshold | 1 (skip), 4 (refuse rather than skip silently) |
| 1.5 emit `secret.redacted` | 6 |
| 1.5 redaction runs before the hub | 5 and 6, by construction: both write paths, no client filter |
| 1.6 values never in a cache key | 9, and 7's delivery, which keeps them out of the environment |
| 1.6 key on source, version and a value digest | 9 |
| 1.6 stable across an expansion's children | 9: resolution is once per run, so the digest is constant |
| 1.6 secret-consuming steps impure by default | already true: `Pure()` is opt-in |
| §6.11 a client never receives a value | 5, 6 |
| §10 the whole v0 line | 1 through 10 |

**Out of scope, stated:** k8s delegation via IRSA or Pod Identity (§1.4, explicitly v1 in §10); container and ssh delivery rows (no such executor in v0); the credential-TTL plan-time warning (mamori does not surface `NotAfter` to `Load`'s caller); `middleware.Audit` (a mamori-side option a pipeline author passes to `Load`, not something senro wires).

**Signature consistency.** `redact.Value{Label, Value}` is produced by `secrets.Set.RedactValues` (Task 4) and consumed by `redact.New` (Task 1). `redact.Set.Redact` returns `([]byte, int)` in Task 1 and is called that way in Task 5. `redact.Set.Writer` returns `*redact.Writer` in Task 3 and is used as one in Tasks 6 and 9. `secrets.Set.Identity` is declared in Task 4's interface block and called in Task 9. `plan.SecretEnvVar` is defined in Task 7 and called in Task 7's delivery and Task 9's validation rule. `cache.SecretIdentity` is defined in Task 9 and built in Task 9's engine wiring. `engine.Options.Secrets` is `*secrets.Set` in Task 4 and used as one in Tasks 7, 8 and 9.

**Task ordering.** 1, 2 and 3 build one library. 4 makes it reachable. 5 and 6 put it on the two write paths. 7 delivers. 8 refuses. 9 keys. 10 documents and proves. Nothing depends on a later task, and Tasks 5's and 6's tests are written so the redaction assertions fail before the change and the structural assertions pass, which is the ordering that shows a test is not vacuous.

**Placeholders.** Two, both deliberate and both a step of their own that must run before the code around them: the plan digest literal in Task 7 Step 1, and the cache key digest literal in Task 9 Step 1. Both are values that can only be recorded from the tree as it stands before the change, and both are named `PASTE ...` so they cannot be mistaken for finished text. There are no others.

---

## Open questions

Four, none of which blocks any task. Each has a decision recorded above and would only change if the answer came back differently.

1. **mamori's `go vet` analyzer.** §1.3 says "adopt mamori's `go vet` analyzer in senro's own CI, and recommend it for pipeline repositories." No such analyzer could be verified in the published `v1.12.1` module: the analyzer would live in one of mamori's submodules, and those (`providers/*`, `server`, `x/*`, `cmd/mamori`) are not separately tagged and do not resolve from outside the mamori repository (`go get github.com/xavidop/mamori/providers/aws@latest` fails on `unknown revision v0.1.0`). So this plan implements the *intent* with senro's own mechanism instead, `TestRevealAppearsInExactlyOnePlaceInProductionCode`, and the README recommends the analyzer "where it is available to you". If the analyzer is published and importable, wiring it into `.github/workflows` is one line and can be added on top.

2. **`plan.json` and the source URI (design decision 2).** §0's invariant 1 calls `plan.json` "the resolved artifact: pinned image digests, resolved secret *references*", and §1.2 says it stores the `source:` URI. This plan stores the field reference and leaves `SecretSpec.Source` empty, because `Build` has no access to the config struct and post-`Build` enrichment would move `plan_digest` between what a caller inspects and what `plan.resolved` reports. If keeping the URI in `plan.json` is the higher priority, the resolution is to have the *pipeline* learn the config type, for instance `senro.New("ci", senro.Secrets[Config]())`, which is a builder-API change large enough to belong in its own plan.

3. **Salting the cache key's value digest (design decision 6).** §1.6 says "an 8-byte digest of the value"; this plan uses `sha256(source + NUL + value)`, first eight hex digits, because an unsalted 32-bit digest of a low-entropy credential is invertible by anyone holding the cache directory. This is a deliberate strengthening rather than a deviation in intent, and Task 10 records it in `docs/design.md`. If the literal reading is required, drop the source from the input and delete `TestSecretDigestIsSaltedBySource`.

4. **`SecretEnv` puts a path where a tool expects a value (design decision 3).** §12's example is `SecretEnv("NPM_TOKEN", "NPMToken")` on a `pnpm install` step, and `NPM_TOKEN` will hold a file path, so the step has to read it (`$(cat "$NPM_TOKEN")`). §1.4's "path in `SENRO_SECRET_<NAME>`" and §12's own comment "tmpfs file + env pointing at it" both say this is intended, and putting a value in the environment block is the one thing this plan refuses outright. If a value-in-the-environment escape hatch is ever wanted, it needs a separately named method whose doc states the `/proc/<pid>/environ` exposure, not a quiet change to this one.
