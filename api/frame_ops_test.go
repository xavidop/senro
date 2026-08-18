package api

import (
	"os"
	"regexp"
	"testing"
)

// declaredOps mirrors the Op* const block, and an unchecked mirror drifts.
// This test reads frame.go's source and extracts every Op* constant, making
// the const block the single source of truth: a new op added to the block
// and not the map fails here, at the moment it is added.
func TestDeclaredOpsMatchesTheConstants(t *testing.T) {
	src, err := os.ReadFile("frame.go")
	if err != nil {
		t.Fatalf("reading frame.go: %v", err)
	}

	// Matches `OpRunCancel = "run.cancel"` in the const block, and not the
	// map entries below it, which are `OpRunCancel: true`.
	re := regexp.MustCompile(`(?m)^\s*(Op[A-Za-z]+)\s*=\s*"([^"]+)"`)
	matches := re.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("found no Op* constants in frame.go: this test's pattern has stopped matching " +
			"the const block, which makes it a test that always passes")
	}

	inSource := make(map[string]string, len(matches))
	for _, m := range matches {
		inSource[m[2]] = m[1]
	}

	for value, name := range inSource {
		if !declaredOps[value] {
			t.Errorf("frame.go declares %s = %q and declaredOps does not carry it: "+
				"add it, and rule on it in internal/webui's controllableOps", name, value)
		}
	}
	for value := range declaredOps {
		if _, ok := inSource[value]; !ok {
			t.Errorf("declaredOps carries %q, which no Op* constant in frame.go declares", value)
		}
	}

	if got, want := len(DeclaredOps()), len(inSource); got != want {
		t.Errorf("DeclaredOps() returned %d ops, frame.go declares %d", got, want)
	}
}

// DeclaredOps is sorted, so callers rendering it get a stable order rather
// than Go's randomised map iteration.
func TestDeclaredOpsIsSorted(t *testing.T) {
	ops := DeclaredOps()
	for i := 1; i < len(ops); i++ {
		if ops[i-1] >= ops[i] {
			t.Fatalf("DeclaredOps() is not sorted: %q before %q", ops[i-1], ops[i])
		}
	}
}

func TestOpDeclared(t *testing.T) {
	if !OpDeclared(OpRunCancel) {
		t.Errorf("OpDeclared(%q) = false", OpRunCancel)
	}
	if OpDeclared("run.definitely-not-an-op") {
		t.Error("OpDeclared reported an undeclared op as declared")
	}
}

// ws.snapshot is one string naming two things: the control operation and
// the event it causes. That is deliberate (see OpWSSnapshot), and it is
// only safe because the two vocabularies never share a channel: a Frame
// carries an op in Type, GET /api/stream carries an event in its own Type.
// Pinned here so a later rename of either cannot quietly split the pair, or
// quietly collide a DIFFERENT op with an event name.
func TestWSSnapshotIsBothADeclaredOpAndADeclaredEventType(t *testing.T) {
	if OpWSSnapshot != string(WSSnapshot) {
		t.Errorf("OpWSSnapshot = %q and WSSnapshot = %q: the operation is named after the event it "+
			"causes, so the two must stay one string", OpWSSnapshot, WSSnapshot)
	}
	if !OpDeclared(OpWSSnapshot) || !declaredTypes[WSSnapshot] {
		t.Errorf("ws.snapshot is declared as an op (%t) and as an event type (%t): it must be both",
			OpDeclared(OpWSSnapshot), declaredTypes[WSSnapshot])
	}
	for _, op := range DeclaredOps() {
		if op != OpWSSnapshot && declaredTypes[Type(op)] {
			t.Errorf("control op %q is also a declared event type, and only ws.snapshot is meant to "+
				"be: an op named after an event it does not cause reads as a mistake", op)
		}
	}
}
