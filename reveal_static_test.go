package senro_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRevealAppearsInExactlyOnePlaceInProductionCode mechanises the audit
// this codebase relies on: Reveal() appears in exactly one production file,
// internal/secrets/reveal.go, so "where could a value leak" is one grep, run
// in CI rather than remembered.
//
// Exclusions: _test.go files (a test proving the argv guard has to CONSTRUCT
// the mistake), and the api module (a separate module with no secrets). The
// check matches the bare identifier rather than ".Reveal(", so a call made
// through reflect.Value.MethodByName("Reveal") is caught too.
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

	// The canary: a permitted file that stopped containing the seam must not
	// read as "zero offenders".
	if !sawAllowed {
		t.Fatalf("%s does not mention Reveal; the seam moved or was deleted, and this "+
			"check would now pass over any tree at all", allowed)
	}
	if len(offenders) > 0 {
		t.Errorf("Reveal appears outside %s, in: %s\n\n"+
			"Revealing a secret's value must happen in exactly one place in the codebase, so "+
			"the audit for where a value could leak is one grep. Route the value through "+
			"internal/secrets instead of reading it here.",
			allowed, strings.Join(offenders, ", "))
	}
}

// TestSetValueCallSitesAreAllowlisted mechanises the second audit boundary:
// Reveal (above) audits the seam INTO internal/secrets, and Set.Value is the
// one accessor a value leaves it through again, with three allowlisted
// callers.
//
// Scoped to the literal "secrets.Value(" rather than the bare "Value(",
// which would match context.Value, reflect.Value and every other unrelated
// call: "secrets.Value(" is the exact shape every current caller uses, so a
// match outside the allowlist is worth a human reading.
func TestSetValueCallSitesAreAllowlisted(t *testing.T) {
	// run.go is the fourth: buildExecutors reads the ONE credential a target
	// itself declares (container.RegistryAuth) and hands it straight to the
	// executor's constructor, keeping no copy of its own. The executor holds
	// it only to send one X-Registry-Auth header on one pull.
	allowed := map[string]bool{
		"internal/engine/attempt.go": true,
		"internal/engine/handler.go": true,
		"internal/engine/cache.go":   true,
		"run.go":                     true,
	}
	root := moduleRoot(t)

	var offenders []string
	seen := make(map[string]bool, len(allowed))
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
		if !strings.Contains(string(b), "secrets.Value(") {
			return nil
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if allowed[rel] {
			seen[rel] = true
			return nil
		}
		offenders = append(offenders, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	// The canary, same shape as sawAllowed above: a seam that moved or was
	// deleted must not read as "zero offenders".
	if len(seen) != len(allowed) {
		t.Fatalf("saw %d of %d allowlisted call sites (%v); the allowlist is stale or the "+
			"seam moved", len(seen), len(allowed), seen)
	}
	if len(offenders) > 0 {
		t.Errorf("secrets.Value( appears outside the allowlist, in: %s\n\n"+
			"Set.Value is the second seam a plaintext credential leaves internal/secrets "+
			"through (the first is Reveal, see the test above). Add the new call site to "+
			"this allowlist only after confirming it keeps no copy of the value.",
			strings.Join(offenders, ", "))
	}
}
