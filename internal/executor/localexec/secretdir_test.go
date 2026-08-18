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

// TestPutSecretWritesOutsideTheRunDirectory pins where secrets must not
// live: a run directory is what a user tars up and attaches to a bug report.
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
