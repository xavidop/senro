package executor_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xavidop/senro/internal/executor"
)

// The exit/error split is the whole basis of retry classification: infra
// failures are retryable without judgement, a non-zero exit is the workload's
// verdict and retrying it deletes information.
func TestIsInfra(t *testing.T) {
	wrapped := fmt.Errorf("dialing host: %w", executor.ErrInfra)
	if !executor.IsInfra(wrapped) {
		t.Error("a wrapped ErrInfra must classify as infrastructure failure")
	}
	if executor.IsInfra(errors.New("go test failed")) {
		t.Error("an ordinary error must not classify as infrastructure failure")
	}
	if executor.IsInfra(nil) {
		t.Error("nil is not a failure")
	}
}

func TestPlatformString(t *testing.T) {
	if got := (executor.Platform{OS: "linux", Arch: "arm64"}).String(); got != "linux/arm64" {
		t.Errorf("String = %q, want linux/arm64", got)
	}
}
