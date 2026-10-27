//go:build !unix

package localexec

import (
	"os"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// processUsage reports nothing off unix: there is no rusage to read.
func processUsage(*os.ProcessState) (senroexec.ResourceUsage, bool) {
	return senroexec.ResourceUsage{}, false
}
