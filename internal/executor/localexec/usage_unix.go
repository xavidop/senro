//go:build unix

package localexec

import (
	"os"
	"runtime"
	"syscall"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// processUsage reads the rusage wait4 returned for a finished process. It
// covers the process and every descendant it waited for, which for a shell
// running a pipeline is the whole pipeline.
//
// Maxrss is kilobytes on Linux and the BSDs and bytes on Darwin; it is
// normalised to bytes here.
func processUsage(ps *os.ProcessState) (senroexec.ResourceUsage, bool) {
	if ps == nil {
		return senroexec.ResourceUsage{}, false
	}
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil {
		return senroexec.ResourceUsage{}, false
	}
	rss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" && runtime.GOOS != "ios" {
		rss *= 1024
	}
	return senroexec.ResourceUsage{
		CPUUser:   time.Duration(ru.Utime.Nano()),
		CPUSystem: time.Duration(ru.Stime.Nano()),
		MaxRSS:    rss,
	}, true
}
