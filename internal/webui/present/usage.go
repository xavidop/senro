package present

import (
	"fmt"
	"time"

	"github.com/xavidop/senro/api"
)

// FormatCPU is a usage's user plus system CPU time rounded to 100 ms, as
// "1.2s". It is in this package, which the WebAssembly client compiles,
// because the browser and the terminal must word a step's cost the same way.
func FormatCPU(u *api.ResourceUsage) string {
	if u == nil {
		return ""
	}
	return (u.CPUUser + u.CPUSystem).Round(100 * time.Millisecond).String()
}

// FormatRSS is a usage's peak resident memory as "640 KB", "85 MB" or
// "1.2 GB" (binary units, whole numbers below a gigabyte).
func FormatRSS(u *api.ResourceUsage) string {
	if u == nil {
		return ""
	}
	const kb, mb, gb = 1 << 10, 1 << 20, 1 << 30
	n := u.MaxRSS
	switch {
	case n >= gb:
		return fmt.Sprintf("%.1f GB", float64(n)/gb)
	case n >= mb:
		return fmt.Sprintf("%d MB", (n+mb/2)/mb)
	case n >= kb:
		return fmt.Sprintf("%d KB", (n+kb/2)/kb)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// FormatUsage is "cpu 1.2s  rss 85 MB", or "" when the executor could not
// measure the step.
func FormatUsage(u *api.ResourceUsage) string {
	if u == nil {
		return ""
	}
	return "cpu " + FormatCPU(u) + "  rss " + FormatRSS(u)
}
