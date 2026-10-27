package present

import (
	"fmt"
	"time"

	"github.com/xavidop/senro/api"
)

// FormatCPU is a usage's user plus system CPU time as seconds with one
// decimal ("0.3s", "75.4s", "3725.0s"): one unit at every magnitude, so a
// column of steps reads in a single scale. It is in this package, which the
// WebAssembly client compiles, because the browser and the terminal must
// word a step's cost the same way.
func FormatCPU(u *api.ResourceUsage) string {
	if u == nil {
		return ""
	}
	d := (u.CPUUser + u.CPUSystem).Round(100 * time.Millisecond)
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// FormatRSS is a usage's peak resident memory as "640 KB", "85 MB" or
// "1.2 GB" (binary units, whole numbers below a gigabyte). The unit is the
// largest one whose rounded mantissa stays below 1024, so 1023.6 MB reads
// "1.0 GB", never "1024 MB".
func FormatRSS(u *api.ResourceUsage) string {
	if u == nil {
		return ""
	}
	const kb, mb, gb = 1 << 10, 1 << 20, 1 << 30
	n := u.MaxRSS
	switch {
	case n >= gb-mb/2:
		return fmt.Sprintf("%.1f GB", float64(n)/gb)
	case n >= mb-kb/2:
		return fmt.Sprintf("%d MB", (n+mb/2)/mb)
	case n >= kb:
		return fmt.Sprintf("%d KB", (n+kb/2)/kb)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// FormatUsage is "cpu 1.2s  rss 85 MB", or "" when the executor could not
// measure the step. A zero MaxRSS means memory was not measured, so the rss
// part is left out rather than printed as "0 B".
func FormatUsage(u *api.ResourceUsage) string {
	if u == nil {
		return ""
	}
	out := "cpu " + FormatCPU(u)
	if u.MaxRSS > 0 {
		out += "  rss " + FormatRSS(u)
	}
	return out
}
