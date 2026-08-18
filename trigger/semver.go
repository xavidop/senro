package trigger

import (
	"strconv"
	"strings"
)

// version is a parsed semantic version. Build metadata is parsed and then
// discarded: semver 2.0.0 says it takes no part in ordering, and keeping a
// field nothing compares would invite a comparison that should not exist.
type version struct {
	major, minor, patch uint64
	// pre are the dot-separated prerelease identifiers, empty for a release.
	pre []string
}

// parseVersion parses a tag as a semantic version, reporting whether it is
// one at all. The one concession to how tags are written is a leading "v" or
// "V"; everything else is semver 2.0.0. Not-a-version is false, never a zero
// version: "release-2024" read as 0.0.0 would satisfy ">=0.0.0" and get
// deployed.
func parseVersion(s string) (version, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return version{}, false
	}
	if s[0] == 'v' || s[0] == 'V' {
		s = s[1:]
	}
	// Build metadata first: it may itself contain "-", so splitting it off
	// before the prerelease is the only order that does not misread
	// "1.0.0+exp-1" as a prerelease.
	if i := strings.IndexByte(s, '+'); i >= 0 {
		build := s[i+1:]
		if !validDotSeparated(build, false) {
			return version{}, false
		}
		s = s[:i]
	}
	var pre []string
	if i := strings.IndexByte(s, '-'); i >= 0 {
		p := s[i+1:]
		if !validDotSeparated(p, true) {
			return version{}, false
		}
		pre = strings.Split(p, ".")
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return version{}, false
	}
	nums := make([]uint64, 3)
	for i, p := range parts {
		n, ok := parseNumericID(p)
		if !ok {
			return version{}, false
		}
		nums[i] = n
	}
	return version{major: nums[0], minor: nums[1], patch: nums[2], pre: pre}, true
}

// parseNumericID parses one numeric identifier: digits only, no leading zero
// unless exactly "0", as semver requires.
func parseNumericID(s string) (uint64, bool) {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// validDotSeparated checks a prerelease or build string: dot-separated
// identifiers of [0-9A-Za-z-], none empty. A prerelease identifier that is
// all digits additionally may not have a leading zero, which build metadata
// may, because build metadata is never compared numerically.
func validDotSeparated(s string, prerelease bool) bool {
	if s == "" {
		return false
	}
	for _, id := range strings.Split(s, ".") {
		if id == "" {
			return false
		}
		numeric := true
		for i := 0; i < len(id); i++ {
			c := id[i]
			switch {
			case c >= '0' && c <= '9':
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '-':
				numeric = false
			default:
				return false
			}
		}
		if prerelease && numeric && len(id) > 1 && id[0] == '0' {
			return false
		}
	}
	return true
}

// compareVersions orders two versions the way semver 2.0.0 says: numerically
// by major, minor and patch, and then a version WITH a prerelease below the
// same version without one.
func compareVersions(a, b version) int {
	if c := cmpUint(a.major, b.major); c != 0 {
		return c
	}
	if c := cmpUint(a.minor, b.minor); c != 0 {
		return c
	}
	if c := cmpUint(a.patch, b.patch); c != 0 {
		return c
	}
	return comparePre(a.pre, b.pre)
}

// comparePre orders prerelease identifier lists. A release (no identifiers)
// is above any prerelease of the same version, which is the rule that makes
// Semver(">=1.0.0") reject 1.0.0-rc.1.
func comparePre(a, b []string) int {
	switch {
	case len(a) == 0 && len(b) == 0:
		return 0
	case len(a) == 0:
		return 1
	case len(b) == 0:
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := comparePreID(a[i], b[i]); c != 0 {
			return c
		}
	}
	// All the identifiers they share are equal, so the longer list is
	// larger: 1.0.0-rc.1 is below 1.0.0-rc.1.1.
	return cmpInt(len(a), len(b))
}

// comparePreID orders one pair of prerelease identifiers: numeric ones
// numerically and below any alphanumeric one, alphanumeric ones by ASCII.
func comparePreID(a, b string) int {
	an, aNum := parseNumericID(a)
	bn, bNum := parseNumericID(b)
	switch {
	case aNum && bNum:
		return cmpUint(an, bn)
	case aNum:
		return -1
	case bNum:
		return 1
	default:
		return strings.Compare(a, b)
	}
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
