package kubelock

import (
	"crypto/sha256"
	"encoding/hex"
)

// shortHash is 32 hex characters of sha256, which with the "senro-ws-"
// prefix stays inside both the 253-byte DNS subdomain limit and the 63-byte
// label limit. Hashed rather than sanitized; LeaseName's comment says why.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}
