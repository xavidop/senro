package persist

import "time"

// The two things a test must be able to fake: an aged-out workspace and an
// oversize one, both otherwise reachable only by waiting or killing a
// process mid-run. In export_test.go so nothing outside the tests can
// rewrite a workspace's recorded age.

// SetLastUsedForTest backdates a workspace's recorded last use.
func (s *Store) SetLastUsedForTest(name string, when time.Time) error {
	m, _, err := s.readMeta(name)
	if err != nil {
		return err
	}
	m.LastUsed = when.UTC()
	return s.writeMeta(name, m)
}

// SetRecordedBytesForTest rewrites the size a workspace's last release
// recorded, which is what the next acquisition enforces MaxSize against.
func (s *Store) SetRecordedBytesForTest(name string, n int64) error {
	m, _, err := s.readMeta(name)
	if err != nil {
		return err
	}
	if m.LastUsed.IsZero() {
		// "Never released" makes an acquisition skip both bound checks; a
		// test setting a size wants the size checked.
		m.LastUsed = time.Now().UTC()
	}
	m.Bytes = n
	return s.writeMeta(name, m)
}
