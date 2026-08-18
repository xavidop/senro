package persist

import (
	"context"
	"fmt"
	"os"
)

// fileLocker is the exclusion for a workspace the coordinator owns: an
// advisory lock on a file beside the tree. Complete because nothing off
// this machine can reach the tree, free, and released by the kernel the
// moment the process dies. It stops being complete when the tree stops
// being local, which is why Locker exists; see kubelock.
type fileLocker struct{ store *Store }

func (fileLocker) Kind() string { return "a file lock on this machine" }

func (f *fileLocker) TryAcquire(_ context.Context, name, runID string) (Unlocker, error) {
	s := f.store
	// Created here, not assumed: a Locker handed out through StoreLocker is
	// called with no Acquire in front of it, and a lock file that cannot be
	// created reads exactly like "somebody else holds this".
	if err := os.MkdirAll(s.slot(name), 0o755); err != nil {
		return nil, fmt.Errorf("persist: prepare workspace %q: %w", name, err)
	}
	fh, err := os.OpenFile(s.lockPath(name), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("persist: lock workspace %q: %w", name, err)
	}
	locked, err := tryLock(fh)
	if err != nil {
		_ = fh.Close()
		return nil, fmt.Errorf("persist: lock workspace %q: %w", name, err)
	}
	if !locked {
		_ = fh.Close()
		return nil, s.heldBy(name)
	}
	// The holder record is written by Acquire, not here.
	_ = runID
	return &fileUnlocker{f: fh}, nil
}

type fileUnlocker struct{ f *os.File }

func (u *fileUnlocker) Release(context.Context) error {
	if u.f == nil {
		return nil
	}
	// Unlock explicitly rather than relying on the close: another run is
	// waiting on the release, which should not depend on a side effect.
	err := unlock(u.f)
	cerr := u.f.Close()
	u.f = nil
	if err != nil {
		return err
	}
	return cerr
}
