package golog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// LockWriterDirectory acquires the exclusive writer lock of an existing Go
// WAL directory. The caller must close the returned file to release the lock.
func LockWriterDirectory(directory string) (*os.File, error) {
	lock, err := os.OpenFile(filepath.Join(directory, writerLock), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Go-log writer lock: %w", err)
	}
	if err := lockWriter(lock); err != nil {
		return nil, errors.Join(fmt.Errorf("lock Go-log directory %s: %w", directory, err), lock.Close())
	}
	return lock, nil
}
