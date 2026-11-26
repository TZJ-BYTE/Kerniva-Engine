//go:build !windows

package database

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockSnapshotFile(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func syncSnapshotDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func publishSnapshotFile(source, target string) error {
	return os.Rename(source, target)
}

func replaceSnapshotFile(source, target string) error {
	return os.Rename(source, target)
}

func tryLockSnapshotFile(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}
