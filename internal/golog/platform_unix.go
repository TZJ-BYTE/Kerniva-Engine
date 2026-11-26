//go:build !windows

package golog

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockWriter(file *os.File) error {
	return flock(file, unix.LOCK_EX|unix.LOCK_NB)
}

func lockMaintenance(file *os.File) error {
	return flock(file, unix.LOCK_EX)
}

func flock(file *os.File, flags int) error {
	for {
		err := unix.Flock(int(file.Fd()), flags)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func publishFile(source, target string) error {
	return os.Rename(source, target)
}
