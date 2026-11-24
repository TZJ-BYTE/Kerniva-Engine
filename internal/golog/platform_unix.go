//go:build !windows

package golog

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockWriter(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
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
