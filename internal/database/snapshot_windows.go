package database

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockSnapshotFile(file *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &overlapped)
}

// MoveFileEx with WRITE_THROUGH provides durable publication on Windows,
// where os.File.Sync does not support directories.
func syncSnapshotDirectory(string) error {
	return nil
}

func publishSnapshotFile(source, target string) error {
	return moveSnapshotFile(source, target, windows.MOVEFILE_WRITE_THROUGH)
}

func replaceSnapshotFile(source, target string) error {
	return moveSnapshotFile(source, target, windows.MOVEFILE_WRITE_THROUGH|windows.MOVEFILE_REPLACE_EXISTING)
}

func moveSnapshotFile(source, target string, flags uint32) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, flags)
}

func tryLockSnapshotFile(file *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
}
