package golog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SetMaintenanceAllowed serializes configuration changes with the Rust worker's
// entire scan, publication and reclamation pass. Both processes lock the same
// permanent inode. An empty policy is the legacy default (maintenance allowed).
func SetMaintenanceAllowed(directory string, allowed bool) (err error) {
	if err := makeDirectoryDurable(directory); err != nil {
		return err
	}
	path := filepath.Join(directory, ".snapshot-maintenance")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if err := lockMaintenance(file); err != nil {
		return fmt.Errorf("lock snapshot maintenance policy: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	switch info.Size() {
	case 0:
		if allowed {
			return nil
		}
	case 1:
		var current [1]byte
		if _, err := file.ReadAt(current[:], 0); err != nil {
			return err
		}
		if current[0] != '0' && current[0] != '1' {
			return fmt.Errorf("invalid snapshot maintenance policy in %s", path)
		}
		if allowed == (current[0] == '1') {
			// A previous attempt may have written the byte but failed to sync.
			return errors.Join(file.Sync(), syncDirectory(directory))
		}
	default:
		return fmt.Errorf("invalid snapshot maintenance policy in %s", path)
	}
	value := byte('0')
	if allowed {
		value = '1'
	}
	if _, err := file.WriteAt([]byte{value}, 0); err != nil {
		return err
	}
	return errors.Join(file.Sync(), syncDirectory(directory))
}
