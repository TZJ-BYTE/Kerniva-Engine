// Package gologstate defines the persistence authority record shared by WAL consumers.
package gologstate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	WAL             = "wal"
	Handoff         = "handoff"
	Snapshot        = "snapshot"
	SnapshotImport  = "snapshot-import"
	WALDir          = "go-log-v1"
	ModeFile        = ".go-persistence-mode-v1"
	SourceIDFile    = ".replica-source-id"
	SourceLeaseFile = ".replica-source.lock"
)

// Read returns an empty phase if the authority record has never been written.
// A zero database count in a WAL phase denotes the original two-field format.
func Read(root string) (phase string, generation uint64, dbCount int, err error) {
	path := filepath.Join(root, ModeFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", 0, 0, nil
	}
	if err != nil {
		return "", 0, 0, fmt.Errorf("stat Go persistence mode: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", 0, 0, fmt.Errorf("invalid Go persistence mode file in %s", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", 0, 0, fmt.Errorf("read Go persistence mode: %w", err)
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 2 && len(fields) != 3 {
		return "", 0, 0, fmt.Errorf("invalid Go persistence mode in %s", path)
	}
	phase = fields[0]
	generation, err = strconv.ParseUint(fields[1], 10, 64)
	wal := phase == WAL && generation == 0 || phase == Handoff && generation != 0
	snapshot := phase == Snapshot || phase == SnapshotImport
	if err != nil || (!wal && !snapshot) {
		return "", 0, 0, fmt.Errorf("invalid Go persistence mode in %s", path)
	}
	if len(fields) == 3 {
		dbCount, err = strconv.Atoi(fields[2])
		if err != nil || dbCount < 0 || wal && dbCount == 0 || snapshot && dbCount != 0 {
			return "", 0, 0, fmt.Errorf("invalid Go persistence WAL database count in %s", path)
		}
	}
	return phase, generation, dbCount, nil
}
