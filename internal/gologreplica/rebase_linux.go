//go:build linux

package gologreplica

import (
	"math"
	"os"
	"path/filepath"
	"time"
)

const (
	minimumRebaseBytes = 64 << 20
	rebaseRetryDelay   = 30 * time.Second
)

func (db *replicaDB) considerSnapshot(current *view, hint *snapshotHint) {
	if hint == nil {
		db.evaluatedSnapshot = 0
		db.requestedSnapshot = 0
		return
	}
	if current == nil || hint.Sequence > current.Sequence || hint.Sequence == db.evaluatedSnapshot {
		return
	}
	db.evaluatedSnapshot = hint.Sequence
	db.requestedSnapshot = 0
	if hint.Sequence <= current.Snapshot || hint.Size > math.MaxInt64/2 {
		return
	}
	// Only count segments that the new generation can actually drop. An
	// overlapping segment is still needed to replay records after the cutoff.
	directory := filepath.Join(db.directory, current.Generation)
	segments, err := listSegments(directory, current.Snapshot)
	if err != nil {
		return
	}
	remaining := max(db.minimumRebase, 2*hint.Size)
	for _, segment := range segments {
		if segment.last > hint.Sequence {
			continue
		}
		info, err := os.Stat(filepath.Join(directory, segment.name))
		if err != nil {
			return
		}
		if info.Size() >= remaining {
			db.requestedSnapshot = hint.Sequence
			return
		}
		remaining -= info.Size()
	}
}
