package database

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

const snapshotMagic = "KVSNAP01"

type snapshotEntry struct {
	key   string
	value []byte
}

type snapshotImage [][]snapshotEntry

// SaveSnapshot publishes a complete image of the supplied logical databases.
// The databases must be in ID order and fully resident. Capture acquires the
// execution gate, so the caller must enter without holding it.
func SaveSnapshot(dir string, dbs []*Database) error {
	return saveSnapshot(dir, dbs, false)
}

func saveSnapshot(dir string, dbs []*Database, final bool) error {
	if err := validateSnapshotDatabases(dbs); err != nil {
		return err
	}
	if err := ensureSnapshotDirectory(dir); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	lock, err := openSnapshotLock(dir)
	if err != nil {
		return err
	}
	defer lock.Close()

	generations, err := snapshotGenerations(dir)
	if err != nil {
		return err
	}
	generation := uint64(1)
	if len(generations) != 0 {
		latest := generations[len(generations)-1]
		if latest == math.MaxUint64 {
			return fmt.Errorf("snapshot generation counter exhausted")
		}
		generation = latest + 1
	}

	image, err := captureSnapshot(dbs, final)
	if err != nil {
		return err
	}
	if err := publishSnapshot(dir, generation, image); err != nil {
		return err
	}

	// Keep the immediately preceding generation as well as the new one.
	var cleanupErr error
	for _, older := range generations[:max(0, len(generations)-1)] {
		path := snapshotPath(dir, older)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove old snapshot: %w", err))
		}
	}
	return cleanupErr
}

// LoadSnapshot restores the newest committed generation into empty, standalone
// Go databases. An absent snapshot is an empty startup. A corrupt latest
// generation fails startup, since loading an older one can resurrect deletes.
func LoadSnapshot(dir string, dbs []*Database) error {
	return loadSnapshotGeneration(dir, dbs, nil)
}

// Managers pin the generation selected under the persistence root lock.
// Recheck under the snapshot lock before restoring or treating a root as empty.
func loadSnapshotGeneration(dir string, dbs []*Database, expected *uint64) error {
	if err := validateSnapshotDatabases(dbs); err != nil {
		return err
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if expected != nil && *expected != 0 {
			return fmt.Errorf("required Go snapshot generation %d is unavailable in %s", *expected, dir)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	lock, err := openSnapshotLock(dir)
	if err != nil {
		return err
	}
	defer lock.Close()

	generations, err := snapshotGenerations(dir)
	if err != nil {
		return err
	}
	var latest uint64
	if len(generations) > 0 {
		latest = generations[len(generations)-1]
	}
	if expected != nil && latest != *expected {
		return fmt.Errorf("expected Go snapshot generation %d in %s, found %d", *expected, dir, latest)
	}
	if latest == 0 {
		return nil
	}
	path := snapshotPath(dir, latest)
	if err := restoreSnapshot(path, dbs); err != nil {
		return fmt.Errorf("restore snapshot %q: %w", path, err)
	}
	return nil
}

func validateSnapshotDatabases(dbs []*Database) error {
	for i, db := range dbs {
		if db == nil || db.id != i {
			return fmt.Errorf("snapshot requires database ID %d in position %d", i, i)
		}
	}
	return nil
}

func lockSnapshotDatabases(dbs []*Database) func() {
	for _, db := range dbs {
		db.operationMu.Lock()
	}
	return func() {
		for i := len(dbs) - 1; i >= 0; i-- {
			dbs[i].operationMu.Unlock()
		}
	}
}

func captureSnapshot(dbs []*Database, final bool) (snapshotImage, error) {
	LockExecution()
	defer UnlockExecution()
	unlock := lockSnapshotDatabases(dbs)
	defer unlock()

	image, err := captureSnapshotLocked(dbs)
	if final {
		// Holding every operation lock also covers direct Go writes that
		// bypass the server execution gate. No write can enter between
		// capturing the image and closing the database handles.
		for _, db := range dbs {
			if closeErr := db.closeLocked(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close database %d: %w", db.id, closeErr))
			}
		}
	}
	return image, err
}

func captureSnapshotLocked(dbs []*Database) (snapshotImage, error) {
	now := time.Now().UnixMilli()
	image := make(snapshotImage, len(dbs))
	for i, db := range dbs {
		if db.closed {
			return nil, fmt.Errorf("capture database %d: %w", i, errDatabaseClosed)
		}
		if db.rustEngine != nil {
			return nil, fmt.Errorf("capture database %d: snapshots require a resident Go database", i)
		}
		for shardIndex := range db.shards {
			for key, value := range db.shards[shardIndex].data {
				if value.ExpireTime != 0 && now > value.ExpireTime {
					continue
				}
				encoded, err := value.Serialize()
				if err != nil {
					return nil, fmt.Errorf("encode database %d key %q: %w", i, key, err)
				}
				image[i] = append(image[i], snapshotEntry{key: key, value: encoded})
			}
		}
	}
	return image, nil
}

func ensureSnapshotDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%q is not a directory", dir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir || dir == "" {
		return err
	}
	if err := ensureSnapshotDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return syncSnapshotDirectory(parent)
}

func openSnapshotLock(dir string) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(dir, "snapshot.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open snapshot lock: %w", err)
	}
	if err := lockSnapshotFile(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("acquire snapshot lock: %w", err)
	}
	return file, nil
}

func snapshotPath(dir string, generation uint64) string {
	return filepath.Join(dir, fmt.Sprintf("snapshot-%020d.bin", generation))
}

func snapshotGenerations(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list snapshot generations: %w", err)
	}
	var generations []uint64
	for _, entry := range entries {
		digits, ok := strings.CutPrefix(entry.Name(), "snapshot-")
		if !ok {
			continue
		}
		digits, ok = strings.CutSuffix(digits, ".bin")
		if !ok || len(digits) != 20 {
			continue
		}
		generation, err := strconv.ParseUint(digits, 10, 64)
		if err == nil && generation != 0 {
			generations = append(generations, generation)
		}
	}
	slices.Sort(generations)
	return generations, nil
}

func publishSnapshot(dir string, generation uint64, image snapshotImage) error {
	file, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}
	defer os.Remove(file.Name())

	buffer := bufio.NewWriterSize(file, 128*1024)
	digest := sha256.New()
	writeErr := writeSnapshot(io.MultiWriter(buffer, digest), image)
	if writeErr == nil {
		_, writeErr = buffer.Write(digest.Sum(nil))
	}
	if writeErr == nil {
		writeErr = buffer.Flush()
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := publishSnapshotFile(file.Name(), snapshotPath(dir, generation)); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	if err := syncSnapshotDirectory(dir); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}

func writeSnapshot(w io.Writer, image snapshotImage) error {
	if _, err := io.WriteString(w, snapshotMagic); err != nil {
		return err
	}
	var header [16]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(len(image)))
	if _, err := w.Write(header[:8]); err != nil {
		return err
	}
	for _, entries := range image {
		binary.LittleEndian.PutUint64(header[:8], uint64(len(entries)))
		if _, err := w.Write(header[:8]); err != nil {
			return err
		}
		for _, entry := range entries {
			binary.LittleEndian.PutUint64(header[:8], uint64(len(entry.key)))
			binary.LittleEndian.PutUint64(header[8:], uint64(len(entry.value)))
			if _, err := w.Write(header[:]); err != nil {
				return err
			}
			if _, err := io.WriteString(w, entry.key); err != nil {
				return err
			}
			if _, err := w.Write(entry.value); err != nil {
				return err
			}
		}
	}
	return nil
}

type restoredSnapshotDB struct {
	shards [ShardCount]map[string]*datastruct.DataValue
	bytes  int64
}

func restoreSnapshot(path string, dbs []*Database) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < int64(len(snapshotMagic)+8+sha256.Size) {
		return fmt.Errorf("invalid snapshot file")
	}

	buffer := bufio.NewReaderSize(file, 128*1024)
	digest := sha256.New()
	body := &io.LimitedReader{R: io.TeeReader(buffer, digest), N: info.Size() - sha256.Size}
	staged, err := readSnapshot(body, dbs)
	if err != nil {
		return err
	}
	if body.N != 0 {
		return fmt.Errorf("unexpected trailing snapshot data")
	}
	var checksum [sha256.Size]byte
	if _, err := io.ReadFull(buffer, checksum[:]); err != nil {
		return err
	}
	if !bytes.Equal(checksum[:], digest.Sum(nil)) {
		return fmt.Errorf("snapshot checksum mismatch")
	}

	unlock := lockSnapshotDatabases(dbs)
	defer unlock()
	for i, db := range dbs {
		if db.closed || db.hasPersistence() || atomic.LoadInt64(&db.usedMemory) != 0 {
			return fmt.Errorf("restore requires a fresh, standalone Go database at ID %d", i)
		}
	}
	for i, db := range dbs {
		for j, shard := range staged[i].shards {
			db.shards[j].data = shard
		}
		atomic.StoreInt64(&db.usedMemory, staged[i].bytes)
		db.ResetVersionState()
	}
	return nil
}

func readSnapshot(body *io.LimitedReader, dbs []*Database) ([]restoredSnapshotDB, error) {
	var header [16]byte
	if _, err := io.ReadFull(body, header[:]); err != nil {
		return nil, err
	}
	if string(header[:8]) != snapshotMagic {
		return nil, fmt.Errorf("invalid snapshot format")
	}
	if count := binary.LittleEndian.Uint64(header[8:]); count != uint64(len(dbs)) {
		return nil, fmt.Errorf("snapshot contains %d logical databases, configured %d", count, len(dbs))
	}

	now := time.Now().UnixMilli()
	staged := make([]restoredSnapshotDB, len(dbs))
	for i, db := range dbs {
		if _, err := io.ReadFull(body, header[:8]); err != nil {
			return nil, err
		}
		count := binary.LittleEndian.Uint64(header[:8])
		for j := uint64(0); j < count; j++ {
			if err := readSnapshotEntry(body, &staged[i], now, db.maxMemory); err != nil {
				return nil, fmt.Errorf("decode database %d entry %d: %w", i, j, err)
			}
		}
	}
	return staged, nil
}

func readSnapshotEntry(body *io.LimitedReader, db *restoredSnapshotDB, now, maxMemory int64) error {
	var header [16]byte
	if _, err := io.ReadFull(body, header[:]); err != nil {
		return err
	}
	keyLen := binary.LittleEndian.Uint64(header[:8])
	valueLen := binary.LittleEndian.Uint64(header[8:])
	remaining := uint64(body.N)
	if keyLen > math.MaxInt || valueLen > math.MaxInt || keyLen > remaining || valueLen == 0 || valueLen > remaining-keyLen {
		return fmt.Errorf("invalid snapshot entry length")
	}
	keyBytes := make([]byte, int(keyLen))
	if _, err := io.ReadFull(body, keyBytes); err != nil {
		return err
	}
	encoded := make([]byte, int(valueLen))
	if _, err := io.ReadFull(body, encoded); err != nil {
		return err
	}
	value, err := deserializeDataValue(encoded)
	if err != nil {
		return err
	}
	if value.ExpireTime != 0 && now > value.ExpireTime {
		return nil
	}
	key := string(keyBytes)
	i := getShardIndex(key)
	shard := db.shards[i]
	if _, exists := shard[key]; exists {
		return fmt.Errorf("duplicate snapshot key %q", key)
	}
	size := int64(len(key)) + value.ApproximateSize()
	if size > maxMemory-db.bytes {
		return fmt.Errorf("restored database exceeds maxmemory")
	}
	if shard == nil {
		shard = make(map[string]*datastruct.DataValue)
		db.shards[i] = shard
	}
	shard[key] = value
	db.bytes += size
	return nil
}
