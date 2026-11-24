//go:build linux

// Package gologreplica follows a single Go-log source using immutable files.
package gologreplica

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	maxManifestBytes     = 1 << 20
	maxBatchFiles        = 128
	batchTargetBytes     = 64 << 20
	sealedLogHeaderBytes = 8
)

type fileEntry struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type snapshotHint struct {
	Sequence uint64 `json:"sequence"`
	Size     int64  `json:"size"`
}

type manifest struct {
	Available *snapshotHint `json:"available_snapshot,omitempty"`
	Rebase    bool          `json:"rebase,omitempty"`
	Version   int           `json:"version"`
	Source    string        `json:"source"`
	DB        int           `json:"db"`
	After     uint64        `json:"after"`
	Last      uint64        `json:"last"`
	Snapshot  uint64        `json:"snapshot,omitempty"`
	Files     []fileEntry   `json:"files,omitempty"`
}

func (m manifest) validate(db int, after uint64) error {
	if m.Version != 1 || !validSource(m.Source) || m.DB != db || m.After != after ||
		m.Last < after || len(m.Files) > maxBatchFiles {
		return errors.New("invalid replication manifest")
	}
	if m.Available != nil && (m.Available.Sequence == 0 || m.Available.Size < 28 ||
		m.Snapshot != 0 && m.Snapshot != m.Available.Sequence) {
		return errors.New("invalid available snapshot")
	}
	if m.Rebase {
		if m.Available == nil || m.Snapshot == 0 || m.Snapshot > after || m.Last != after ||
			len(m.Files) != 1 || m.Files[0].Name != snapshotName(m.Snapshot) ||
			m.Files[0].Size != m.Available.Size {
			return errors.New("invalid replica rebase")
		}
		return nil
	}
	sequence := after
	start := 0
	if m.Snapshot != 0 {
		if m.Snapshot <= after || len(m.Files) == 0 || m.Files[0].Name != snapshotName(m.Snapshot) ||
			m.Files[0].Size < 28 {
			return errors.New("invalid replication snapshot")
		}
		sequence = m.Snapshot
		start++
	}
	for _, entry := range m.Files[start:] {
		first, last, err := segmentRange(entry.Name)
		overlapsSnapshot := start == 1 && sequence == m.Snapshot && first <= sequence
		if err != nil || sequence == ^uint64(0) || last <= sequence ||
			(first != sequence+1 && !overlapsSnapshot) || entry.Size <= sealedLogHeaderBytes {
			return fmt.Errorf("invalid replication segment %q after %d", entry.Name, sequence)
		}
		sequence = last
	}
	if sequence != m.Last {
		return errors.New("replication manifest does not reach reported sequence")
	}
	return nil
}

func readManifest(reader io.Reader) (manifest, error) {
	var m manifest
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return m, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxManifestBytes {
		return m, errors.New("invalid replication manifest size")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return m, err
	}
	err := json.Unmarshal(data, &m)
	return m, err
}

func validSource(id string) bool {
	if len(id) != 32 || strings.ToLower(id) != id {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func dbName(db int) string {
	return "db_" + strconv.Itoa(db)
}

func parseDB(raw string) (int, error) {
	db, err := strconv.Atoi(raw)
	if err != nil || db < 0 || strconv.Itoa(db) != raw {
		return 0, errors.New("invalid database number")
	}
	return db, nil
}

func snapshotName(sequence uint64) string {
	return fmt.Sprintf("snapshot-%020d.bin", sequence)
}

func segmentRange(name string) (uint64, uint64, error) {
	value := strings.TrimSuffix(strings.TrimPrefix(name, "log-"), ".seg")
	first, last, ok := strings.Cut(value, "-")
	a, aErr := strconv.ParseUint(first, 10, 64)
	b, bErr := strconv.ParseUint(last, 10, 64)
	if !ok || aErr != nil || bErr != nil || a == 0 || a > b ||
		name != fmt.Sprintf("log-%020d-%020d.seg", a, b) {
		return 0, 0, fmt.Errorf("invalid sealed log name %q", name)
	}
	return a, b, nil
}

func readCutoff(directory string) (uint64, error) {
	file, err := os.Open(filepath.Join(directory, "CURRENT"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	bytes, err := io.ReadAll(io.LimitReader(file, 22))
	if err != nil {
		return 0, err
	}
	text := strings.TrimSuffix(string(bytes), "\n")
	sequence, err := strconv.ParseUint(text, 10, 64)
	if err != nil || sequence == 0 || strconv.FormatUint(sequence, 10) != text {
		return 0, errors.New("invalid snapshot cutoff in CURRENT")
	}
	return sequence, nil
}

func lockFile(ctx context.Context, path string, mode int, wait bool) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			break
		}
		err = unix.Flock(int(file.Fd()), mode|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !wait || (!errors.Is(err, unix.EINTR) && !errors.Is(err, unix.EWOULDBLOCK)) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	file.Close()
	return nil, err
}

func makeDurableDir(directory string) error {
	info, err := os.Stat(directory)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", directory)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(directory)
	if parent == directory {
		return err
	}
	if err := makeDurableDir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return syncDir(parent)
}

func syncDir(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func writeSynced(file *os.File, data []byte) error {
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
