package database

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/gologstate"
)

const (
	goModeWAL             = gologstate.WAL
	goModeHandoff         = gologstate.Handoff
	goModeSnapshot        = gologstate.Snapshot
	goModeImport          = gologstate.SnapshotImport
	goPersistenceModeFile = gologstate.ModeFile
)

// goPersistenceMode serializes manager startups across the two Go persistence
// paths. Each transition publishes its replacement recovery baseline before
// recording the new authority.
type goPersistenceMode struct {
	root             string
	logDir           string
	snapshotDir      string
	lock             *os.File
	useWAL           bool
	needsImport      bool
	walExportDBCount int
	state            string
	generation       uint64
	walDBCount       int
}

func openGoPersistenceMode(dir string, useWAL, useSnapshot bool, dbCount int) (*goPersistenceMode, error) {
	mode, err := lockGoPersistenceRoot(dir, useWAL)
	if err != nil {
		return nil, err
	}
	if err := mode.checkStartup(useSnapshot, dbCount); err != nil {
		return nil, errors.Join(err, mode.close())
	}
	return mode, nil
}

func lockGoPersistenceRoot(dir string, useWAL bool) (*goPersistenceMode, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := ensureSnapshotDirectory(root); err != nil {
		return nil, fmt.Errorf("prepare Go persistence directory: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(root, goPersistenceModeFile+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Go persistence lock: %w", err)
	}
	mode := &goPersistenceMode{
		root:        root,
		logDir:      filepath.Join(root, gologstate.WALDir),
		snapshotDir: filepath.Join(root, "go-snapshot-v1"),
		lock:        lock,
		useWAL:      useWAL,
	}
	if err := tryLockSnapshotFile(lock); err != nil {
		return nil, errors.Join(fmt.Errorf("lock Go persistence directory %s: %w", root, err), lock.Close())
	}
	return mode, nil
}

func (m *goPersistenceMode) checkStartup(useSnapshot bool, dbCount int) error {
	if err := m.readState(); err != nil {
		return err
	}
	if m.useWAL {
		if m.state == goModeSnapshot || m.state == goModeImport {
			m.needsImport = true
			if err := m.checkSnapshotGeneration(); err != nil {
				return err
			}
			return m.checkWALDatabases(true, dbCount)
		}
		if m.state == "" {
			generation, err := m.latestSnapshot()
			if err != nil {
				return err
			}
			if generation != 0 {
				return fmt.Errorf("Go snapshots have no recorded WAL authority in %s", m.root)
			}
		}
		return m.checkWALDatabases(useSnapshot, dbCount)
	}

	switch m.state {
	case goModeWAL:
		return m.checkWALDatabases(true, dbCount)
	case "":
		exists, err := m.hasExistingWAL()
		if err != nil {
			return err
		}
		if exists {
			generation, err := m.latestSnapshot()
			if err != nil {
				return err
			}
			if generation != 0 {
				return fmt.Errorf("Go WAL and snapshots have no recorded authority in %s", m.root)
			}
			return m.checkWALDatabases(true, dbCount)
		}
	}
	return m.checkSnapshotGeneration()
}

func (m *goPersistenceMode) hasExistingWAL() (bool, error) {
	entries, err := os.ReadDir(m.logDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() != ".snapshot-maintenance" && entry.Name() != gologstate.SourceLeaseFile {
			return true, nil
		}
	}
	return false, nil
}

func (m *goPersistenceMode) checkSnapshotGeneration() error {
	generation, err := m.latestSnapshot()
	if err != nil {
		return err
	}
	if generation < m.generation || m.state == goModeHandoff && generation != m.generation {
		return fmt.Errorf("required Go snapshot generation %d is unavailable in %s", m.generation, m.snapshotDir)
	}
	m.generation = generation
	return nil
}

func (m *goPersistenceMode) checkWALDatabases(useSnapshot bool, dbCount int) error {
	entries, err := os.ReadDir(m.logDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	known := m.walDBCount
	if m.state == goModeWAL || m.state == goModeHandoff {
		// Two-field legacy records only prove that DB 0 was opened.
		known = max(1, known)
	}
	databases := make(map[int]error, len(entries))
	for _, entry := range entries {
		suffix, ok := strings.CutPrefix(entry.Name(), "db_")
		if !ok {
			continue
		}
		id, err := strconv.Atoi(suffix)
		if err != nil || id < 0 || id == math.MaxInt || strconv.Itoa(id) != suffix {
			continue
		}
		if m.needsImport && id >= dbCount {
			return fmt.Errorf("db_count=%d excludes retained WAL database %d", dbCount, id)
		}
		err = checkWALDatabase(filepath.Join(m.logDir, entry.Name()))
		databases[id] = err
		if err == nil || !m.useWAL {
			known = max(known, id+1)
		}
	}
	for id := 0; id < known; id++ {
		err, exists := databases[id]
		if !exists {
			err = os.ErrNotExist
		}
		if err != nil {
			return fmt.Errorf("open authoritative WAL database %s: %w", filepath.Join(m.logDir, fmt.Sprintf("db_%d", id)), err)
		}
	}
	if useSnapshot && dbCount < known {
		return fmt.Errorf("db_count=%d excludes WAL database %d from the Go snapshot", dbCount, known-1)
	}
	if !m.useWAL {
		if known == 0 {
			return fmt.Errorf("no authoritative WAL databases found in %s", m.logDir)
		}
		m.walExportDBCount = known
	}
	m.walDBCount = max(known, dbCount)
	return nil
}

func checkWALDatabase(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		// A clean close may seal and remove active.log. Rust maintenance may
		// replace all sealed logs with a snapshot referenced by CURRENT.
		if !entry.IsDir() && (name == "active.log" || name == "CURRENT" ||
			len(name) == 49 && strings.HasPrefix(name, "log-") && strings.HasSuffix(name, ".seg")) {
			return nil
		}
	}
	return fmt.Errorf("no active log, sealed segment, or published snapshot found")
}

func (m *goPersistenceMode) readState() error {
	state, generation, walDBCount, err := gologstate.Read(m.root)
	if err != nil {
		return err
	}
	m.state = state
	m.generation = generation
	m.walDBCount = walDBCount
	return nil
}

func (m *goPersistenceMode) startWriting() error {
	if m.useWAL {
		return m.writeState(goModeWAL, 0)
	}
	return m.writeState(m.snapshotAuthority(), m.generation)
}

func (m *goPersistenceMode) snapshotAuthority() string {
	if m.state == goModeImport {
		return goModeImport
	}
	return goModeSnapshot
}

func (m *goPersistenceMode) finalSnapshotSaved() error {
	if !m.useWAL {
		return m.recordSnapshotGeneration()
	}
	generation, err := m.latestSnapshot()
	if err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("final Go snapshot generation is unavailable in %s", m.snapshotDir)
	}
	return m.writeState(goModeHandoff, generation)
}

func (m *goPersistenceMode) recordSnapshotGeneration() error {
	if m.useWAL {
		return nil
	}
	generation, err := m.latestSnapshot()
	if err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("saved Go snapshot generation is unavailable in %s", m.snapshotDir)
	}
	return m.writeState(m.snapshotAuthority(), generation)
}

func (m *goPersistenceMode) latestSnapshot() (uint64, error) {
	generations, err := snapshotGenerations(m.snapshotDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil || len(generations) == 0 {
		return 0, err
	}
	return generations[len(generations)-1], nil
}

func (m *goPersistenceMode) writeState(state string, generation uint64) error {
	file, err := os.CreateTemp(m.root, goPersistenceModeFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create Go persistence mode: %w", err)
	}
	defer os.Remove(file.Name())
	walDBCount := 0
	if state == goModeWAL || state == goModeHandoff {
		walDBCount = m.walDBCount
	}
	_, writeErr := fmt.Fprintf(file, "%s %d %d\n", state, generation, walDBCount)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("write Go persistence mode: %w", err)
	}
	if err := replaceSnapshotFile(file.Name(), filepath.Join(m.root, goPersistenceModeFile)); err != nil {
		return fmt.Errorf("publish Go persistence mode: %w", err)
	}
	if err := syncSnapshotDirectory(m.root); err != nil {
		return fmt.Errorf("sync Go persistence mode: %w", err)
	}
	m.state, m.generation = state, generation
	return nil
}

func (m *goPersistenceMode) close() error {
	return m.lock.Close()
}
