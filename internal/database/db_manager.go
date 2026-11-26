package database

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

// DBManager 数据库管理器
type DBManager struct {
	databases       []*Database
	memoryDatabases []*rustengine.MemoryEngine
	config          *config.Config

	persistenceEnabled bool
	dataDir            string
	goPersistence      *goPersistenceMode
	snapshotStop       chan struct{}
	snapshotDone       chan struct{}
	closeOnce          sync.Once
	closeErr           error
}

// NewDBManager 创建数据库管理器
func NewDBManager(cfg *config.Config) *DBManager {
	manager, err := NewDBManagerChecked(cfg)
	if err != nil {
		panic(err)
	}
	return manager
}

func NewDBManagerChecked(cfg *config.Config) (*DBManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.StorageBackend == config.StorageRustMemory || cfg.StorageBackend == config.StorageRustMemorySync {
		return newMemoryDBManager(cfg)
	}
	goLogEnabled := cfg.UsesGoLog()
	snapshotEnabled := cfg.UsesGoSnapshot()
	writePersistence := goLogEnabled || cfg.PersistenceEnabled && cfg.StorageBackend != config.StorageGoLog
	manager := &DBManager{
		config:             cfg,
		databases:          make([]*Database, 0, cfg.DBCount),
		persistenceEnabled: writePersistence || snapshotEnabled,
	}
	if manager.persistenceEnabled {
		manager.dataDir = cfg.DataDir
		if manager.dataDir == "" {
			manager.dataDir = "./data"
		}
	}
	if goLogEnabled || snapshotEnabled {
		mode, err := openGoPersistenceMode(cfg.DataDir, goLogEnabled, snapshotEnabled, cfg.DBCount)
		if err != nil {
			return nil, err
		}
		manager.goPersistence = mode
		if err := setGoMaintenance(mode.logDir, goLogEnabled, cfg.AllowsGoLogMaintenance() && !mode.needsImport); err != nil {
			return nil, errors.Join(err, mode.close())
		}
		if goLogEnabled {
			manager.dataDir = mode.logDir
		} else {
			manager.dataDir = mode.snapshotDir
		}
	} else if cfg.StorageBackend == config.StorageGoLog && cfg.DataDir != "" {
		logDir := filepath.Join(cfg.DataDir, "go-log-v1")
		if _, err := os.Stat(logDir); err == nil {
			mode, err := lockGoPersistenceRoot(cfg.DataDir, false)
			if err != nil {
				return nil, err
			}
			err = errors.Join(setGoMaintenance(mode.logDir, false, false), mode.close())
			if err != nil {
				return nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	engineOptions := DefaultEngineOptions()
	if cfg.EngineSegmentSize > 0 {
		engineOptions.SegmentSizeBytes = uint64(cfg.EngineSegmentSize)
	}
	if cfg.EngineCheckpointAfterOps > 0 {
		engineOptions.CheckpointAfterOps = uint64(cfg.EngineCheckpointAfterOps)
	}
	maxMemory := cfg.MaxMemory
	if maxMemory == 0 {
		maxMemory = math.MaxInt64
	}
	if manager.goPersistence != nil && manager.goPersistence.needsImport {
		if err := manager.goPersistence.importSnapshot(cfg, engineOptions, maxMemory); err != nil {
			return nil, errors.Join(err, manager.closeDatabases())
		}
	}
	for i := 0; i < cfg.DBCount; i++ {
		dbConfig := &DatabaseConfig{
			StorageBackend:  cfg.StorageBackend,
			MaxMemory:       maxMemory,
			MaxMemoryPolicy: cfg.MaxMemoryPolicy,
		}
		if writePersistence {
			dbOptions := *engineOptions
			dbConfig.Type = Persistent
			dbConfig.DataDir = filepath.Join(manager.dataDir, fmt.Sprintf("db_%d", i))
			dbConfig.Options = &dbOptions
			dbConfig.ColdStartStrategy = cfg.ColdStartStrategy
			dbConfig.GoLogSegmentAge = time.Duration(cfg.GoLogSegmentAgeSeconds) * time.Second
			dbConfig.GoLogGroupMax = cfg.GoLogGroupMax
			dbConfig.WriteMode = cfg.PersistenceWriteMode
			dbConfig.Durability = cfg.PersistenceDurability
		}
		db, err := NewDatabaseWithConfig(i, dbConfig)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("initialize database %d: %w", i, err), manager.closeDatabases())
		}
		manager.databases = append(manager.databases, db)
	}

	if snapshotEnabled && !goLogEnabled {
		if err := LoadSnapshot(manager.goPersistence.snapshotDir, manager.databases); err != nil {
			return nil, errors.Join(fmt.Errorf("restore Go snapshot: %w", err), manager.closeDatabases())
		}
	}
	if manager.goPersistence != nil {
		if manager.goPersistence.needsImport && cfg.AllowsGoLogMaintenance() {
			if err := setGoMaintenance(manager.goPersistence.logDir, true, true); err != nil {
				return nil, errors.Join(err, manager.closeDatabases())
			}
		}
		if err := manager.goPersistence.startWriting(); err != nil {
			return nil, errors.Join(err, manager.closeDatabases())
		}
	}
	for _, db := range manager.databases {
		db.StartExpireCleaner()
	}
	if snapshotEnabled {
		manager.startSnapshots(cfg.SnapshotInterval())
	}
	if manager.persistenceEnabled {
		logger.Info("初始化 %d 个持久化数据库，数据目录：%s", cfg.DBCount, manager.dataDir)
	} else {
		logger.Info("初始化 %d 个数据库（纯内存模式）", cfg.DBCount)
	}
	return manager, nil
}

func setGoMaintenance(logDir string, useWAL, allowed bool) error {
	if !useWAL {
		if _, err := os.Stat(logDir); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
	}
	if err := golog.SetMaintenanceAllowed(logDir, allowed); err != nil {
		return fmt.Errorf("configure Go WAL maintenance: %w", err)
	}
	return nil
}

func (m *DBManager) startSnapshots(interval time.Duration) {
	m.snapshotStop = make(chan struct{})
	m.snapshotDone = make(chan struct{})
	go func() {
		defer close(m.snapshotDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.snapshotStop:
				return
			case <-ticker.C:
				if err := m.savePeriodicSnapshot(); err != nil {
					logger.Error("保存 Go 快照失败：%v", err)
				}
			}
		}
	}()
}

func (m *DBManager) savePeriodicSnapshot() error {
	if err := SaveSnapshot(m.goPersistence.snapshotDir, m.databases); err != nil {
		return err
	}
	return m.goPersistence.recordSnapshotGeneration()
}

func newMemoryDBManager(cfg *config.Config) (*DBManager, error) {
	manager := &DBManager{
		config:             cfg,
		memoryDatabases:    make([]*rustengine.MemoryEngine, 0, cfg.DBCount),
		persistenceEnabled: cfg.StorageBackend == config.StorageRustMemorySync,
	}
	if manager.persistenceEnabled {
		manager.dataDir = filepath.Join(cfg.DataDir, "rust-memory-v1")
	}
	var limit *uint64
	if cfg.MaxMemory != 0 {
		maxPayload := uint64(cfg.MaxMemory)
		limit = &maxPayload
	}
	for i := 0; i < cfg.DBCount; i++ {
		var engine *rustengine.MemoryEngine
		var err error
		if manager.persistenceEnabled {
			directory := filepath.Join(manager.dataDir, fmt.Sprintf("db_%d", i))
			if err = os.MkdirAll(directory, 0o750); err == nil {
				engine, err = rustengine.OpenDurableMemory(directory, cfg.RustMemoryShards, limit, cfg.RustMemoryReadDuringSync)
			}
			if err == nil && cfg.RustMemoryGroupMax > 1 {
				if err = engine.EnableWriteGrouping(cfg.RustMemoryGroupMax); err != nil {
					err = errors.Join(err, engine.Close())
				}
			}
			if err != nil {
				err = fmt.Errorf("%s: %w", directory, err)
			}
		} else {
			engine, err = rustengine.OpenMemory(cfg.RustMemoryShards, limit)
		}
		if err != nil {
			return nil, errors.Join(fmt.Errorf("initialize %s database %d: %w", cfg.StorageBackend, i, err), manager.closeMemoryDatabases())
		}
		manager.memoryDatabases = append(manager.memoryDatabases, engine)
	}
	if manager.persistenceEnabled {
		logger.Info("初始化 %d 个 Rust 同步内存数据库，每库 %d 个分片，目录：%s", cfg.DBCount, cfg.RustMemoryShards, manager.dataDir)
	} else {
		logger.Info("初始化 %d 个 Rust 内存数据库，每库 %d 个分片", cfg.DBCount, cfg.RustMemoryShards)
	}
	return manager, nil
}

func (m *DBManager) UsesRustMemory() bool {
	return m != nil && len(m.memoryDatabases) != 0
}

func (m *DBManager) UsesRustMemorySync() bool {
	return m.UsesRustMemory() && m.config.StorageBackend == config.StorageRustMemorySync
}

func (m *DBManager) RustMemoryReadDuringSync() bool {
	return m.UsesRustMemorySync() && m.config.RustMemoryReadDuringSync
}

func (m *DBManager) GetMemoryDBByIndex(index int) (*rustengine.MemoryEngine, error) {
	if index < 0 || index >= len(m.memoryDatabases) {
		return nil, fmt.Errorf("DB index out of range")
	}
	return m.memoryDatabases[index], nil
}

// GetDefaultDB 获取默认数据库 (DB 0)
func (m *DBManager) GetDefaultDB() *Database {
	if m.UsesRustMemory() {
		return nil
	}
	return m.databases[0]
}

// GetDBByIndex 根据索引获取数据库
func (m *DBManager) GetDBByIndex(index int) (*Database, error) {
	if index < 0 || index >= len(m.databases) {
		return nil, fmt.Errorf("DB index out of range")
	}
	return m.databases[index], nil
}

// FlushAll 清空所有数据库
func (m *DBManager) FlushAll() error {
	if m.UsesRustMemory() {
		return fmt.Errorf("FLUSHALL is unavailable in rust-memory mode")
	}
	for _, db := range m.databases {
		if err := db.Clear(); err != nil {
			return err
		}
	}
	logger.Info("清空所有数据库")
	return nil
}

// DBCount 返回数据库数量
func (m *DBManager) DBCount() int {
	if m.UsesRustMemory() {
		return len(m.memoryDatabases)
	}
	return len(m.databases)
}

// Close 关闭所有数据库
func (m *DBManager) Close() error {
	m.closeOnce.Do(func() {
		if m.UsesRustMemory() {
			m.closeErr = m.closeMemoryDatabases()
			return
		}
		if m.snapshotStop != nil {
			close(m.snapshotStop)
			<-m.snapshotDone
			for _, db := range m.databases {
				db.disableExpireCleaner()
			}
			if err := saveSnapshot(m.goPersistence.snapshotDir, m.databases, true); err != nil {
				m.closeErr = errors.Join(fmt.Errorf("save final Go snapshot: %w", err), m.closeDatabaseHandles())
			}
		} else {
			m.closeErr = m.closeDatabaseHandles()
		}
		if m.goPersistence != nil {
			if m.closeErr == nil && m.snapshotStop != nil {
				m.closeErr = m.goPersistence.finalSnapshotSaved()
			}
			m.closeErr = errors.Join(m.closeErr, m.goPersistence.close())
		}
	})
	return m.closeErr
}

func (m *DBManager) closeDatabases() error {
	err := m.closeDatabaseHandles()
	if m.goPersistence != nil {
		err = errors.Join(err, m.goPersistence.close())
	}
	return err
}

func (m *DBManager) closeDatabaseHandles() error {
	var result error
	for _, db := range m.databases {
		if err := db.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close database %d: %w", db.id, err))
		}
	}
	return result
}

func (m *DBManager) closeMemoryDatabases() error {
	var result error
	for _, engine := range m.memoryDatabases {
		result = errors.Join(result, engine.Close())
	}
	return result
}

// GetStats 获取统计信息
func (m *DBManager) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["db_count"] = m.DBCount()
	stats["persistence_enabled"] = m.persistenceEnabled
	if m.UsesRustMemory() {
		mode := "memory"
		stats["storage_backend"] = config.StorageRustMemory
		if m.UsesRustMemorySync() {
			mode = "memory-sync"
			stats["storage_backend"] = config.StorageRustMemorySync
			stats["data_dir"] = m.dataDir
			stats["memory_read_during_sync"] = m.RustMemoryReadDuringSync()
		}
		dbStats := make([]map[string]interface{}, len(m.memoryDatabases))
		for i, db := range m.memoryDatabases {
			item := map[string]interface{}{"db_id": i, "mode": mode}
			fields, err := db.Stats()
			if err != nil {
				item["stats_error"] = err.Error()
			} else {
				item["memory_keys"] = fields.Keys
				item["used_memory_bytes"] = fields.PayloadBytes
				item["shards"] = fields.Shards
			}
			dbStats[i] = item
		}
		stats["databases"] = dbStats
		return stats
	}

	if m.persistenceEnabled {
		stats["data_dir"] = m.dataDir
	}
	if m.config.StorageBackend == config.StorageGoLog {
		stats["storage_backend"] = config.StorageGoLog
		stats["go_log_enabled"] = m.config.UsesGoLog()
		stats["snapshot_enabled"] = m.config.UsesGoSnapshot()
		stats["log_maintenance_allowed"] = m.config.AllowsGoLogMaintenance()
		if m.config.UsesGoSnapshot() {
			stats["snapshot_interval_seconds"] = int64(m.config.SnapshotInterval() / time.Second)
		}
	}

	// 收集所有数据库的统计信息
	dbStats := make([]map[string]interface{}, len(m.databases))
	for i, db := range m.databases {
		dbStats[i] = db.GetStats()
	}
	stats["databases"] = dbStats

	return stats
}
