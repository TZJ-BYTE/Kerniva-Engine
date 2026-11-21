package database

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

// DBManager 数据库管理器
type DBManager struct {
	databases []*Database
	lock      sync.RWMutex
	config    *config.Config

	persistenceEnabled bool
	dataDir            string
	engineOptions      *EngineOptions
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
	databases := make([]*Database, cfg.DBCount)

	// 检查是否启用持久化
	persistenceEnabled := cfg.PersistenceEnabled
	var dataDir string
	var engineOptions *EngineOptions

	if persistenceEnabled {
		dataDir = cfg.DataDir
		if dataDir == "" {
			dataDir = "./data"
		}

		engineOptions = DefaultEngineOptions()
		if cfg.EngineSegmentSize > 0 {
			engineOptions.SegmentSizeBytes = uint64(cfg.EngineSegmentSize)
		}
		if cfg.EngineCheckpointAfterOps > 0 {
			engineOptions.CheckpointAfterOps = uint64(cfg.EngineCheckpointAfterOps)
		}

		logger.Info("启用 Rust 持久化引擎，数据目录：%s", dataDir)
	}

	// 创建所有数据库
	for i := 0; i < cfg.DBCount; i++ {
		var db *Database
		var err error
		maxMemory := cfg.MaxMemory
		if maxMemory == 0 {
			maxMemory = math.MaxInt64
		}
		dbConfig := &DatabaseConfig{Type: MemoryOnly, MaxMemory: maxMemory, MaxMemoryPolicy: cfg.MaxMemoryPolicy}

		if persistenceEnabled {
			// 为每个数据库创建独立的子目录
			dbDataDir := filepath.Join(dataDir, fmt.Sprintf("db_%d", i))

			dbOptions := *engineOptions

			dbConfig = &DatabaseConfig{
				Type:              Persistent,
				DataDir:           dbDataDir,
				Options:           &dbOptions,
				ColdStartStrategy: cfg.ColdStartStrategy,
				WriteMode:         cfg.PersistenceWriteMode,
				Durability:        cfg.PersistenceDurability,
				MaxMemory:         maxMemory,
				MaxMemoryPolicy:   cfg.MaxMemoryPolicy,
			}

			db, err = NewDatabaseWithConfig(i, dbConfig)
		} else {
			db, err = NewDatabaseWithConfig(i, dbConfig)
			logger.Debug("数据库 %d 使用纯内存模式", i)
		}
		if err != nil {
			for _, opened := range databases[:i] {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("initialize database %d: %w", i, err)
		}

		databases[i] = db
		db.StartExpireCleaner()
	}

	if persistenceEnabled {
		logger.Info("初始化 %d 个数据库（Rust 持久化模式）", cfg.DBCount)
	} else {
		logger.Info("初始化 %d 个数据库（纯内存模式）", cfg.DBCount)
	}

	return &DBManager{
		databases:          databases,
		config:             cfg,
		persistenceEnabled: persistenceEnabled,
		dataDir:            dataDir,
		engineOptions:      engineOptions,
	}, nil
}

// GetDefaultDB 获取默认数据库 (DB 0)
func (m *DBManager) GetDefaultDB() *Database {
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
	return len(m.databases)
}

// Close 关闭所有数据库
func (m *DBManager) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	var lastErr error
	for i, db := range m.databases {
		err := db.Close()
		if err != nil {
			logger.Error("关闭数据库 %d 失败：%v", i, err)
			lastErr = err
		}
	}

	if lastErr == nil {
		logger.Info("已关闭所有数据库")
	}

	return lastErr
}

// GetStats 获取统计信息
func (m *DBManager) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["db_count"] = len(m.databases)
	stats["persistence_enabled"] = m.persistenceEnabled

	if m.persistenceEnabled {
		stats["data_dir"] = m.dataDir
	}

	// 收集所有数据库的统计信息
	dbStats := make([]map[string]interface{}, len(m.databases))
	for i, db := range m.databases {
		dbStats[i] = db.GetStats()
	}
	stats["databases"] = dbStats

	return stats
}
