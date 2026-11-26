package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	StorageLegacy         = "legacy"
	StorageGoLog          = "go-log"
	StorageRustMemory     = "rust-memory"
	StorageRustMemorySync = "rust-memory-sync"
)

// ColdStartStrategy 冷启动数据加载策略
type ColdStartStrategy int

const (
	// NoLoad 不加载（默认，快速启动）
	NoLoad ColdStartStrategy = iota
	// LoadAll 启动时全量加载到内存
	LoadAll
	// LazyLoad 懒加载（读取时 fallback 到持久化引擎）
	LazyLoad
)

// Config 服务器配置
type Config struct {
	envError    error
	Host        string `yaml:"host" json:"host"`
	Port        int    `yaml:"port" json:"port"`
	NetworkType string `yaml:"network_type" json:"network_type"` // "std", "gnet"

	RespMaxBulkLen  int `yaml:"resp_max_bulk_len" json:"resp_max_bulk_len"`
	RespMaxArrayLen int `yaml:"resp_max_array_len" json:"resp_max_array_len"`

	// 数据库配置
	DBCount          int    `yaml:"db_count" json:"db_count"`
	StorageBackend   string `yaml:"storage_backend" json:"storage_backend"`
	RustMemoryShards int    `yaml:"rust_memory_shards" json:"rust_memory_shards"`

	// Applies only to the experimental synchronous Rust backend.
	RustMemoryReadDuringSync bool `yaml:"rust_memory_read_during_sync" json:"rust_memory_read_during_sync"`
	RustMemoryGroupMax       int  `yaml:"rust_memory_group_max" json:"rust_memory_group_max"`

	// 内存配置
	MaxMemory       int64  `yaml:"max_memory" json:"max_memory"`
	MaxMemoryPolicy string `yaml:"max_memory_policy" json:"max_memory_policy"` // "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random"

	// 持久化配置
	PersistenceEnabled bool   `yaml:"persistence_enabled" json:"persistence_enabled"`
	PersistenceType    string `yaml:"persistence_type" json:"persistence_type"` // "kerniva" / "rust"
	DataDir            string `yaml:"data_dir" json:"data_dir"`
	ColdStartStrategy  string `yaml:"cold_start_strategy" json:"cold_start_strategy"` // "no_load", "load_all", "lazy_load"
	AOFEnabled         bool   `yaml:"aof_enabled" json:"aof_enabled"`                 // 保留向后兼容
	AOFPath            string `yaml:"aof_path" json:"aof_path"`
	RDBPath            string `yaml:"rdb_path" json:"rdb_path"`

	PersistenceWriteMode     string `yaml:"persistence_write_mode" json:"persistence_write_mode"` // "strong", "weak"
	PersistenceDurability    string `yaml:"persistence_durability" json:"persistence_durability"` // "none", "flush", "sync"
	EngineSegmentSize        int64  `yaml:"engine_segment_size" json:"engine_segment_size"`
	EngineCheckpointAfterOps int    `yaml:"engine_checkpoint_after_ops" json:"engine_checkpoint_after_ops"`
	// A nil GoLogEnabled preserves WAL for existing go-log configurations.
	GoLogEnabled            *bool `yaml:"go_log_enabled,omitempty" json:"go_log_enabled,omitempty"`
	SnapshotEnabled         *bool `yaml:"snapshot_enabled,omitempty" json:"snapshot_enabled,omitempty"`
	SnapshotIntervalSeconds int   `yaml:"snapshot_interval_seconds" json:"snapshot_interval_seconds"`
	GoLogSegmentAgeSeconds  int   `yaml:"go_log_segment_age_seconds" json:"go_log_segment_age_seconds"`
	GoLogGroupMax           int   `yaml:"go_log_group_max" json:"go_log_group_max"`

	// 日志配置
	LogLevel      string `yaml:"log_level" json:"log_level"`
	LogPath       string `yaml:"log_path" json:"log_path"`
	LogMaxSizeMB  int    `yaml:"log_max_size_mb" json:"log_max_size_mb"`
	LogMaxBackups int    `yaml:"log_max_backups" json:"log_max_backups"`
	LogMaxAgeDays int    `yaml:"log_max_age_days" json:"log_max_age_days"`

	PprofEnabled bool   `yaml:"pprof_enabled" json:"pprof_enabled"`
	PprofAddr    string `yaml:"pprof_addr" json:"pprof_addr"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	cfg := defaultConfigNoEnv()
	cfg.applyEnvOverrides()
	return cfg
}

func (c *Config) applyEnvOverrides() {
	stringsByName := map[string]*string{
		"KERNIVA_HOST": &c.Host, "KERNIVA_NETWORK_TYPE": &c.NetworkType,
		"KERNIVA_STORAGE_BACKEND":  &c.StorageBackend,
		"KERNIVA_PERSISTENCE_TYPE": &c.PersistenceType, "KERNIVA_DATA_DIR": &c.DataDir,
		"KERNIVA_PERSISTENCE_WRITE_MODE": &c.PersistenceWriteMode, "KERNIVA_PERSISTENCE_DURABILITY": &c.PersistenceDurability,
		"KERNIVA_COLD_START_STRATEGY": &c.ColdStartStrategy, "KERNIVA_MAX_MEMORY_POLICY": &c.MaxMemoryPolicy,
		"KERNIVA_PPROF_ADDR": &c.PprofAddr, "KERNIVA_LOG_LEVEL": &c.LogLevel, "KERNIVA_LOG_PATH": &c.LogPath,
	}
	for name, target := range stringsByName {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			*target = value
		}
	}
	integersByName := map[string]*int{
		"KERNIVA_PORT": &c.Port, "KERNIVA_DB_COUNT": &c.DBCount,
		"KERNIVA_RUST_MEMORY_SHARDS":    &c.RustMemoryShards,
		"KERNIVA_RUST_MEMORY_GROUP_MAX": &c.RustMemoryGroupMax,
		"KERNIVA_RESP_MAX_BULK_LEN":     &c.RespMaxBulkLen, "KERNIVA_RESP_MAX_ARRAY_LEN": &c.RespMaxArrayLen,
		"KERNIVA_ENGINE_CHECKPOINT_AFTER_OPS": &c.EngineCheckpointAfterOps,
		"KERNIVA_GO_LOG_SEGMENT_AGE_SECONDS":  &c.GoLogSegmentAgeSeconds,
		"KERNIVA_GO_LOG_GROUP_MAX":            &c.GoLogGroupMax,
		"KERNIVA_SNAPSHOT_INTERVAL_SECONDS":   &c.SnapshotIntervalSeconds,
		"KERNIVA_LOG_MAX_SIZE_MB":             &c.LogMaxSizeMB, "KERNIVA_LOG_MAX_BACKUPS": &c.LogMaxBackups, "KERNIVA_LOG_MAX_AGE_DAYS": &c.LogMaxAgeDays,
	}
	for name, target := range integersByName {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				c.envError = fmt.Errorf("invalid %s: %w", name, err)
				continue
			}
			*target = parsed
		}
	}
	for name, target := range map[string]*int64{"KERNIVA_ENGINE_SEGMENT_SIZE": &c.EngineSegmentSize, "KERNIVA_MAX_MEMORY": &c.MaxMemory} {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				c.envError = fmt.Errorf("invalid %s: %w", name, err)
				continue
			}
			*target = parsed
		}
	}
	for name, target := range map[string]*bool{
		"KERNIVA_PERSISTENCE_ENABLED": &c.PersistenceEnabled, "KERNIVA_PPROF_ENABLED": &c.PprofEnabled,
		"KERNIVA_RUST_MEMORY_READ_DURING_SYNC": &c.RustMemoryReadDuringSync,
	} {
		if value, ok := os.LookupEnv(name); ok {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				c.envError = fmt.Errorf("invalid %s: %w", name, err)
				continue
			}
			*target = parsed
		}
	}
	for name, target := range map[string]**bool{
		"KERNIVA_GO_LOG_ENABLED":   &c.GoLogEnabled,
		"KERNIVA_SNAPSHOT_ENABLED": &c.SnapshotEnabled,
	} {
		if value, ok := os.LookupEnv(name); ok {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				c.envError = fmt.Errorf("invalid %s: %w", name, err)
				continue
			}
			*target = &parsed
		}
	}
}

func (c *Config) UsesGoLog() bool {
	return c.PersistenceEnabled && c.StorageBackend == StorageGoLog && (c.GoLogEnabled == nil || *c.GoLogEnabled)
}

func (c *Config) UsesGoSnapshot() bool {
	return c.PersistenceEnabled && c.StorageBackend == StorageGoLog && c.SnapshotEnabled != nil && *c.SnapshotEnabled
}

// AllowsGoLogMaintenance preserves legacy Rust compaction for configurations
// that omit both Go persistence switches.
func (c *Config) AllowsGoLogMaintenance() bool {
	return c.UsesGoLog() && (c.UsesGoSnapshot() || c.GoLogEnabled == nil && c.SnapshotEnabled == nil)
}

func (c *Config) SnapshotInterval() time.Duration {
	if c.SnapshotIntervalSeconds == 0 {
		return 5 * time.Minute
	}
	return time.Duration(c.SnapshotIntervalSeconds) * time.Second
}

// GetColdStartStrategy 解析冷启动策略
func (c *Config) GetColdStartStrategy() ColdStartStrategy {
	switch c.ColdStartStrategy {
	case "load_all":
		return LoadAll
	case "lazy_load":
		return LazyLoad
	default:
		return NoLoad
	}
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("configuration is required")
	}
	if c.envError != nil {
		return c.envError
	}
	if strings.TrimSpace(c.Host) == "" {
		return fmt.Errorf("invalid host")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port")
	}
	if c.DBCount <= 0 {
		return fmt.Errorf("db_count must be positive")
	}
	if c.MaxMemory < 0 || c.RespMaxBulkLen < 0 || c.RespMaxArrayLen < 0 {
		return fmt.Errorf("memory and protocol limits must not be negative")
	}
	if c.LogMaxSizeMB < 0 || c.LogMaxBackups < 0 || c.LogMaxAgeDays < 0 {
		return fmt.Errorf("log rotation limits must not be negative")
	}
	if err := validateChoice("network_type", c.NetworkType, "", "std", "gnet"); err != nil {
		return err
	}
	if err := validateChoice("max_memory_policy", c.MaxMemoryPolicy, "", "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random"); err != nil {
		return err
	}
	if err := validateChoice("storage_backend", c.StorageBackend, "", StorageLegacy, StorageGoLog, StorageRustMemory, StorageRustMemorySync); err != nil {
		return err
	}
	if c.RustMemoryGroupMax < 0 || c.RustMemoryGroupMax > 8 {
		return fmt.Errorf("rust_memory_group_max must be from 0 to 8")
	}
	if c.RustMemoryGroupMax > 1 && c.StorageBackend != StorageRustMemorySync {
		return fmt.Errorf("rust_memory_group_max above 1 requires rust-memory-sync")
	}
	if c.StorageBackend == StorageRustMemory || c.StorageBackend == StorageRustMemorySync {
		if c.StorageBackend == StorageRustMemory && c.PersistenceEnabled {
			return fmt.Errorf("rust-memory requires persistence_enabled=false")
		}
		if c.MaxMemoryPolicy != "" && c.MaxMemoryPolicy != "noeviction" {
			return fmt.Errorf("%s requires the noeviction memory policy", c.StorageBackend)
		}
		if c.RustMemoryShards <= 0 || c.RustMemoryShards&(c.RustMemoryShards-1) != 0 {
			return fmt.Errorf("rust_memory_shards must be a positive power of two")
		}
	}
	if c.GoLogSegmentAgeSeconds < 0 || c.GoLogSegmentAgeSeconds > 86400 {
		return fmt.Errorf("go_log_segment_age_seconds must be from 0 to 86400")
	}
	if c.GoLogGroupMax < 0 || c.GoLogGroupMax > 32 {
		return fmt.Errorf("go_log_group_max must be from 0 to 32")
	}
	if c.GoLogGroupMax > 1 && (c.StorageBackend != StorageGoLog || c.UsesGoLog() && c.PersistenceDurability == "none") {
		return fmt.Errorf("go_log_group_max above 1 requires go-log with sync durability")
	}
	if c.SnapshotIntervalSeconds < 0 || uint64(c.SnapshotIntervalSeconds) > uint64((1<<63-1)/time.Second) {
		return fmt.Errorf("snapshot_interval_seconds is outside the supported duration range")
	}
	if c.PersistenceEnabled && c.SnapshotEnabled != nil && *c.SnapshotEnabled && c.StorageBackend != StorageGoLog {
		return fmt.Errorf("snapshot_enabled requires storage_backend=go-log")
	}
	if c.PersistenceEnabled && c.GoLogEnabled != nil && *c.GoLogEnabled && c.StorageBackend != StorageGoLog {
		return fmt.Errorf("go_log_enabled requires storage_backend=go-log")
	}
	if (c.UsesGoLog() || c.UsesGoSnapshot()) && strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("Go persistence requires data_dir")
	}
	if c.UsesGoLog() {
		if c.ColdStartStrategy != "load_all" || c.MaxMemoryPolicy != "noeviction" {
			return fmt.Errorf("go-log requires cold_start_strategy=load_all and max_memory_policy=noeviction")
		}
		if c.PersistenceWriteMode != "" && c.PersistenceWriteMode != "strong" {
			return fmt.Errorf("go-log requires the strong write mode")
		}
		if c.PersistenceDurability != "" && c.PersistenceDurability != "sync" && c.PersistenceDurability != "none" {
			return fmt.Errorf("go-log supports sync or none durability")
		}
	}
	if c.StorageBackend == StorageRustMemorySync {
		if !c.PersistenceEnabled || c.PersistenceDurability != "sync" {
			return fmt.Errorf("rust-memory-sync requires persistence_enabled=true and persistence_durability=sync")
		}
		if c.PersistenceWriteMode != "" && c.PersistenceWriteMode != "strong" {
			return fmt.Errorf("rust-memory-sync requires the strong write mode")
		}
		if c.MaxMemoryPolicy != "noeviction" {
			return fmt.Errorf("rust-memory-sync requires the noeviction memory policy")
		}
		if strings.TrimSpace(c.DataDir) == "" {
			return fmt.Errorf("rust-memory-sync requires data_dir")
		}
	}
	if err := validateChoice("cold_start_strategy", c.ColdStartStrategy, "", "no_load", "load_all", "lazy_load"); err != nil {
		return err
	}
	if err := validateChoice("persistence_write_mode", c.PersistenceWriteMode, "", "strong", "weak"); err != nil {
		return err
	}
	if err := validateChoice("persistence_durability", c.PersistenceDurability, "", "none", "flush", "sync", "wal", "wal_fsync", "lsm"); err != nil {
		return err
	}
	if err := validateChoice("log_level", c.LogLevel, "", "debug", "info", "warn", "error"); err != nil {
		return err
	}
	if c.PersistenceEnabled {
		typ := strings.ToLower(strings.TrimSpace(c.PersistenceType))
		if typ != "" && typ != "rust" && typ != "kerniva" {
			return fmt.Errorf("invalid persistence_type")
		}
		if c.EngineSegmentSize != 0 && c.EngineSegmentSize < 4096 {
			return fmt.Errorf("invalid engine_segment_size")
		}
		if c.EngineCheckpointAfterOps < 0 {
			return fmt.Errorf("invalid engine_checkpoint_after_ops")
		}
	}
	return nil
}

func validateChoice(name, value string, choices ...string) error {
	for _, choice := range choices {
		if value == choice {
			return nil
		}
	}
	return fmt.Errorf("invalid %s %q", name, value)
}
