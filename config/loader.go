package config

import (
	"bytes"
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

func defaultConfigNoEnv() *Config {
	return &Config{
		Host:            "127.0.0.1",
		Port:            16379,
		NetworkType:     "std",
		RespMaxBulkLen:  64 << 20,
		RespMaxArrayLen: 1 << 20,
		DBCount:         16,
		MaxMemory:       256 * 1024 * 1024,
		MaxMemoryPolicy: "noeviction",

		PersistenceEnabled:       true,
		PersistenceType:          "kerniva",
		DataDir:                  "./data",
		ColdStartStrategy:        "lazy_load",
		PersistenceWriteMode:     "strong",
		PersistenceDurability:    "sync",
		EngineSegmentSize:        4 << 20,
		EngineCheckpointAfterOps: 1024,

		AOFEnabled: false,
		AOFPath:    "./data/appendonly.aof",
		RDBPath:    "./data/dump.rdb",

		LogLevel:      "info",
		LogPath:       "./logs/kerniva.log",
		LogMaxSizeMB:  100,
		LogMaxBackups: 3,
		LogMaxAgeDays: 7,

		PprofEnabled: false,
		PprofAddr:    "127.0.0.1:6060",
	}
}

func LoadFromFile(path string) (*Config, error) {
	cfg := defaultConfigNoEnv()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(b))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	cfg.applyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}
