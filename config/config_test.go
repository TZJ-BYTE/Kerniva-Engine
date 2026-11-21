package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigRejectsInvalidPersistenceAndLimits(t *testing.T) {
	for _, modify := range []func(*Config){
		func(c *Config) { c.DBCount = 0 },
		func(c *Config) { c.NetworkType = "gnte" },
		func(c *Config) { c.PersistenceDurability = "sycn" },
		func(c *Config) { c.PersistenceWriteMode = "strnog" },
		func(c *Config) { c.ColdStartStrategy = "lasy_load" },
		func(c *Config) { c.EngineSegmentSize = 1024 },
		func(c *Config) { c.MaxMemory = -1 },
	} {
		cfg := defaultConfigNoEnv()
		modify(cfg)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected invalid configuration to fail")
		}
	}
}

func TestConfigRejectsMalformedEnvironment(t *testing.T) {
	t.Setenv("KERNIVA_PORT", "not-a-number")
	if err := DefaultConfig().Validate(); err == nil {
		t.Fatal("expected malformed port to fail")
	}
}

func TestConfigRejectsUnknownYAMLField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("persistence_durabilty: sync\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFromFile(path); err == nil {
		t.Fatal("expected unknown field to fail")
	}
}
