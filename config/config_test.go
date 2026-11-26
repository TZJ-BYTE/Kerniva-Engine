package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
		func(c *Config) { c.RustMemoryGroupMax = 4 },
		func(c *Config) { c.StorageBackend, c.RustMemoryGroupMax = StorageRustMemorySync, 9 },
		func(c *Config) { c.GoLogGroupMax = 2 },
	} {
		cfg := defaultConfigNoEnv()
		modify(cfg)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected invalid configuration to fail")
		}
	}
}

func TestGoLogGroupConfiguration(t *testing.T) {
	t.Setenv("KERNIVA_GO_LOG_GROUP_MAX", "8")
	cfg := DefaultConfig()
	cfg.StorageBackend = StorageGoLog
	cfg.ColdStartStrategy = "load_all"
	cfg.MaxMemoryPolicy = "noeviction"
	if cfg.GoLogGroupMax != 8 {
		t.Fatalf("environment group size = %d, want 8", cfg.GoLogGroupMax)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.PersistenceDurability = "none"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected group sync to reject none durability")
	}
	cfg.PersistenceDurability = "sync"
	for _, n := range []int{0, 1, 8, 16, 17, 32} {
		cfg.GoLogGroupMax = n
		if err := cfg.Validate(); err != nil {
			t.Fatalf("rejected synchronous group size %d: %v", n, err)
		}
	}
	cfg.GoLogGroupMax = 33
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected oversized group to fail validation")
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

func TestGoPersistenceSwitches(t *testing.T) {
	enabled, disabled := true, false
	for _, tc := range []struct {
		name                                   string
		log, snapshot                          *bool
		master                                 bool
		wantLog, wantSnapshot, wantMaintenance bool
	}{
		{name: "legacy", master: true, wantLog: true, wantMaintenance: true},
		{name: "enable only WAL setting", log: &enabled, master: true, wantLog: true},
		{name: "disable only WAL setting", log: &disabled, master: true},
		{name: "disable only snapshot setting", snapshot: &disabled, master: true, wantLog: true},
		{name: "default log with snapshots", snapshot: &enabled, master: true, wantLog: true, wantSnapshot: true, wantMaintenance: true},
		{name: "both", log: &enabled, snapshot: &enabled, master: true, wantLog: true, wantSnapshot: true, wantMaintenance: true},
		{name: "log only", log: &enabled, snapshot: &disabled, master: true, wantLog: true},
		{name: "snapshot only", log: &disabled, snapshot: &enabled, master: true, wantSnapshot: true},
		{name: "memory", log: &disabled, snapshot: &disabled, master: true},
		{name: "master switch", log: &enabled, snapshot: &enabled, master: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultConfigNoEnv()
			cfg.StorageBackend = StorageGoLog
			cfg.PersistenceEnabled = tc.master
			cfg.ColdStartStrategy = "load_all"
			cfg.GoLogEnabled, cfg.SnapshotEnabled = tc.log, tc.snapshot
			if !tc.wantLog && !tc.wantSnapshot {
				cfg.DataDir = ""
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			if got := cfg.UsesGoLog(); got != tc.wantLog {
				t.Fatalf("WAL enabled = %t, want %t", got, tc.wantLog)
			}
			if got := cfg.UsesGoSnapshot(); got != tc.wantSnapshot {
				t.Fatalf("snapshot enabled = %t, want %t", got, tc.wantSnapshot)
			}
			if got := cfg.AllowsGoLogMaintenance(); got != tc.wantMaintenance {
				t.Fatalf("WAL maintenance enabled = %t, want %t", got, tc.wantMaintenance)
			}
		})
	}
}

func TestGoPersistenceYAMLAndEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("storage_backend: go-log\ncold_start_strategy: load_all\ngo_log_enabled: false\nsnapshot_enabled: true\nsnapshot_interval_seconds: 60\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UsesGoLog() || !cfg.UsesGoSnapshot() || cfg.SnapshotInterval() != time.Minute {
		t.Fatal("expected snapshot-only YAML configuration with a one-minute interval")
	}
	t.Setenv("KERNIVA_GO_LOG_ENABLED", "true")
	t.Setenv("KERNIVA_SNAPSHOT_ENABLED", "false")
	t.Setenv("KERNIVA_SNAPSHOT_INTERVAL_SECONDS", "120")
	cfg, err = LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.UsesGoLog() || cfg.UsesGoSnapshot() || cfg.SnapshotInterval() != 2*time.Minute {
		t.Fatal("expected log-only environment overrides and a two-minute interval")
	}
	t.Setenv("KERNIVA_SNAPSHOT_INTERVAL_SECONDS", "-1")
	if _, err := LoadFromFile(path); err == nil {
		t.Fatal("expected negative snapshot interval to fail validation")
	}
}

func TestExplicitWALRequiresGoLogBackend(t *testing.T) {
	cfg := defaultConfigNoEnv()
	enabled := true
	cfg.GoLogEnabled = &enabled
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected explicit Go log on legacy backend to fail")
	}
}
