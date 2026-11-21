package command_test

import (
	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"testing"
)

func TestDebugXReadGroupCmd(t *testing.T) {
	command.InitDefaultCommands()
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	manager := database.NewDBManager(cfg)
	defer manager.Close()
	db := manager.GetDefaultDB()

	execResp(t, db, "XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM")
	execRespString(t, db, "XADD", "mystream", "*", "f1", "v1")
	execRespString(t, db, "XADD", "mystream", "*", "f2", "v2")

	// Call through registry
	resp := execResp(t, db, "XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")
	t.Logf("Response: type=%c value=%v error=%v", resp.Type, resp.Value, resp.Error)
}
