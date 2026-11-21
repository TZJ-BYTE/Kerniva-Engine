package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogLevelAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.log")
	if err := Init(path, "warn"); err != nil {
		t.Fatal(err)
	}
	Debug("hidden debug")
	Info("hidden info")
	Warn("visible warning")
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "hidden") || !strings.Contains(string(contents), "visible warning") {
		t.Fatalf("unexpected log %q", contents)
	}
}
