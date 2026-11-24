//go:build linux

package gologreplica

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRotateSourceIDRequiresAllServingLeasesToClose(t *testing.T) {
	root := t.TempDir()
	check(t, os.Mkdir(filepath.Join(root, dbName(0)), 0700))

	handler, first, err := OpenLeasedSource(root)
	check(t, err)
	defer first.Close()
	_, second, err := OpenLeasedSource(root)
	check(t, err)
	defer second.Close()

	path := filepath.Join(root, sourceIDFile)
	original, err := os.ReadFile(path)
	check(t, err)
	refuseRotation := func() {
		t.Helper()
		id, err := RotateSourceID(root)
		if id != "" || !errors.Is(err, unix.EWOULDBLOCK) {
			t.Fatalf("rotation should reject a live serving lease, got %q, %v", id, err)
		}
		persisted, err := os.ReadFile(path)
		check(t, err)
		if string(persisted) != string(original) {
			t.Fatalf("refused rotation changed identity to %q", persisted)
		}
	}
	old := httptest.NewRecorder()
	handler.ServeHTTP(old, httptest.NewRequest(http.MethodGet, "/pull?db=0&after=0", nil))
	if old.Code != http.StatusOK {
		t.Fatalf("old source failed to respond: %d %s", old.Code, old.Body)
	}
	previous, err := readManifest(old.Body)
	check(t, err)
	if previous.Source != string(original) {
		t.Fatalf("old manifest has identity %q, want %q", previous.Source, original)
	}

	refuseRotation()
	check(t, first.Close())
	refuseRotation()
	check(t, second.Close())

	fresh, err := rotateSourceID(root, func(directory string) error {
		_, lease, err := OpenLeasedSource(root)
		if lease != nil {
			lease.Close()
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			t.Fatalf("serving must wait until rotation releases the lease, got %v", err)
		}
		return syncDir(directory)
	})
	check(t, err)
	if !validSource(fresh) || fresh == string(original) {
		t.Fatalf("expected new source identity, got %q", fresh)
	}

	handler, lease, err := OpenLeasedSource(root)
	check(t, err)
	defer lease.Close()
	wrong := httptest.NewRecorder()
	handler.ServeHTTP(wrong, httptest.NewRequest(http.MethodGet, "/pull?db=0&after=0&source="+previous.Source, nil))
	if wrong.Code != http.StatusConflict || !strings.Contains(wrong.Body.String(), "identity changed") {
		t.Fatalf("new source accepted the old identity: %d %s", wrong.Code, wrong.Body)
	}
	current := httptest.NewRecorder()
	handler.ServeHTTP(current, httptest.NewRequest(http.MethodGet, "/pull?db=0&after=0", nil))
	if current.Code != http.StatusOK {
		t.Fatalf("new source returned %d %s", current.Code, current.Body)
	}
	info, err := readManifest(current.Body)
	check(t, err)
	if info.Source != fresh {
		t.Fatalf("new manifest has identity %q, want %q", info.Source, fresh)
	}
}

func TestRotateSourceIDReportsReplacementWhenDirectorySyncFails(t *testing.T) {
	root := t.TempDir()
	prior, err := sourceID(root)
	check(t, err)
	replaced, err := rotateSourceID(root, func(string) error { return unix.EIO })
	if !validSource(replaced) || replaced == prior || !errors.Is(err, unix.EIO) ||
		!strings.Contains(err.Error(), replaced) {
		t.Fatalf("rename before failed sync should report the new identity, got %q, %v", replaced, err)
	}
	actual, err := os.ReadFile(filepath.Join(root, sourceIDFile))
	check(t, err)
	if string(actual) != replaced {
		t.Fatalf("identity after failed directory sync is %q, want %q", actual, replaced)
	}
	_, lease, err := OpenLeasedSource(root)
	check(t, err)
	check(t, lease.Close())
}

func TestRotateSourceIDRemovesTemporaryFileAfterRenameFails(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, sourceIDFile)
	check(t, os.Mkdir(path, 0700))
	id, err := RotateSourceID(root)
	if id != "" || err == nil {
		t.Fatalf("rename over a directory should fail before changing identity, got %q, %v", id, err)
	}
	info, err := os.Stat(path)
	check(t, err)
	if !info.IsDir() {
		t.Fatal("identity destination changed despite failed rename")
	}
	staged, err := filepath.Glob(filepath.Join(root, sourceIDFile+"-*"))
	check(t, err)
	if len(staged) != 0 {
		t.Fatalf("unexpected temporary source files after failed rename: %v", staged)
	}
}

func TestOpenLeasedSourceClosesLeaseAfterInvalidIdentity(t *testing.T) {
	root := t.TempDir()
	check(t, os.WriteFile(filepath.Join(root, sourceIDFile), []byte("invalid"), 0600))
	_, lease, err := OpenLeasedSource(root)
	if lease != nil {
		lease.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "invalid persisted replication source identity") {
		t.Fatalf("invalid identity should reject serving, got %v", err)
	}
	_, err = RotateSourceID(root)
	check(t, err)
}
