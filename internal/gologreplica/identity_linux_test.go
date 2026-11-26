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

	"github.com/TZJ-BYTE/Kerniva/internal/gologstate"
	"golang.org/x/sys/unix"
)

func TestSourceOpeningChecksManagedWALAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, marker, reason string
	}{
		{name: "missing"},
		{name: "legacy WAL", marker: "wal 0\n"},
		{name: "WAL database count", marker: "wal 0 16\n"},
		{name: "handoff", marker: "handoff 2 16\n"},
		{name: "snapshot", marker: "snapshot 1\n", reason: "WAL is not authoritative"},
		{name: "snapshot import", marker: "snapshot-import 2\n", reason: "WAL is not authoritative"},
		{name: "bad generation", marker: "wal 1 16\n", reason: "invalid Go persistence mode"},
		{name: "bad count", marker: "wal 0 0\n", reason: "invalid Go persistence WAL database count"},
		{name: "unknown phase", marker: "unknown 0\n", reason: "invalid Go persistence mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := t.TempDir()
			root := filepath.Join(data, gologstate.WALDir)
			check(t, os.MkdirAll(filepath.Join(root, dbName(0)), 0700))
			if tc.marker != "" {
				check(t, os.WriteFile(filepath.Join(data, gologstate.ModeFile), []byte(tc.marker), 0600))
			}
			legacy, legacyErr := NewSource(root)
			leased, lease, leaseErr := OpenLeasedSource(root)
			if lease != nil {
				defer lease.Close()
			}
			if tc.reason != "" {
				_, rotateErr := RotateSourceID(root)
				for _, err := range []error{legacyErr, leaseErr, rotateErr} {
					if err == nil || !strings.Contains(err.Error(), tc.reason) {
						t.Fatalf("want %q, got %v", tc.reason, err)
					}
				}
				if _, err := os.Stat(filepath.Join(root, sourceIDFile)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("refused source created its identity: %v", err)
				}
				return
			}
			check(t, legacyErr)
			check(t, leaseErr)
			identity, err := os.ReadFile(filepath.Join(root, sourceIDFile))
			check(t, err)
			for _, handler := range []http.Handler{legacy, leased} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/pull?db=0&after=0", nil))
				if response.Code != http.StatusOK {
					t.Fatalf("pull returned %d: %s", response.Code, response.Body)
				}
				info, err := readManifest(response.Body)
				check(t, err)
				if info.Source != string(identity) {
					t.Fatalf("pull reported source %q, want %q", info.Source, identity)
				}
			}
		})
	}
}

func TestRunningSourceStopsServingAfterWALHandoff(t *testing.T) {
	data := t.TempDir()
	root := filepath.Join(data, gologstate.WALDir)
	check(t, os.MkdirAll(filepath.Join(root, dbName(0)), 0700))
	handler, lease, err := OpenLeasedSource(root)
	check(t, err)
	defer lease.Close()
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, tc := range []struct {
		phase string
		want  int
	}{
		{"wal 0 1", http.StatusOK}, {"handoff 2 1", http.StatusOK},
		{"snapshot 2 0", http.StatusServiceUnavailable}, {"snapshot-import 2 0", http.StatusServiceUnavailable},
	} {
		check(t, os.WriteFile(filepath.Join(data, gologstate.ModeFile), []byte(tc.phase), 0600))
		resp, err := server.Client().Get(server.URL + "/pull?db=0&after=0")
		check(t, err)
		check(t, resp.Body.Close())
		if resp.StatusCode != tc.want {
			t.Fatalf("pull in %q returned %d, want %d", tc.phase, resp.StatusCode, tc.want)
		}
	}
}

func TestNewSourceSharesLeaseDuringIdentityCreation(t *testing.T) {
	if _, err := NewSource(""); err == nil {
		t.Fatal("legacy opening accepted a missing root")
	}
	data := t.TempDir()
	check(t, os.WriteFile(filepath.Join(data, gologstate.ModeFile), []byte("snapshot 1\n"), 0600))
	root := filepath.Join(data, "custom-wal")
	check(t, os.Mkdir(root, 0700))
	exclusive, err := acquireSourceLease(root, unix.LOCK_EX)
	check(t, err)
	t.Cleanup(func() { exclusive.Close() })
	if _, err := NewSource(root); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("legacy opening should respect an exclusive lease, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, sourceIDFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy opening created an identity under an exclusive lease: %v", err)
	}
	check(t, exclusive.Close())
	_, err = NewSource(root)
	check(t, err)
	// Legacy opening releases the temporary lease once the identity exists.
	lease, err := acquireSourceLease(root, unix.LOCK_EX)
	check(t, err)
	check(t, lease.Close())
}

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
