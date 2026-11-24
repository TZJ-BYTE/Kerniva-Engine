//go:build linux

package gologreplica

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

var (
	errCursor              = errors.New("source has not sealed the requested cursor")
	errSnapshotUnavailable = errors.New("source has no snapshot ahead of the requested cursor")
)

const (
	sourceIDFile    = ".replica-source-id"
	sourceLeaseFile = ".replica-source.lock"
)

type source struct {
	root string
	id   string
}

// NewSource serves sealed files from the primary's go-log-v1 directory.
// Call OpenLeasedSource when administrative source rotation is available.
func NewSource(root string) (http.Handler, error) {
	if err := checkSourceRoot(root); err != nil {
		return nil, err
	}
	return sourceHandler(root)
}

// OpenLeasedSource serves the same files and holds a shared source lease.
// The caller closes the lease once it can no longer serve replication requests.
func OpenLeasedSource(root string) (http.Handler, io.Closer, error) {
	lease, err := acquireSourceLease(root, unix.LOCK_SH)
	if err != nil {
		return nil, nil, err
	}
	handler, err := sourceHandler(root)
	if err != nil {
		lease.Close()
		return nil, nil, err
	}
	return handler, lease, nil
}

// RotateSourceID establishes a new replication history for an offline source.
// A nonempty identity with an error means the rename took effect, and directory
// sync needs to be retried before that identity is known to survive a crash.
func RotateSourceID(root string) (string, error) {
	return rotateSourceID(root, syncDir)
}

func rotateSourceID(root string, syncRoot func(string) error) (string, error) {
	lease, err := acquireSourceLease(root, unix.LOCK_EX)
	if err != nil {
		return "", err
	}
	defer lease.Close()

	id, temporary, err := writeSourceID(root)
	if err != nil {
		return "", err
	}
	defer os.Remove(temporary)
	if err := os.Rename(temporary, filepath.Join(root, sourceIDFile)); err != nil {
		return "", err
	}
	if err := syncRoot(root); err != nil {
		return id, fmt.Errorf("sync source directory after changing identity to %s: %w", id, err)
	}
	return id, nil
}

func checkSourceRoot(root string) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("replication source root must be a directory")
	}
	return nil
}

func acquireSourceLease(root string, mode int) (*os.File, error) {
	if err := checkSourceRoot(root); err != nil {
		return nil, err
	}
	lease, err := lockFile(context.Background(), filepath.Join(root, sourceLeaseFile), mode, false)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return nil, fmt.Errorf("replication source lease is busy: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("lock replication source: %w", err)
	}
	return lease, nil
}

func sourceHandler(root string) (http.Handler, error) {
	id, err := sourceID(root)
	if err != nil {
		return nil, fmt.Errorf("open replication source identity: %w", err)
	}
	s := &source{root: root, id: id}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /pull", s.pull)
	return mux, nil
}

func sourceID(root string) (string, error) {
	path := filepath.Join(root, sourceIDFile)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		_, temporary, err := writeSourceID(root)
		if err != nil {
			return "", err
		}
		defer os.Remove(temporary)
		if err := os.Link(temporary, path); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	id := string(data)
	if !validSource(id) {
		return "", errors.New("invalid persisted replication source identity")
	}
	return id, syncDir(root)
}

func writeSourceID(root string) (id, temporary string, err error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", "", err
	}
	id = hex.EncodeToString(nonce[:])
	file, err := os.CreateTemp(root, sourceIDFile+"-")
	if err != nil {
		return "", "", err
	}
	if err := writeSynced(file, []byte(id)); err != nil {
		os.Remove(file.Name())
		return "", "", err
	}
	return id, file.Name(), nil
}

type segment struct {
	name        string
	first, last uint64
}

type export struct {
	info  manifest
	files []*os.File
}

func (e *export) close() {
	for _, file := range e.files {
		file.Close()
	}
}

func (s *source) pull(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	db, dbErr := parseDB(query.Get("db"))
	after, sequenceErr := strconv.ParseUint(query.Get("after"), 10, 64)
	if dbErr != nil || sequenceErr != nil {
		http.Error(w, "invalid database or cursor", http.StatusBadRequest)
		return
	}
	if expected := query.Get("source"); expected != "" && expected != s.id {
		http.Error(w, "replication source identity changed", http.StatusConflict)
		return
	}
	var rebase uint64
	if raw := query.Get("rebase"); raw != "" {
		rebase, sequenceErr = strconv.ParseUint(raw, 10, 64)
		if sequenceErr != nil || rebase == 0 || rebase > after {
			http.Error(w, "invalid rebase cursor", http.StatusBadRequest)
			return
		}
	}
	forceSnapshot := query.Has("snapshot")
	if forceSnapshot && (query.Get("snapshot") != "1" || rebase != 0) {
		http.Error(w, "invalid snapshot request", http.StatusBadRequest)
		return
	}
	current, err := s.capture(r.Context(), db, after, rebase, forceSnapshot)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, errCursor), errors.Is(err, errSnapshotUnavailable):
			status = http.StatusConflict
		case errors.Is(err, os.ErrNotExist):
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	defer current.close()

	data, err := json.Marshal(current.info)
	if err != nil || len(data) > maxManifestBytes {
		http.Error(w, "invalid replication response manifest", http.StatusInternalServerError)
		return
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	w.Header().Set("Content-Type", "application/vnd.kerniva.golog-replica")
	w.Header().Set("Cache-Control", "no-store")
	stream := pullResponseWriter{body: w, controller: http.NewResponseController(w)}
	if _, err := stream.Write(header[:]); err != nil {
		return
	}
	if _, err := stream.Write(data); err != nil {
		return
	}
	for index, file := range current.files {
		if _, err := io.CopyN(stream, file, current.info.Files[index].Size); err != nil {
			// The receiver rejects any incomplete generation.
			return
		}
	}
}

func (s *source) capture(ctx context.Context, db int, after, rebase uint64, forceSnapshot bool) (*export, error) {
	directory := filepath.Join(s.root, dbName(db))
	// Rust uses the same Linux flock for CURRENT publication and file removal.
	// Retaining descriptors makes this view independent of future unlink calls.
	lock, err := lockFile(ctx, filepath.Join(directory, ".recovery.lock"), unix.LOCK_SH, true)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	cutoff, err := readCutoff(directory)
	if err != nil {
		return nil, err
	}
	if forceSnapshot && after >= cutoff {
		return nil, errSnapshotUnavailable
	}
	allSegments, err := scanSegments(directory, cutoff, true)
	if err != nil {
		return nil, err
	}
	var bridge []segment
	if after != 0 && after < cutoff && rebase == 0 && !forceSnapshot {
		bridge = retainedBridge(allSegments, after, cutoff)
	}
	segments := allSegments[:0]
	sealedHead := cutoff
	for _, segment := range allSegments {
		if segment.last <= cutoff {
			continue
		}
		if (segment.first <= sealedHead && sealedHead != cutoff) ||
			segment.first-1 > sealedHead || segment.last <= sealedHead {
			return nil, fmt.Errorf("invalid sealed segment %q after %d", segment.name, sealedHead)
		}
		sealedHead = segment.last
		segments = append(segments, segment)
	}
	if after > sealedHead {
		return nil, errCursor
	}

	result := &export{info: manifest{Version: 1, Source: s.id, DB: db, After: after, Last: after}}
	if cutoff != 0 {
		info, err := os.Lstat(filepath.Join(directory, snapshotName(cutoff)))
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("published snapshot is not a regular file")
		}
		result.info.Available = &snapshotHint{Sequence: cutoff, Size: info.Size()}
	}
	var bytes int64
	appendFile := func(name string) (int64, error) {
		file, err := os.Open(filepath.Join(directory, name))
		if err != nil {
			return 0, err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return 0, err
		}
		if !info.Mode().IsRegular() {
			file.Close()
			return 0, fmt.Errorf("replication file %q is not regular", name)
		}
		size := info.Size()
		result.files = append(result.files, file)
		result.info.Files = append(result.info.Files, fileEntry{Name: name, Size: size})
		if bytes < batchTargetBytes {
			bytes += min(size, batchTargetBytes)
		}
		return size, nil
	}
	batchFull := func() bool {
		return len(result.files) == maxBatchFiles || bytes >= batchTargetBytes
	}
	// An online replica already has the tail after this snapshot. On a changed
	// cutoff, the response supplies the latest hint or follows normal catch-up.
	result.info.Rebase = rebase != 0 && rebase == cutoff && cutoff <= after
	needsSnapshot := after < cutoff
	// A regular delta needs exact ranges across the published cutoff.
	// Snapshot recovery alone permits the first suffix to overlap it.
	if len(bridge) != 0 && (len(segments) == 0 || segments[0].first-1 == cutoff) {
		needsSnapshot = false
		for _, segment := range bridge {
			if batchFull() {
				break
			}
			size, err := appendFile(segment.name)
			if err != nil || size <= sealedLogHeaderBytes {
				// The published snapshot supersedes the entire covered bridge.
				result.close()
				result.files = nil
				result.info.Files = nil
				result.info.Last = after
				bytes = 0
				needsSnapshot = true
				break
			}
			result.info.Last = segment.last
		}
	}
	if needsSnapshot || result.info.Rebase {
		result.info.Snapshot = cutoff
		result.info.Last = max(after, cutoff)
		if _, err := appendFile(snapshotName(cutoff)); err != nil {
			return nil, err
		}
	}
	for _, segment := range segments {
		if segment.last <= result.info.Last {
			continue
		}
		if result.info.Rebase || batchFull() {
			break
		}
		if _, err := appendFile(segment.name); err != nil {
			result.close()
			return nil, err
		}
		result.info.Last = segment.last
	}
	if err := result.info.validate(db, after); err != nil {
		result.close()
		return nil, err
	}
	return result, nil
}

// retainedBridge returns a complete sequence from the replica cursor to the
// current snapshot cutoff, or nil when these optional files cannot be used.
func retainedBridge(segments []segment, after, cutoff uint64) []segment {
	var bridge []segment
	sequence := after
	for _, segment := range segments {
		if segment.last <= after || segment.last > cutoff {
			continue
		}
		if segment.first-1 != sequence {
			return nil
		}
		bridge = append(bridge, segment)
		sequence = segment.last
	}
	if sequence != cutoff {
		return nil
	}
	return bridge
}

func listSegments(directory string, cutoff uint64) ([]segment, error) {
	return scanSegments(directory, cutoff, false)
}

func scanSegments(directory string, cutoff uint64, includeCovered bool) ([]segment, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var segments []segment
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "log-") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		first, last, err := segmentRange(name)
		if err != nil {
			return nil, err
		}
		if !entry.Type().IsRegular() {
			if includeCovered && last <= cutoff {
				continue
			}
			return nil, fmt.Errorf("invalid sealed file %q", name)
		}
		if includeCovered || last > cutoff {
			segments = append(segments, segment{name: name, first: first, last: last})
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].name < segments[j].name
	})
	return segments, nil
}
