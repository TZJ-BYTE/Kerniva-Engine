//go:build linux

package gologreplica

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"golang.org/x/sys/unix"
)

// This error is returned only during recovery of a staged retained delta,
// before the generation can be published or made visible.
var errRetainedDeltaRecovery = errors.New("could not recover retained replication delta")

type Status struct {
	Rebases    uint64 `json:"rebases"`
	Ready      bool   `json:"ready"`
	Source     string `json:"source"`
	Sequence   uint64 `json:"sequence"`
	Snapshot   uint64 `json:"snapshot"`
	Installs   uint64 `json:"installs"`
	Bootstraps uint64 `json:"bootstraps"`
	LastError  string `json:"last_error"`
}

type installed struct {
	Generation string `json:"generation"`
	Source     string `json:"source"`
	Sequence   uint64 `json:"sequence"`
	Snapshot   uint64 `json:"snapshot"`
}

type view struct {
	installed
	values  map[string]*datastruct.DataValue
	changes map[string]*datastruct.DataValue // staged incremental view with nil tombstones
}

type report struct {
	view   *view
	status Status
}

type currentPublishStage byte

const (
	currentBeforeRename currentPublishStage = iota + 1
	currentAfterDirectorySync
)

type replicaDB struct {
	minimumRebase     int64
	evaluatedSnapshot uint64
	requestedSnapshot uint64
	retryRebaseAt     time.Time
	number            int
	directory         string
	pollGateOnce      sync.Once
	pollGate          chan struct{}
	viewMu            sync.RWMutex // protects the published values across incremental installs
	result            atomic.Pointer[report]
	pending           *view
	onCurrentPublish  func(currentPublishStage) // set before polling to observe publication boundaries
}

type Follower struct {
	pullURL       string
	root          string
	source        string
	databases     []*replicaDB
	byNumber      map[int]*replicaDB
	lease         *os.File
	client        *http.Client
	pullIdle      time.Duration
	snapshotSlots chan struct{}
	sourceMu      sync.RWMutex
	lifecycleMu   sync.Mutex
	lifetime      context.Context
	cancelPolling context.CancelFunc
	active        sync.WaitGroup
	closeOnce     sync.Once
	closed        bool
}

// OpenFollower loads the last published generation of each configured database.
func OpenFollower(root, upstream string, databases []int) (*Follower, error) {
	return OpenFollowerWithPullIdleTimeout(root, upstream, databases, DefaultTransferIdleTimeout)
}

// OpenFollowerWithPullIdleTimeout configures the maximum time without a response or new bytes in a pull.
func OpenFollowerWithPullIdleTimeout(root, upstream string, databases []int, pullIdle time.Duration) (*Follower, error) {
	return OpenFollowerWithOptions(root, upstream, databases, FollowerOptions{PullIdleTimeout: pullIdle})
}

type FollowerOptions struct {
	PullIdleTimeout time.Duration
	// SnapshotConcurrency bounds concurrent snapshot pulls. Zero leaves them unlimited.
	SnapshotConcurrency int
}

// OpenFollowerWithOptions loads the replica and configures its pull policy.
func OpenFollowerWithOptions(root, upstream string, databases []int, options FollowerOptions) (*Follower, error) {
	address, err := url.Parse(upstream)
	if err != nil || (address.Scheme != "http" && address.Scheme != "https") ||
		address.Host == "" || address.User != nil || address.RawQuery != "" || address.Fragment != "" {
		return nil, errors.New("invalid replication source URL")
	}
	if root == "" || len(databases) == 0 {
		return nil, errors.New("replica root and databases are required")
	}
	if options.PullIdleTimeout <= 0 {
		return nil, errors.New("pull idle timeout must be positive")
	}
	if options.SnapshotConcurrency < 0 {
		return nil, errors.New("snapshot concurrency must be nonnegative")
	}
	root, err = filepath.Abs(filepath.Join(root, "follower-v1"))
	if err != nil {
		return nil, err
	}
	if err := makeDurableDir(root); err != nil {
		return nil, err
	}
	lease, err := lockFile(context.Background(), filepath.Join(root, ".replica-follower.lock"), unix.LOCK_EX, false)
	if err != nil {
		return nil, fmt.Errorf("lock replica root: %w", err)
	}
	f := &Follower{
		root:     root,
		pullURL:  address.JoinPath("pull").String(),
		byNumber: make(map[int]*replicaDB, len(databases)),
		lease:    lease,
		pullIdle: options.PullIdleTimeout,
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	if options.SnapshotConcurrency != 0 {
		f.snapshotSlots = make(chan struct{}, options.SnapshotConcurrency)
	}
	id, err := os.ReadFile(filepath.Join(root, "UPSTREAM"))
	if err == nil {
		if !validSource(string(id)) {
			f.Close()
			return nil, errors.New("invalid persisted upstream identity")
		}
		f.source = string(id)
	} else if !errors.Is(err, os.ErrNotExist) {
		f.Close()
		return nil, err
	}
	for _, number := range databases {
		if err := f.openDatabase(root, number); err != nil {
			f.Close()
			return nil, fmt.Errorf("open replica database %d: %w", number, err)
		}
	}
	if err := syncDir(root); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (f *Follower) openDatabase(root string, number int) error {
	if _, duplicate := f.byNumber[number]; duplicate || number < 0 {
		return errors.New("invalid or duplicate replica database")
	}
	db := &replicaDB{number: number, directory: filepath.Join(root, dbName(number)), minimumRebase: minimumRebaseBytes}
	if err := os.MkdirAll(db.directory, 0700); err != nil {
		return err
	}
	state := &report{}
	file, err := os.Open(filepath.Join(db.directory, "CURRENT"))
	if err == nil {
		var prior installed
		data, err := io.ReadAll(io.LimitReader(file, 1025))
		file.Close()
		if err != nil || len(data) > 1024 || json.Unmarshal(data, &prior) != nil || !validGeneration(prior.Generation) || !validSource(prior.Source) ||
			prior.Source != f.source || prior.Snapshot > prior.Sequence {
			return errors.New("invalid persisted replica generation")
		}
		state.view, err = loadView(filepath.Join(db.directory, prior.Generation), prior)
		if err != nil {
			return err
		}
		state.status = statusFor(state.view)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	keep := ""
	if state.view != nil {
		keep = state.view.Generation
	}
	if err := syncDir(db.directory); err != nil {
		return err
	}
	db.clean(keep)
	db.result.Store(state)
	f.byNumber[number] = db
	f.databases = append(f.databases, db)
	return nil
}

// Close cancels polling and waits for active operations before releasing the root lease.
func (f *Follower) Close() error {
	var err error
	f.closeOnce.Do(func() {
		f.lifecycleMu.Lock()
		f.closed = true
		if f.cancelPolling != nil {
			f.cancelPolling()
		}
		f.lifecycleMu.Unlock()

		f.active.Wait()
		if f.client != nil {
			f.client.CloseIdleConnections()
		}
		if f.lease != nil {
			err = f.lease.Close()
		}
	})
	return err
}

func (f *Follower) beginPolling(ctx context.Context) (context.Context, func(), error) {
	f.lifecycleMu.Lock()
	defer f.lifecycleMu.Unlock()
	if f.closed {
		return nil, nil, errors.New("follower is closed")
	}
	if f.lifetime == nil {
		f.lifetime, f.cancelPolling = context.WithCancel(context.Background())
	}
	f.active.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(f.lifetime, cancel)
	return ctx, func() {
		stop()
		cancel()
		f.active.Done()
	}, nil
}

// Poll attempts to advance each database in order, preserving its prior view on errors.
func (f *Follower) Poll(ctx context.Context) error {
	ctx, finish, err := f.beginPolling(ctx)
	if err != nil {
		return err
	}
	defer finish()

	var failures []error
	for _, db := range f.databases {
		if err := f.pollDatabase(ctx, db); err != nil {
			failures = append(failures, fmt.Errorf("replica database %d: %w", db.number, err))
		}
	}
	return errors.Join(failures...)
}

// Run polls each database independently until ctx is canceled or the follower is closed.
// Each worker waits interval after its own poll finishes. Pull errors appear in database status.
func (f *Follower) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("poll interval must be positive")
	}
	ctx, finish, err := f.beginPolling(ctx)
	if err != nil {
		return err
	}
	defer finish()

	var workers sync.WaitGroup
	for _, db := range f.databases {
		workers.Go(func() {
			for {
				f.pollDatabase(ctx, db)
				select {
				case <-ctx.Done():
					return
				case <-time.After(interval):
				}
			}
		})
	}
	workers.Wait()
	return nil
}

func (f *Follower) pollDatabase(ctx context.Context, db *replicaDB) error {
	db.pollGateOnce.Do(func() {
		db.pollGate = make(chan struct{}, 1)
	})
	select {
	case db.pollGate <- struct{}{}:
		defer func() { <-db.pollGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	previous := db.result.Load()
	next, err := f.fetch(ctx, db, previous.view)
	status := previous.status
	if next != nil {
		status.Ready = true
		status.Source = next.Source
		status.Sequence = next.Sequence
		status.Installs++
		if next.Snapshot > status.Snapshot {
			if next.Sequence == previous.status.Sequence {
				status.Rebases++
			} else {
				status.Bootstraps++
			}
		}
		status.Snapshot = next.Snapshot
	} else {
		next = previous.view
	}
	status.LastError = ""
	if err != nil {
		status.LastError = err.Error()
	}
	if next != previous.view {
		db.publishView(previous.view, next, status)
		db.clean(next.Generation)
	} else if status != previous.status {
		db.result.Store(&report{view: next, status: status})
	}
	return err
}

func (db *replicaDB) publishView(previous, next *view, status Status) {
	db.viewMu.Lock()
	defer db.viewMu.Unlock()
	if next.changes != nil {
		next.values = previous.values
		for key, value := range next.changes {
			if value == nil {
				delete(next.values, key)
			} else {
				next.values[key] = value
			}
		}
		next.changes = nil
	}
	db.result.Store(&report{view: next, status: status})
}

func statusFor(state *view) Status {
	return Status{Ready: true, Source: state.Source, Sequence: state.Sequence, Snapshot: state.Snapshot}
}

func (f *Follower) fetch(ctx context.Context, db *replicaDB, previous *view) (*view, error) {
	if db.pending != nil {
		if err := syncDir(db.directory); err != nil {
			return nil, err
		}
		next := db.pending
		db.pending = nil
		return next, nil
	}
	next, err := f.pull(ctx, db, previous, false)
	if !errors.Is(err, errRetainedDeltaRecovery) || ctx.Err() != nil {
		return next, err
	}
	next, snapshotErr := f.pull(ctx, db, previous, true)
	if snapshotErr != nil {
		return nil, errors.Join(err, fmt.Errorf("pull replacement snapshot: %w", snapshotErr))
	}
	return next, nil
}

func (f *Follower) pull(ctx context.Context, db *replicaDB, previous *view, forceSnapshot bool) (*view, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.pullURL, nil)
	if err != nil {
		return nil, err
	}
	var after uint64
	query := request.URL.Query()
	if previous != nil {
		after = previous.Sequence
	}
	if source := f.currentSource(); source != "" {
		query.Set("source", source)
	}
	query.Set("db", strconv.Itoa(db.number))
	query.Set("after", strconv.FormatUint(after, 10))
	var requested uint64
	now := time.Now()
	if forceSnapshot {
		query.Set("snapshot", "1")
	} else if previous != nil && db.requestedSnapshot > previous.Snapshot && !now.Before(db.retryRebaseAt) {
		requested = db.requestedSnapshot
		query.Set("rebase", strconv.FormatUint(requested, 10))
		db.retryRebaseAt = now.Add(rebaseRetryDelay)
	}
	request.URL.RawQuery = query.Encode()
	if f.snapshotSlots == nil {
		return f.pullOnce(request, db, previous, after, requested, forceSnapshot, false, false)
	}
	if !forceSnapshot {
		next, err := f.pullOnce(request, db, previous, after, requested, false, true, false)
		if !errors.Is(err, errSnapshotRequired) {
			return next, err
		}
	}
	// The probe has closed before we enter the queue. Timing starts again
	// only after a permit is available and the next request is sent.
	select {
	case f.snapshotSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-f.snapshotSlots
		return nil, err
	}
	if requested != 0 {
		db.retryRebaseAt = time.Now().Add(rebaseRetryDelay)
	}
	return f.pullOnce(request, db, previous, after, requested, forceSnapshot, false, true)
}

func (f *Follower) pullOnce(request *http.Request, db *replicaDB, previous *view, after, requested uint64, forceSnapshot, probeSnapshot, snapshotPermit bool) (*view, error) {
	defer func() {
		if snapshotPermit {
			<-f.snapshotSlots
		}
	}()
	ctx, progress := watchPullProgress(request.Context(), f.pullIdle)
	defer progress.close()
	request = request.Clone(ctx)
	if probeSnapshot {
		query := request.URL.Query()
		query.Set(deferSnapshotQuery, "1")
		request.URL.RawQuery = query.Encode()
	}
	response, err := f.client.Do(request)
	if err != nil {
		if errors.Is(context.Cause(ctx), errPullIdle) {
			return nil, errPullIdle
		}
		return nil, err
	}
	defer response.Body.Close()
	progress.received()
	if probeSnapshot && response.StatusCode == http.StatusNoContent && response.Header.Get(snapshotRequiredHeader) == "1" {
		return nil, errSnapshotRequired
	}
	body := bufio.NewReader(pullReader{ctx: ctx, body: response.Body, progress: progress})
	if response.StatusCode != http.StatusOK {
		message, err := io.ReadAll(io.LimitReader(body, 512))
		if errors.Is(err, errPullIdle) {
			return nil, err
		}
		return nil, fmt.Errorf("source HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	info, err := readManifest(body)
	if err != nil {
		return nil, fmt.Errorf("read replication manifest: %w", err)
	}
	if err := info.validate(db.number, after); err != nil {
		return nil, err
	}
	if source := f.currentSource(); source != "" && info.Source != source {
		return nil, errors.New("replication source identity changed")
	}
	if forceSnapshot && (info.Snapshot == 0 || info.Rebase) {
		return nil, errors.New("source did not return a replacement snapshot")
	}
	if info.Rebase && info.Snapshot != requested {
		return nil, errors.New("unexpected replica rebase")
	}
	if probeSnapshot && info.Snapshot != 0 {
		// Older sources ignore the probe option and stream the snapshot.
		// Reuse their response only when a permit can be acquired immediately.
		select {
		case f.snapshotSlots <- struct{}{}:
			snapshotPermit = true
		default:
			return nil, errSnapshotRequired
		}
	}
	if snapshotPermit && info.Snapshot == 0 {
		// The source can publish a different cutoff between the two requests.
		<-f.snapshotSlots
		snapshotPermit = false
	}
	if previous != nil && len(info.Files) == 0 {
		if err := readEnd(body); err != nil {
			return nil, err
		}
		db.considerSnapshot(previous, info.Available)
		return nil, nil
	}
	next, err := db.install(body, previous, info, f.pinSource)
	if err == nil {
		db.considerSnapshot(next, info.Available)
	}
	return next, err
}

func (f *Follower) currentSource() string {
	f.sourceMu.RLock()
	defer f.sourceMu.RUnlock()
	return f.source
}

func (f *Follower) pinSource(id string) error {
	f.sourceMu.Lock()
	defer f.sourceMu.Unlock()
	if f.source != "" && f.source != id {
		return errors.New("replication source identity changed")
	}
	if f.source == id {
		return nil
	}
	file, err := os.CreateTemp(f.root, ".UPSTREAM-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := writeSynced(file, []byte(id)); err != nil {
		return err
	}
	path := filepath.Join(f.root, "UPSTREAM")
	if err := os.Link(file.Name(), path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		retained, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(retained) != id {
			return errors.New("replication source identity changed")
		}
	}
	if err := syncDir(f.root); err != nil {
		return err
	}
	f.source = id
	return nil
}

func readEnd(body *bufio.Reader) error {
	if _, err := body.ReadByte(); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("trailing bytes in replication response")
	}
	return nil
}

func (db *replicaDB) install(body *bufio.Reader, previous *view, info manifest, pinSource func(string) error) (*view, error) {
	generation, err := os.MkdirTemp(db.directory, "generation-")
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			os.RemoveAll(generation)
		}
	}()
	base := info.Snapshot
	if previous != nil && (base == 0 || info.Rebase) {
		if base == 0 {
			base = previous.Snapshot
		}
		if err := linkPrevious(db.directory, generation, previous, base, !info.Rebase); err != nil {
			return nil, err
		}
	}
	if base != 0 {
		current, err := os.Create(filepath.Join(generation, "CURRENT"))
		if err != nil {
			return nil, err
		}
		if err := writeSynced(current, []byte(strconv.FormatUint(base, 10)+"\n")); err != nil {
			return nil, err
		}
	}
	for _, entry := range info.Files {
		path := filepath.Join(generation, entry.Name)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		_, err = io.CopyN(file, body, entry.Size)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("receive %s: %w", entry.Name, err)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return nil, err
		}
	}
	if err := readEnd(body); err != nil {
		return nil, err
	}
	meta := installed{
		Generation: filepath.Base(generation),
		Source:     info.Source,
		Sequence:   info.Last,
		Snapshot:   base,
	}
	var next *view
	if previous != nil && info.Snapshot == 0 && len(info.Files) > 0 {
		next, err = loadDeltaView(generation, meta, previous.Sequence, info.Files)
		if errors.Is(err, rustengine.ErrGoLogDeltaUnavailable) {
			next, err = loadView(generation, meta)
		}
		if err != nil && info.Available != nil && info.Available.Sequence > previous.Sequence {
			return nil, fmt.Errorf("%w: %w", errRetainedDeltaRecovery, err)
		}
	} else {
		next, err = loadView(generation, meta)
	}
	if err != nil {
		return nil, err
	}
	if err := syncDir(generation); err != nil {
		return nil, err
	}
	if err := syncDir(db.directory); err != nil {
		return nil, err
	}
	if err := pinSource(info.Source); err != nil {
		return nil, err
	}
	renamed, err := db.publish(meta)
	if !renamed {
		return nil, err
	}
	keep = true
	if err != nil {
		db.pending = next
		return nil, err
	}
	return next, nil
}

func loadDeltaView(generation string, meta installed, after uint64, files []fileEntry) (*view, error) {
	ranges := make([]rustengine.GoLogRange, 0, len(files))
	for _, entry := range files {
		first, last, err := segmentRange(entry.Name)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, rustengine.GoLogRange{First: first, Last: last})
	}
	changes := make(map[string]*datastruct.DataValue)
	reset, err := rustengine.RecoverGoLogDelta(generation, after, ranges, func(key, raw []byte, deleted bool) error {
		var value *datastruct.DataValue
		if !deleted {
			entry, err := datastruct.DeserializeDataValue(raw)
			if err != nil {
				return err
			}
			if !entry.IsExpired() {
				value = entry
			}
		}
		changes[string(key)] = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	if reset {
		for key, value := range changes {
			if value == nil {
				delete(changes, key)
			}
		}
		return &view{installed: meta, values: changes}, nil
	}
	return &view{installed: meta, changes: changes}, nil
}

func loadView(generation string, meta installed) (*view, error) {
	cutoff, err := readCutoff(generation)
	if err != nil {
		return nil, err
	}
	if cutoff != meta.Snapshot {
		return nil, errors.New("replica snapshot disagrees with published cutoff")
	}
	values := make(map[string]*datastruct.DataValue)
	recovered, err := rustengine.RecoverGoLog(generation, func(key, raw []byte) error {
		entry, err := datastruct.DeserializeDataValue(raw)
		if err != nil {
			return err
		}
		if !entry.IsExpired() {
			values[string(key)] = entry
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if recovered.LastSequence != meta.Sequence || recovered.ActiveEnd != 0 {
		return nil, errors.New("recovered replica sequence disagrees with published generation")
	}
	return &view{installed: meta, values: values}, nil
}

func linkPrevious(directory, destination string, previous *view, cutoff uint64, retainSnapshot bool) error {
	if previous.Sequence == 0 {
		return nil
	}
	src := filepath.Join(directory, previous.Generation)
	segments, err := listSegments(src, cutoff)
	if err != nil {
		return err
	}
	// Keep the installed recovery chain complete even when its old bytes do not
	// need to be decoded again to advance the in-memory view.
	sequence := cutoff
	for _, segment := range segments {
		if segment.first-1 > sequence || (sequence != cutoff && segment.first <= sequence) || segment.last <= sequence {
			return fmt.Errorf("retained segment %q does not follow sequence %d", segment.name, sequence)
		}
		if err := os.Link(filepath.Join(src, segment.name), filepath.Join(destination, segment.name)); err != nil {
			return err
		}
		sequence = segment.last
	}
	if sequence != previous.Sequence {
		return errors.New("retained segments do not reach the previous replica sequence")
	}
	if retainSnapshot && previous.Snapshot != 0 {
		name := snapshotName(previous.Snapshot)
		return os.Link(filepath.Join(src, name), filepath.Join(destination, name))
	}
	return nil
}

func (db *replicaDB) publish(meta installed) (renamed bool, err error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return false, err
	}
	file, err := os.CreateTemp(db.directory, ".CURRENT-")
	if err != nil {
		return false, err
	}
	defer os.Remove(file.Name())
	if err := writeSynced(file, data); err != nil {
		return false, err
	}
	observe := db.onCurrentPublish
	if observe != nil {
		observe(currentBeforeRename)
	}
	if err := os.Rename(file.Name(), filepath.Join(db.directory, "CURRENT")); err != nil {
		return false, err
	}
	// Once rename succeeds, preserve both generations on a sync failure.
	err = syncDir(db.directory)
	if err == nil && observe != nil {
		observe(currentAfterDirectorySync)
	}
	return true, err
}

func validGeneration(name string) bool {
	suffix, ok := strings.CutPrefix(name, "generation-")
	if !ok || suffix == "" {
		return false
	}
	_, err := strconv.ParseUint(suffix, 10, 64)
	return err == nil
}

func (db *replicaDB) clean(keep string) {
	entries, err := os.ReadDir(db.directory)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name != keep && (validGeneration(name) || strings.HasPrefix(name, ".CURRENT-")) {
			os.RemoveAll(filepath.Join(db.directory, name))
		}
	}
}

// Handler exposes read-only string lookups and independent database cursors.
func (f *Follower) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", f.serveStatus)
	mux.HandleFunc("GET /get", f.serveGet)
	return mux
}

func (f *Follower) lookupDB(w http.ResponseWriter, r *http.Request) *replicaDB {
	w.Header().Set("Cache-Control", "no-store")
	number, err := parseDB(r.URL.Query().Get("db"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}
	db, ok := f.byNumber[number]
	if !ok {
		http.Error(w, "database is not followed", http.StatusNotFound)
		return nil
	}
	return db
}

func (f *Follower) serveStatus(w http.ResponseWriter, r *http.Request) {
	if db := f.lookupDB(w, r); db != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(db.result.Load().status)
	}
}

func (f *Follower) serveGet(w http.ResponseWriter, r *http.Request) {
	db := f.lookupDB(w, r)
	if db == nil {
		return
	}
	query := r.URL.Query()
	key := query.Get("key")
	db.viewMu.RLock()
	state := db.result.Load().view
	var value *datastruct.DataValue
	if state != nil {
		value = state.values[key]
	}
	db.viewMu.RUnlock()
	if state == nil {
		http.Error(w, "replica has not bootstrapped", http.StatusServiceUnavailable)
		return
	}
	if !query.Has("key") {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	if value == nil || value.IsExpired() {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	switch value := value.Value.(type) {
	case *datastruct.String:
		io.WriteString(w, value.Data)
	case *datastruct.BytesString:
		w.Write(value.Data)
	case string:
		io.WriteString(w, value)
	default:
		http.Error(w, "key does not contain a string", http.StatusConflict)
	}
}
