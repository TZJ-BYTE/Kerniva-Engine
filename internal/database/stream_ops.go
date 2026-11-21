package database

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

type streamID struct {
	ms  uint64
	seq uint64
}

func compareStreamID(a, b streamID) int {
	if a.ms < b.ms {
		return -1
	}
	if a.ms > b.ms {
		return 1
	}
	if a.seq < b.seq {
		return -1
	}
	if a.seq > b.seq {
		return 1
	}
	return 0
}

func formatStreamID(id streamID) string {
	return strconv.FormatUint(id.ms, 10) + "-" + strconv.FormatUint(id.seq, 10)
}

func parseStreamIDString(raw string) (streamID, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	return streamID{ms: ms, seq: seq}, nil
}

func parseStreamCursor(raw string) (streamID, error) {
	if !strings.Contains(raw, "-") {
		raw += "-0"
	}
	return parseStreamIDString(raw)
}

func parseStreamBound(raw string, end bool) (streamID, error) {
	switch raw {
	case "-":
		return streamID{}, nil
	case "+":
		return streamID{ms: ^uint64(0), seq: ^uint64(0)}, nil
	}

	if !strings.Contains(raw, "-") {
		ms, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		if end {
			return streamID{ms: ms, seq: ^uint64(0)}, nil
		}
		return streamID{ms: ms, seq: 0}, nil
	}

	return parseStreamIDString(raw)
}

func nextStreamID(s *datastruct.Stream, raw string) (streamID, error) {
	last := streamID{ms: s.LastMS, seq: s.LastSeq}

	if raw == "*" {
		now := uint64(time.Now().UnixMilli())
		if now < last.ms {
			now = last.ms
		}
		if now == last.ms {
			return streamID{ms: now, seq: last.seq + 1}, nil
		}
		return streamID{ms: now, seq: 0}, nil
	}

	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	var seq uint64
	if parts[1] == "*" {
		if ms < last.ms {
			return streamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if ms == last.ms {
			seq = last.seq + 1
		} else {
			seq = 0
		}
	} else {
		seq, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
	}

	id := streamID{ms: ms, seq: seq}
	if id.ms == 0 && id.seq == 0 {
		return streamID{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if compareStreamID(id, last) <= 0 {
		return streamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return id, nil
}

func cloneStreamFields(fields []string) []string {
	out := make([]string, len(fields))
	for i := range fields {
		out[i] = strings.Clone(fields[i])
	}
	return out
}

func cloneStreamEntries(entries []datastruct.StreamEntry) []datastruct.StreamEntry {
	out := make([]datastruct.StreamEntry, len(entries))
	for i := range entries {
		out[i] = datastruct.StreamEntry{
			ID:     strings.Clone(entries[i].ID),
			Fields: cloneStreamFields(entries[i].Fields),
		}
	}
	return out
}

func (db *Database) xAddLocked(key, rawID string, fields []string, maxLen int, noMkStream bool) (string, error) {
	if len(fields) == 0 || len(fields)%2 != 0 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'xadd' command")
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return "", err
	}
	if !db.evictIfNeeded() {
		return "", fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var lsmBytes []byte
	var serErr error
	var createdID string

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}

	dv, exists := sh.data[key]
	if !exists {
		if noMkStream {
			sh.lock.Unlock()
			return "", nil
		}
		stableKey = strings.Clone(key)
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		stableKey = key
	}

	oldSize := dv.ApproximateSize()
	var stream *datastruct.Stream
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		if noMkStream {
			delete(sh.data, stableKey)
			if !exists {
				memDelta -= int64(len(stableKey))
			}
			sh.lock.Unlock()
			return "", nil
		}
		stream = &datastruct.Stream{}
		dv.Value = stream
		dv.ExpireTime = 0
	} else {
		sv, ok := dv.Value.(*datastruct.Stream)
		if !ok {
			sh.lock.Unlock()
			return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		stream = sv
	}

	id, err := nextStreamID(stream, rawID)
	if err != nil {
		sh.lock.Unlock()
		return "", err
	}

	stream.Entries = append(stream.Entries, datastruct.StreamEntry{
		ID:     formatStreamID(id),
		Fields: cloneStreamFields(fields),
	})
	stream.LastMS = id.ms
	stream.LastSeq = id.seq
	if maxLen >= 0 && len(stream.Entries) > maxLen {
		stream.Entries = append([]datastruct.StreamEntry(nil), stream.Entries[len(stream.Entries)-maxLen:]...)
	}
	createdID = formatStreamID(id)

	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta += newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", stableKey, serErr)
		} else {
			return "", serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(stableKey, lsmBytes); err != nil {
			return "", err
		}
	}
	db.BumpKeyVersions(stableKey)
	return createdID, nil
}

func (db *Database) xLenLocked(key string) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()
	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return 0, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.RUnlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n := len(stream.Entries)
	sh.lock.RUnlock()
	return int64(n), nil
}

func (db *Database) xRangeLocked(key, start, end string, count int, reverse bool) ([]datastruct.StreamEntry, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}

	startID, err := parseStreamBound(start, false)
	if err != nil {
		return nil, err
	}
	endID, err := parseStreamBound(end, true)
	if err != nil {
		return nil, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()
	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return []datastruct.StreamEntry{}, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	out := make([]datastruct.StreamEntry, 0)
	appendEntry := func(entry datastruct.StreamEntry) bool {
		out = append(out, datastruct.StreamEntry{
			ID:     strings.Clone(entry.ID),
			Fields: cloneStreamFields(entry.Fields),
		})
		return count > 0 && len(out) >= count
	}

	if reverse {
		for i := len(stream.Entries) - 1; i >= 0; i-- {
			id, err := parseStreamIDString(stream.Entries[i].ID)
			if err != nil {
				continue
			}
			if compareStreamID(id, startID) > 0 || compareStreamID(id, endID) < 0 {
				continue
			}
			if appendEntry(stream.Entries[i]) {
				break
			}
		}
	} else {
		for i := range stream.Entries {
			id, err := parseStreamIDString(stream.Entries[i].ID)
			if err != nil {
				continue
			}
			if compareStreamID(id, startID) < 0 || compareStreamID(id, endID) > 0 {
				continue
			}
			if appendEntry(stream.Entries[i]) {
				break
			}
		}
	}
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) xDelLocked(key string, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error
	var removed int64

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	toDelete := make(map[string]struct{}, len(ids))
	for i := range ids {
		toDelete[ids[i]] = struct{}{}
	}
	filtered := stream.Entries[:0]
	for i := range stream.Entries {
		if _, ok := toDelete[stream.Entries[i].ID]; ok {
			removed++
			continue
		}
		filtered = append(filtered, stream.Entries[i])
	}
	if removed == 0 {
		sh.lock.Unlock()
		return 0, nil
	}
	stream.Entries = append([]datastruct.StreamEntry(nil), filtered...)
	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
		} else {
			return 0, serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(key)
	return removed, nil
}

func (db *Database) xTrimLocked(key string, maxLen int) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error
	var removed int64

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if maxLen < 0 {
		sh.lock.Unlock()
		return 0, fmt.Errorf("ERR value is out of range, must be positive")
	}
	if len(stream.Entries) <= maxLen {
		sh.lock.Unlock()
		return 0, nil
	}

	oldSize := dv.ApproximateSize()
	removed = int64(len(stream.Entries) - maxLen)
	stream.Entries = append([]datastruct.StreamEntry(nil), stream.Entries[len(stream.Entries)-maxLen:]...)
	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
		} else {
			return 0, serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(key)
	return removed, nil
}

// XReadResult XREAD 单条 stream 的结果
type XReadResult struct {
	Key     string
	Entries []datastruct.StreamEntry
}

// XRead 从多个 stream 读取数据（从指定 id 之后开始）
func (db *Database) xReadLocked(keys []string, ids []string, count int) ([]XReadResult, error) {
	if len(keys) == 0 || len(keys) != len(ids) {
		return nil, fmt.Errorf("ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified")
	}
	now := time.Now().UnixMilli()
	results := make([]XReadResult, 0, len(keys))

	for ki := range keys {
		key := keys[ki]
		rawID := ids[ki]
		if _, _, err := db.getLiveValue(key); err != nil {
			return nil, err
		}

		// Resolve "$" to the current last ID of the stream
		if rawID == "$" {
			sh := db.getShard(key)
			sh.lock.RLock()
			dv2, exists2 := sh.data[key]
			if exists2 && dv2 != nil {
				if s, ok := dv2.Value.(*datastruct.Stream); ok && len(s.Entries) > 0 {
					rawID = s.Entries[len(s.Entries)-1].ID
				} else {
					rawID = "0-0"
				}
			} else {
				rawID = "0-0"
			}
			sh.lock.RUnlock()
		}

		startID, err := parseStreamBound(rawID, false)
		if err != nil {
			return nil, err
		}

		sh := db.getShard(key)
		sh.lock.RLock()
		dv, exists := sh.data[key]
		if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
			sh.lock.RUnlock()
			continue
		}
		stream, ok := dv.Value.(*datastruct.Stream)
		if !ok {
			sh.lock.RUnlock()
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		out := make([]datastruct.StreamEntry, 0)
		for i := range stream.Entries {
			id, parseErr := parseStreamIDString(stream.Entries[i].ID)
			if parseErr != nil {
				continue
			}
			if compareStreamID(id, startID) <= 0 {
				continue
			}
			out = append(out, datastruct.StreamEntry{
				ID:     strings.Clone(stream.Entries[i].ID),
				Fields: cloneStreamFields(stream.Entries[i].Fields),
			})
			if count > 0 && len(out) >= count {
				break
			}
		}
		sh.lock.RUnlock()

		if len(out) > 0 {
			results = append(results, XReadResult{Key: key, Entries: out})
		}
	}

	return results, nil
}

// XGroupCreate 创建消费者组
func (db *Database) xGroupCreateLocked(key, groupName, startID string, mkStream bool) error {
	if _, _, err := db.getLiveValue(key); err != nil {
		return err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}

	var stream *datastruct.Stream
	var oldSize int64

	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		if !mkStream {
			sh.lock.Unlock()
			return fmt.Errorf("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
		}
		stableKey := strings.Clone(key)
		stream = &datastruct.Stream{}
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now, Value: stream}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		var ok bool
		stream, ok = dv.Value.(*datastruct.Stream)
		if !ok {
			sh.lock.Unlock()
			return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		oldSize = dv.ApproximateSize()
		// Check if group already exists
		for _, g := range stream.Groups {
			if g.Name == groupName {
				sh.lock.Unlock()
				return fmt.Errorf("BUSYGROUP Consumer Group name already exists")
			}
		}
	}

	var groupID streamID
	if startID == "$" {
		groupID = streamID{ms: stream.LastMS, seq: stream.LastSeq}
	} else {
		var err error
		groupID, err = parseStreamCursor(startID)
		if err != nil {
			sh.lock.Unlock()
			return err
		}
	}
	newGroup := datastruct.ConsumerGroup{
		Name:            strings.Clone(groupName),
		LastDeliveredID: formatStreamID(groupID),
	}
	stream.Groups = append(stream.Groups, newGroup)
	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		return nil
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return err
		}
	}
	db.BumpKeyVersions(key)
	return nil
}

// XGroupDestroy 销毁消费者组
func (db *Database) xGroupDestroyLocked(key, groupName string) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error
	var destroyed int64

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	for i := range stream.Groups {
		if stream.Groups[i].Name == groupName {
			stream.Groups = append(stream.Groups[:i], stream.Groups[i+1:]...)
			destroyed = 1
			break
		}
	}
	if destroyed == 0 {
		sh.lock.Unlock()
		return 0, nil
	}

	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
			return destroyed, nil
		}
		return 0, serErr
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(key)
	return destroyed, nil
}

// XGroupSetID 设置消费者组最后投递 ID
func (db *Database) xGroupSetIDLocked(key, groupName, lastID string) error {
	if _, _, err := db.getLiveValue(key); err != nil {
		return err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return fmt.Errorf("ERR no such key")
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.Unlock()
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	group := stream.FindGroup(groupName)
	if group == nil {
		sh.lock.Unlock()
		return fmt.Errorf("NOGROUP No such consumer group '%s' for key name '%s'", groupName, key)
	}

	var groupID streamID
	if lastID == "$" {
		groupID = streamID{ms: stream.LastMS, seq: stream.LastSeq}
	} else {
		var err error
		groupID, err = parseStreamBound(lastID, false)
		if err != nil {
			sh.lock.Unlock()
			return err
		}
	}

	oldSize := dv.ApproximateSize()
	group.LastDeliveredID = formatStreamID(groupID)
	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
			return nil
		}
		return serErr
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return err
		}
	}
	db.BumpKeyVersions(key)
	return nil
}

// XReadGroup 从消费者组读取数据
func (db *Database) xReadGroupLocked(groupName, consumerName, key string, id string, count int, blockMs int64, noAck bool) ([]datastruct.StreamEntry, error) {
	history := id != ">"
	var startID streamID
	if history {
		var err error
		startID, err = parseStreamCursor(id)
		if err != nil {
			return nil, err
		}
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return nil, fmt.Errorf("NOGROUP No such consumer group '%s' for key name '%s'", groupName, key)
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.Unlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	group := stream.FindGroup(groupName)
	if group == nil {
		sh.lock.Unlock()
		return nil, fmt.Errorf("NOGROUP No such consumer group '%s' for key name '%s'", groupName, key)
	}

	if !history {
		var err error
		startID, err = parseStreamCursor(group.LastDeliveredID)
		if err != nil {
			sh.lock.Unlock()
			return nil, err
		}
	}
	oldSize := dv.ApproximateSize()
	consumer := group.FindConsumer(consumerName)
	if consumer == nil {
		group.Consumers = append(group.Consumers, datastruct.Consumer{
			Name:     strings.Clone(consumerName),
			SeenTime: now,
		})
	} else {
		consumer.SeenTime = now
	}

	// Older versions could persist several PEL rows for one ID. Retain the
	// latest delivery's owner and metadata before applying this delivery.
	pendingByID := make(map[string]int, len(group.Pel))
	pending := group.Pel[:0]
	for _, entry := range group.Pel {
		if index, duplicate := pendingByID[entry.ID]; duplicate {
			if entry.DeliveredAt >= pending[index].DeliveredAt {
				pending[index] = entry
			}
			continue
		}
		pendingByID[entry.ID] = len(pending)
		pending = append(pending, entry)
	}
	group.Pel = pending
	out := make([]datastruct.StreamEntry, 0)
	if history {
		sort.Slice(group.Pel, func(i, j int) bool {
			a, _ := parseStreamIDString(group.Pel[i].ID)
			b, _ := parseStreamIDString(group.Pel[j].ID)
			return compareStreamID(a, b) < 0
		})
		entriesByID := make(map[string]datastruct.StreamEntry, len(stream.Entries))
		for _, entry := range stream.Entries {
			entriesByID[entry.ID] = entry
		}
		for i := range group.Pel {
			entry := &group.Pel[i]
			entryID, err := parseStreamIDString(entry.ID)
			if err != nil || entry.ConsumerName != consumerName || compareStreamID(entryID, startID) <= 0 {
				continue
			}
			result := datastruct.StreamEntry{ID: strings.Clone(entry.ID)}
			if value, found := entriesByID[entry.ID]; found {
				result.Fields = cloneStreamFields(value.Fields)
				entry.DeliveredAt = now
				entry.DeliveryCount++
			}
			out = append(out, result)
			if count > 0 && len(out) >= count {
				break
			}
		}
	} else {
		for _, entry := range stream.Entries {
			entryID, err := parseStreamIDString(entry.ID)
			if err != nil || compareStreamID(entryID, startID) <= 0 {
				continue
			}
			out = append(out, datastruct.StreamEntry{
				ID:     strings.Clone(entry.ID),
				Fields: cloneStreamFields(entry.Fields),
			})
			if !noAck {
				pending := datastruct.PendingEntry{
					ID:            entry.ID,
					ConsumerName:  strings.Clone(consumerName),
					DeliveredAt:   now,
					DeliveryCount: 1,
				}
				if index, exists := pendingByID[entry.ID]; exists {
					group.Pel[index] = pending
				} else {
					pendingByID[entry.ID] = len(group.Pel)
					group.Pel = append(group.Pel, pending)
				}
			}
			group.LastDeliveredID = entry.ID
			if count > 0 && len(out) >= count {
				break
			}
		}
		sort.Slice(group.Pel, func(i, j int) bool {
			a, _ := parseStreamIDString(group.Pel[i].ID)
			b, _ := parseStreamIDString(group.Pel[j].ID)
			return compareStreamID(a, b) < 0
		})
	}

	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "strong" {
			return nil, serErr
		}
		logger.Error("Failed to serialize value for key %s: %v", key, serErr)
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return nil, err
		}
	}
	db.BumpKeyVersions(key)
	return out, nil
}

// XAck 确认消费组消息
func (db *Database) xAckLocked(key, groupName string, ids []string) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error
	var acknowledged int64

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	group := stream.FindGroup(groupName)
	if group == nil {
		sh.lock.Unlock()
		return 0, nil
	}

	oldSize := dv.ApproximateSize()
	for _, id := range ids {
		if group.RemovePelEntry(id) {
			acknowledged++
		}
	}

	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta = newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "strong" {
			return 0, serErr
		}
		logger.Error("Failed to serialize value for key %s: %v", key, serErr)
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(key)
	return acknowledged, nil
}

// XPendingSummary XPENDING 汇总结果
type XPendingSummary struct {
	Total       int64
	StartID     string
	EndID       string
	ConsumerMap map[string]int64
}

// XPendingDetail XPENDING 明细条目
type XPendingDetail struct {
	ID            string
	ConsumerName  string
	IdleMs        int64
	DeliveryCount int64
}

// XPending 获取 PEL 信息
func (db *Database) xPendingLocked(key, groupName string, start, end string, count int, consumer string) (interface{}, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return nil, nil
	}
	stream, ok := dv.Value.(*datastruct.Stream)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	group := stream.FindGroup(groupName)
	if group == nil {
		sh.lock.RUnlock()
		return nil, nil
	}

	// If no start/end, return summary
	if start == "" && end == "" {
		summary := XPendingSummary{
			Total:       int64(len(group.Pel)),
			ConsumerMap: make(map[string]int64),
		}
		if len(group.Pel) > 0 {
			summary.StartID = group.Pel[0].ID
			summary.EndID = group.Pel[len(group.Pel)-1].ID
		}
		for _, pe := range group.Pel {
			summary.ConsumerMap[pe.ConsumerName]++
		}
		sh.lock.RUnlock()
		return summary, nil
	}

	// Filter by ID range and consumer
	startID, err := parseStreamIDString(start)
	if err != nil {
		startID = streamID{}
	}
	endID, err := parseStreamIDString(end)
	if err != nil {
		endID = streamID{ms: ^uint64(0), seq: ^uint64(0)}
	}

	details := make([]XPendingDetail, 0)
	for _, pe := range group.Pel {
		if consumer != "" && pe.ConsumerName != consumer {
			continue
		}
		pid, parseErr := parseStreamIDString(pe.ID)
		if parseErr != nil {
			continue
		}
		if compareStreamID(pid, startID) < 0 || compareStreamID(pid, endID) > 0 {
			continue
		}
		details = append(details, XPendingDetail{
			ID:            pe.ID,
			ConsumerName:  pe.ConsumerName,
			IdleMs:        now - pe.DeliveredAt,
			DeliveryCount: pe.DeliveryCount,
		})
		if count > 0 && len(details) >= count {
			break
		}
	}
	sh.lock.RUnlock()
	return details, nil
}

func (db *Database) XAdd(key, rawID string, fields []string, maxLen int, noMkStream bool) (result0 string, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xAddLocked(key, rawID, fields, maxLen, noMkStream)
}

func (db *Database) XLen(key string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.xLenLocked(key)
}

func (db *Database) XRange(key, start, end string, count int, reverse bool) (result0 []datastruct.StreamEntry, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.xRangeLocked(key, start, end, count, reverse)
}

func (db *Database) XDel(key string, ids []string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xDelLocked(key, ids)
}

func (db *Database) XTrim(key string, maxLen int) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xTrimLocked(key, maxLen)
}

func (db *Database) XRead(keys []string, ids []string, count int) (result0 []XReadResult, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.xReadLocked(keys, ids, count)
}

func (db *Database) XGroupCreate(key, groupName, startID string, mkStream bool) (err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xGroupCreateLocked(key, groupName, startID, mkStream)
}

func (db *Database) XGroupDestroy(key, groupName string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xGroupDestroyLocked(key, groupName)
}

func (db *Database) XGroupSetID(key, groupName, lastID string) (err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xGroupSetIDLocked(key, groupName, lastID)
}

func (db *Database) XReadGroup(groupName, consumerName, key string, id string, count int, blockMs int64, noAck bool) (result0 []datastruct.StreamEntry, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xReadGroupLocked(groupName, consumerName, key, id, count, blockMs, noAck)
}

func (db *Database) XAck(key, groupName string, ids []string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.xAckLocked(key, groupName, ids)
}

func (db *Database) XPending(key, groupName string, start, end string, count int, consumer string) (result0 interface{}, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.xPendingLocked(key, groupName, start, end, count, consumer)
}
