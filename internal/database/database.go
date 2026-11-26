package database

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// DatabaseType 数据库类型
type DatabaseType int

const (
	// MemoryOnly 纯内存模式（默认）
	MemoryOnly DatabaseType = iota
	// Persistent 使用 Kerniva Engine 的持久化模式
	Persistent
)

const (
	// ShardCount 分段锁数量
	ShardCount = 256
)

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Type              DatabaseType
	DataDir           string
	Options           *EngineOptions
	ColdStartStrategy string
	StorageBackend    string
	GoLogSegmentAge   time.Duration
	GoLogGroupMax     int
	WriteMode         string
	Durability        string
	MaxMemory         int64
	MaxMemoryPolicy   string
}

type EngineOptions struct {
	SegmentSizeBytes   uint64
	CheckpointAfterOps uint64
}

func DefaultEngineOptions() *EngineOptions {
	return &EngineOptions{
		SegmentSizeBytes:   4 << 20,
		CheckpointAfterOps: 1024,
	}
}

// shard 分段结构
type shard struct {
	data map[string]*datastruct.DataValue
	lock sync.RWMutex
}

// Database 数据库结构
type Database struct {
	operationMu sync.RWMutex
	hotLocks    [ShardCount]sync.RWMutex
	// Each hot lock protects its pending entries. Only durable string writes
	// allocate entries, and same-key callers wait for publication or failure.
	pendingStringWrites [ShardCount]map[string]chan struct{}
	mutation            *mutationState
	closed              bool
	changeMu            sync.Mutex
	changeCh            chan struct{}
	id                  int
	shards              [ShardCount]shard
	versions            [ShardCount]keyVersionShard
	modEpoch            atomic.Uint64

	// Kerniva Engine（可选）
	rustEngine    *rustengine.Engine
	goLog         *golog.Writer
	writeBatch    func([]rustengine.WriteOp) error
	config        *DatabaseConfig
	keyHeat       *KeyTopK
	lsmLastError  atomic.Value
	lsmDeleteStop atomic.Value

	// 内存管理
	usedMemory     int64 // 当前使用内存（字节），原子操作
	pendingMemory  atomic.Int64
	maxMemory      int64  // 最大内存限制（字节）
	evictionPolicy string // 淘汰策略

	expireMu     sync.Mutex
	expireStop   chan struct{}
	expireWG     sync.WaitGroup
	expireClosed bool

	expireRuns          atomic.Uint64
	expireScanned       atomic.Uint64
	expireExpired       atomic.Uint64
	expireDurationNanos atomic.Uint64
}

// DefaultDatabaseConfig 返回默认配置
func DefaultDatabaseConfig() *DatabaseConfig {
	return &DatabaseConfig{
		Type:              MemoryOnly,
		DataDir:           "",
		ColdStartStrategy: "no_load",
	}
}

// NewDatabase 创建新数据库（使用默认配置，纯内存）
func NewDatabase(id int) *Database {
	cfg := config.DefaultConfig()
	db := &Database{
		id:             id,
		config:         DefaultDatabaseConfig(),
		maxMemory:      cfg.MaxMemory,
		evictionPolicy: cfg.MaxMemoryPolicy,
	}
	if strings.Contains(db.evictionPolicy, "lru") {
		db.keyHeat = NewKeyTopK(1024)
	}
	return db
}

// NewDatabaseWithConfig 使用配置创建数据库
func NewDatabaseWithConfig(id int, dbConfig *DatabaseConfig) (*Database, error) {
	if dbConfig == nil {
		dbConfig = DefaultDatabaseConfig()
	}
	// 获取全局配置
	globalConfig := config.DefaultConfig()

	db := &Database{
		id:             id,
		config:         dbConfig,
		maxMemory:      globalConfig.MaxMemory,
		evictionPolicy: globalConfig.MaxMemoryPolicy,
	}
	if dbConfig.MaxMemory > 0 {
		db.maxMemory = dbConfig.MaxMemory
	}
	if dbConfig.MaxMemoryPolicy != "" {
		db.evictionPolicy = dbConfig.MaxMemoryPolicy
	}
	if strings.Contains(db.evictionPolicy, "lru") {
		db.keyHeat = NewKeyTopK(1024)
	}

	// 如果启用持久化，则初始化 Kerniva Engine
	if dbConfig.Type == Persistent {
		if dbConfig.DataDir == "" {
			return nil, fmt.Errorf("data directory is required for persistence mode")
		}

		options := dbConfig.Options
		if options == nil {
			options = DefaultEngineOptions()
		}

		if dbConfig.StorageBackend == config.StorageGoLog {
			if err := db.openGoLog(options); err != nil {
				return nil, err
			}
			return db, nil
		}

		var err error
		db.rustEngine, err = rustengine.Open(rustengine.Options{
			DataDir:            dbConfig.DataDir,
			SegmentSizeBytes:   options.SegmentSizeBytes,
			CheckpointAfterOps: options.CheckpointAfterOps,
			SyncPolicy:         rustSyncPolicy(dbConfig.Durability),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to open Rust engine: %v", err)
		}
		db.writeBatch = db.rustEngine.WriteBatch

		strategy := strings.ToLower(strings.TrimSpace(dbConfig.ColdStartStrategy))
		switch strategy {
		case "load_all":
			// 全量加载到内存
			if err := db.loadAllFromPersistence(); err != nil {
				_ = db.rustEngine.Close()
				return nil, fmt.Errorf("load persisted data: %w", err)
			}
		case "lazy_load":
			logger.Info("Persistence lazy load enabled, will fallback on read")
		case "no_load", "":
		default:
			logger.Info("Persistence cold start: no data loading")
		}
	}

	return db, nil
}

func (db *Database) StartExpireCleaner() {
	db.expireMu.Lock()
	defer db.expireMu.Unlock()
	if db.expireClosed || db.expireStop != nil {
		return
	}
	ch := make(chan struct{})
	db.expireWG.Add(1)
	db.expireStop = ch
	go func() {
		defer db.expireWG.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				start := time.Now()
				db.expireRuns.Add(1)
				scanned, expired := db.expireSample(time.Now().UnixMilli(), 64)
				db.expireScanned.Add(uint64(scanned))
				db.expireExpired.Add(uint64(expired))
				db.expireDurationNanos.Add(uint64(time.Since(start).Nanoseconds()))
				if db.expireRuns.Load()%50 == 0 {
					db.runMaintenance()
				}
			case <-ch:
				return
			}
		}
	}()
}

func (db *Database) StopExpireCleaner() {
	db.expireMu.Lock()
	defer db.expireMu.Unlock()
	db.stopExpireCleanerLocked()
}

func (db *Database) stopExpireCleanerLocked() {
	if db.expireStop == nil {
		return
	}
	close(db.expireStop)
	db.expireStop = nil
	db.expireWG.Wait()
}

func (db *Database) expireSample(nowMs int64, samples int) (scanned int, expired int) {
	if !executionGate.TryRLock() {
		return
	}
	defer UnlockSharedExecution()
	// Expiration is best effort. A queued RWMutex writer would stop new hot
	// readers while an unrelated durable write holds the lifecycle read lock.
	if !db.operationMu.TryLock() {
		return
	}
	defer db.operationMu.Unlock()
	if db.closed {
		return
	}
	db.beginMutation()
	defer func() {
		if db.finishMutation(nil) != nil {
			expired = 0
		}
	}()
	for i := 0; i < samples; i++ {
		shard := &db.shards[rand.Intn(ShardCount)]
		shard.lock.RLock()
		var k string
		var v *datastruct.DataValue
		for kk, vv := range shard.data {
			k = kk
			v = vv
			break
		}
		shard.lock.RUnlock()
		if v == nil {
			continue
		}
		scanned++
		if v.ExpireTime > 0 && nowMs > v.ExpireTime {
			if db.deleteLocked(k) {
				expired++
			}
		}
	}
	return scanned, expired
}

// getShardIndex 获取分段索引
func getShardIndex(key string) int {
	h := fnv.New32a()
	h.Write(stringToBytesRO(key))
	return int(h.Sum32()) % ShardCount
}

// getShard 获取 key 对应的 shard
func (db *Database) getShard(key string) *shard {
	sh := &db.shards[getShardIndex(key)]
	if db.mutation != nil {
		db.captureMemoryValue(key, sh)
	}
	return sh
}

func deserializeDataValue(data []byte) (*datastruct.DataValue, error) {
	return datastruct.DeserializeDataValue(data)
}

// updateMemoryUsage 更新内存使用量
func (db *Database) updateMemoryUsage(delta int64) {
	atomic.AddInt64(&db.usedMemory, delta)
}

func (db *Database) persistenceWriteMode() string {
	if db == nil || db.config == nil || db.config.WriteMode == "" {
		return "strong"
	}
	return strings.ToLower(db.config.WriteMode)
}

func (db *Database) persistenceDurability() string {
	if db == nil || db.config == nil || db.config.Durability == "" {
		return "sync"
	}
	return strings.ToLower(db.config.Durability)
}

func (db *Database) recordLSMError(err error) {
	if err == nil {
		return
	}
	db.lsmLastError.Store(err.Error())
}

// evictIfNeeded 检查内存是否超限并尝试淘汰
func (db *Database) evictIfNeeded() bool {
	if db.rustEngine != nil {
		return true
	}
	if db.evictionPolicy == "noeviction" {
		// 如果策略是不淘汰，检查是否超限
		return atomic.LoadInt64(&db.usedMemory) <= db.maxMemory
	}

	for atomic.LoadInt64(&db.usedMemory) > db.maxMemory {
		// 尝试淘汰一个 key
		if !db.evictOneKey() {
			// 如果无法淘汰任何 key（例如数据库为空，或者 volatile 策略下没有过期 key）
			return false
		}
	}
	return true
}

// evictOneKey 尝试淘汰一个 key
func (db *Database) evictOneKey() bool {
	// 尝试多次随机选择 shard，以应对数据稀疏的情况
	startShard := rand.Intn(ShardCount)
	for i := 0; i < ShardCount; i++ {
		shardIdx := (startShard + i) % ShardCount
		shard := &db.shards[shardIdx]

		shard.lock.RLock()
		// 如果 shard 为空，尝试下一个
		if len(shard.data) == 0 {
			shard.lock.RUnlock()
			continue
		}
		// fmt.Printf("Sampling shard %d with %d keys\n", shardIdx, len(shard.data))

		// 采样
		var bestKey string
		found := false
		bestScore := int64(math.MinInt64)

		samples := 5
		count := 0

		for key, val := range shard.data {
			if db.mutation != nil {
				if _, changed := db.mutation.versions[key]; changed {
					continue
				}
			}
			if val == nil {
				continue
			}
			score := int64(math.MinInt64)

			switch db.evictionPolicy {
			case "allkeys-lru":
				// LRU: 越久未访问 (LastAccessedAt 越小)，分数越高
				// Score = MaxInt64 - LastAccessedAt
				// 为了方便比较，我们直接找 LastAccessedAt 最小的
				score = ^val.LastAccessedAt // 取反，越小的值变成越大的值

			case "volatile-lru":
				if val.ExpireTime > 0 {
					score = ^val.LastAccessedAt
				}

			case "allkeys-random":
				score = 1 // 只要找到就可以

			case "volatile-random":
				if val.ExpireTime > 0 {
					score = 1
				}
			}

			if score > bestScore {
				bestKey = key
				found = true
				bestScore = score
			}

			count++
			if count >= samples {
				break
			}
		}
		shard.lock.RUnlock()

		if found {
			// 执行删除
			db.deleteLocked(bestKey)
			return true
		}
	}

	return false
}

// Get 获取键值
func (db *Database) getLocked(key string) (*datastruct.DataValue, bool) {
	value, exists, err := db.getLiveValue(key)
	if err != nil {
		db.recordLSMError(err)
		logger.Warn("Failed to read key %s from persistence: %v", key, err)
		return nil, false
	}
	if exists && db.keyHeat != nil {
		db.keyHeat.Add(key)
	}
	return value, exists
}

func (db *Database) getBytesLocked(key []byte) (*datastruct.DataValue, bool) {
	return db.getLocked(bytesToString(key))
}

// Set 设置键值
func (db *Database) setLocked(key string, value *datastruct.DataValue) error {
	if value == nil || value.Value == nil {
		return fmt.Errorf("value is required")
	}
	value = db.cloneValue(value)
	if value == nil {
		return fmt.Errorf("unable to copy value")
	}
	// 检查内存并尝试淘汰
	if !db.evictIfNeeded() {
		return fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	newKey := false
	shard := db.getShard(key)
	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}
	old, exists := shard.data[key]
	newKey = !exists
	key = strings.Clone(key)
	shard.data[key] = value
	shard.lock.Unlock()

	if db.hasPersistence() {
		dataBytes, err := value.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", key, err)
			} else {
				return err
			}
		} else {
			if err := db.persistencePut(key, dataBytes); err != nil {
				return err
			}
		}
	}

	// 更新内存统计
	var oldSize int64
	if old != nil {
		oldSize = old.ApproximateSize()
	}
	var newSize int64
	if value != nil {
		newSize = value.ApproximateSize()
	}
	memDelta := newSize - oldSize
	if newKey {
		memDelta += int64(len(key))
	}
	db.updateMemoryUsage(memDelta)
	db.BumpKeyVersions(key)
	return nil
}

func (db *Database) setBytesLocked(key []byte, value *datastruct.DataValue) error {
	return db.setLocked(bytesToString(key), value)
}

func (db *Database) deleteWithErrorLocked(key string) (bool, error) {
	_, exists, err := db.getLiveValue(key)
	if err != nil {
		db.recordLSMError(err)
		return false, err
	}

	shard := db.getShard(key)
	shard.lock.Lock()
	if val := shard.data[key]; val != nil {
		delete(shard.data, key)
		memDelta := int64(len(key)) + val.ApproximateSize()
		db.updateMemoryUsage(-memDelta)
	}
	shard.lock.Unlock()

	if err := db.persistenceDelete(key); err != nil {
		return exists, err
	}
	if exists {
		db.BumpKeyVersions(key)
	}
	return exists, nil
}

func (db *Database) deleteLocked(key string) bool {
	ok, _ := db.deleteWithErrorLocked(key)
	return ok
}

func (db *Database) deleteBytesLocked(key []byte) bool {
	return db.deleteLocked(bytesToString(key))
}

// Exists 检查键是否存在
func (db *Database) existsLocked(key string) bool {
	_, exists, err := db.getLiveValue(key)
	if err != nil {
		db.recordLSMError(err)
		logger.Warn("Failed to read key %s from persistence: %v", key, err)
		return false
	}
	return exists
}

// Keys 返回所有键
func (db *Database) keysLocked() []string {
	keys := make([]string, 0)
	seen := make(map[string]struct{})

	for i := 0; i < ShardCount; i++ {
		shard := &db.shards[i]
		shard.lock.RLock()
		for key, value := range shard.data {
			if value != nil && !value.IsExpired() {
				keys = append(keys, key)
				seen[key] = struct{}{}
			}
		}
		shard.lock.RUnlock()
	}

	if db.rustEngine != nil {
		persisted, err := db.scanPersistedValues()
		if err != nil {
			db.recordLSMError(err)
			logger.Warn("Failed to list keys from persistence: %v", err)
			return keys
		}
		for key := range persisted {
			if _, ok := seen[key]; ok {
				continue
			}
			keys = append(keys, key)
		}
	}
	return keys
}

// Size 返回数据库大小
func (db *Database) sizeLocked() int {
	count := 0
	seen := make(map[string]struct{})
	for i := 0; i < ShardCount; i++ {
		shard := &db.shards[i]
		shard.lock.RLock()
		for key, value := range shard.data {
			if value != nil && !value.IsExpired() {
				count++
				seen[key] = struct{}{}
			}
		}
		shard.lock.RUnlock()
	}

	if db.rustEngine != nil {
		persisted, err := db.scanPersistedValues()
		if err != nil {
			db.recordLSMError(err)
			logger.Warn("Failed to count keys from persistence: %v", err)
			return count
		}
		for key := range persisted {
			if _, ok := seen[key]; ok {
				continue
			}
			count++
		}
	}
	return count
}

// Clear 清空数据库
func (db *Database) Clear() error {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		return errDatabaseClosed
	}
	if err := db.persistenceReset(); err != nil {
		return err
	}
	for i := 0; i < ShardCount; i++ {
		shard := &db.shards[i]
		shard.lock.Lock()
		shard.data = nil
		shard.lock.Unlock()
	}
	atomic.StoreInt64(&db.usedMemory, 0)
	if db.keyHeat != nil {
		db.keyHeat.Clear()
	}
	db.ResetVersionState()

	return nil
}

// Close 关闭数据库
func (db *Database) Close() error {
	db.disableExpireCleaner()
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.closeLocked()
}

func (db *Database) disableExpireCleaner() {
	db.expireMu.Lock()
	defer db.expireMu.Unlock()
	db.expireClosed = true
	db.stopExpireCleanerLocked()
}

func (db *Database) closeLocked() error {
	if db.closed {
		return nil
	}
	db.closed = true
	db.notifyChange()

	if db.hasPersistence() {
		return db.persistenceClose()
	}
	return nil
}

// GetStats 获取数据库统计信息
func (db *Database) GetStats() map[string]interface{} {
	db.operationMu.RLock()
	defer db.operationMu.RUnlock()
	stats := make(map[string]interface{})

	stats["db_id"] = db.id
	stats["storage_backend"] = config.StorageLegacy
	if db.config.StorageBackend == config.StorageGoLog {
		stats["storage_backend"] = config.StorageGoLog
	}
	keyCount := 0
	for i := 0; i < ShardCount; i++ {
		shard := &db.shards[i]
		shard.lock.RLock()
		keyCount += len(shard.data)
		shard.lock.RUnlock()
	}
	stats["memory_keys"] = keyCount
	stats["used_memory_bytes"] = atomic.LoadInt64(&db.usedMemory)
	stats["pending_memory_bytes"] = db.pendingMemory.Load()
	stats["max_memory_bytes"] = db.maxMemory
	stats["max_memory_policy"] = db.evictionPolicy
	stats["expire_runs"] = db.expireRuns.Load()
	stats["expire_scanned"] = db.expireScanned.Load()
	stats["expire_expired"] = db.expireExpired.Load()
	stats["expire_duration_ms"] = float64(db.expireDurationNanos.Load()) / 1e6
	stats["persistence_write_mode"] = db.persistenceWriteMode()
	stats["persistence_durability"] = db.persistenceDurability()
	if v := db.lsmLastError.Load(); v != nil {
		stats["persistence_last_error"] = v.(string)
	} else {
		stats["persistence_last_error"] = ""
	}

	if db.hasPersistence() {
		stats["mode"] = db.persistenceMode()
		stats["persistence_enabled"] = true
		stats["persistence"] = db.persistenceStats()
	} else {
		stats["mode"] = "Memory"
		stats["persistence_enabled"] = false
	}

	return stats
}

func (db *Database) Get(key string) (result0 *datastruct.DataValue, result1 bool) {
	parallel := db.beginKeyOperation(key, false)
	defer db.endKeyOperation(key, false, parallel)
	if db.closed {
		return
	}
	result0, result1 = db.getLocked(key)
	if result1 {
		result0 = db.cloneValue(result0)
		result1 = result0 != nil
	}
	return
}

func (db *Database) GetBytes(key []byte) (result0 *datastruct.DataValue, result1 bool) {
	parallel := db.beginKeyOperation(bytesToString(key), false)
	defer db.endKeyOperation(bytesToString(key), false, parallel)
	if db.closed {
		return
	}
	result0, result1 = db.getBytesLocked(key)
	if result1 {
		result0 = db.cloneValue(result0)
		result1 = result0 != nil
	}
	return
}

func (db *Database) Set(key string, value *datastruct.DataValue) (err error) {
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

	return db.setLocked(key, value)
}

func (db *Database) SetBytes(key []byte, value *datastruct.DataValue) (err error) {
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

	return db.setBytesLocked(key, value)
}

func (db *Database) DeleteWithError(key string) (result0 bool, err error) {
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

	return db.deleteWithErrorLocked(key)
}

func (db *Database) Delete(key string) (result0 bool) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		return
	}

	db.beginMutation()
	defer func() {
		if commitErr := db.finishMutation(nil); commitErr != nil {
			var zero0 bool
			result0 = zero0
		}
	}()

	return db.deleteLocked(key)
}

func (db *Database) DeleteBytes(key []byte) (result0 bool) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		return
	}

	db.beginMutation()
	defer func() {
		if commitErr := db.finishMutation(nil); commitErr != nil {
			var zero0 bool
			result0 = zero0
		}
	}()

	return db.deleteBytesLocked(key)
}

func (db *Database) Exists(key string) (result0 bool) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.existsLocked(key)
}

func (db *Database) Keys() (result0 []string) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.keysLocked()
}

func (db *Database) Size() (result0 int) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.sizeLocked()
}
