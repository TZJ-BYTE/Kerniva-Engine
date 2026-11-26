package server

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
)

// ConnectionContext 连接上下文
// 存储每个客户端连接的私有状态
type ConnectionContext struct {
	Context        context.Context
	Cancel         context.CancelFunc
	requests       chan connectionRequest
	pendingBytes   atomic.Int64
	needsWake      atomic.Bool
	waiting        atomic.Bool
	waitStarted    chan struct{}
	ClientID       int64
	ClientName     string
	LibraryName    string
	LibraryVersion string
	// 当前选中的数据库
	DB *database.Database

	memoryDBIndex int
	memoryReadBuf []byte

	// 认证状态（未来支持）
	Authenticated bool

	respBuf  []byte
	writeBuf []byte

	Tx *TransactionState

	writeMu sync.Mutex

	trace        *writetrace.Recorder
	tracePort    int
	traceOrdinal uint64              // network reader only
	traceRequest *writetrace.Request // command worker only

	pubsubMu       sync.Mutex
	pubsubChannels map[string]struct{}
	pubsubPatterns map[string]struct{}
	pubsubHub      *PubSubHub
	pubsubWriter   PubSubWriter
}

type QueuedCommand struct {
	DB   *database.Database
	Cmd  string
	Args [][]byte
}

type WatchedKey struct {
	DB      *database.Database
	Key     string
	Version uint64
	Epoch   uint64
}

type TransactionState struct {
	InMulti     bool
	Queue       []QueuedCommand
	QueueFailed bool
	Watched     []WatchedKey
	QueueBytes  int
}

func (tx *TransactionState) ResetQueue() {
	if tx == nil {
		return
	}
	tx.InMulti = false
	tx.QueueFailed = false
	tx.Queue = tx.Queue[:0]
	tx.QueueBytes = 0
}

func (tx *TransactionState) ResetAll() {
	tx.ResetQueue()
	tx.ResetWatches()
}

func (tx *TransactionState) ResetWatches() {
	if tx == nil {
		return
	}
	for _, watched := range tx.Watched {
		watched.DB.UnwatchKeyVersion(watched.Key)
	}
	tx.Watched = nil
}

func cloneQueuedCommand(db *database.Database, cmd string, args [][]byte) QueuedCommand {
	clonedArgs := make([][]byte, len(args))
	for i := range args {
		if args[i] == nil {
			continue
		}
		clonedArgs[i] = append([]byte(nil), args[i]...)
	}
	return QueuedCommand{
		DB:   db,
		Cmd:  strings.ToUpper(strings.Clone(cmd)),
		Args: clonedArgs,
	}
}

func (ctx *ConnectionContext) pubsubSubscriptionCountLocked() int64 {
	if ctx == nil {
		return 0
	}
	return int64(len(ctx.pubsubChannels) + len(ctx.pubsubPatterns))
}

// NewConnectionContext 创建新的连接上下文
func NewConnectionContext(defaultDB *database.Database, hub *PubSubHub, writer PubSubWriter) *ConnectionContext {
	return newConnectionContext(context.Background(), defaultDB, hub, writer)
}

var nextClientID atomic.Int64

func newConnectionContext(parent context.Context, defaultDB *database.Database, hub *PubSubHub, writer PubSubWriter) *ConnectionContext {
	ctx, cancel := context.WithCancel(parent)
	return &ConnectionContext{
		Context:        ctx,
		Cancel:         cancel,
		ClientID:       nextClientID.Add(1),
		requests:       make(chan connectionRequest, 128),
		waitStarted:    make(chan struct{}, 1),
		DB:             defaultDB,
		Authenticated:  false,
		respBuf:        make([]byte, 0, 512),
		writeBuf:       make([]byte, 0, 4096),
		Tx:             &TransactionState{},
		pubsubChannels: make(map[string]struct{}),
		pubsubPatterns: make(map[string]struct{}),
		pubsubHub:      hub,
		pubsubWriter:   writer,
	}
}

func (ctx *ConnectionContext) requestContext() context.Context {
	if ctx.Context == nil {
		return context.Background()
	}
	return ctx.Context
}
