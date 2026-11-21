package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/network"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func writeAllConn(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		b = b[n:]
	}
	return nil
}

// StdServer 基于标准库 net 的 Redis 服务器
type StdServer struct {
	config       *config.Config
	listener     net.Listener
	dbManager    *database.DBManager
	registry     *command.CommandRegistry
	pubsub       *PubSubHub
	ctx          context.Context
	cancel       context.CancelFunc
	connections  map[net.Conn]struct{}
	connectionMu sync.Mutex
	workers      sync.WaitGroup
	stopOnce     sync.Once
	stopErr      error
	initErr      error
}

// NewStdServer 创建标准服务器
func NewStdServer(cfg *config.Config) network.Server {
	ctx, cancel := context.WithCancel(context.Background())

	// 初始化命令注册表
	command.InitDefaultCommands()
	manager, initErr := database.NewDBManagerChecked(cfg)
	command.SetDBManager(manager)

	return &StdServer{
		config:      cfg,
		dbManager:   manager,
		registry:    command.DefaultRegistry,
		pubsub:      NewPubSubHub(),
		ctx:         ctx,
		cancel:      cancel,
		connections: make(map[net.Conn]struct{}),
		initErr:     initErr,
	}
}

// Start 启动服务器
func (s *StdServer) Start() error {
	if s.initErr != nil {
		return s.initErr
	}
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	s.connectionMu.Lock()
	if s.ctx.Err() != nil {
		s.connectionMu.Unlock()
		_ = listener.Close()
		return nil
	}
	s.listener = listener
	s.connectionMu.Unlock()
	logger.Info("Kerniva 服务器启动在 %s (Standard Net)", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil
			default:
				logger.Error("接受连接失败：%v", err)
				continue
			}
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(1 << 20)
			_ = tcpConn.SetWriteBuffer(1 << 20)
		}

		s.connectionMu.Lock()
		if s.ctx.Err() != nil {
			s.connectionMu.Unlock()
			_ = conn.Close()
			return nil
		}
		s.connections[conn] = struct{}{}
		s.workers.Add(1)
		s.connectionMu.Unlock()
		go func() {
			defer s.workers.Done()
			defer func() { s.connectionMu.Lock(); delete(s.connections, conn); s.connectionMu.Unlock() }()
			s.handleConnection(conn)
		}()
	}
}

// handleConnection 处理客户端连接
func (s *StdServer) handleConnection(conn net.Conn) {
	logger.Info("新连接：%s", conn.RemoteAddr().String())

	connCtx := newConnectionContext(s.ctx, s.dbManager.GetDefaultDB(), s.pubsub, nil)
	writer := newStdPubSubWriter(conn, connCtx)
	connCtx.pubsubWriter = writer
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		runConnectionCommands(connCtx, s.dbManager, s.registry, func(payload []byte) error {
			return connCtx.writeNetConn(conn, payload)
		}, func() { _ = conn.Close() })
	}()
	defer func() {
		connCtx.Cancel()
		_ = conn.Close()
		<-workerDone
		s.pubsub.CleanupConnection(connCtx)
		_ = writer.Close()
		<-writer.done
		_ = conn.Close()
	}()
	buf := make([]byte, 256*1024)
	start, end := 0, 0

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		if end == len(buf) {
			if start > 0 {
				copy(buf, buf[start:end])
				end -= start
				start = 0
			} else {
				if len(buf) >= maxConnectionBufferedBytes {
					return
				}
				nb := make([]byte, len(buf)*2)
				copy(nb, buf)
				buf = nb
			}
		}

		n, err := conn.Read(buf[end:])
		if err != nil {
			if err == io.EOF {
				logger.Info("连接关闭：%s", conn.RemoteAddr().String())
				return
			}
			return
		}
		if n == 0 {
			continue
		}

		end += n

		for {
			req, consumed, perr := protocol.ParseOneRequestFast(buf[start:end])
			if perr != nil {
				if connCtx.enqueueRequest(nil, perr, true) {
					select {
					case <-workerDone:
					case <-s.ctx.Done():
					}
				}
				return
			}
			if consumed == 0 {
				break
			}

			accepted := connCtx.enqueueRequest(req, nil, true)
			protocol.ReleaseRequest(req)
			if !accepted {
				return
			}
			start += consumed
			if start >= end {
				start, end = 0, 0
			}
			if start >= end {
				break
			}
		}
	}
}

type stdPubSubWriter struct {
	conn      net.Conn
	ctx       *ConnectionContext
	queue     chan []byte
	pending   atomic.Int64
	done      chan struct{}
	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
}

func newStdPubSubWriter(conn net.Conn, ctx *ConnectionContext) *stdPubSubWriter {
	w := &stdPubSubWriter{
		conn:  conn,
		ctx:   ctx,
		queue: make(chan []byte, 64),
		done:  make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *stdPubSubWriter) WriteAsync(payload []byte) error {
	if w.pending.Add(int64(len(payload))) > maxConnectionBufferedBytes {
		w.pending.Add(-int64(len(payload)))
		w.ctx.Cancel()
		_ = w.conn.Close()
		return errors.New("ERR pubsub output buffer is full")
	}
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		w.pending.Add(-int64(len(payload)))
		return net.ErrClosed
	}
	select {
	case w.queue <- payload:
		w.mu.RUnlock()
		return nil
	default:
		w.mu.RUnlock()
		w.pending.Add(-int64(len(payload)))
		_ = w.Close()
		w.ctx.Cancel()
		_ = w.conn.Close()
		return errors.New("ERR pubsub output buffer is full")
	}
}

func (w *stdPubSubWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		w.mu.Unlock()
	})
	return nil
}

func (w *stdPubSubWriter) run() {
	defer close(w.done)
	for payload := range w.queue {
		err := w.ctx.writeNetConn(w.conn, payload)
		w.pending.Add(-int64(len(payload)))
		if err != nil {
			_ = w.conn.Close()
			return
		}
	}
}

func (ctx *ConnectionContext) writeNetConn(conn net.Conn, payload []byte) error {
	ctx.writeMu.Lock()
	defer ctx.writeMu.Unlock()
	return writeAllConn(conn, payload)
}

// handleSelectCommand 处理 SELECT 命令
func (s *StdServer) handleSelectCommand(conn net.Conn, ctx *ConnectionContext, args [][]byte) {
	if len(args) != 1 {
		resp := protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for 'select' command"))
		ctx.writeBuf = protocol.AppendResponse(ctx.writeBuf, resp)
		protocol.ReleaseResponse(resp)
		return
	}

	index, err := protocol.ParseInt(args[0])
	if err != nil {
		resp := protocol.MakeError(fmt.Errorf("ERR invalid DB index"))
		ctx.writeBuf = protocol.AppendResponse(ctx.writeBuf, resp)
		protocol.ReleaseResponse(resp)
		return
	}

	newDB, err := s.dbManager.GetDBByIndex(index)
	if err != nil {
		resp := protocol.MakeError(fmt.Errorf("ERR DB index is out of range"))
		ctx.writeBuf = protocol.AppendResponse(ctx.writeBuf, resp)
		protocol.ReleaseResponse(resp)
		return
	}

	ctx.DB = newDB
	resp := protocol.MakeSimpleString("OK")
	ctx.writeBuf = protocol.AppendResponse(ctx.writeBuf, resp)
	protocol.ReleaseResponse(resp)
}

// Stop 停止服务器
func (s *StdServer) Stop() error {
	s.stopOnce.Do(func() {
		s.cancel()
		s.connectionMu.Lock()
		if s.listener != nil {
			_ = s.listener.Close()
		}
		for conn := range s.connections {
			_ = conn.Close()
		}
		s.connectionMu.Unlock()
		s.workers.Wait()
		if s.dbManager != nil {
			s.stopErr = s.dbManager.Close()
		}
	})
	return s.stopErr
}
