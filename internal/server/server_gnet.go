package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/network"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
	"github.com/panjf2000/gnet/v2"
)

type GnetServer struct {
	config    *config.Config
	dbManager *database.DBManager
	registry  *command.CommandRegistry
	pubsub    *PubSubHub
	eng       gnet.Engine
	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	lifecycle sync.Mutex
	stopOnce  sync.Once
	stopErr   error
	initErr   error
	booted    bool
}

func NewGnetServer(cfg *config.Config) network.Server {
	ctx, cancel := context.WithCancel(context.Background())
	command.InitDefaultCommands()
	manager, initErr := database.NewDBManagerChecked(cfg)
	if !manager.UsesRustMemory() {
		command.SetDBManager(manager)
	}
	return &GnetServer{
		ctx:       ctx,
		cancel:    cancel,
		initErr:   initErr,
		config:    cfg,
		dbManager: manager,
		registry:  command.DefaultRegistry,
		pubsub:    NewPubSubHub(),
	}
}

func (s *GnetServer) Start() error {
	if s.initErr != nil {
		return s.initErr
	}
	if s.ctx.Err() != nil {
		return nil
	}
	addr := fmt.Sprintf("tcp://%s:%d", s.config.Host, s.config.Port)
	logger.Info("Kerniva 服务器启动在 %s (gnet)", addr)
	return gnet.Run(s, addr, gnet.WithMulticore(true), gnet.WithReusePort(true))
}

func (s *GnetServer) Stop() error {
	s.stopOnce.Do(func() {
		s.lifecycle.Lock()
		s.cancel()
		booted := s.booted
		s.lifecycle.Unlock()
		if booted {
			s.stopErr = s.eng.Stop(context.Background())
		}
		s.workers.Wait()
		if s.dbManager != nil {
			if err := s.dbManager.Close(); err != nil && s.stopErr == nil {
				s.stopErr = err
			}
		}
	})
	return s.stopErr
}

func (s *GnetServer) OnBoot(eng gnet.Engine) gnet.Action {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	s.eng = eng
	s.booted = true
	if s.ctx.Err() != nil {
		return gnet.Shutdown
	}
	logger.Info("gnet engine is running")
	return gnet.None
}

func (s *GnetServer) OnTick() (delay time.Duration, action gnet.Action) {
	return 1 * time.Second, gnet.None
}

func (s *GnetServer) OnShutdown(eng gnet.Engine) {
	logger.Info("gnet server shutdown")
}

func (s *GnetServer) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	remote := c.RemoteAddr()
	logger.Info("New connection: %s", remote.String())
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.ctx.Err() != nil {
		return nil, gnet.Close
	}
	ctx := newConnectionContext(s.ctx, s.dbManager.GetDefaultDB(), s.pubsub, nil)
	ctx.initWriteTrace(remote)
	ctx.pubsubWriter = &gnetPubSubWriter{conn: c, ctx: ctx}
	c.SetContext(ctx)
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		defer s.pubsub.CleanupConnection(ctx)
		runConnectionCommands(ctx, s.dbManager, s.registry, func(payload []byte) error {
			done := make(chan error, 1)
			if err := c.AsyncWrite(payload, func(_ gnet.Conn, err error) error {
				done <- err
				if err == nil && ctx.needsWake.Swap(false) {
					return c.Wake(nil)
				}
				return nil
			}); err != nil {
				return err
			}
			select {
			case err := <-done:
				return err
			case <-ctx.Context.Done():
				return ctx.Context.Err()
			}
		}, func() { _ = c.Close() })
	}()
	return nil, gnet.None
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) gnet.Action {
	logger.Info("Connection closed: %s", c.RemoteAddr().String())
	if connCtx, ok := c.Context().(*ConnectionContext); ok {
		connCtx.Cancel()
	}
	return gnet.None
}

func (s *GnetServer) OnTraffic(c gnet.Conn) gnet.Action {
	// 处理粘包
	data, err := c.Peek(-1)
	if err != nil {
		logger.Error("Peek error: %v", err)
		return gnet.Close
	}
	if len(data) == 0 {
		return gnet.None
	}

	offset := 0
	connCtx := c.Context().(*ConnectionContext)

	for {
		if len(connCtx.requests) == cap(connCtx.requests) {
			connCtx.needsWake.Store(true)
			break
		}
		req, n, err := protocol.ParseOneRequestFast(data[offset:])
		if err != nil {
			logger.Error("Parser error: %v", err)
			if !connCtx.enqueueRequest(nil, err, false) {
				return gnet.Close
			}
			_, _ = c.Discard(len(data))
			return gnet.None
		}

		if n == 0 {
			// 数据不足，等待更多数据
			break
		}

		accepted := connCtx.enqueueRequest(req, nil, false)
		protocol.ReleaseRequest(req)
		if !accepted {
			return gnet.Close
		}

		offset += n

		if offset >= len(data) {
			break
		}
	}

	// 丢弃已处理的数据
	if offset > 0 {
		if _, err := c.Discard(offset); err != nil {
			logger.Error("Discard error: %v", err)
			return gnet.Close
		}
	}
	if c.InboundBuffered() > maxConnectionBufferedBytes {
		return gnet.Close
	}

	return gnet.None
}

// handleSelectCommand 处理 SELECT 命令
func (s *GnetServer) handleSelectCommand(c gnet.Conn, ctx *ConnectionContext, args [][]byte) {
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

	// 使用 GetDBByIndex 验证索引有效性，但不改变全局 currentDB
	db, err := s.dbManager.GetDBByIndex(index)
	if err == nil {
		// 更新当前连接的上下文
		ctx.DB = db

		resp := protocol.MakeSimpleString("OK")
		ctx.writeBuf = protocol.AppendResponse(ctx.writeBuf, resp)
		protocol.ReleaseResponse(resp)
	} else {
		resp := protocol.MakeError(fmt.Errorf("ERR DB index is out of range"))
		ctx.writeBuf = protocol.AppendResponse(ctx.writeBuf, resp)
		protocol.ReleaseResponse(resp)
	}
}

type gnetPubSubWriter struct {
	conn    gnet.Conn
	ctx     *ConnectionContext
	pending atomic.Int64
}

func (w *gnetPubSubWriter) WriteAsync(payload []byte) error {
	if w.pending.Add(int64(len(payload))) > maxConnectionBufferedBytes {
		w.pending.Add(-int64(len(payload)))
		w.ctx.Cancel()
		_ = w.conn.Close()
		return errors.New("ERR pubsub output buffer is full")
	}
	err := w.conn.AsyncWrite(payload, func(_ gnet.Conn, err error) error {
		w.pending.Add(-int64(len(payload)))
		if err != nil {
			w.ctx.Cancel()
			_ = w.conn.Close()
		}
		return nil
	})
	if err != nil {
		w.pending.Add(-int64(len(payload)))
	}
	return err
}
