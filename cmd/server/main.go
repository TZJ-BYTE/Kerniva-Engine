package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/server"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func main() {
	configPath := flag.String("config", "config.yaml", "configuration file")
	stopFile := flag.String("stop-file", "", "local stop request file used by the control script")
	platform := platformFlags()
	flag.Parse()
	if err := platform(*configPath, *stopFile); err != nil {
		fmt.Fprintf(os.Stderr, "Kerniva: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath, stopFile string) error {
	return serve(configPath, stopFile, nil, nil)
}

func serve(configPath, stopFile string, serviceStop <-chan struct{}, ready func()) (err error) {
	cfg, loadErr := config.LoadFromFile(configPath)
	if loadErr != nil {
		if ready != nil || !os.IsNotExist(loadErr) {
			return loadErr
		}
		cfg = config.DefaultConfig()
	}
	if ready != nil {
		if cfg.NetworkType != "std" || cfg.StorageBackend != config.StorageGoLog {
			return errors.New("Windows service mode requires network_type: std and storage_backend: go-log")
		}
		if !filepath.IsAbs(cfg.DataDir) || !filepath.IsAbs(cfg.LogPath) {
			return errors.New("Windows service mode requires absolute data_dir and log_path")
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if output := os.Getenv("KERNIVA_GOLOG_BATCH_TRACE"); output != "" {
		if !cfg.UsesGoLog() {
			return errors.New("KERNIVA_GOLOG_BATCH_TRACE requires an enabled Go log")
		}
		trace, traceErr := writetrace.Open(output)
		if traceErr != nil {
			return traceErr
		}
		defer func() { err = errors.Join(err, trace.Close()) }()
	}
	rotation := logger.Rotation{MaxSizeMB: cfg.LogMaxSizeMB, MaxBackups: cfg.LogMaxBackups, MaxAgeDays: cfg.LogMaxAgeDays}
	if err := logger.InitWithRotation(cfg.LogPath, cfg.LogLevel, rotation); err != nil {
		return err
	}
	defer logger.Close()
	if loadErr != nil {
		logger.Warn("配置文件 %s 不存在，使用默认配置", configPath)
	} else {
		logger.Info("已加载配置文件 %s", configPath)
	}
	if cfg.PprofEnabled {
		profiler := &http.Server{Addr: cfg.PprofAddr, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := profiler.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("pprof server: %v", err)
			}
		}()
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = profiler.Shutdown(ctx)
		}()
	}
	srv := server.NewServer(cfg)
	stopped := make(chan error, 1)
	go func() {
		if ready != nil {
			stopped <- srv.(*server.StdServer).StartWithReady(ready)
		} else {
			stopped <- srv.Start()
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	var ticks <-chan time.Time
	if stopFile != "" {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case startErr := <-stopped:
			closeErr := srv.Stop()
			if startErr != nil {
				return startErr
			}
			return closeErr
		case <-serviceStop:
			return srv.Stop()
		case <-signals:
			logger.Info("收到退出信号，正在关闭")
			return srv.Stop()
		case <-ticks:
			if _, err := os.Stat(stopFile); err == nil {
				_ = os.Remove(stopFile)
				logger.Info("收到本地停止请求，正在关闭")
				return srv.Stop()
			}
		}
	}
}
