package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Rotation struct {
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
}

var state struct {
	sync.RWMutex
	logger *log.Logger
	writer *lumberjack.Logger
	level  int
}

func Init(logPath, level string) error {
	return InitWithRotation(logPath, level, Rotation{MaxSizeMB: 100, MaxBackups: 3, MaxAgeDays: 7})
}

func InitWithRotation(logPath, level string, rotation Rotation) error {
	levels := map[string]int{"debug": 0, "info": 1, "warn": 2, "error": 3}
	if level == "" {
		level = "info"
	}
	threshold, ok := levels[strings.ToLower(level)]
	if !ok {
		return fmt.Errorf("invalid log level %q", level)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0750); err != nil {
		return err
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	writer := &lumberjack.Logger{Filename: logPath, MaxSize: rotation.MaxSizeMB, MaxBackups: rotation.MaxBackups, MaxAge: rotation.MaxAgeDays, Compress: true}
	state.Lock()
	defer state.Unlock()
	if state.writer != nil {
		_ = state.writer.Close()
	}
	state.writer = writer
	state.logger = log.New(io.MultiWriter(os.Stderr, writer), "", log.Ldate|log.Ltime)
	state.level = threshold
	return nil
}

func Close() error {
	state.Lock()
	defer state.Unlock()
	if state.writer == nil {
		return nil
	}
	err := state.writer.Close()
	state.writer = nil
	state.logger = nil
	return err
}

func write(level int, name, format string, values ...interface{}) {
	state.RLock()
	defer state.RUnlock()
	if state.logger != nil && level >= state.level {
		state.logger.Printf("["+name+"] "+format, values...)
	}
}

func Debug(format string, values ...interface{}) { write(0, "DEBUG", format, values...) }
func Info(format string, values ...interface{})  { write(1, "INFO", format, values...) }
func Warn(format string, values ...interface{})  { write(2, "WARN", format, values...) }
func Error(format string, values ...interface{}) { write(3, "ERROR", format, values...) }
