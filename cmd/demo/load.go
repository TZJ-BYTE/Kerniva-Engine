package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const windowSeconds = 3

type loadSettings struct {
	Rate            int `json:"rate"`
	Clients         int `json:"clients"`
	DurationSeconds int `json:"duration_seconds"`
	ReadPercent     int `json:"read_percent"`
}

func (s loadSettings) validate() error {
	if s.Rate < 1 || s.Rate > 20000 || s.Clients < 1 || s.Clients > 64 || s.DurationSeconds < 3 || s.DurationSeconds > 120 || s.ReadPercent < 0 || s.ReadPercent > 100 {
		return fmt.Errorf("速率范围 1–20000 ops/s，连接数 1–64，总测量时长 3–120 秒，GET 比例 0–100")
	}
	return nil
}

type completedWindow struct {
	Index       int             `json:"index"`
	Source      string          `json:"source"`
	Measurement json.RawMessage `json:"measurement"`
}

type loadRun struct {
	ID          string            `json:"id"`
	Phase       string            `json:"phase"`
	StartedUTC  string            `json:"started_utc"`
	FinishedUTC string            `json:"finished_utc,omitempty"`
	Settings    loadSettings      `json:"settings"`
	Window      int               `json:"current_window"`
	Windows     []completedWindow `json:"windows"`
	Error       string            `json:"error,omitempty"`
	cancel      context.CancelFunc
}

func (a *application) startLoad(w http.ResponseWriter, r *http.Request) {
	var settings loadSettings
	if err := decodeJSON(w, r, &settings); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := settings.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, err := os.Stat(a.benchmark)
	if err != nil || !info.Mode().IsRegular() {
		writeError(w, http.StatusBadRequest, "找不到 bench 可执行文件，请先通过 scripts/demo 启动入口构建")
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lifetime.Err() != nil {
		writeError(w, http.StatusServiceUnavailable, "演示正在退出")
		return
	}
	if a.current != nil && a.current.FinishedUTC == "" {
		writeError(w, http.StatusConflict, "已有演示负载正在运行或停止")
		return
	}
	directory, err := os.MkdirTemp(a.history.Name(), "demo-"+time.Now().UTC().Format("20060102T150405")+"-")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(a.lifetime, time.Duration(settings.DurationSeconds+60)*time.Second)
	run := &loadRun{
		ID: filepath.Base(directory), Phase: "prefilling", StartedUTC: time.Now().UTC().Format(time.RFC3339Nano),
		Settings: settings, Windows: make([]completedWindow, 0), cancel: cancel,
	}
	a.current = run
	a.work.Add(1)
	go a.runLoad(ctx, run, directory)
	writeJSON(w, http.StatusAccepted, map[string]string{"id": run.ID, "phase": run.Phase})
}

func (a *application) stopLoad() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != nil && a.current.FinishedUTC == "" {
		a.current.Phase = "stopping"
		a.current.cancel()
	}
}

func (a *application) runLoad(ctx context.Context, run *loadRun, directory string) {
	defer a.work.Done()
	defer run.cancel()
	phase := "completed"
	var failure error
	defer func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if ctx.Err() != nil {
			phase = "stopped"
		}
		if ctx.Err() == context.DeadlineExceeded {
			phase = "failed"
			failure = fmt.Errorf("演示负载超过总运行时间上限")
		}
		run.Phase, run.FinishedUTC = phase, time.Now().UTC().Format(time.RFC3339Nano)
		if failure != nil {
			run.Error = failure.Error()
		}
	}()
	remaining := run.Settings.DurationSeconds
	for index := 1; remaining > 0; index++ {
		seconds := min(windowSeconds, remaining)
		a.mu.Lock()
		run.Window = index
		if ctx.Err() == nil {
			run.Phase = "preparing"
			if index == 1 {
				run.Phase = "prefilling"
			}
		}
		a.mu.Unlock()
		measurement, err := a.measureWindow(ctx, run, directory, index, seconds)
		if err != nil {
			if ctx.Err() == nil {
				phase, failure = "failed", err
			}
			return
		}
		filename := fmt.Sprintf("window-%03d.json", index)
		path := filepath.Join(run.ID, filename)
		if err := a.history.WriteFile(path+".tmp", append(measurement, '\n'), 0600); err != nil {
			phase, failure = "failed", err
			return
		}
		if err := a.history.Rename(path+".tmp", path); err != nil {
			phase, failure = "failed", err
			return
		}
		a.mu.Lock()
		run.Windows = append(run.Windows, completedWindow{index, filepath.ToSlash(filepath.Join(run.ID, filename)), measurement})
		a.mu.Unlock()
		remaining -= seconds
	}
}

func (a *application) measureWindow(ctx context.Context, run *loadRun, directory string, index, seconds int) (json.RawMessage, error) {
	ready := filepath.Join(directory, fmt.Sprintf("window-%03d.ready", index))
	start := filepath.Join(directory, fmt.Sprintf("window-%03d.start", index))
	settings := run.Settings
	arguments := []string{
		"-mode", "mixed", "-addr", a.database,
		"-clients", strconv.Itoa(settings.Clients), "-rate", strconv.Itoa(settings.Rate),
		"-duration", strconv.Itoa(seconds) + "s", "-warmup", "0s", "-timeout", "1s",
		"-keys", "1000", "-read-percent", strconv.Itoa(settings.ReadPercent), "-seed", "42",
		"-prefill=" + strconv.FormatBool(index == 1), "-queue-per-client", "4096",
		"-ready-file", ready, "-start-file", start,
		"-network", "demo-unspecified", "-durability", "demo-unspecified",
	}
	commandCtx, cancel := context.WithTimeout(ctx, time.Duration(seconds+35)*time.Second)
	defer cancel()
	command := exec.CommandContext(commandCtx, a.benchmark, arguments...)
	var output, stderr bytes.Buffer
	command.Stdout, command.Stderr = &output, &stderr
	if err := command.Start(); err != nil {
		return nil, err
	}
	// This handle belongs to this demo. Cancellation affects only this child.
	finished := make(chan error, 1)
	var waited sync.WaitGroup
	waited.Add(1)
	go func() {
		defer waited.Done()
		finished <- command.Wait()
	}()
	defer waited.Wait()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	started := false
	for {
		select {
		case err := <-finished:
			if err != nil {
				return nil, fmt.Errorf("bench 窗口 %d 失败 %w %s", index, err, stderr.String())
			}
			measurement, err := extractMeasurement(output.Bytes())
			if err != nil {
				return nil, fmt.Errorf("bench JSON 解析失败 %w", err)
			}
			return measurement, nil
		case <-ticker.C:
			if started {
				continue
			}
			if _, err := os.Stat(ready); err == nil {
				if err := os.WriteFile(start, []byte("start\n"), 0600); err != nil {
					cancel()
					return nil, err
				}
				started = true
				a.mu.Lock()
				if ctx.Err() == nil {
					run.Phase = "measuring"
				}
				a.mu.Unlock()
			}
		}
	}
}
