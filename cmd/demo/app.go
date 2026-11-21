package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/respclient"
)

type application struct {
	database  string
	benchmark string
	history   *os.Root
	lifetime  context.Context
	mu        sync.Mutex
	current   *loadRun
	work      sync.WaitGroup
}

func (a *application) handler(web fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("POST /api/command", a.command)
	mux.HandleFunc("POST /api/info", a.serverInfo)
	mux.HandleFunc("POST /api/load/start", a.startLoad)
	mux.HandleFunc("POST /api/load/stop", func(w http.ResponseWriter, r *http.Request) {
		a.stopLoad()
		a.status(w, r)
	})
	mux.HandleFunc("GET /api/history", a.listHistory)
	mux.HandleFunc("GET /api/history/file", a.readHistory)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("GET /", http.FileServer(http.FS(web)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-store")
		host := r.Host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			host = parsed
		}
		host = strings.Trim(host, "[]")
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			writeError(w, http.StatusForbidden, "Host 必须是回环地址")
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			parsed, err := url.Parse(origin)
			if err != nil || parsed.Scheme != "http" || parsed.Host != r.Host {
				writeError(w, http.StatusForbidden, "请求来源与演示地址不一致")
				return
			}
		}
		if r.Method == http.MethodPost {
			mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if mediaType != "application/json" {
				writeError(w, http.StatusUnsupportedMediaType, "请求需要 application/json")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 512<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("请求必须包含一个 JSON 对象")
	}
	return nil
}

func (a *application) status(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	var snapshot *loadRun
	if a.current != nil {
		copy := *a.current
		copy.Windows = append([]completedWindow{}, a.current.Windows...)
		snapshot = &copy
	}
	a.mu.Unlock()
	writeJSON(w, http.StatusOK, struct {
		Database string   `json:"database"`
		Bench    string   `json:"bench"`
		History  string   `json:"history_root"`
		Run      *loadRun `json:"run"`
	}{a.database, a.benchmark, a.history.Name(), snapshot})
}

func (a *application) command(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Operation string `json:"operation"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	request.Operation = strings.ToUpper(request.Operation)
	if request.Operation != "GET" && request.Operation != "SET" {
		writeError(w, http.StatusBadRequest, "演示命令支持 GET 和 SET")
		return
	}
	if len(request.Key) > 1024 || len(request.Value) > 64<<10 {
		writeError(w, http.StatusBadRequest, "演示键最多 1024 字节，值最多 64 KiB")
		return
	}
	arguments := []string{request.Operation, request.Key}
	if request.Operation == "SET" {
		arguments = append(arguments, request.Value)
	}
	reply, elapsed, err := a.exchange(r.Context(), arguments...)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"operation": request.Operation, "key": request.Key, "reply_type": string(reply.Type),
		"reply": reply.String(), "elapsed_us": elapsed,
		"source": a.database, "measured_utc": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (a *application) exchange(ctx context.Context, arguments ...string) (respclient.Reply, float64, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	began := clockNow()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", a.database)
	if err != nil {
		return respclient.Reply{}, 0, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return respclient.Reply{}, 0, err
	}
	if err := respclient.WriteRequest(conn, arguments...); err != nil {
		return respclient.Reply{}, 0, err
	}
	reply, err := respclient.ReadReply(bufio.NewReader(conn))
	return reply, float64(clockElapsed(began)) / float64(time.Microsecond), err
}

func (a *application) serverInfo(w http.ResponseWriter, r *http.Request) {
	if err := decodeJSON(w, r, &struct{}{}); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	reply, elapsed, err := a.exchange(r.Context(), "INFO")
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if reply.Type != '$' || reply.Null {
		writeError(w, http.StatusBadGateway, "INFO 返回了非预期响应 "+reply.String())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"info": reply.Text, "elapsed_us": elapsed,
		"source": a.database, "measured_utc": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

type historyEntry struct {
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Modified string `json:"modified_utc"`
}

func (a *application) listHistory(w http.ResponseWriter, _ *http.Request) {
	entries := make([]historyEntry, 0)
	truncated := false
	err := fs.WalkDir(a.history.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&fs.ModeSymlink != 0 || entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".json" && filepath.Ext(path) != ".jsonl" {
			return nil
		}
		if len(entries) >= 500 {
			truncated = true
			return fs.SkipAll
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && info.Size() <= 4<<20 {
			entries = append(entries, historyEntry{path, info.Size(), info.ModTime().UTC().Format(time.RFC3339Nano)})
		}
		return nil
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Modified > entries[j].Modified })
	writeJSON(w, http.StatusOK, map[string]any{"root": a.history.Name(), "files": entries, "truncated": truncated})
}

func extractMeasurement(document []byte) (json.RawMessage, error) {
	document = bytes.TrimPrefix(document, []byte{0xef, 0xbb, 0xbf})
	var decoded struct {
		Mode        string          `json:"mode"`
		Version     int             `json:"schema_version"`
		Measurement json.RawMessage `json:"measurement"`
	}
	if err := json.Unmarshal(document, &decoded); err != nil {
		return nil, err
	}
	if len(decoded.Measurement) > 0 {
		return extractMeasurement(decoded.Measurement)
	}
	if decoded.Mode != "mixed" || decoded.Version != 2 {
		return nil, fmt.Errorf("请选择含 scripts/bench mixed schema_version=2 测量的 JSON")
	}
	return json.RawMessage(document), nil
}

func (a *application) readHistory(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if !fs.ValidPath(path) || strings.Contains(path, "\\") || (filepath.Ext(path) != ".json" && filepath.Ext(path) != ".jsonl") {
		writeError(w, http.StatusBadRequest, "历史路径必须是结果目录内的相对 JSON 路径")
		return
	}
	file, err := a.history.Open(path)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
		writeError(w, http.StatusBadRequest, "历史文件必须是最多 4 MiB 的普通文件")
		return
	}
	document, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil || len(document) > 4<<20 {
		writeError(w, http.StatusBadRequest, "历史文件读取失败或超过 4 MiB")
		return
	}
	measurement, err := extractMeasurement(document)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source": path, "measurement": measurement,
		"document": json.RawMessage(bytes.TrimPrefix(document, []byte{0xef, 0xbb, 0xbf})),
	})
}
