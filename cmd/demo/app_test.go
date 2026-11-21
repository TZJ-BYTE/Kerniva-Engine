package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/respclient"
)

const fixtureMeasurement = `{"schema_version":2,"mode":"mixed","started_utc":"2026-11-17T00:00:00Z","duration_seconds":3,"elapsed_seconds":3.1,"actual_success_rate":9.677,"offered":30,"succeeded":30,"failures":0,"get":{"succeeded":20,"failures":0,"success_latency":{"count":20,"p99_us":7}},"set":{"succeeded":10,"failures":0,"success_latency":{"count":10,"p99_us":9}},"windows":[{"planned_second":0,"get":{"succeeded":20,"failures":0,"success_latency":{"count":20,"p99_us":7}},"set":{"succeeded":10,"failures":0,"success_latency":{"count":10,"p99_us":9}}}]}`

func TestMain(m *testing.M) {
	if helper := os.Getenv("KERNIVA_DEMO_TEST_HELPER"); helper != "" {
		if helper == "unrelated" {
			time.Sleep(time.Minute)
			os.Exit(0)
		}
		var ready, start string
		for index, argument := range os.Args {
			if argument == "-ready-file" {
				ready = os.Args[index+1]
			}
			if argument == "-start-file" {
				start = os.Args[index+1]
			}
		}
		if err := os.WriteFile(ready, []byte("ready\n"), 0600); err != nil {
			os.Exit(2)
		}
		for {
			if _, err := os.Stat(start); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		delay, _ := time.ParseDuration(os.Getenv("KERNIVA_DEMO_TEST_DELAY"))
		time.Sleep(delay)
		io.WriteString(os.Stdout, fixtureMeasurement)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func testApplication(t *testing.T) *application {
	t.Helper()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	app := &application{database: "127.0.0.1:1", history: root, lifetime: context.Background()}
	t.Cleanup(func() {
		app.stopLoad()
		app.work.Wait()
		root.Close()
	})
	return app
}

func testRequest(t *testing.T, app *application, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	web, err := fs.Sub(assets, "web")
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	app.handler(web).ServeHTTP(response, request)
	return response
}

func TestLoopbackAndBrowserRequestBoundaries(t *testing.T) {
	for _, address := range []string{"0.0.0.0:18080", ":18080", "192.168.1.2:16379", "example.com:80"} {
		if _, err := loopbackAddress(address); err == nil {
			t.Fatalf("accepted non-loopback address %s", address)
		}
	}
	for _, address := range []string{"127.0.0.1:18080", "localhost:16379", "[::1]:18080"} {
		if _, err := loopbackAddress(address); err != nil {
			t.Fatal(err)
		}
	}
	app := testApplication(t)
	web, _ := fs.Sub(assets, "web")
	for _, test := range []struct {
		host, origin, contentType string
		want                      int
	}{
		{"evil.example", "", "application/json", http.StatusForbidden},
		{"127.0.0.1", "http://evil.example", "application/json", http.StatusForbidden},
		{"127.0.0.1", "", "text/plain", http.StatusUnsupportedMediaType},
		{"127.0.0.1", "http://127.0.0.1", "application/json", http.StatusOK},
	} {
		request := httptest.NewRequest(http.MethodPost, "http://"+test.host+"/api/load/stop", strings.NewReader("{}"))
		request.Header.Set("Origin", test.origin)
		request.Header.Set("Content-Type", test.contentType)
		response := httptest.NewRecorder()
		app.handler(web).ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatalf("host=%s origin=%s got=%d want=%d", test.host, test.origin, response.Code, test.want)
		}
	}
	if response := testRequest(t, app, http.MethodGet, "/", ""); response.Code != 200 || !strings.Contains(response.Body.String(), "内存数据库演示") {
		t.Fatalf("embedded page unavailable: %d", response.Code)
	}
}

func TestHistoryMeasurementWrapperAndTraversal(t *testing.T) {
	app := testApplication(t)
	if err := app.history.WriteFile("result.json", []byte(`{"status":"complete","measurement":`+fixtureMeasurement+`}`), 0600); err != nil {
		t.Fatal(err)
	}
	response := testRequest(t, app, http.MethodGet, "/api/history/file?path=result.json", "")
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"actual_success_rate":9.677`) {
		t.Fatalf("history measurement lost: %d %s", response.Code, response.Body)
	}
	if response := testRequest(t, app, http.MethodGet, "/api/history", ""); response.Code != 200 || !strings.Contains(response.Body.String(), "result.json") {
		t.Fatalf("history list unavailable: %d %s", response.Code, response.Body)
	}
	for _, path := range []string{"../outside.json", "/outside.json", `..\outside.json`} {
		response := testRequest(t, app, http.MethodGet, "/api/history/file?path="+url.QueryEscape(path), "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("accepted history traversal %s: %d", path, response.Code)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte(fixtureMeasurement), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(app.history.Name(), "escape.json")); err != nil {
		t.Logf("symlink creation unavailable on this host: %v", err)
		return
	}
	if response := testRequest(t, app, http.MethodGet, "/api/history/file?path=escape.json", ""); response.Code == 200 {
		t.Fatal("history escaped its root through a symlink")
	}
}

func TestCommandChineseEmptyValuesAndIdleStatus(t *testing.T) {
	app := testApplication(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	app.database = listener.Addr().String()
	var accepted atomic.Int64
	var values sync.Map
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer conn.Close()
				request, err := respclient.ReadReply(bufio.NewReader(conn))
				if err != nil || len(request.Items) < 1 {
					return
				}
				if request.Items[0].Text == "INFO" {
					info := "# database\nmemory_keys:1\nused_memory_bytes:42\nmode:Memory\n"
					wire := respclient.Request(info)
					conn.Write(wire[4:])
					return
				}
				if len(request.Items) < 2 {
					return
				}
				key := request.Items[1].Text
				if request.Items[0].Text == "SET" {
					values.Store(key, request.Items[2].Text)
					io.WriteString(conn, "+OK\r\n")
					return
				}
				value, found := values.Load(key)
				if !found {
					io.WriteString(conn, "$-1\r\n")
					return
				}
				wire := respclient.Request(value.(string))
				conn.Write(wire[4:]) // Strip *1 CRLF to return a single bulk string.
			}()
		}
	}()
	if response := testRequest(t, app, http.MethodGet, "/api/status", ""); response.Code != 200 || accepted.Load() != 0 {
		t.Fatal("idle status generated database traffic")
	}
	for _, value := range []string{"你好，世界", ""} {
		payload, _ := json.Marshal(map[string]string{"operation": "SET", "key": "中文 键", "value": value})
		if response := testRequest(t, app, http.MethodPost, "/api/command", string(payload)); response.Code != 200 {
			t.Fatalf("SET failed: %d %s", response.Code, response.Body)
		}
		response := testRequest(t, app, http.MethodPost, "/api/command", `{"operation":"GET","key":"中文 键"}`)
		var result struct {
			Reply   string  `json:"reply"`
			Elapsed float64 `json:"elapsed_us"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != 200 || result.Reply != (respclient.Reply{Type: '$', Text: value}).String() || result.Elapsed <= 0 {
			t.Fatalf("GET corrupted UTF-8/empty value: %d %s, %v", response.Code, response.Body, err)
		}
	}
	requestsBeforeInfo := accepted.Load()
	testRequest(t, app, http.MethodGet, "/api/status", "")
	if accepted.Load() != requestsBeforeInfo {
		t.Fatal("local status polling issued a database command")
	}
	response := testRequest(t, app, http.MethodPost, "/api/info", "{}")
	var info struct {
		Text   string `json:"info"`
		Source string `json:"source"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &info); err != nil || response.Code != http.StatusOK {
		t.Fatalf("manual INFO failed: %d %s, %v", response.Code, response.Body, err)
	}
	if !strings.Contains(info.Text, "memory_keys:1") || info.Source != app.database || accepted.Load() != requestsBeforeInfo+1 {
		t.Fatalf("manual INFO lost its raw source: %+v", info)
	}
}

func TestLoadWindowsUseBenchJSONAndBoundInputs(t *testing.T) {
	t.Setenv("KERNIVA_DEMO_TEST_HELPER", "bench")
	app := testApplication(t)
	app.benchmark, _ = os.Executable()
	if response := testRequest(t, app, http.MethodPost, "/api/load/start", `{"rate":20001,"clients":4,"duration_seconds":6,"read_percent":95}`); response.Code != http.StatusBadRequest {
		t.Fatal("load rate bound ignored")
	}
	response := testRequest(t, app, http.MethodPost, "/api/load/start", `{"rate":10,"clients":1,"duration_seconds":6,"read_percent":95}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("load start failed: %d %s", response.Code, response.Body)
	}
	app.work.Wait()
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.current.Phase != "completed" || len(app.current.Windows) != 2 || app.current.Error != "" {
		t.Fatalf("load windows incomplete: %+v", app.current)
	}
	for _, window := range app.current.Windows {
		if strings.TrimSpace(string(window.Measurement)) != fixtureMeasurement {
			t.Fatal("bench measurement changed by the demo")
		}
		if _, err := app.history.Stat(window.Source); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStopCancelsOnlyOwnedBench(t *testing.T) {
	t.Setenv("KERNIVA_DEMO_TEST_HELPER", "bench")
	t.Setenv("KERNIVA_DEMO_TEST_DELAY", "30s")
	app := testApplication(t)
	app.benchmark, _ = os.Executable()
	unrelated := exec.Command(app.benchmark)
	unrelated.Env = append(os.Environ(), "KERNIVA_DEMO_TEST_HELPER=unrelated")
	if err := unrelated.Start(); err != nil {
		t.Fatal(err)
	}
	unrelatedExit := make(chan struct{})
	go func() { unrelated.Wait(); close(unrelatedExit) }()
	t.Cleanup(func() { unrelated.Process.Kill(); <-unrelatedExit })
	settings := `{"rate":10,"clients":1,"duration_seconds":3,"read_percent":95}`
	if response := testRequest(t, app, http.MethodPost, "/api/load/start", settings); response.Code != http.StatusAccepted {
		t.Fatalf("load start failed: %d %s", response.Code, response.Body)
	}
	if response := testRequest(t, app, http.MethodPost, "/api/load/start", settings); response.Code != http.StatusConflict {
		t.Fatal("concurrent load start accepted")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		app.mu.Lock()
		phase := app.current.Phase
		app.mu.Unlock()
		if phase == "measuring" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("owned bench did not start, phase=%s", phase)
		}
		time.Sleep(5 * time.Millisecond)
	}
	testRequest(t, app, http.MethodPost, "/api/load/stop", "{}")
	app.work.Wait()
	app.mu.Lock()
	phase, windows := app.current.Phase, len(app.current.Windows)
	app.mu.Unlock()
	if phase != "stopped" || windows != 0 {
		t.Fatalf("incomplete window published after stop: phase=%s windows=%d", phase, windows)
	}
	select {
	case <-unrelatedExit:
		t.Fatal("unrelated process exited")
	default:
	}
}
