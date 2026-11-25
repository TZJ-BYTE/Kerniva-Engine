package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"html/template"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"runtime"
	"strings"

	"github.com/wailsapp/wails/v3/pkg/application"
)

var errorPage = template.Must(template.New("unavailable").Parse(`<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>工作台连接</title><style>
body{background:#f9f7f2;color:#302e2a;font:16px/1.7 system-ui,sans-serif;margin:12vh auto;padding:2rem;max-width:45rem}
h1{font-size:1.65rem}p{overflow-wrap:anywhere}a{color:#8e4b34}</style>
<main><h1>暂时无法打开工作台</h1><p>{{.}}</p><a href="/">重新连接</a></main></html>`))

func unavailable(w http.ResponseWriter, home bool, err error) {
	w.Header().Set("Cache-Control", "no-store")
	if home {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Render the recovery view even on WebViews that substitute a native 5xx page.
		_ = errorPage.Execute(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func nativeOrigin() string {
	if runtime.GOOS == "windows" {
		return "http://wails.localhost"
	}
	return "wails://localhost"
}

// Only the native asset server calls this handler. It exposes the same UI and
// operations as the browser at one fixed, validated loopback origin. Database
// destinations are owned by the existing workbench configuration and adapter.
func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, host, _ := strings.Cut(nativeOrigin(), "://")
	if r.Host != host {
		http.Error(w, "工作台窗口来源无效", http.StatusForbidden)
		return
	}
	path := r.URL.Path
	home := path == "/" || path == "/index.html"
	api := strings.HasPrefix(path, "/api/")
	var body io.Reader
	if r.Method == http.MethodPost && api {
		origin, site := r.Header.Get("Origin"), r.Header.Get("Sec-Fetch-Site")
		if (origin != "" && origin != nativeOrigin()) || (site != "" && site != "same-origin" && site != "none") {
			http.Error(w, "仅接受当前工作台窗口的请求", http.StatusForbidden)
			return
		}
		contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if contentType != "application/json" {
			http.Error(w, "需要 application/json", http.StatusUnsupportedMediaType)
			return
		}
		const maxBody = 2 << 20 // matches workbench MAX_BODY
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "请求正文无效或过大", http.StatusRequestEntityTooLarge)
			return
		}
		body = bytes.NewReader(data)
	} else if r.Method != http.MethodGet {
		http.Error(w, "不支持该请求", http.StatusMethodNotAllowed)
		return
	}
	if path == "/api/desktop/choose-directory" {
		if r.Method != http.MethodPost || b.chooseDirectory == nil {
			http.Error(w, "请在桌面窗口中选择目录", http.StatusMethodNotAllowed)
			return
		}
		if !b.dialogMu.TryLock() {
			http.Error(w, "目录选择窗口已打开", http.StatusConflict)
			return
		}
		defer b.dialogMu.Unlock()
		dir, err := b.chooseDirectory()
		if err != nil {
			unavailable(w, false, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"path": dir})
		return
	}
	origin, err := b.endpoint(home)
	if err != nil {
		unavailable(w, home, err)
		return
	}
	dest, _ := url.Parse(origin)
	dest.Path, dest.RawQuery = path, r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, dest.String(), body)
	if err != nil {
		unavailable(w, home, err)
		return
	}
	if body != nil {
		req.Header.Set("Origin", origin)
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := b.client.Do(req)
	if err != nil {
		unavailable(w, home, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		http.Error(w, "管理服务返回了意外的重定向", http.StatusBadGateway)
		return
	}
	for _, name := range []string{"Content-Type", "Content-Security-Policy", "Cache-Control", "Referrer-Policy", "X-Content-Type-Options"} {
		if value := resp.Header.Get(name); value != "" {
			w.Header().Set(name, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var opts options
	flag.StringVar(&opts.attach, "url", "", "Attach to an existing local workbench URL")
	flag.StringVar(&opts.config, "config", "", "Use this configuration for the managed workbench")
	flag.StringVar(&opts.demoURL, "demo-url", "", "Attach the managed workbench to this local replication controller")
	flag.StringVar(&opts.source, "source", "", "Run the workbench backend from this source checkout")
	flag.Parse()
	b := newBackend(opts)
	defer b.close()
	app := application.New(application.Options{
		Name:       "Kerniva Workbench",
		Mac:        application.MacOptions{ApplicationShouldTerminateAfterLastWindowClosed: true},
		Assets:     application.AssetOptions{Handler: b, DisableLogging: true},
		OnShutdown: b.close,
	})
	window := app.Window.NewWithOptions(application.WebviewWindowOptions{
		Title: "Kerniva · 数据库工作台", URL: "/", Width: 1200, Height: 820,
		MinWidth: 600, MinHeight: 480, BackgroundColour: application.NewRGB(249, 247, 242),
	})
	b.chooseDirectory = func() (string, error) {
		return app.Dialog.OpenFile().AttachToWindow(window).SetTitle("选择 Kerniva 目录").
			CanChooseFiles(false).CanChooseDirectories(true).CanCreateDirectories(true).PromptForSingleSelection()
	}
	return app.Run()
}
