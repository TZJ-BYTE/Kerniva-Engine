package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkbenchOrigin(t *testing.T) {
	if origin, err := workbenchOrigin("http://localhost:18782/"); err != nil || origin != "http://127.0.0.1:18782" {
		t.Fatalf("localhost: %q, %v", origin, err)
	}
	for _, raw := range []string{
		"https://127.0.0.1:18782", "http://remote.invalid:18782", "http://user@localhost:18782",
		"http://localhost:18782/api/state", "http://localhost:18782/?port=20000",
		"http://localhost:18782/#elsewhere", "http://localhost:65536", "http://localhost:0",
	} {
		if _, err := workbenchOrigin(raw); err == nil {
			t.Errorf("accepted invalid workbench origin %s", raw)
		}
	}
}

func TestWindowUsesOnlyAttachedOrigin(t *testing.T) {
	var mutations int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"apiVersion":2,"configId":"test"}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "workbench") })
	mux.HandleFunc("/api/settings", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") != "http://"+r.Host {
			t.Error("service received a foreign Origin")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != `{"pageSize":17}` {
			t.Errorf("wrong POST body %q, %v", body, err)
		}
		mutations++
		io.WriteString(w, "saved")
	})
	mux.HandleFunc("/moved", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/settings", http.StatusTemporaryRedirect)
	})
	service := httptest.NewServer(mux)
	defer service.Close()
	b := newBackend(options{attach: service.URL, source: "unused"})
	defer b.close()
	request := func(method, path, origin, host string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, nativeOrigin()+path, strings.NewReader(`{"pageSize":17}`))
		req.Header.Set("Content-Type", "application/json")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if host != "" {
			req.Host = host
		}
		rec := httptest.NewRecorder()
		b.ServeHTTP(rec, req)
		return rec
	}
	if got := request("GET", "/", "", ""); got.Body.String() != "workbench" {
		t.Fatalf("window did not attach: %d %s", got.Code, got.Body)
	}
	if got := request("POST", "/api/settings", "http://other.invalid", ""); got.Code != http.StatusForbidden {
		t.Errorf("foreign Origin returned %d", got.Code)
	}
	if got := request("POST", "/api/settings", "", "other.invalid"); got.Code != http.StatusForbidden {
		t.Errorf("foreign Host returned %d", got.Code)
	}
	if got := request("POST", "/api/settings", nativeOrigin(), ""); got.Body.String() != "saved" {
		t.Errorf("setting was not saved: %s", got.Body)
	}
	if got := request("GET", "/moved", "", ""); got.Code != http.StatusBadGateway || got.Header().Get("Location") != "" {
		t.Errorf("redirect escaped gateway: %d %v", got.Code, got.Header())
	}
	if mutations != 1 {
		t.Errorf("got %d mutations, want one", mutations)
	}
}
