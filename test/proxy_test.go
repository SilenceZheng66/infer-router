package test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"infer-router/internal/proxy"
	"infer-router/internal/registry"
)

// upstream 模拟 OpenAI 兼容流式后端。
func newUpstream() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"t%d\"}}]}\n\n", i)
			flusher.Flush()
			time.Sleep(5 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
}

func TestProxyStreamsThrough(t *testing.T) {
	up := newUpstream()
	defer up.Close()

	state := registry.NewState(registry.Backend{Name: "up", BaseURL: up.URL}, 3, 15)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
	rec := httptest.NewRecorder()

	proxy.Serve(rec, req, state)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, token := range []string{"t0", "t1", "t2", "[DONE]"} {
		if !strings.Contains(body, token) {
			t.Fatalf("body missing %q: %s", token, body)
		}
	}
	if got := state.InFlight(); got != 0 {
		t.Fatalf("in_flight=%d want 0 after serve", got)
	}
}

func TestProxyMarksFailureOnUpstreamError(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer up.Close()

	state := registry.NewState(registry.Backend{Name: "bad", BaseURL: up.URL}, 1, 60*time.Second)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
	rec := httptest.NewRecorder()

	proxy.Serve(rec, req, state)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
	if state.Healthy() {
		t.Fatal("5xx should mark failure and open breaker (threshold=1)")
	}
}
