package namespace

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

type rejectLimiter struct{}

type limitFunctions struct{ err error }

func (rejectLimiter) Check(string) (time.Duration, bool) { return 1500 * time.Millisecond, false }
func (f limitFunctions) Invoke(context.Context, string, []byte) ([]byte, error) {
	return nil, f.err
}

func TestRESTAPIKeyAndPublicHealth(t *testing.T) {
	h := REST{Namespace: New(NewTree()), APIKey: "secret"}.Handler()
	for _, tc := range []struct {
		path, key string
		want      int
	}{{"/health", "", 200}, {"/sessions", "", 401}, {"/sessions", "wrong", 401}, {"/namespace/cluster/health", "secret", 200}} {
		r := httptest.NewRequest("GET", tc.path, nil)
		if tc.key != "" {
			r.Header.Set("Authorization", "Bearer "+tc.key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s key=%q code=%d body=%s", tc.path, tc.key, w.Code, w.Body.String())
		}
	}
}
func TestRESTFunctionLimitReturnsRetryMetadata(t *testing.T) {
	ns := New(NewTree()).WithFunctions(limitFunctions{err: legionruntime.LimitError{Function: "busy", Kind: legionruntime.LimitBusy, RetryAfter: 100 * time.Millisecond}})
	h := REST{Namespace: ns}.Handler()
	r := httptest.NewRequest("POST", "/functions/busy/invoke", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || body["retry_after_ms"] != float64(100) {
		t.Fatalf("code=%d retry=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

func TestRESTSessionRateLimitReturnsRetryAfter(t *testing.T) {
	ns := New(NewTree()).WithResources(NewSessionResources(&fakeStore{}, fakeLoop{&fakeStore{}}))
	h := REST{Namespace: ns, SessionRateLimiter: rejectLimiter{}}.Handler()
	r := httptest.NewRequest("POST", "/sessions/00000000-0000-0000-0000-000000000001/messages", strings.NewReader(`{"content":"hi"}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	body, _ := io.ReadAll(w.Result().Body)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "2" {
		t.Fatalf("code=%d retry=%q body=%s", w.Code, w.Header().Get("Retry-After"), body)
	}
}
