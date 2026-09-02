package namespace

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rcarmo/legion/internal/core"
	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

func (n *LegionNamespace) ReadPath(r *http.Request, p string) ([]byte, error) {
	return n.read(r.Context(), p)
}
func (n *LegionNamespace) WritePath(r *http.Request, p string, b []byte) ([]byte, error) {
	if err := n.write(r.Context(), p, b); err != nil {
		return nil, err
	}
	return n.read(r.Context(), p)
}

type SessionRateLimiter interface {
	Check(string) (time.Duration, bool)
}
type MetricsRenderer interface {
	Render(context.Context) (string, error)
}

type REST struct {
	Namespace          *LegionNamespace
	APIKey             string
	SessionRateLimiter SessionRateLimiter
	Metrics            MetricsRenderer
}

func (a REST) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /namespace/{path...}", a.path)
	mux.HandleFunc("PUT /namespace/{path...}", a.path)
	mux.HandleFunc("POST /sessions", a.sessions)
	mux.HandleFunc("GET /sessions", a.sessions)
	mux.HandleFunc("GET /sessions/{id}", a.session)
	mux.HandleFunc("GET /sessions/{id}/log", a.session)
	mux.HandleFunc("POST /sessions/{id}/messages", a.session)
	mux.HandleFunc("POST /sessions/{id}/reconcile", a.session)
	mux.HandleFunc("GET /cluster/{field}", a.cluster)
	mux.HandleFunc("POST /functions/{name}/invoke", a.function)
	mux.HandleFunc("POST /deploy/{operation}", a.deploy)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { writeHTTP(w, []byte(`{"ok":true}`), nil) })
	return a.authenticate(mux)
}
func (a REST) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.APIKey == "" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if provided == "" {
			provided = r.Header.Get("X-Legion-Key")
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(a.APIKey)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "API key required or invalid"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (a REST) metrics(w http.ResponseWriter, r *http.Request) {
	if a.Metrics == nil {
		writeHTTP(w, nil, fmt.Errorf("metrics unavailable"))
		return
	}
	body, err := a.Metrics.Render(r.Context())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if err != nil {
		writeHTTP(w, nil, err)
		return
	}
	_, _ = io.WriteString(w, body)
}
func (a REST) path(w http.ResponseWriter, r *http.Request) {
	p := "/" + r.PathValue("path")
	var b []byte
	var err error
	if r.Method == http.MethodGet {
		b, err = a.Namespace.ReadPath(r, p)
	} else {
		var body []byte
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
		if err == nil {
			b, err = a.Namespace.WritePath(r, p, body)
		}
	}
	writeHTTP(w, b, err)
}
func (a REST) sessions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			writeHTTP(w, nil, err)
			return
		}
		b, err := a.Namespace.WritePath(r, "/sessions/new", body)
		writeHTTP(w, b, err)
		return
	}
	if sr, ok := a.Namespace.resources.(*SessionResources); ok {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		xs, err := sr.Store.ListSessions(r.Context(), coreFilter(r.URL.Query().Get("status"), limit, offset))
		b, _ := json.Marshal(xs)
		writeHTTP(w, b, err)
		return
	}
	writeHTTP(w, nil, fmt.Errorf("session resources unavailable"))
}
func coreFilter(status string, limit, offset int) core.SessionFilter {
	if limit == 0 {
		limit = 100
	}
	return core.SessionFilter{Status: status, Limit: limit, Offset: offset}
}
func (a REST) session(w http.ResponseWriter, r *http.Request) {
	base := "/sessions/" + r.PathValue("id")
	var p string
	switch {
	case strings.HasSuffix(r.URL.Path, "/log"):
		p = base + "/turns"
	case strings.HasSuffix(r.URL.Path, "/messages"):
		p = base + "/turns"
	case strings.HasSuffix(r.URL.Path, "/reconcile"):
		p = base + "/reconcile"
	default:
		p = base + "/status"
	}
	if r.Method == http.MethodPost {
		if strings.HasSuffix(r.URL.Path, "/messages") && a.SessionRateLimiter != nil {
			if retry, ok := a.SessionRateLimiter.Check(r.PathValue("id")); !ok {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", strconv.FormatInt(max(1, int64((retry+time.Second-1)/time.Second)), 10))
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "session rate limit exceeded", "retry_after_ms": retry.Milliseconds()})
				return
			}
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			writeHTTP(w, nil, err)
			return
		}
		b, err := a.Namespace.WritePath(r, p, body)
		writeHTTP(w, b, err)
	} else {
		b, err := a.Namespace.ReadPath(r, p)
		writeHTTP(w, b, err)
	}
}
func (a REST) cluster(w http.ResponseWriter, r *http.Request) {
	b, err := a.Namespace.ReadPath(r, "/cluster/"+r.PathValue("field"))
	writeHTTP(w, b, err)
}
func (a REST) deploy(w http.ResponseWriter, r *http.Request) {
	operation := r.PathValue("operation")
	if operation != "push" && operation != "register" && operation != "route" && operation != "promote" {
		writeHTTP(w, nil, fmt.Errorf("unknown deployment operation %q", operation))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeHTTP(w, nil, err)
		return
	}
	b, err := a.Namespace.WritePath(r, "/deploy/"+operation, body)
	writeHTTP(w, b, err)
}
func (a REST) function(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeHTTP(w, nil, err)
		return
	}
	b, err := a.Namespace.WritePath(r, "/fn/"+r.PathValue("name"), body)
	writeHTTP(w, b, err)
}
func writeLimitHTTP(w http.ResponseWriter, err error) bool {
	var limit legionruntime.LimitError
	if !errors.As(err, &limit) {
		return false
	}
	status := http.StatusUnprocessableEntity
	switch limit.Kind {
	case legionruntime.LimitInput, legionruntime.LimitOutput:
		status = http.StatusRequestEntityTooLarge
	case legionruntime.LimitBusy, legionruntime.LimitRate:
		status = http.StatusTooManyRequests
	case legionruntime.LimitTimeout:
		status = http.StatusGatewayTimeout
	}
	if limit.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.FormatInt(max(1, int64((limit.RetryAfter+time.Second-1)/time.Second)), 10))
	}
	w.WriteHeader(status)
	body := map[string]any{"error": err.Error()}
	if limit.RetryAfter > 0 {
		body["retry_after_ms"] = limit.RetryAfter.Milliseconds()
	}
	_ = json.NewEncoder(w).Encode(body)
	return true
}
func writeHTTP(w http.ResponseWriter, b []byte, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		if writeLimitHTTP(w, err) {
			return
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if len(b) == 0 {
		b = []byte("null")
	}
	_, _ = w.Write(b)
}
