package namespace

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/rcarmo/legion/internal/core"
	"strings"
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

type REST struct{ Namespace *LegionNamespace }

func (a REST) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /namespace/{path...}", a.path)
	mux.HandleFunc("PUT /namespace/{path...}", a.path)
	mux.HandleFunc("POST /sessions", a.sessions)
	mux.HandleFunc("GET /sessions", a.sessions)
	mux.HandleFunc("GET /sessions/{id}", a.session)
	mux.HandleFunc("GET /sessions/{id}/log", a.session)
	mux.HandleFunc("POST /sessions/{id}/messages", a.session)
	mux.HandleFunc("GET /cluster/{field}", a.cluster)
	mux.HandleFunc("POST /functions/{name}/invoke", a.function)
	return mux
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
	default:
		p = base + "/status"
	}
	if r.Method == http.MethodPost {
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
func (a REST) function(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeHTTP(w, nil, err)
		return
	}
	b, err := a.Namespace.WritePath(r, "/fn/"+r.PathValue("name"), body)
	writeHTTP(w, b, err)
}
func writeHTTP(w http.ResponseWriter, b []byte, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if len(b) == 0 {
		b = []byte("null")
	}
	_, _ = w.Write(b)
}
