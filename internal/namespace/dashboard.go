package namespace

import (
	_ "embed"
	"encoding/json"
	"net/http"
)

//go:embed static/dashboard.html
var dashboardHTML []byte

func dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(dashboardHTML)
}
func (a REST) functions(w http.ResponseWriter, r *http.Request) {
	manifests := []json.RawMessage{}
	for _, name := range a.Namespace.tree.List("/fn") {
		b, err := a.Namespace.ReadPath(r, "/fn/"+name+"/manifest.json")
		if err != nil {
			writeHTTP(w, nil, err)
			return
		}
		manifests = append(manifests, json.RawMessage(b))
	}
	b, err := json.Marshal(map[string]any{"functions": manifests})
	writeHTTP(w, b, err)
}
