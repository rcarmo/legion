package namespace

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/ecosystem"
)

func TestEcosystemREST(t *testing.T) {
	agents := ecosystem.NewAgentToolRegistry(core.NewMemoryEventStore())
	h := (REST{APIKey: "secret", Agents: agents, Tools: core.EchoToolRegistry{}}).Handler()
	cases := []struct {
		method, path, body, key string
		status                  int
	}{
		{"GET", "/agents", "", "", 401},
		{"POST", "/agents", `{"name":"researcher","config":{}}`, "secret", 201},
		{"GET", "/agents", "", "secret", 200},
		{"POST", "/agents", `{"name":"INVALID"}`, "secret", 400},
		{"POST", "/agents/researcher/invoke", `{"prompt":"x"}`, "secret", 422},
		{"POST", "/workflows/run", `{"nodes":[{"id":"a","tool":"echo","args":{"v":1}}]}`, "secret", 200},
		{"POST", "/workflows/run", `{"nodes":[{"id":"a","depends_on":["a"]}]}`, "secret", 422},
		{"DELETE", "/agents/researcher", "", "secret", 204},
		{"DELETE", "/agents/researcher", "", "secret", 404},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
		if c.key != "" {
			req.Header.Set("Authorization", "Bearer "+c.key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != c.status {
			t.Fatalf("%s %s: %d %s", c.method, c.path, w.Code, w.Body.String())
		}
	}
}
