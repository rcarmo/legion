package namespace

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/ecosystem"
)

func TestDashboardAPIContracts(t *testing.T) {
	store := core.NewMemoryEventStore()
	id := uuid.New()
	ctx := context.Background()
	_ = store.CreateSession(ctx, id, core.RunConfig{})
	_, _ = store.Append(ctx, id, core.NewUserMessage("hello"))
	tree := NewTree()
	_ = tree.SetJSON("/cluster/peers/test", map[string]string{"endpoint_id": "test"})
	_ = tree.SetJSON("/fn/echo/manifest.json", map[string]string{"name": "echo"})
	ns := New(tree).WithResources(NewSessionResources(store, nil))
	agents := ecosystem.NewAgentToolRegistry(store)
	h := (REST{Namespace: ns, Agents: agents, APIKey: "secret"}).Handler()
	for _, path := range []string{"/", "/dashboard"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "Legion Dashboard") {
			t.Fatal(path, w.Code)
		}
	}
	for _, tc := range []struct{ path, field string }{{"/sessions", "sessions"}, {"/sessions/" + id.String(), "status"}, {"/sessions/" + id.String() + "/log", "entries"}, {"/agents", "agents"}, {"/functions", "functions"}, {"/cluster/peers", "peers"}} {
		req := httptest.NewRequest("GET", tc.path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 401 {
			t.Fatal("unprotected", tc.path, w.Code)
		}
		req.Header.Set("Authorization", "Bearer secret")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, req)
		var body map[string]json.RawMessage
		if w.Code != 200 {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if _, ok := body[tc.field]; !ok {
			t.Fatal(tc.path, w.Body.String())
		}
	}
	tools := ClusterTools{Namespace: ns, Base: core.EchoToolRegistry{}}
	b, err := tools.Dispatch(ctx, "cluster.peers", nil)
	if err != nil || !strings.Contains(string(b), "test") {
		t.Fatal(string(b), err)
	}
}
