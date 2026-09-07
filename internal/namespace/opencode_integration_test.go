package namespace

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rcarmo/legion/internal/agent"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/ecosystem"
	"github.com/rcarmo/legion/internal/store"
)

func TestM5OpenCodeFreeIntegration(t *testing.T) {
	if os.Getenv("LEGION_OPENCODE_INTEGRATION") != "1" {
		t.Skip("opt-in live free OpenCode test")
	}
	model := os.Getenv("LEGION_TEST_MODEL")
	if model == "" {
		model = "opencode/nemotron-3.5-lightning-free"
	}
	if !strings.HasPrefix(model, "opencode/") || !strings.HasSuffix(model, "-free") {
		t.Fatal("live gate accepts only explicitly free OpenCode models")
	}
	if os.Getenv("OPENCODE_API_KEY") == "" {
		t.Setenv("OPENCODE_API_KEY", "public")
	}
	db, err := store.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	agents := ecosystem.NewAgentToolRegistry(db)
	tools := ecosystem.Tools{Agents: agents, Base: core.EchoToolRegistry{}}
	loop := agent.New(db, tools, agent.GoAI{})
	agents.Bind(loop)
	server := httptest.NewServer((REST{Namespace: New(NewTree()).WithResources(NewSessionResources(db, loop)), Agents: agents, Tools: tools}).Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	post := func(path string, input any) map[string]json.RawMessage {
		t.Helper()
		body, _ := json.Marshal(input)
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		b, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode >= 300 {
			t.Fatalf("%s: HTTP %d %s", path, response.StatusCode, b)
		}
		var result map[string]json.RawMessage
		if err = json.Unmarshal(b, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	created := post("/sessions", map[string]any{"model": model, "system_prompt": "Answer briefly. Do not use tools."})
	var id string
	_ = json.Unmarshal(created["id"], &id)
	result := post("/sessions/"+id+"/messages", map[string]string{"content": "Reply with exactly LEGION_OK."})
	var text string
	_ = json.Unmarshal(result["response"], &text)
	if !strings.Contains(text, "LEGION_OK") {
		t.Fatalf("unexpected live response: %q", text)
	}
	post("/agents", map[string]any{"name": "live-reviewer", "config": map[string]string{"model": model}})
	child := post("/agents/live-reviewer/invoke", map[string]any{"prompt": "Reply with exactly CHILD_OK.", "parent_run_id": id, "at_seq": 0})
	var parent string
	_ = json.Unmarshal(child["parent_run_id"], &parent)
	if parent != id || !strings.Contains(string(child["output"]), "CHILD_OK") {
		t.Fatalf("unexpected live child: %s", child["output"])
	}
	t.Logf("%s: REST reply %q and supervised child passed", model, text)
}
