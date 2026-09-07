package namespace

import (
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	goai "github.com/rcarmo/go-ai"
	"github.com/rcarmo/go-ai/inference/provider/faux"
	"github.com/rcarmo/legion/internal/agent"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/ecosystem"
	"github.com/rcarmo/legion/internal/store"
)

// This integration gate runs the unchanged channel adapters through real HTTP,
// GoAI and SQLite. Only inference is replaced by go-ai's deterministic provider.
func TestM5ChannelAgentIntegration(t *testing.T) {
	if os.Getenv("LEGION_M5_INTEGRATION") != "1" {
		t.Skip("set LEGION_M5_INTEGRATION=1 for Bun channel integration")
	}
	reg := faux.Register(&faux.Options{Provider: "legion-m5-faux", Models: []faux.ModelDef{{ID: "model"}}})
	responses := make([]faux.ResponseStep, 32)
	for i := range responses {
		responses[i] = faux.TextMessage("local model reply")
	}
	reg.SetResponses(responses)
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	agents := ecosystem.NewAgentToolRegistry(db)
	tools := ecosystem.Tools{Agents: agents, Base: core.EchoToolRegistry{}}
	loop := agent.New(db, tools, agent.GoAI{})
	agents.Bind(loop)
	server := httptest.NewServer((REST{Namespace: New(NewTree()).WithResources(NewSessionResources(db, loop)), Agents: agents, Tools: tools, APIKey: "integration-key"}).Handler())
	defer server.Close()
	cmd := exec.Command("bun", "../../tests/integration/go_channel_smoke.ts")
	cmd.Env = append(os.Environ(), "LEGION_TEST_URL="+server.URL, "LEGION_TEST_API_KEY=integration-key")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("channel integration: %v\n%s", err, out)
	} else {
		t.Log(string(out))
	}
	// Prove an LLM tool call can use the same registry to start another agent.
	reg.SetResponses([]faux.ResponseStep{&goai.Message{Role: goai.RoleAssistant, Content: []goai.ContentBlock{{Type: "toolCall", ID: "child-call", Name: "agent.reviewer", Arguments: map[string]any{"prompt": "review"}}}, StopReason: goai.StopReasonToolUse}, faux.TextMessage("child reply"), faux.TextMessage("parent reply")})
	ctx := context.Background()
	id, err := loop.Start(ctx, core.RunConfig{Model: "legion-m5-faux/model", Tools: []string{"agent.reviewer"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = loop.Resume(ctx, id, core.UserMessage("delegate")); err != nil {
		t.Fatal(err)
	}
	if _, err = loop.Resolve(ctx, id); err != nil {
		t.Fatal(err)
	}
	log, err := db.ReadLog(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range log {
		if e.Event.Kind.Kind == "tool_result" && strings.Contains(string(e.Event.Payload), "child reply") {
			found = true
		}
	}
	if !found {
		t.Fatal("agent child result absent from durable parent log")
	}
	server.Close()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.ReadLog(ctx, id)
	if err != nil || len(persisted) != len(log) {
		t.Fatal("durable replay", len(persisted), len(log), err)
	}
}
