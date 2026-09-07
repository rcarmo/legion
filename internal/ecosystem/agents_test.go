package ecosystem

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

type stubLoop struct {
	store  core.EventStore
	starts int
}

func (s *stubLoop) Start(c context.Context, cfg core.RunConfig) (core.RunID, error) {
	s.starts++
	id := uuid.New()
	if err := s.store.CreateSession(c, id, cfg); err != nil {
		return id, err
	}
	_, err := s.store.Append(c, id, core.SessionStarted(cfg))
	return id, err
}
func (s *stubLoop) Recover(context.Context, core.RunID) error { return nil }
func (s *stubLoop) Resume(c context.Context, id core.RunID, e core.ExternalEvent) error {
	_, err := s.store.Append(c, id, core.NewUserMessage(e.Content))
	return err
}
func (s *stubLoop) Resolve(c context.Context, id core.RunID) (core.TurnEnvelope, error) {
	e := core.AssistantMessage(map[string]string{"content": "done"}, "stub/model", 1, 1, 1)
	seq, err := s.store.Append(c, id, e)
	if err != nil {
		return core.TurnEnvelope{}, err
	}
	err = s.store.SetStatus(c, id, core.StatusCompleted)
	return core.TurnEnvelope{RunID: id, Seq: seq, Event: e}, err
}
func TestAgentFreshAndSupervised(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	loop := &stubLoop{store: store}
	r := NewAgentToolRegistry(store)
	r.Bind(loop)
	if err := r.Register(AgentProfile{Name: "researcher"}); err != nil {
		t.Fatal(err)
	}
	fresh, err := r.Run(ctx, "researcher", "assignment", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ParentRunID != nil || fresh.Status.Status != "completed" || loop.starts != 1 {
		t.Fatal(fresh)
	}
	before, _ := store.ReadLog(ctx, fresh.RunID)
	child, err := r.Run(ctx, "researcher", "child assignment", &ParentRun{RunID: fresh.RunID, AtSeq: 0})
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentRunID == nil || *child.ParentRunID != fresh.RunID || child.RunID == fresh.RunID || loop.starts != 1 {
		t.Fatal(child)
	}
	after, _ := store.ReadLog(ctx, fresh.RunID)
	if len(after) != len(before) {
		t.Fatal("parent mutated")
	}
	log, err := store.ReadLog(ctx, child.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(log[len(log)-2].Event.Payload), "child assignment") {
		t.Fatal(log)
	}
	if _, err = r.Run(ctx, "researcher", "bad", &ParentRun{RunID: fresh.RunID, AtSeq: 999}); err == nil {
		t.Fatal("accepted missing sequence")
	}
}
func TestAgentRegistryValidation(t *testing.T) {
	r := NewAgentToolRegistry(core.NewMemoryEventStore())
	for _, name := range []string{"", "UPPER", "-x", "x-", "a.b", strings.Repeat("x", 64)} {
		if err := r.Register(AgentProfile{Name: name}); err == nil {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"z", "a"} {
		if err := r.Register(AgentProfile{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	defs, err := r.Definitions(context.Background())
	if err != nil || len(defs) != 2 || defs[0].Name != "agent.a" || defs[0].Effect != core.EffectWrite {
		t.Fatal(defs, err)
	}
	if _, err = r.Run(context.Background(), "a", "", nil); err == nil {
		t.Fatal("unbound executor accepted")
	}
	if !r.Unregister("a") || r.Unregister("a") {
		t.Fatal("unregister")
	}
}
func TestAgentDispatch(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	r := NewAgentToolRegistry(store)
	r.Bind(&stubLoop{store: store})
	_ = r.Register(AgentProfile{Name: "a"})
	for _, args := range []string{`{}`, `{"prompt":1}`, `{"prompt":"x","parent_run_id":"bad","at_seq":0}`, `{"prompt":"x","at_seq":0}`, `{"prompt":"x","parent_run_id":"00000000-0000-0000-0000-000000000000"}`, `{"prompt":"x","at_seq":-1}`} {
		if _, err := r.Dispatch(ctx, "agent.a", json.RawMessage(args)); err == nil {
			t.Fatal(args)
		}
	}
	b, err := r.Dispatch(ctx, "agent.a", json.RawMessage(`{"prompt":"review","dependencies":{"a":{"answer":42}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var child ChildRun
	if err = json.Unmarshal(b, &child); err != nil {
		t.Fatal(err)
	}
	log, _ := store.ReadLog(ctx, child.RunID)
	if !strings.Contains(string(log[1].Event.Payload), "Dependency outputs:") {
		t.Fatal(string(log[1].Event.Payload))
	}
}
