package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	goai "github.com/rcarmo/go-ai"
	"github.com/rcarmo/go-ai/inference/provider/faux"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/store"
)

type scripted struct {
	messages []*goai.Message
	calls    int
	contexts []*goai.Context
	options  []*goai.StreamOptions
}

func (s *scripted) Stream(_ context.Context, _ string, c *goai.Context, options *goai.StreamOptions) (<-chan goai.Event, error) {
	s.contexts = append(s.contexts, c)
	s.options = append(s.options, options)
	if s.calls >= len(s.messages) {
		return nil, fmt.Errorf("no response")
	}
	m := s.messages[s.calls]
	s.calls++
	ch := make(chan goai.Event, 1)
	ch <- &goai.DoneEvent{Reason: m.StopReason, Message: m}
	close(ch)
	return ch, nil
}
func text(v string) *goai.Message {
	return &goai.Message{Role: goai.RoleAssistant, Content: []goai.ContentBlock{{Type: "text", Text: v}}, StopReason: goai.StopReasonStop, Usage: &goai.Usage{Input: 10, Output: 2}}
}
func tool(id, name string, args map[string]any) *goai.Message {
	return &goai.Message{Role: goai.RoleAssistant, Content: []goai.ContentBlock{{Type: "toolCall", ID: id, Name: name, Arguments: args}}, StopReason: goai.StopReasonToolUse, Usage: &goai.Usage{Input: 10, Output: 2}}
}
func config(b core.Budget) core.RunConfig {
	return core.RunConfig{Model: "test/model", Tools: []string{"echo", "fail"}, Budget: b}
}
func kinds(log []core.TurnEnvelope) []string {
	out := []string{}
	for _, e := range log {
		out = append(out, e.Event.Kind.Kind)
	}
	return out
}

func TestTurnStateRejectsInvalidTransition(t *testing.T) {
	state := newTurnState()
	if err := state.transition(core.PhaseSetup, core.PhaseRunning); err != nil {
		t.Fatal(err)
	}
	if err := state.transition(core.PhaseSetup, core.PhaseTools); err == nil {
		t.Fatal("invalid phase transition accepted")
	}
	if err := state.transition(core.PhaseRunning, core.PhaseFinalizing); err != nil {
		t.Fatal(err)
	}
	if err := state.transition(core.PhaseFinalizing, core.PhaseCompleted); err != nil {
		t.Fatal(err)
	}
}

func TestLoopToolContinuationAndCompletion(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	ai := &scripted{messages: []*goai.Message{tool("call-1", "echo", map[string]any{"message": "hello"}), text("finished")}}
	loop := New(store, core.EchoToolRegistry{}, ai)
	cfg := config(core.Budget{})
	cfg.Metadata = json.RawMessage(`{"trace_id":"turn-1"}`)
	id, err := loop.Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = loop.Resume(ctx, id, core.UserMessage("begin")); err != nil {
		t.Fatal(err)
	}
	final, err := loop.Resolve(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	_ = json.Unmarshal(final.Event.Payload, &p)
	if p["content"] != "finished" {
		t.Fatal(p)
	}
	st, _ := store.SessionStatus(ctx, id)
	if st.Status != "completed" {
		t.Fatal(st)
	}
	log, _ := store.ReadLog(ctx, id)
	want := []string{"session_started", "user_message", "model_call_intent", "assistant_message", "tool_call_intent", "tool_result", "model_call_intent", "assistant_message"}
	if fmt.Sprint(kinds(log)) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", kinds(log), want)
	}
	if ai.calls != 2 || len(ai.contexts[1].Messages) != 3 {
		t.Fatalf("calls=%d context=%#v", ai.calls, ai.contexts[1].Messages)
	}
	if ai.options[0].SessionID != id.String() || ai.options[0].Metadata["trace_id"] != "turn-1" || ai.options[0].TelemetryContext == nil {
		t.Fatalf("stream options %#v", ai.options[0])
	}
	tr := ai.contexts[1].Messages[2]
	if tr.Role != goai.RoleToolResult || tr.ToolCallID != "call-1" || tr.IsError {
		t.Fatalf("tool result %#v", tr)
	}
}
func TestLoopPersistsToolErrorsForContinuation(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	ai := &scripted{messages: []*goai.Message{tool("bad", "fail", map[string]any{}), text("recovered")}}
	loop := New(store, core.EchoToolRegistry{}, ai)
	id, _ := loop.Start(ctx, config(core.Budget{}))
	_ = loop.Resume(ctx, id, core.UserMessage("go"))
	if _, err := loop.Resolve(ctx, id); err != nil {
		t.Fatal(err)
	}
	if !ai.contexts[1].Messages[2].IsError {
		t.Fatal("tool error not visible")
	}
}
func TestLoopBudgetHaltBeforeTool(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	one := uint32(1)
	ai := &scripted{messages: []*goai.Message{tool("call", "echo", map[string]any{})}}
	loop := New(store, core.EchoToolRegistry{}, ai)
	id, _ := loop.Start(ctx, config(core.Budget{MaxTurns: &one}))
	_ = loop.Resume(ctx, id, core.UserMessage("go"))
	_, err := loop.Resolve(ctx, id)
	var be core.BudgetExceededError
	if !errors.As(err, &be) || be.Field != "max_turns" {
		t.Fatal(err)
	}
	log, _ := store.ReadLog(ctx, id)
	for _, k := range kinds(log) {
		if k == "tool_call_intent" || k == "tool_result" {
			t.Fatalf("tool executed: %v", kinds(log))
		}
	}
	st, _ := store.SessionStatus(ctx, id)
	if st.Status != "budget_halt" {
		t.Fatal(st)
	}
}
func TestBudgetStateReconstructsCostFromDurableLog(t *testing.T) {
	message := text("costly")
	message.Usage.Cost.Total = 0.25
	event := core.AssistantMessage(map[string]any{"content": "costly", "message": message}, "test/model", 10, 2, 3)
	budget := budgetFromLog([]core.TurnEnvelope{{Event: event}})
	if budget.CostUSD != 0.25 || budget.Turns != 1 || budget.TokensIn != 10 || budget.TokensOut != 2 || budget.WallMS != 3 {
		t.Fatalf("budget=%#v", budget)
	}
}

func TestResumeRejectsUnknownExternalEvent(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	loop := New(store, core.EchoToolRegistry{}, &scripted{})
	id, err := loop.Start(ctx, config(core.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	if err = loop.Resume(ctx, id, core.ExternalEvent{Type: "unknown"}); err == nil {
		t.Fatal("unknown event accepted")
	}
}

func TestRecoveryClassifiesInterruptedEffects(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	loop := New(store, core.EchoToolRegistry{}, &scripted{})
	id, _ := loop.Start(ctx, config(core.Budget{}))
	_, _ = store.Append(ctx, id, core.ModelCallIntent())
	if err := loop.Recover(ctx, id); err != nil {
		t.Fatal(err)
	}
	st, _ := store.SessionStatus(ctx, id)
	if st.Status != "idle" {
		t.Fatal(st)
	}
	_, _ = store.Append(ctx, id, core.ToolCallIntent("echo", "dangling", core.EffectWrite, map[string]any{}))
	if err := loop.Recover(ctx, id); !errors.Is(err, core.ErrPendingReconciliation) {
		t.Fatal(err)
	}
	st, _ = store.SessionStatus(ctx, id)
	if st.Status != "pending_reconciliation" || st.CallID != "dangling" {
		t.Fatal(st)
	}
}
func TestReconcileSkipAndRetry(t *testing.T) {
	for _, action := range []string{"skip", "retry"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			store := core.NewMemoryEventStore()
			loop := New(store, core.EchoToolRegistry{}, &scripted{})
			id, _ := loop.Start(ctx, config(core.Budget{}))
			_, _ = store.Append(ctx, id, core.ToolCallIntent("echo", "dangling", core.EffectWrite, map[string]any{"message": "again"}))
			if err := loop.Recover(ctx, id); !errors.Is(err, core.ErrPendingReconciliation) {
				t.Fatal(err)
			}
			if err := loop.Reconcile(ctx, id, action); err != nil {
				t.Fatal(err)
			}
			status, _ := store.SessionStatus(ctx, id)
			if status != core.StatusIdle {
				t.Fatal(status)
			}
			log, _ := store.ReadLog(ctx, id)
			got := kinds(log)
			if got[len(got)-2] != "tool_result" || got[len(got)-1] != "tool_call_reconciled" {
				t.Fatal(got)
			}
		})
	}
}
func TestReconcileRetryRejectsLegacyIntent(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	loop := New(store, core.EchoToolRegistry{}, &scripted{})
	id, _ := loop.Start(ctx, config(core.Budget{}))
	_, _ = store.Append(ctx, id, core.TurnEvent{Kind: core.EventKind{Kind: "tool_call_intent", ToolName: "echo", CallID: "legacy"}})
	_ = loop.Recover(ctx, id)
	if err := loop.Reconcile(ctx, id, "retry"); err == nil {
		t.Fatal("legacy retry accepted")
	}
	status, _ := store.SessionStatus(ctx, id)
	if status.Status != "pending_reconciliation" {
		t.Fatal(status)
	}
}
func TestSQLiteSessionCompletesAndReplaysAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legion.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ai := &scripted{messages: []*goai.Message{
		tool("call-1", "echo", map[string]any{"message": "durable"}),
		text("complete"),
	}}
	loop := New(db, core.EchoToolRegistry{}, ai)
	id, err := loop.Start(ctx, config(core.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	if err = loop.Resume(ctx, id, core.UserMessage("begin")); err != nil {
		t.Fatal(err)
	}
	if _, err = loop.Resolve(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	log, err := db.ReadLog(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"session_started", "user_message", "model_call_intent", "assistant_message", "tool_call_intent", "tool_result", "model_call_intent", "assistant_message"}
	if fmt.Sprint(kinds(log)) != fmt.Sprint(want) {
		t.Fatalf("replayed log got %v want %v", kinds(log), want)
	}
	replayed := New(db, core.EchoToolRegistry{}, &scripted{})
	if err = replayed.Recover(ctx, id); err != nil {
		t.Fatal(err)
	}
	status, err := db.SessionStatus(ctx, id)
	if err != nil || status != core.StatusCompleted {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}

func TestGoAIAdapterUsesFauxProvider(t *testing.T) {
	provider := "legion-faux"
	reg := faux.Register(&faux.Options{Provider: provider, Models: []faux.ModelDef{{ID: "model"}}})
	reg.SetResponses([]faux.ResponseStep{faux.TextMessage("from go-ai")})
	store := core.NewMemoryEventStore()
	loop := New(store, core.EchoToolRegistry{}, GoAI{})
	ctx := context.Background()
	id, err := loop.Start(ctx, core.RunConfig{Model: provider + "/model"})
	if err != nil {
		t.Fatal(err)
	}
	_ = loop.Resume(ctx, id, core.UserMessage("hello"))
	final, err := loop.Resolve(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Content string `json:"content"`
	}
	_ = json.Unmarshal(final.Event.Payload, &p)
	if p.Content != "from go-ai" || reg.State.CallCount != 1 {
		t.Fatalf("payload=%s calls=%d", final.Event.Payload, reg.State.CallCount)
	}
}
