// Package agent implements Legion's durable four-verb agent loop.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	goai "github.com/rcarmo/go-ai"
	"github.com/rcarmo/legion/internal/core"
)

type Loop struct {
	store         core.EventStore
	tools         core.ToolRegistry
	inference     Inference
	contextWindow int
}

func New(store core.EventStore, tools core.ToolRegistry, inference Inference) *Loop {
	return &Loop{store: store, tools: tools, inference: inference, contextWindow: 40}
}
func (l *Loop) Start(ctx context.Context, c core.RunConfig) (core.RunID, error) {
	id := uuid.New()
	if err := l.store.CreateSession(ctx, id, c); err != nil {
		return uuid.Nil, err
	}
	if _, err := l.store.Append(ctx, id, core.SessionStarted(c)); err != nil {
		return uuid.Nil, err
	}
	if err := l.store.SetStatus(ctx, id, core.StatusIdle); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}
func (l *Loop) Resume(ctx context.Context, id core.RunID, e core.ExternalEvent) error {
	switch e.Type {
	case "user_message":
		if _, err := l.store.Append(ctx, id, core.NewUserMessage(e.Content)); err != nil {
			return err
		}
		return l.store.SetStatus(ctx, id, core.StatusRunning)
	case "approval_granted":
		return l.resumeWithPayload(ctx, id, map[string]any{"approval": "granted"})
	case "approval_denied":
		return l.store.SetStatus(ctx, id, core.StatusAborted)
	case "external_trigger":
		var payload any
		if e.Payload != nil {
			_ = json.Unmarshal(e.Payload, &payload)
		}
		return l.resumeWithPayload(ctx, id, map[string]any{"trigger": e.Name, "payload": payload})
	default:
		return fmt.Errorf("unknown external event type: %s", e.Type)
	}
}

func (l *Loop) resumeWithPayload(ctx context.Context, id core.RunID, payload any) error {
	encoded, _ := json.Marshal(payload)
	if _, err := l.store.Append(ctx, id, core.TurnEvent{Kind: core.EventKind{Kind: "session_resumed"}, Payload: encoded}); err != nil {
		return err
	}
	return l.store.SetStatus(ctx, id, core.StatusResuming)
}
func (l *Loop) Recover(ctx context.Context, id core.RunID) error {
	out, err := recoverSession(ctx, l.store, id)
	if err != nil {
		return err
	}
	switch out {
	case RecoveryStartFresh, RecoveryRetryLLM:
		return l.store.SetStatus(ctx, id, core.StatusIdle)
	case RecoveryDanglingTool:
		return core.ErrPendingReconciliation
	default:
		return nil
	}
}
func (l *Loop) Resolve(ctx context.Context, id core.RunID) (core.TurnEnvelope, error) {
	log, err := l.store.ReadLog(ctx, id)
	if err != nil {
		return core.TurnEnvelope{}, err
	}
	config, err := configFromLog(log)
	if err != nil {
		return core.TurnEnvelope{}, err
	}
	budget := budgetFromLog(log)
	return l.run(ctx, id, config, &budget)
}

func (l *Loop) run(ctx context.Context, id core.RunID, c core.RunConfig, budget *core.BudgetState) (core.TurnEnvelope, error) {
	if field := budget.ExceededBy(c.Budget); field != "" {
		return core.TurnEnvelope{}, l.halt(ctx, id, field)
	}
	state := newTurnState()
	for {
		if field := budget.ExceededBy(c.Budget); field != "" {
			return core.TurnEnvelope{}, l.halt(ctx, id, field)
		}
		recent, err := l.store.ReadRecent(ctx, id, l.contextWindow)
		if err != nil {
			return core.TurnEnvelope{}, err
		}
		conversation := buildContext(recent, c)
		if err := state.transition(core.PhaseSetup, core.PhaseRunning); err != nil {
			return core.TurnEnvelope{}, err
		}
		defs, err := l.tools.Definitions(ctx)
		if err != nil {
			return core.TurnEnvelope{}, err
		}
		effects := map[string]core.EffectClass{}
		for _, d := range defs {
			if contains(c.Tools, d.Name) {
				conversation.Tools = append(conversation.Tools, goai.Tool{Name: d.Name, Description: d.Description, Parameters: d.Parameters})
				effects[d.Name] = d.Effect
			}
		}
		if _, err = l.store.Append(ctx, id, core.ModelCallIntent()); err != nil {
			return core.TurnEnvelope{}, err
		}
		if err = l.store.SetStatus(ctx, id, core.StatusRunning); err != nil {
			return core.TurnEnvelope{}, err
		}
		started := time.Now()
		events, err := l.inference.Stream(ctx, c.Model, conversation, streamOptions(id, c))
		if err != nil {
			return core.TurnEnvelope{}, err
		}
		var message *goai.Message
		for event := range events {
			switch e := event.(type) {
			case *goai.DoneEvent:
				message = e.Message
			case *goai.ErrorEvent:
				if e.Err != nil {
					return core.TurnEnvelope{}, e.Err
				}
				return core.TurnEnvelope{}, fmt.Errorf("model error")
			}
		}
		if message == nil {
			return core.TurnEnvelope{}, fmt.Errorf("model stream ended without terminal message")
		}
		wall := uint64(time.Since(started).Milliseconds())
		var in, out uint32
		cost := 0.0
		if message.Usage != nil {
			in = uint32(max(message.Usage.Input, 0))
			out = uint32(max(message.Usage.Output, 0))
			cost = message.Usage.Cost.Total
		}
		calls := []map[string]any{}
		for _, block := range message.Content {
			if block.Type == "toolCall" {
				calls = append(calls, map[string]any{"id": block.ID, "name": block.Name, "args": block.Arguments})
			}
		}
		payload := map[string]any{"content": goai.GetTextContent(message), "tool_calls": calls, "message": message}
		event := core.AssistantMessage(payload, c.Model, in, out, wall)
		seq, err := l.store.Append(ctx, id, event)
		if err != nil {
			return core.TurnEnvelope{}, err
		}
		budget.Turns++
		budget.TokensIn += uint64(in)
		budget.TokensOut += uint64(out)
		budget.WallMS += wall
		budget.CostUSD += cost
		result := core.TurnEnvelope{RunID: id, Seq: seq, Event: event, CreatedAt: core.NowMS()}
		if len(calls) == 0 {
			if err := state.transition(core.PhaseRunning, core.PhaseFinalizing); err != nil {
				return core.TurnEnvelope{}, err
			}
			if field := budget.ExceededBy(c.Budget); field != "" {
				_ = l.halt(ctx, id, field)
			} else {
				_ = l.store.SetStatus(ctx, id, core.StatusCompleted)
			}
			if err := state.transition(core.PhaseFinalizing, core.PhaseCompleted); err != nil {
				return core.TurnEnvelope{}, err
			}
			return result, nil
		}
		if err := state.transition(core.PhaseRunning, core.PhaseTools); err != nil {
			return core.TurnEnvelope{}, err
		}
		if field := budget.ExceededBy(c.Budget); field != "" && field != "max_tool_calls" {
			return core.TurnEnvelope{}, l.halt(ctx, id, field)
		}
		for _, call := range calls {
			if c.Budget.MaxToolCalls != nil && budget.ToolCalls >= *c.Budget.MaxToolCalls {
				return core.TurnEnvelope{}, l.halt(ctx, id, "max_tool_calls")
			}
			name, _ := call["name"].(string)
			callID, _ := call["id"].(string)
			args, _ := json.Marshal(call["args"])
			effect := effects[name]
			if effect == "" {
				effect = core.EffectWrite
			}
			if _, err = l.store.Append(ctx, id, core.ToolCallIntent(name, callID, effect, call["args"])); err != nil {
				return core.TurnEnvelope{}, err
			}
			_ = l.store.SetStatus(ctx, id, core.StatusToolPending)
			value, dispatchErr := l.tools.Dispatch(ctx, name, args)
			var stored any
			if dispatchErr != nil {
				stored = map[string]string{"error": dispatchErr.Error()}
			} else if json.Unmarshal(value, &stored) != nil {
				stored = string(value)
			}
			if _, err = l.store.Append(ctx, id, core.ToolResult(callID, stored)); err != nil {
				return core.TurnEnvelope{}, err
			}
			budget.ToolCalls++
			if field := budget.ExceededBy(c.Budget); field != "" && field != "max_tool_calls" {
				return core.TurnEnvelope{}, l.halt(ctx, id, field)
			}
		}
		if err := state.transition(core.PhaseTools, core.PhaseSetup); err != nil {
			return core.TurnEnvelope{}, err
		}
	}
}
func (l *Loop) halt(ctx context.Context, id core.RunID, field string) error {
	_, _ = l.store.Append(ctx, id, core.BudgetHalt(field))
	_ = l.store.SetStatus(ctx, id, core.SessionStatus{Status: "budget_halt", BudgetField: field})
	return core.BudgetExceededError{Field: field}
}
func streamOptions(id core.RunID, config core.RunConfig) *goai.StreamOptions {
	options := &goai.StreamOptions{SessionID: id.String()}
	if config.Metadata == nil {
		return options
	}
	var metadata map[string]any
	if json.Unmarshal(config.Metadata, &metadata) == nil {
		options.Metadata = metadata
	}
	var telemetry any
	if json.Unmarshal(config.Metadata, &telemetry) == nil {
		options.TelemetryContext = &goai.TelemetryContext{Value: telemetry}
	}
	return options
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func configFromLog(log []core.TurnEnvelope) (core.RunConfig, error) {
	for _, e := range log {
		if e.Event.Kind.Kind == "session_started" {
			var c core.RunConfig
			if err := json.Unmarshal(e.Event.Payload, &c); err == nil {
				return c, nil
			}
		}
	}
	return core.RunConfig{}, fmt.Errorf("no session_started event")
}
func budgetFromLog(log []core.TurnEnvelope) core.BudgetState {
	var b core.BudgetState
	for _, e := range log {
		switch e.Event.Kind.Kind {
		case "assistant_message":
			b.Turns++
			var payload struct {
				Message *goai.Message `json:"message"`
			}
			if json.Unmarshal(e.Event.Payload, &payload) == nil && payload.Message != nil && payload.Message.Usage != nil {
				b.CostUSD += payload.Message.Usage.Cost.Total
			}
			if e.Event.TokensIn != nil {
				b.TokensIn += uint64(*e.Event.TokensIn)
			}
			if e.Event.TokensOut != nil {
				b.TokensOut += uint64(*e.Event.TokensOut)
			}
			if e.Event.WallMS != nil {
				b.WallMS += *e.Event.WallMS
			}
		case "tool_result":
			b.ToolCalls++
		}
	}
	return b
}
