// Package core defines Legion's I/O-free contracts and wire types.
package core

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type RunID = uuid.UUID
type SeqNum = uint64

type Budget struct {
	MaxTurns     *uint32  `json:"max_turns"`
	MaxToolCalls *uint32  `json:"max_tool_calls"`
	MaxTokensIn  *uint64  `json:"max_tokens_in"`
	MaxTokensOut *uint64  `json:"max_tokens_out"`
	MaxWallMS    *uint64  `json:"max_wall_ms"`
	MaxCostUSD   *float64 `json:"max_cost_usd"`
}

type BudgetState struct {
	Turns     uint32  `json:"turns"`
	ToolCalls uint32  `json:"tool_calls"`
	TokensIn  uint64  `json:"tokens_in"`
	TokensOut uint64  `json:"tokens_out"`
	WallMS    uint64  `json:"wall_ms"`
	CostUSD   float64 `json:"cost_usd"`
}

func (s BudgetState) ExceededBy(b Budget) string {
	if b.MaxTurns != nil && s.Turns >= *b.MaxTurns {
		return "max_turns"
	}
	if b.MaxToolCalls != nil && s.ToolCalls >= *b.MaxToolCalls {
		return "max_tool_calls"
	}
	if b.MaxTokensIn != nil && s.TokensIn >= *b.MaxTokensIn {
		return "max_tokens_in"
	}
	if b.MaxTokensOut != nil && s.TokensOut >= *b.MaxTokensOut {
		return "max_tokens_out"
	}
	if b.MaxWallMS != nil && s.WallMS >= *b.MaxWallMS {
		return "max_wall_ms"
	}
	if b.MaxCostUSD != nil && s.CostUSD >= *b.MaxCostUSD {
		return "max_cost_usd"
	}
	return ""
}

type RunConfig struct {
	SystemPrompt *string         `json:"system_prompt"`
	Model        string          `json:"model"`
	Budget       Budget          `json:"budget"`
	Tools        []string        `json:"tools"`
	Metadata     json.RawMessage `json:"metadata"`
}

type EffectClass string

const (
	EffectRead       EffectClass = "read"
	EffectIdempotent EffectClass = "idempotent"
	EffectWrite      EffectClass = "write"
	EffectLLMCall    EffectClass = "llm_call"
)

type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Effect      EffectClass     `json:"effect"`
}

type ParkReason struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	EventName   string `json:"event_name,omitempty"`
}

type ExternalEvent struct {
	Type    string          `json:"type"`
	Content string          `json:"content,omitempty"`
	Name    string          `json:"name,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func UserMessage(content string) ExternalEvent {
	return ExternalEvent{Type: "user_message", Content: content}
}

type SessionStatus struct {
	Status      string      `json:"status"`
	Reason      *ParkReason `json:"reason,omitempty"`
	BudgetField string      `json:"budget_field,omitempty"`
	ToolName    string      `json:"tool_name,omitempty"`
	CallID      string      `json:"call_id,omitempty"`
}

var (
	StatusIdle        = SessionStatus{Status: "idle"}
	StatusRunning     = SessionStatus{Status: "running"}
	StatusToolPending = SessionStatus{Status: "tool_pending"}
	StatusResuming    = SessionStatus{Status: "resuming"}
	StatusCompleted   = SessionStatus{Status: "completed"}
	StatusAborted     = SessionStatus{Status: "aborted"}
)

func (s SessionStatus) IsTerminal() bool {
	return s.Status == "completed" || s.Status == "budget_halt" || s.Status == "aborted"
}

type TurnPhase string

const (
	PhaseSetup      TurnPhase = "setup"
	PhaseRunning    TurnPhase = "running"
	PhaseTools      TurnPhase = "tools"
	PhaseFinalizing TurnPhase = "finalizing"
	PhaseCompleted  TurnPhase = "completed"
	PhaseAborted    TurnPhase = "aborted"
)

type EventKind struct {
	Kind        string      `json:"kind"`
	ToolName    string      `json:"tool_name,omitempty"`
	CallID      string      `json:"call_id,omitempty"`
	Effect      EffectClass `json:"effect,omitempty"`
	Action      string      `json:"action,omitempty"`
	ParentRunID *RunID      `json:"parent_run_id,omitempty"`
	AtSeq       *SeqNum     `json:"at_seq,omitempty"`
	Reason      *ParkReason `json:"reason,omitempty"`
	BudgetField string      `json:"budget_field,omitempty"`
}

type TurnEvent struct {
	Kind       EventKind       `json:"kind"`
	Payload    json.RawMessage `json:"payload"`
	PayloadCID *string         `json:"payload_cid"`
	Model      *string         `json:"model"`
	TokensIn   *uint32         `json:"tokens_in"`
	TokensOut  *uint32         `json:"tokens_out"`
	WallMS     *uint64         `json:"wall_ms"`
}

func raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func NewUserMessage(content string) TurnEvent {
	return TurnEvent{Kind: EventKind{Kind: "user_message"}, Payload: raw(map[string]any{"content": content})}
}
func ModelCallIntent() TurnEvent { return TurnEvent{Kind: EventKind{Kind: "model_call_intent"}} }
func ToolCallIntent(name, id string, effect EffectClass, args any) TurnEvent {
	return TurnEvent{Kind: EventKind{Kind: "tool_call_intent", ToolName: name, CallID: id, Effect: effect}, Payload: raw(map[string]any{"arguments": args})}
}
func ToolResult(id string, result any) TurnEvent {
	return TurnEvent{Kind: EventKind{Kind: "tool_result", CallID: id}, Payload: raw(result)}
}
func SessionStarted(c RunConfig) TurnEvent {
	return TurnEvent{Kind: EventKind{Kind: "session_started"}, Payload: raw(c)}
}
func AssistantMessage(content any, model string, in, out uint32, wall uint64) TurnEvent {
	return TurnEvent{Kind: EventKind{Kind: "assistant_message"}, Payload: raw(content), Model: &model, TokensIn: &in, TokensOut: &out, WallMS: &wall}
}
func BudgetHalt(field string) TurnEvent {
	return TurnEvent{Kind: EventKind{Kind: "session_budget_halt", BudgetField: field}, Payload: raw(map[string]string{"budget_field": field})}
}

type TurnEnvelope struct {
	RunID     RunID     `json:"run_id"`
	Seq       SeqNum    `json:"seq"`
	PrevHash  [32]byte  `json:"prev_hash"`
	Event     TurnEvent `json:"event"`
	CreatedAt int64     `json:"created_at"`
}

func NowMS() int64 { return time.Now().UnixMilli() }

type SessionSummary struct {
	RunID     RunID         `json:"run_id"`
	Status    SessionStatus `json:"status"`
	Model     string        `json:"model"`
	Turns     uint64        `json:"turns"`
	CreatedAt int64         `json:"created_at"`
	UpdatedAt int64         `json:"updated_at"`
}
type SessionFilter struct {
	Status        string
	Limit, Offset int
}
