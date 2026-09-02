package core

import (
	"context"
	"encoding/json"
)

type EventStore interface {
	Append(context.Context, RunID, TurnEvent) (SeqNum, error)
	ReadLog(context.Context, RunID) ([]TurnEnvelope, error)
	ReadRecent(context.Context, RunID, int) ([]TurnEnvelope, error)
	SessionStatus(context.Context, RunID) (SessionStatus, error)
	SetStatus(context.Context, RunID, SessionStatus) error
	Fork(context.Context, RunID, SeqNum) (RunID, error)
	ListSessions(context.Context, SessionFilter) ([]SessionSummary, error)
	CreateSession(context.Context, RunID, RunConfig) error
}

type AgentLoop interface {
	Start(context.Context, RunConfig) (RunID, error)
	Recover(context.Context, RunID) error
	Resume(context.Context, RunID, ExternalEvent) error
	Resolve(context.Context, RunID) (TurnEnvelope, error)
}

type Reconciler interface {
	Reconcile(context.Context, RunID, string) error
}

type ToolRegistry interface {
	Definitions(context.Context) ([]ToolDefinition, error)
	Dispatch(context.Context, string, json.RawMessage) (json.RawMessage, error)
}
