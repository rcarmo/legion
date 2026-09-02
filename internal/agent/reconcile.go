package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rcarmo/legion/internal/core"
)

type ReconcileAction string

const (
	ReconcileSkip  ReconcileAction = "skip"
	ReconcileRetry ReconcileAction = "retry"
)

// Reconcile resolves the write-ahead tool call recorded by recovery.
// A failed retry deliberately leaves the session pending for another decision.
func (l *Loop) Reconcile(ctx context.Context, id core.RunID, action string) error {
	status, err := l.store.SessionStatus(ctx, id)
	if err != nil {
		return err
	}
	if status.Status != "pending_reconciliation" || status.CallID == "" {
		return fmt.Errorf("session %s is not pending reconciliation", id)
	}
	log, err := l.store.ReadLog(ctx, id)
	if err != nil {
		return err
	}
	var intent *core.TurnEnvelope
	for i := len(log) - 1; i >= 0; i-- {
		if log[i].Event.Kind.Kind == "tool_call_intent" && log[i].Event.Kind.CallID == status.CallID {
			value := log[i]
			intent = &value
			break
		}
	}
	if intent == nil {
		return fmt.Errorf("pending tool intent not found for call %s", status.CallID)
	}
	var result any
	switch ReconcileAction(action) {
	case ReconcileSkip:
		result = map[string]any{"skipped": true, "reconciled": true}
	case ReconcileRetry:
		var payload struct {
			Arguments json.RawMessage `json:"arguments"`
		}
		if err = json.Unmarshal(intent.Event.Payload, &payload); err != nil || len(payload.Arguments) == 0 || string(payload.Arguments) == "null" {
			return fmt.Errorf("cannot retry legacy tool intent without stored arguments")
		}
		value, dispatchErr := l.tools.Dispatch(ctx, status.ToolName, payload.Arguments)
		if dispatchErr != nil {
			return dispatchErr
		}
		if json.Unmarshal(value, &result) != nil {
			result = string(value)
		}
	default:
		return fmt.Errorf("action must be skip or retry")
	}
	if _, err = l.store.Append(ctx, id, core.ToolResult(status.CallID, result)); err != nil {
		return err
	}
	if _, err = l.store.Append(ctx, id, core.ToolCallReconciled(status.CallID, action)); err != nil {
		return err
	}
	return l.store.SetStatus(ctx, id, core.StatusIdle)
}
