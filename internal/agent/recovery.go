package agent

import (
	"context"

	"github.com/rcarmo/legion/internal/core"
)

type RecoveryOutcome string

const (
	RecoveryAlreadyComplete RecoveryOutcome = "already_complete"
	RecoveryStartFresh      RecoveryOutcome = "start_fresh"
	RecoveryRetryLLM        RecoveryOutcome = "retry_llm"
	RecoveryDanglingTool    RecoveryOutcome = "dangling_tool"
	RecoveryParked          RecoveryOutcome = "parked"
)

func recoverSession(ctx context.Context, store core.EventStore, id core.RunID) (RecoveryOutcome, error) {
	st, err := store.SessionStatus(ctx, id)
	if err != nil {
		return "", err
	}
	if st.IsTerminal() {
		return RecoveryAlreadyComplete, nil
	}
	if st.Status == "parked" {
		return RecoveryParked, nil
	}
	log, err := store.ReadLog(ctx, id)
	if err != nil {
		return "", err
	}
	for i, e := range log {
		if e.Event.Kind.Kind != "tool_call_intent" {
			continue
		}
		found := false
		for _, later := range log[i+1:] {
			if later.Event.Kind.Kind == "tool_result" && later.Event.Kind.CallID == e.Event.Kind.CallID {
				found = true
				break
			}
		}
		if !found {
			_ = store.SetStatus(ctx, id, core.SessionStatus{Status: "pending_reconciliation", ToolName: e.Event.Kind.ToolName, CallID: e.Event.Kind.CallID})
			return RecoveryDanglingTool, nil
		}
	}
	var intent, assistant *core.SeqNum
	for i := range log {
		e := log[i]
		if e.Event.Kind.Kind == "model_call_intent" {
			v := e.Seq
			intent = &v
		}
		if e.Event.Kind.Kind == "assistant_message" {
			v := e.Seq
			assistant = &v
		}
	}
	if intent != nil && (assistant == nil || *intent > *assistant) {
		return RecoveryRetryLLM, nil
	}
	return RecoveryStartFresh, nil
}
