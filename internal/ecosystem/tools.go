package ecosystem

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/rcarmo/legion/internal/core"
)

// Tools keeps built-in tools available while routing agent calls without
// falling back after an execution error (which could duplicate side effects).
type Tools struct {
	Agents *AgentToolRegistry
	Base   core.ToolRegistry
}

func (t Tools) Definitions(ctx context.Context) ([]core.ToolDefinition, error) {
	base, err := t.Base.Definitions(ctx)
	if err != nil {
		return nil, err
	}
	agents, err := t.Agents.Definitions(ctx)
	if err != nil {
		return nil, err
	}
	return append(base, agents...), nil
}
func (t Tools) Dispatch(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if strings.HasPrefix(name, "agent.") {
		return t.Agents.Dispatch(ctx, name, args)
	}
	return t.Base.Dispatch(ctx, name, args)
}
