package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rcarmo/legion/internal/core"
)

type ManifestRegistry interface {
	Names() []string
	Manifest(string) (Manifest, bool)
}

// FunctionTools exposes the current deployment registry without retaining stale
// definitions after registration or promotion. Invocation still resolves routes.
type FunctionTools struct {
	Registry  ManifestRegistry
	Functions Functions
	Base      core.ToolRegistry
}

func (t FunctionTools) Definitions(ctx context.Context) ([]core.ToolDefinition, error) {
	defs, err := t.Base.Definitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range t.Registry.Names() {
		m, ok := t.Registry.Manifest(name)
		if !ok {
			continue
		}
		effect := core.EffectWrite
		if m.Idempotent {
			effect = core.EffectIdempotent
		}
		params := m.Parameters
		if len(params) == 0 {
			params = json.RawMessage(`{"type":"object"}`)
		}
		defs = append(defs, core.ToolDefinition{Name: "fn." + name, Description: m.Description, Parameters: params, Effect: effect})
	}
	return defs, nil
}
func (t FunctionTools) Dispatch(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	function, ok := strings.CutPrefix(name, "fn.")
	if !ok {
		return t.Base.Dispatch(ctx, name, args)
	}
	raw, err := t.Functions.Invoke(ctx, function, args)
	if err != nil {
		return nil, err
	}
	var result Result
	if err = json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	if result.Error != "" {
		return nil, fmt.Errorf("function %s: %s", function, result.Error)
	}
	if len(result.Output) == 0 {
		return json.RawMessage("null"), nil
	}
	return result.Output, nil
}
