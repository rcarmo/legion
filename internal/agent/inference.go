package agent

import (
	"context"
	"fmt"
	"strings"

	goai "github.com/rcarmo/go-ai"
	_ "github.com/rcarmo/go-ai/inference/provider/anthropic"
	_ "github.com/rcarmo/go-ai/inference/provider/google"
	_ "github.com/rcarmo/go-ai/inference/provider/openai"
	_ "github.com/rcarmo/go-ai/inference/provider/openairesponses"
)

type Inference interface {
	Stream(context.Context, string, *goai.Context, *goai.StreamOptions) (<-chan goai.Event, error)
}

// go-ai keeps its catalogue and wire protocols opt-in. Register both before
// resolving configured models; faux-only tests do not exercise this setup.
func init() { goai.RegisterBuiltinModels() }

type GoAI struct{}

func (GoAI) Stream(ctx context.Context, name string, conversation *goai.Context, options *goai.StreamOptions) (<-chan goai.Event, error) {
	provider, modelID, ok := strings.Cut(name, "/")
	if !ok {
		return nil, fmt.Errorf("model must use provider/id format: %s", name)
	}
	model := goai.GetModel(goai.Provider(provider), modelID)
	if model == nil {
		return nil, fmt.Errorf("model not found: %s", name)
	}
	return goai.Stream(ctx, model, conversation, options), nil
}
