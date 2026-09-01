package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type Resolver interface {
	Resolve(string, string) (Manifest, error)
}

// Functions resolves the active artifact and dispatches an invocation by runtime.
type Functions struct {
	Registry Resolver
	WASM     Invoker
	Bun      Invoker
	Joker    Invoker
}

func (f Functions) Invoke(ctx context.Context, name string, args []byte) ([]byte, error) {
	var value any
	if len(args) == 0 {
		args = []byte(`{}`)
	}
	if err := json.Unmarshal(args, &value); err != nil {
		return nil, fmt.Errorf("arguments: %w", err)
	}
	callID := uuid.NewString()
	manifest, err := f.Registry.Resolve(name, callID)
	if err != nil {
		return nil, err
	}
	req := Request{FunctionName: name, CallID: callID, ArtifactCID: manifest.ArtifactCID, Env: manifest.Env, Args: json.RawMessage(args)}
	var invoker Invoker
	switch manifest.Runtime {
	case WASM:
		invoker = f.WASM
	case Bun:
		invoker = f.Bun
	case Joker:
		invoker = f.Joker
	default:
		return nil, fmt.Errorf("unsupported runtime %q", manifest.Runtime)
	}
	if invoker == nil {
		return nil, fmt.Errorf("runtime %q unavailable", manifest.Runtime)
	}
	result, err := invoker.Invoke(ctx, req)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}
