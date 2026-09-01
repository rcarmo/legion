package wasm

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

func TestTimeoutClosesWazeroInvocation(t *testing.T) {
	module := filepath.Join(os.Getenv("HOME"), "go", "pkg", "mod", "github.com", "extism", "go-sdk@v1.7.1", "wasm", "sleep.wasm")
	bytes, err := os.ReadFile(module)
	if os.IsNotExist(err) {
		t.Skip("Extism sleep fixture unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	limits := legionruntime.DefaultLimits()
	limits.Timeout = 20 * time.Millisecond
	r := New(source(bytes), nil, nil, limits)
	defer r.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), limits.Timeout)
	defer cancel()
	_, err = r.Invoke(ctx, legionruntime.Request{FunctionName: "slow", CallID: "slow", ArtifactCID: "sleep", Args: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected timeout")
	}
}
func TestMemoryLimitRejectsLargeMinimum(t *testing.T) {
	path := filepath.Join("..", "..", "..", "target", "wasm32-wasip1", "release", "wasm_hello.wasm")
	module, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("make wasm-fixture")
	}
	if err != nil {
		t.Fatal(err)
	}
	limits := legionruntime.DefaultLimits()
	limits.MaxMemoryBytes = 64 << 10
	r := New(source(module), nil, nil, limits)
	defer r.Close(context.Background())
	_, err = r.Invoke(context.Background(), legionruntime.Request{FunctionName: "hello", CallID: "memory", ArtifactCID: "large", Args: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected memory limit error")
	}
}
