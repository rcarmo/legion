package wasm

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

type source []byte
type memoryNamespace map[string][]byte
type budget uint64

func (b *budget) Take(request uint64) uint64 {
	if request > uint64(*b) {
		request = uint64(*b)
	}
	*b -= budget(request)
	return request
}

func (m memoryNamespace) Read(_ context.Context, key string) ([]byte, error) {
	return append([]byte(nil), m[key]...), nil
}
func (m memoryNamespace) Write(_ context.Context, key string, value []byte) ([]byte, error) {
	m[key] = append([]byte(nil), value...)
	return value, nil
}

func (s source) Fetch(context.Context, string) ([]byte, error) { return append([]byte(nil), s...), nil }
func TestHostFunctions(t *testing.T) {
	path := filepath.Join("..", "..", "..", "target", "wasm32-wasip1", "release", "wasm_host.wasm")
	module, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("build wasm-host fixture")
	}
	if err != nil {
		t.Fatal(err)
	}
	ns := memoryNamespace{"/value": []byte("before")}
	remaining := budget(10)
	r := New(source(module), ns, &remaining, legionruntime.DefaultLimits())
	defer r.Close(context.Background())
	result, err := r.Invoke(context.Background(), legionruntime.Request{FunctionName: "host", CallID: "host-1", ArtifactCID: "host-cid", Args: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(ns["/value"]) != "after" || remaining != 0 {
		t.Fatalf("namespace=%q budget=%d", ns["/value"], remaining)
	}
	if string(result.Output) != `{"before":"before","granted":10}` {
		t.Fatalf("output=%s", result.Output)
	}
}

func TestExtismFixture(t *testing.T) {
	path := filepath.Join("..", "..", "..", "target", "wasm32-wasip1", "release", "wasm_hello.wasm")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("build with make wasm-fixture")
	}
	if err != nil {
		t.Fatal(err)
	}
	r := New(source(b), nil, nil, legionruntime.DefaultLimits())
	defer r.Close(context.Background())
	req := legionruntime.Request{FunctionName: "hello", CallID: "call-1", ArtifactCID: "cid", Args: json.RawMessage(`{"name":"Rui"}`)}
	result, err := r.Invoke(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Output) != `{"greeting":"Hello, Rui!"}` {
		t.Fatalf("output=%s", result.Output)
	}
	result, err = r.Invoke(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if result.CallID != "call-1" {
		t.Fatal(result)
	}
}
