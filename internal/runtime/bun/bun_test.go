package bun

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

type source []byte

func (s source) Fetch(context.Context, string) ([]byte, error) { return append([]byte(nil), s...), nil }
func bunBin(t *testing.T) string {
	t.Helper()
	if b := os.Getenv("LEGION_BUN_TEST_BIN"); b != "" {
		return b
	}
	p, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("Bun unavailable")
	}
	return p
}
func TestInvokeAndEnvironment(t *testing.T) {
	code := source(`const input=await Bun.stdin.text(); console.log(JSON.stringify({name:JSON.parse(input).name,declared:process.env.GREETING,function:process.env.LEGION_FUNCTION_NAME}))`)
	r := New(bunBin(t), code, legionruntime.DefaultLimits())
	result, err := r.Invoke(context.Background(), legionruntime.Request{FunctionName: "hello", CallID: "call", ArtifactCID: "cid", Args: json.RawMessage(`{"name":"Rui"}`), Env: map[string]string{"GREETING": "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Output) != `{"declared":"hi","function":"hello","name":"Rui"}` {
		t.Fatalf("output=%s error=%s", result.Output, result.Error)
	}
}
func TestTimeout(t *testing.T) {
	limits := legionruntime.DefaultLimits()
	limits.Timeout = 50 * time.Millisecond
	r := New(bunBin(t), source(`await new Promise(r=>setTimeout(r,10000))`), limits)
	_, err := r.Invoke(context.Background(), legionruntime.Request{FunctionName: "slow", CallID: "call", ArtifactCID: "cid", Args: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected timeout")
	}
}
