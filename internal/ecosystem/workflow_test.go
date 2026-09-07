package ecosystem

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rcarmo/legion/internal/core"
)

type testTools struct {
	calls    atomic.Int32
	dispatch func(context.Context, string, json.RawMessage) (json.RawMessage, error)
}

func (t *testTools) Definitions(context.Context) ([]core.ToolDefinition, error) { return nil, nil }
func (t *testTools) Dispatch(c context.Context, n string, a json.RawMessage) (json.RawMessage, error) {
	t.calls.Add(1)
	if t.dispatch != nil {
		return t.dispatch(c, n, a)
	}
	return a, nil
}
func TestWorkflowWaves(t *testing.T) {
	tools := &testTools{}
	w := Workflow{Nodes: []WorkflowNode{{ID: "b", Tool: "echo", Args: json.RawMessage(`{"v":2}`)}, {ID: "a", Tool: "echo", Args: json.RawMessage(`{"v":1}`)}, {ID: "merge", Tool: "echo", DependsOn: []string{"a", "b"}}}}
	got, err := (WorkflowRunner{Tools: tools}).Run(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Waves, [][]string{{"a", "b"}, {"merge"}}) {
		t.Fatal(got.Waves)
	}
	var output struct {
		Dependencies map[string]struct {
			V int `json:"v"`
		} `json:"dependencies"`
	}
	if err := json.Unmarshal(got.Outputs["merge"], &output); err != nil {
		t.Fatal(err)
	}
	if output.Dependencies["a"].V != 1 || output.Dependencies["b"].V != 2 {
		t.Fatal(output)
	}
}
func TestWorkflowValidationBeforeDispatch(t *testing.T) {
	cases := []string{`{"nodes":[]}`, `{"nodes":[{"id":""}]}`, `{"nodes":[{"id":"a"},{"id":"a"}]}`, `{"nodes":[{"id":"a","depends_on":["missing"]}]}`, `{"nodes":[{"id":"a","depends_on":["a"]}]}`, `{"nodes":[{"id":"root"},{"id":"a","depends_on":["b"]},{"id":"b","depends_on":["a"]}]}`, `{"nodes":[{"id":"root"},{"id":"bad","args":[]}]}`}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			var w Workflow
			if err := json.Unmarshal([]byte(s), &w); err != nil {
				t.Fatal(err)
			}
			tools := &testTools{}
			if _, err := (WorkflowRunner{Tools: tools}).Run(context.Background(), w); err == nil {
				t.Fatal("accepted invalid graph")
			}
			if tools.calls.Load() != 0 {
				t.Fatal("dispatched invalid graph")
			}
		})
	}
}
func TestWorkflowConcurrency(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	tools := &testTools{dispatch: func(ctx context.Context, _ string, a json.RawMessage) (json.RawMessage, error) {
		started <- struct{}{}
		select {
		case <-release:
			return a, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (WorkflowRunner{Tools: tools}).Run(ctx, Workflow{Nodes: []WorkflowNode{{ID: "a"}, {ID: "b"}}})
		done <- err
	}()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("wave was not concurrent")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestWorkflowFailureStopsDependants(t *testing.T) {
	sentinel := errors.New("failure")
	tools := &testTools{dispatch: func(context.Context, string, json.RawMessage) (json.RawMessage, error) { return nil, sentinel }}
	_, err := (WorkflowRunner{Tools: tools}).Run(context.Background(), Workflow{Nodes: []WorkflowNode{{ID: "a"}, {ID: "b", DependsOn: []string{"a"}}}})
	if !errors.Is(err, sentinel) || tools.calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, tools.calls.Load())
	}
}
