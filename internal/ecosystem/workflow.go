package ecosystem

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/rcarmo/legion/internal/core"
)

type WorkflowNode struct {
	ID        string          `json:"id"`
	Tool      string          `json:"tool"`
	Args      json.RawMessage `json:"args"`
	DependsOn []string        `json:"depends_on"`
}
type Workflow struct {
	Nodes []WorkflowNode `json:"nodes"`
}
type WorkflowResult struct {
	Outputs map[string]json.RawMessage `json:"outputs"`
	Waves   [][]string                 `json:"waves"`
}
type WorkflowRunner struct{ Tools core.ToolRegistry }

// Plan validates the entire graph before any tool can have side effects.
func (w Workflow) Plan() ([][]string, error) {
	if len(w.Nodes) == 0 {
		return nil, fmt.Errorf("workflow must contain at least one node")
	}
	nodes := map[string]WorkflowNode{}
	for _, n := range w.Nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("workflow node id cannot be empty")
		}
		if _, ok := nodes[n.ID]; ok {
			return nil, fmt.Errorf("workflow node ids must be unique")
		}
		var args map[string]json.RawMessage
		if len(n.Args) > 0 {
			if err := json.Unmarshal(n.Args, &args); err != nil {
				return nil, fmt.Errorf("workflow node %q args must be an object", n.ID)
			}
		}
		nodes[n.ID] = n
	}
	for _, n := range w.Nodes {
		for _, d := range n.DependsOn {
			if _, ok := nodes[d]; !ok || d == n.ID {
				return nil, fmt.Errorf("workflow node %q has invalid dependency %q", n.ID, d)
			}
		}
	}
	done := map[string]bool{}
	var waves [][]string
	for len(done) < len(nodes) {
		var ready []string
		for id, n := range nodes {
			if done[id] {
				continue
			}
			eligible := true
			for _, d := range n.DependsOn {
				if !done[d] {
					eligible = false
					break
				}
			}
			if eligible {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("workflow graph contains a cycle")
		}
		sort.Strings(ready)
		for _, id := range ready {
			done[id] = true
		}
		waves = append(waves, ready)
	}
	return waves, nil
}

// Run dispatches each deterministic wave concurrently and joins it before
// exposing its outputs to the next wave. Failed waves never start dependants.
func (r WorkflowRunner) Run(ctx context.Context, w Workflow) (WorkflowResult, error) {
	waves, err := w.Plan()
	if err != nil {
		return WorkflowResult{}, err
	}
	if r.Tools == nil {
		return WorkflowResult{}, fmt.Errorf("workflow tools unavailable")
	}
	nodes := map[string]WorkflowNode{}
	for _, n := range w.Nodes {
		nodes[n.ID] = n
	}
	result := WorkflowResult{Outputs: map[string]json.RawMessage{}, Waves: waves}
	for _, wave := range waves {
		if err := ctx.Err(); err != nil {
			return WorkflowResult{}, err
		}
		outputs := make([]json.RawMessage, len(wave))
		errs := make([]error, len(wave))
		var wg sync.WaitGroup
		for i, id := range wave {
			n := nodes[id]
			args := map[string]json.RawMessage{}
			if len(n.Args) > 0 {
				_ = json.Unmarshal(n.Args, &args)
			}
			if args == nil {
				args = map[string]json.RawMessage{}
			}
			if len(n.DependsOn) > 0 {
				deps := map[string]json.RawMessage{}
				for _, d := range n.DependsOn {
					deps[d] = result.Outputs[d]
				}
				args["dependencies"], _ = json.Marshal(deps)
			}
			payload, e := json.Marshal(args)
			if e != nil {
				return WorkflowResult{}, e
			}
			wg.Add(1)
			go func(i int, n WorkflowNode, payload json.RawMessage) {
				defer wg.Done()
				outputs[i], errs[i] = r.Tools.Dispatch(ctx, n.Tool, payload)
			}(i, n, payload)
		}
		wg.Wait()
		for i, id := range wave {
			if errs[i] != nil {
				return WorkflowResult{}, fmt.Errorf("workflow node %q: %w", id, errs[i])
			}
			if !json.Valid(outputs[i]) {
				return WorkflowResult{}, fmt.Errorf("workflow node %q returned invalid JSON", id)
			}
			result.Outputs[id] = outputs[i]
		}
	}
	return result, nil
}
