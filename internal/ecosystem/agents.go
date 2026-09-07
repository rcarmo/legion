package ecosystem

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

type AgentProfile struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Config      core.RunConfig `json:"config"`
}
type ChildRun struct {
	RunID       core.RunID         `json:"run_id"`
	ParentRunID *core.RunID        `json:"parent_run_id"`
	Status      core.SessionStatus `json:"status"`
	Output      json.RawMessage    `json:"output"`
}
type ParentRun struct {
	RunID core.RunID
	AtSeq core.SeqNum
}

// AgentToolRegistry is late-bound to avoid a registry/executor construction cycle.
// Profiles are node-local; sessions and forks live in the supplied event store.
type AgentToolRegistry struct {
	mu       sync.RWMutex
	store    core.EventStore
	executor core.AgentLoop
	profiles map[string]AgentProfile
}

func NewAgentToolRegistry(store core.EventStore) *AgentToolRegistry {
	return &AgentToolRegistry{store: store, profiles: map[string]AgentProfile{}}
}
func (r *AgentToolRegistry) Bind(executor core.AgentLoop) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executor = executor
}

var agentName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func cloneProfile(p AgentProfile) AgentProfile {
	b, _ := json.Marshal(p)
	var out AgentProfile
	_ = json.Unmarshal(b, &out)
	return out
}
func (r *AgentToolRegistry) Register(p AgentProfile) error {
	if len(p.Name) > 63 || !agentName.MatchString(p.Name) {
		return fmt.Errorf("agent name must be a lowercase DNS label")
	}
	// Validate before cloning so invalid raw JSON cannot silently lose configuration.
	if _, err := json.Marshal(p); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.profiles[p.Name] = cloneProfile(p)
	return nil
}
func (r *AgentToolRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.profiles[name]
	delete(r.profiles, name)
	return ok
}
func (r *AgentToolRegistry) Profiles() []AgentProfile {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AgentProfile, 0, len(r.profiles))
	for _, p := range r.profiles {
		out = append(out, cloneProfile(p))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (r *AgentToolRegistry) Run(ctx context.Context, name, prompt string, parent *ParentRun) (ChildRun, error) {
	r.mu.RLock()
	p, ok := r.profiles[name]
	p = cloneProfile(p)
	executor := r.executor
	r.mu.RUnlock()
	if !ok {
		return ChildRun{}, fmt.Errorf("agent.%s: tool not found", name)
	}
	if executor == nil {
		return ChildRun{}, fmt.Errorf("agent executor is not bound")
	}
	var id core.RunID
	var parentID *core.RunID
	var err error
	if parent != nil {
		log, e := r.store.ReadLog(ctx, parent.RunID)
		if e != nil {
			return ChildRun{}, e
		}
		found := false
		for _, entry := range log {
			if entry.Seq == parent.AtSeq {
				found = true
				break
			}
		}
		if !found {
			return ChildRun{}, fmt.Errorf("parent sequence %d does not exist for %s", parent.AtSeq, parent.RunID)
		}
		id, err = r.store.Fork(ctx, parent.RunID, parent.AtSeq)
		v := parent.RunID
		parentID = &v
	} else {
		id, err = executor.Start(ctx, p.Config)
	}
	if err != nil {
		return ChildRun{}, err
	}
	if err = executor.Resume(ctx, id, core.UserMessage(prompt)); err != nil {
		return ChildRun{}, err
	}
	envelope, err := executor.Resolve(ctx, id)
	if err != nil {
		return ChildRun{}, err
	}
	status, err := r.store.SessionStatus(ctx, id)
	if err != nil {
		return ChildRun{}, err
	}
	output := envelope.Event.Payload
	if len(output) == 0 {
		output = json.RawMessage("null")
	}
	return ChildRun{RunID: id, ParentRunID: parentID, Status: status, Output: output}, nil
}
func (r *AgentToolRegistry) Definitions(context.Context) ([]core.ToolDefinition, error) {
	profiles := r.Profiles()
	out := make([]core.ToolDefinition, 0, len(profiles))
	for _, p := range profiles {
		out = append(out, core.ToolDefinition{Name: "agent." + p.Name, Description: p.Description, Effect: core.EffectWrite, Parameters: json.RawMessage(`{"type":"object","required":["prompt"],"properties":{"prompt":{"type":"string"},"parent_run_id":{"type":"string","format":"uuid"},"at_seq":{"type":"integer","minimum":0}}}`)})
	}
	return out, nil
}
func (r *AgentToolRegistry) Dispatch(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	profile, ok := strings.CutPrefix(name, "agent.")
	if !ok {
		return nil, fmt.Errorf("%s: tool not found", name)
	}
	var a struct {
		Prompt       *string         `json:"prompt"`
		ParentRunID  *string         `json:"parent_run_id"`
		AtSeq        *core.SeqNum    `json:"at_seq"`
		Dependencies json.RawMessage `json:"dependencies"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	if a.Prompt == nil {
		return nil, fmt.Errorf("prompt is required")
	}
	prompt := *a.Prompt
	if len(a.Dependencies) > 0 {
		var v any
		_ = json.Unmarshal(a.Dependencies, &v)
		pretty, _ := json.MarshalIndent(v, "", "  ")
		prompt += "\n\nDependency outputs:\n" + string(pretty)
	}
	var parent *ParentRun
	if a.ParentRunID != nil {
		id, err := uuid.Parse(*a.ParentRunID)
		if err != nil {
			return nil, fmt.Errorf("parent_run_id must be a UUID")
		}
		if a.AtSeq == nil {
			return nil, fmt.Errorf("at_seq is required with parent_run_id")
		}
		parent = &ParentRun{RunID: id, AtSeq: *a.AtSeq}
	} else if a.AtSeq != nil {
		return nil, fmt.Errorf("parent_run_id is required with at_seq")
	}
	child, err := r.Run(ctx, profile, prompt, parent)
	if err != nil {
		return nil, err
	}
	return json.Marshal(child)
}
