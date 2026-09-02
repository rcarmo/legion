// Package runtime defines function invocation contracts shared by executors.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Kind string

const (
	WASM  Kind = "wasm"
	Bun   Kind = "bun"
	Joker Kind = "joker"
)

func (k Kind) MarshalJSON() ([]byte, error) {
	switch k {
	case WASM, Bun, Joker:
		return json.Marshal(string(k))
	default:
		return nil, fmt.Errorf("unsupported runtime %q", k)
	}
}
func (k *Kind) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	switch Kind(value) {
	case WASM, Bun, Joker:
		*k = Kind(value)
	default:
		return fmt.Errorf("unsupported runtime %q", value)
	}
	return nil
}

type Manifest struct {
	Name        string            `json:"name"`
	Runtime     Kind              `json:"-"`
	Version     string            `json:"version"`
	ArtifactCID string            `json:"artifact_cid,omitempty"`
	DeployedAt  int64             `json:"deployed_at"`
	Parameters  json.RawMessage   `json:"parameters"`
	Description string            `json:"description"`
	Idempotent  bool              `json:"idempotent"`
	Env         map[string]string `json:"env,omitempty"`
}

func (m Manifest) MarshalJSON() ([]byte, error) {
	type wire Manifest
	runtime, executor := m.Runtime, ""
	if runtime == Joker {
		runtime, executor = Bun, "joker"
	}
	return json.Marshal(struct {
		wire
		Runtime  Kind   `json:"runtime"`
		Executor string `json:"executor,omitempty"`
	}{wire: wire(m), Runtime: runtime, Executor: executor})
}
func (m *Manifest) UnmarshalJSON(data []byte) error {
	type wire Manifest
	var value struct {
		wire
		Runtime  Kind   `json:"runtime"`
		Executor string `json:"executor,omitempty"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*m = Manifest(value.wire)
	m.Runtime = value.Runtime
	if value.Runtime == Bun && value.Executor == "joker" {
		m.Runtime = Joker
	}
	if value.Executor != "" && value.Executor != "joker" {
		return fmt.Errorf("unsupported executor %q", value.Executor)
	}
	return nil
}

type Request struct {
	FunctionName string            `json:"function_name"`
	CallID       string            `json:"call_id"`
	ArtifactCID  string            `json:"artifact_cid,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	Args         json.RawMessage   `json:"args"`
}
type Result struct {
	CallID string          `json:"call_id"`
	Output json.RawMessage `json:"output"`
	WallMS uint64          `json:"wall_ms"`
	Error  string          `json:"error,omitempty"`
}
type Invoker interface {
	Invoke(context.Context, Request) (Result, error)
}
type InvocationObserver interface{ Record(string, uint64, bool) }
type rejectionObserver interface{ Reject() }
type ObservedInvoker struct {
	Inner    Invoker
	Observer InvocationObserver
}

func (o ObservedInvoker) Invoke(ctx context.Context, req Request) (result Result, err error) {
	result, err = o.Inner.Invoke(ctx, req)
	if o.Observer != nil {
		o.Observer.Record(req.FunctionName, result.WallMS, err != nil || result.Error != "")
		var limit LimitError
		if errors.As(err, &limit) && (limit.Kind == LimitRate || limit.Kind == LimitBusy) {
			if observer, ok := o.Observer.(rejectionObserver); ok {
				observer.Reject()
			}
		}
	}
	return
}

type ArtifactSource interface {
	Fetch(context.Context, string) ([]byte, error)
}
type CachedArtifactSource interface {
	CachedPath(context.Context, string, string) (string, error)
}
type Namespace interface {
	Read(context.Context, string) ([]byte, error)
	Write(context.Context, string, []byte) ([]byte, error)
}
type Budget interface{ Take(uint64) uint64 }
type Limits struct {
	Timeout                  time.Duration
	MaxMemoryBytes           uint64
	MaxInputBytes            int
	MaxOutputBytes           int
	MaxConcurrentPerFunction int
	MaxRequestsPerWindow     int
	RateWindow               time.Duration
}

func DefaultLimits() Limits {
	return Limits{Timeout: 30 * time.Second, MaxMemoryBytes: 64 << 20, MaxInputBytes: 1 << 20, MaxOutputBytes: 4 << 20, MaxConcurrentPerFunction: 8, MaxRequestsPerWindow: 120, RateWindow: time.Minute}
}
