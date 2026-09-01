package namespace

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

type Resources interface {
	Read(context.Context, string) ([]byte, bool, error)
	Write(context.Context, string, []byte) ([]byte, bool, error)
}
type Peer interface {
	Read(context.Context, string) ([]byte, error)
	Write(context.Context, string, []byte) ([]byte, error)
}
type Functions interface {
	Invoke(context.Context, string, []byte) ([]byte, error)
}
type Deploy interface {
	Read(context.Context, string) ([]byte, bool, error)
	Write(context.Context, string, []byte) ([]byte, bool, error)
}
type Cluster interface {
	Read(context.Context, string) ([]byte, bool, error)
}

type SessionResources struct {
	Store core.EventStore
	Loop  core.AgentLoop
}

func NewSessionResources(store core.EventStore, loop core.AgentLoop) *SessionResources {
	return &SessionResources{Store: store, Loop: loop}
}
func parts(p string) []string             { return strings.Split(strings.Trim(p, "/"), "/") }
func marshal(v any) ([]byte, bool, error) { b, err := json.Marshal(v); return b, true, err }
func parseRunID(s string) (core.RunID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return id, fmt.Errorf("invalid run id: %w", err)
	}
	return id, nil
}
func (r *SessionResources) Read(ctx context.Context, p string) ([]byte, bool, error) {
	v := parts(p)
	if len(v) != 3 || v[0] != "sessions" {
		return nil, false, nil
	}
	id, err := parseRunID(v[1])
	if err != nil {
		return nil, false, err
	}
	switch v[2] {
	case "turns":
		x, e := r.Store.ReadLog(ctx, id)
		if e != nil {
			return nil, false, e
		}
		return marshal(x)
	case "status":
		x, e := r.Store.SessionStatus(ctx, id)
		if e != nil {
			return nil, false, e
		}
		return marshal(x)
	case "context":
		x, e := r.Store.ReadRecent(ctx, id, 64)
		if e != nil {
			return nil, false, e
		}
		return marshal(x)
	case "config":
		x, e := r.Store.ReadLog(ctx, id)
		if e != nil {
			return nil, false, e
		}
		for _, envelope := range x {
			if envelope.Event.Kind.Kind == "session_started" {
				return append([]byte(nil), envelope.Event.Payload...), true, nil
			}
		}
		return marshal(nil)
	default:
		return nil, false, nil
	}
}
func (r *SessionResources) Write(ctx context.Context, p string, data []byte) ([]byte, bool, error) {
	v := parts(p)
	if len(v) == 2 && v[0] == "sessions" && v[1] == "new" {
		var c core.RunConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, false, err
		}
		id, err := r.Loop.Start(ctx, c)
		if err != nil {
			return nil, false, err
		}
		return marshal(map[string]any{"run_id": id})
	}
	if len(v) != 3 || v[0] != "sessions" {
		return nil, false, nil
	}
	id, err := parseRunID(v[1])
	if err != nil {
		return nil, false, err
	}
	switch v[2] {
	case "turns":
		var value any
		if err = json.Unmarshal(data, &value); err != nil {
			return nil, false, err
		}
		var content string
		switch x := value.(type) {
		case string:
			content = x
		case map[string]any:
			content, _ = x["content"].(string)
		}
		if content == "" {
			return nil, false, fmt.Errorf("turn content required")
		}
		if err = r.Loop.Resume(ctx, id, core.UserMessage(content)); err != nil {
			return nil, false, err
		}
		e, err := r.Loop.Resolve(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return marshal(map[string]any{"seq": e.Seq, "event": e.Event})
	case "status":
		command := strings.Trim(strings.TrimSpace(string(data)), "\"")
		var status core.SessionStatus
		switch command {
		case "abort":
			status = core.StatusAborted
		case "resume":
			status = core.StatusResuming
		default:
			return nil, false, fmt.Errorf("status command must be abort or resume")
		}
		if err = r.Store.SetStatus(ctx, id, status); err != nil {
			return nil, false, err
		}
		return marshal(status)
	case "fork":
		var req struct {
			AtSeq core.SeqNum `json:"at_seq"`
		}
		if err = json.Unmarshal(data, &req); err != nil {
			return nil, false, err
		}
		child, err := r.Store.Fork(ctx, id, req.AtSeq)
		if err != nil {
			return nil, false, err
		}
		return marshal(map[string]any{"run_id": child})
	default:
		return nil, false, nil
	}
}
