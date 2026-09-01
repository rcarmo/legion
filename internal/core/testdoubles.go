package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

type memorySession struct {
	Log                  []TurnEnvelope
	Status               SessionStatus
	Config               RunConfig
	CreatedAt, UpdatedAt int64
}
type MemoryEventStore struct {
	mu       sync.RWMutex
	sessions map[RunID]*memorySession
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{sessions: map[RunID]*memorySession{}}
}
func (s *MemoryEventStore) CreateSession(_ context.Context, id RunID, c RunConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[id]; ok {
		return ErrSessionExists
	}
	now := NowMS()
	s.sessions[id] = &memorySession{Status: StatusIdle, Config: c, CreatedAt: now, UpdatedAt: now}
	return nil
}
func (s *MemoryEventStore) Append(_ context.Context, id RunID, e TurnEvent) (SeqNum, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sessions[id]
	if !ok {
		return 0, ErrSessionNotFound
	}
	seq := SeqNum(len(v.Log))
	var prev [32]byte
	if seq > 0 {
		prev = HashEnvelope(v.Log[len(v.Log)-1])
	}
	now := NowMS()
	v.Log = append(v.Log, TurnEnvelope{RunID: id, Seq: seq, PrevHash: prev, Event: e, CreatedAt: now})
	v.UpdatedAt = now
	return seq, nil
}
func (s *MemoryEventStore) ReadLog(_ context.Context, id RunID) ([]TurnEnvelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	out := append([]TurnEnvelope(nil), v.Log...)
	return out, VerifyChain(out, id)
}
func (s *MemoryEventStore) ReadRecent(_ context.Context, id RunID, n int) ([]TurnEnvelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	start := len(v.Log) - n
	if start < 0 {
		start = 0
	}
	return append([]TurnEnvelope(nil), v.Log[start:]...), nil
}
func (s *MemoryEventStore) SessionStatus(_ context.Context, id RunID) (SessionStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.sessions[id]
	if !ok {
		return SessionStatus{}, ErrSessionNotFound
	}
	return v.Status, nil
}
func (s *MemoryEventStore) SetStatus(_ context.Context, id RunID, st SessionStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	v.Status = st
	v.UpdatedAt = NowMS()
	return nil
}
func (s *MemoryEventStore) Fork(_ context.Context, id RunID, at SeqNum) (RunID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sessions[id]
	if !ok {
		return uuid.Nil, ErrSessionNotFound
	}
	if at >= SeqNum(len(v.Log)) {
		return uuid.Nil, fmt.Errorf("fork sequence %d does not exist", at)
	}
	nid := uuid.New()
	now := NowMS()
	s.sessions[nid] = &memorySession{Log: append([]TurnEnvelope(nil), v.Log[:at+1]...), Status: StatusIdle, Config: v.Config, CreatedAt: now, UpdatedAt: now}
	return nid, nil
}
func (s *MemoryEventStore) ListSessions(_ context.Context, f SessionFilter) ([]SessionSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SessionSummary{}
	for id, v := range s.sessions {
		if f.Status != "" && v.Status.Status != f.Status {
			continue
		}
		out = append(out, SessionSummary{id, v.Status, v.Config.Model, uint64(len(v.Log)), v.CreatedAt, v.UpdatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	start := f.Offset
	if start > len(out) {
		start = len(out)
	}
	end := len(out)
	if f.Limit > 0 && start+f.Limit < end {
		end = start + f.Limit
	}
	return out[start:end], nil
}

type EchoToolRegistry struct{}

func (EchoToolRegistry) Definitions(context.Context) ([]ToolDefinition, error) {
	return []ToolDefinition{{Name: "echo", Description: "Returns its input unchanged.", Parameters: json.RawMessage(`{"type":"object"}`), Effect: EffectRead}, {Name: "fail", Description: "Always fails.", Parameters: json.RawMessage(`{"type":"object"}`), Effect: EffectWrite}}, nil
}
func (EchoToolRegistry) Dispatch(_ context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	switch name {
	case "echo":
		return args, nil
	case "fail":
		return nil, fmt.Errorf("intentional tool failure")
	default:
		return nil, fmt.Errorf("%w: %s", ErrToolNotFound, name)
	}
}
