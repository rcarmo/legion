package namespace

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

type fakeStore struct {
	id     core.RunID
	config core.RunConfig
	log    []core.TurnEnvelope
	status core.SessionStatus
}

func (s *fakeStore) Append(_ context.Context, id core.RunID, e core.TurnEvent) (core.SeqNum, error) {
	seq := core.SeqNum(len(s.log))
	s.log = append(s.log, core.TurnEnvelope{RunID: id, Seq: seq, Event: e})
	return seq, nil
}
func (s *fakeStore) ReadLog(context.Context, core.RunID) ([]core.TurnEnvelope, error) {
	return s.log, nil
}
func (s *fakeStore) ReadRecent(_ context.Context, _ core.RunID, n int) ([]core.TurnEnvelope, error) {
	if n > len(s.log) {
		n = len(s.log)
	}
	return s.log[len(s.log)-n:], nil
}
func (s *fakeStore) SessionStatus(context.Context, core.RunID) (core.SessionStatus, error) {
	return s.status, nil
}
func (s *fakeStore) SetStatus(_ context.Context, _ core.RunID, v core.SessionStatus) error {
	s.status = v
	return nil
}
func (s *fakeStore) Fork(context.Context, core.RunID, core.SeqNum) (core.RunID, error) {
	return uuid.New(), nil
}
func (s *fakeStore) ListSessions(context.Context, core.SessionFilter) ([]core.SessionSummary, error) {
	return nil, nil
}
func (s *fakeStore) CreateSession(_ context.Context, id core.RunID, c core.RunConfig) error {
	s.id = id
	s.config = c
	return nil
}

type fakeLoop struct{ store *fakeStore }

func (l fakeLoop) Start(ctx context.Context, c core.RunConfig) (core.RunID, error) {
	id := uuid.New()
	_ = l.store.CreateSession(ctx, id, c)
	_, _ = l.store.Append(ctx, id, core.SessionStarted(c))
	l.store.status = core.StatusIdle
	return id, nil
}
func (fakeLoop) Recover(context.Context, core.RunID) error { return nil }
func (l fakeLoop) Resume(ctx context.Context, id core.RunID, e core.ExternalEvent) error {
	_, err := l.store.Append(ctx, id, core.NewUserMessage(e.Content))
	return err
}
func (l fakeLoop) Resolve(ctx context.Context, id core.RunID) (core.TurnEnvelope, error) {
	seq, _ := l.store.Append(ctx, id, core.AssistantMessage("ok", "fake", 0, 0, 0))
	return l.store.log[seq], nil
}
func TestSessionResourcesAllPaths(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	r := NewSessionResources(store, fakeLoop{store})
	created, ok, err := r.Write(ctx, "/sessions/new", []byte(`{"model":"fake","budget":{},"tools":[]}`))
	if err != nil || !ok {
		t.Fatal(err)
	}
	var result struct {
		RunID core.RunID `json:"run_id"`
	}
	_ = json.Unmarshal(created, &result)
	id := result.RunID.String()
	for _, field := range []string{"turns", "status", "context", "config"} {
		if _, ok, err = r.Read(ctx, "/sessions/"+id+"/"+field); err != nil || !ok {
			t.Fatalf("read %s: %v", field, err)
		}
	}
	if _, ok, err = r.Write(ctx, "/sessions/"+id+"/turns", []byte(`{"content":"hello"}`)); err != nil || !ok {
		t.Fatal(err)
	}
	if _, ok, err = r.Write(ctx, "/sessions/"+id+"/status", []byte("abort")); err != nil || !ok {
		t.Fatal(err)
	}
	if store.status.Status != "aborted" {
		t.Fatal(store.status)
	}
	if _, ok, err = r.Write(ctx, "/sessions/"+id+"/fork", []byte(`{"at_seq":0}`)); err != nil || !ok {
		t.Fatal(err)
	}
}
