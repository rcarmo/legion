package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

func TestMaterializedEnvelopeRequiresExactSequenceAndHash(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := uuid.New()
	if err = s.ApplyCreate(ctx, id, cfg(), 10); err != nil {
		t.Fatal(err)
	}
	first := core.TurnEnvelope{RunID: id, Seq: 0, Event: core.NewUserMessage("first"), CreatedAt: 11}
	if err = s.ApplyEnvelope(ctx, first); err != nil {
		t.Fatal(err)
	}
	bad := core.TurnEnvelope{RunID: id, Seq: 2, Event: core.NewUserMessage("bad"), CreatedAt: 12}
	if err = s.ApplyEnvelope(ctx, bad); err == nil {
		t.Fatal("out-of-order envelope accepted")
	}
	second := core.TurnEnvelope{RunID: id, Seq: 1, PrevHash: core.HashEnvelope(first), Event: core.NewUserMessage("second"), CreatedAt: 12}
	if err = s.ApplyEnvelope(ctx, second); err != nil {
		t.Fatal(err)
	}
	log, err := s.ReadLog(ctx, id)
	if err != nil || len(log) != 2 || log[1].CreatedAt != 12 {
		t.Fatalf("log=%#v err=%v", log, err)
	}
}

func TestMaterializedSnapshotRestorePreservesForkAndChain(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parent := uuid.New()
	if err = s.ApplyCreate(ctx, parent, cfg(), 10); err != nil {
		t.Fatal(err)
	}
	first := core.TurnEnvelope{RunID: parent, Seq: 0, Event: core.NewUserMessage("first"), CreatedAt: 11}
	if err = s.ApplyEnvelope(ctx, first); err != nil {
		t.Fatal(err)
	}
	child := uuid.New()
	if err = s.ApplyFork(ctx, parent, child, 0, 12); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyStatus(ctx, parent, core.StatusCompleted, 13); err != nil {
		t.Fatal(err)
	}
	encoded, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err = restored.Restore(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	for _, id := range []core.RunID{parent, child} {
		log, readErr := restored.ReadLog(ctx, id)
		if readErr != nil || len(log) != 1 {
			t.Fatalf("run=%s log=%#v err=%v", id, log, readErr)
		}
	}
	status, err := restored.SessionStatus(ctx, parent)
	if err != nil || status != core.StatusCompleted {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}
