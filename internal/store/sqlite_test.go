package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

func cfg() core.RunConfig { return core.RunConfig{Model: "faux/test", Tools: []string{"echo"}} }
func TestSQLiteAppendPersistRecentStatusAndFork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legion.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if err = s.CreateSession(ctx, id, cfg()); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"a", "b", "c"} {
		if _, err = s.Append(ctx, id, core.NewUserMessage(m)); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SetStatus(ctx, id, core.StatusRunning); err != nil {
		t.Fatal(err)
	}
	recent, err := s.ReadRecent(ctx, id, 2)
	if err != nil || len(recent) != 2 || recent[0].Seq != 1 {
		t.Fatalf("recent=%#v err=%v", recent, err)
	}
	fork, err := s.Fork(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	flog, err := s.ReadLog(ctx, fork)
	if err != nil || len(flog) != 2 {
		t.Fatalf("fork log=%d err=%v", len(flog), err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	log, err := s.ReadLog(ctx, id)
	if err != nil || len(log) != 3 {
		t.Fatalf("reopen log=%d err=%v", len(log), err)
	}
	st, err := s.SessionStatus(ctx, id)
	if err != nil || st.Status != "running" {
		t.Fatalf("status=%#v err=%v", st, err)
	}
	list, err := s.ListSessions(ctx, core.SessionFilter{Status: "running"})
	if err != nil || len(list) != 1 || list[0].RunID != id {
		t.Fatalf("list=%#v err=%v", list, err)
	}
}
func TestSQLiteStatusFilterAppliesBeforePagination(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, status := range []core.SessionStatus{core.StatusRunning, core.StatusCompleted, core.StatusRunning} {
		id := uuid.New()
		if err = s.CreateSession(ctx, id, cfg()); err != nil {
			t.Fatal(err)
		}
		if err = s.SetStatus(ctx, id, status); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.ListSessions(ctx, core.SessionFilter{Status: "completed", Limit: 1})
	if err != nil || len(list) != 1 || list[0].Status.Status != "completed" {
		t.Fatalf("list=%#v err=%v", list, err)
	}
}

func TestSQLiteRejectsMissingAndOutOfRangeFork(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := uuid.New()
	if _, err = s.Append(ctx, id, core.NewUserMessage("x")); !errors.Is(err, core.ErrSessionNotFound) {
		t.Fatal(err)
	}
	if err = s.CreateSession(ctx, id, cfg()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Fork(ctx, id, 0); err == nil {
		t.Fatal("empty session fork accepted")
	}
}
func TestSQLiteDetectsTampering(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := uuid.New()
	_ = s.CreateSession(ctx, id, cfg())
	_, _ = s.Append(ctx, id, core.NewUserMessage("a"))
	_, _ = s.Append(ctx, id, core.NewUserMessage("b"))
	if _, err = s.db.ExecContext(ctx, "UPDATE turns SET prev_hash=zeroblob(32) WHERE run_id=? AND seq=1", id.String()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadLog(ctx, id); !errors.Is(err, core.ErrTamperEvident) {
		t.Fatalf("got %v", err)
	}
}
func TestMigrationsApplied(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
