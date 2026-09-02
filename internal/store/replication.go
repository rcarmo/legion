package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
)

// MaterializedSnapshot is the versioned logical form persisted in Raft
// snapshots. It is independent of SQLite page layout and safe across upgrades.
type MaterializedSnapshot struct {
	Version  uint16            `json:"version"`
	Sessions []SnapshotSession `json:"sessions"`
	Turns    []SnapshotTurn    `json:"turns"`
}

type SnapshotSession struct {
	RunID, ParentRun string
	ForkSeq          *uint64
	Status, Config   json.RawMessage
	CreatedAt        int64
	UpdatedAt        int64
}

type SnapshotTurn struct {
	RunID    string
	Envelope core.TurnEnvelope
}

// ApplyCreate materializes an already-ordered Raft command exactly.
func (s *SQLiteStore) ApplyCreate(ctx context.Context, id core.RunID, config core.RunConfig, createdAt int64) error {
	cfg, err := json.Marshal(config)
	if err != nil {
		return err
	}
	status, _ := json.Marshal(core.StatusIdle)
	_, err = s.db.ExecContext(ctx, `INSERT INTO sessions(run_id,status,config,created_at,updated_at) VALUES(?,?,?,?,?)`, id.String(), status, cfg, createdAt, createdAt)
	if err != nil && stringsContainsUnique(err) {
		return core.ErrSessionExists
	}
	return err
}

// EnvelopeTail returns the next sequence and predecessor hash without scanning
// the full session log. Raft's serialized FSM uses this to derive append batches
// in O(1) with respect to existing history.
func (s *SQLiteStore) EnvelopeTail(ctx context.Context, id core.RunID) (core.SeqNum, [32]byte, error) {
	var previous [32]byte
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions WHERE run_id=?`, id.String()).Scan(&exists); err != nil {
		return 0, previous, err
	}
	if exists == 0 {
		return 0, previous, core.ErrSessionNotFound
	}
	var next uint64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq)+1,0) FROM turns WHERE run_id=?`, id.String()).Scan(&next); err != nil {
		return 0, previous, err
	}
	if next > 0 {
		last, err := loadTurn(ctx, s.db, id, core.SeqNum(next-1))
		if err != nil {
			return 0, previous, err
		}
		previous = core.HashEnvelope(last)
	}
	return core.SeqNum(next), previous, nil
}

// ApplyEnvelope inserts a leader-generated envelope after validating sequence
// and predecessor hash against the local materialized state.
func (s *SQLiteStore) ApplyEnvelope(ctx context.Context, envelope core.TurnEnvelope) error {
	return s.ApplyEnvelopes(ctx, []core.TurnEnvelope{envelope})
}

// ApplyEnvelopes applies one hash-chained batch in a single SQLite transaction.
// The Raft command still carries typed events, so every voter deterministically
// derives and validates the same envelopes rather than replicating arbitrary SQL.
func (s *SQLiteStore) ApplyEnvelopes(ctx context.Context, envelopes []core.TurnEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	runID := envelopes[0].RunID
	var count uint64
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE run_id=?`, runID.String()).Scan(&count); err != nil {
		return err
	}
	var previous [32]byte
	if count > 0 {
		last, loadErr := loadTurn(ctx, tx, runID, core.SeqNum(count-1))
		if loadErr != nil {
			return loadErr
		}
		previous = core.HashEnvelope(last)
	}
	const maxRowsPerInsert = 100
	var values strings.Builder
	args := make([]any, 0, maxRowsPerInsert*11)
	flush := func() error {
		if len(args) == 0 {
			return nil
		}
		_, flushErr := tx.ExecContext(ctx, `INSERT INTO turns(run_id,seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at) VALUES `+values.String(), args...)
		values.Reset()
		args = args[:0]
		return flushErr
	}
	for index, envelope := range envelopes {
		want := core.SeqNum(count + uint64(index))
		if envelope.RunID != runID {
			return fmt.Errorf("materialized batch mixes run ids")
		}
		if envelope.Seq != want {
			return fmt.Errorf("materialized sequence mismatch: got %d want %d", envelope.Seq, want)
		}
		if envelope.PrevHash != previous {
			return fmt.Errorf("materialized predecessor mismatch at %d", envelope.Seq)
		}
		kind, marshalErr := json.Marshal(envelope.Event.Kind)
		if marshalErr != nil {
			return marshalErr
		}
		var payload any
		if envelope.Event.Payload != nil {
			payload = string(envelope.Event.Payload)
		}
		if len(args) > 0 {
			values.WriteByte(',')
		}
		values.WriteString(`(?,?,?,?,?,?,?,?,?,?,?)`)
		args = append(args, envelope.RunID.String(), envelope.Seq, envelope.PrevHash[:], kind, payload, envelope.Event.PayloadCID, envelope.Event.Model, envelope.Event.TokensIn, envelope.Event.TokensOut, envelope.Event.WallMS, envelope.CreatedAt)
		if (index+1)%maxRowsPerInsert == 0 {
			if err = flush(); err != nil {
				return err
			}
		}
		previous = core.HashEnvelope(envelope)
	}
	if err = flush(); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sessions SET updated_at=? WHERE run_id=?`, envelopes[len(envelopes)-1].CreatedAt, runID.String()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) ApplyStatus(ctx context.Context, id core.RunID, status core.SessionStatus, updatedAt int64) error {
	encoded, err := json.Marshal(status)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sessions SET status=?,updated_at=? WHERE run_id=?`, encoded, updatedAt, id.String())
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return core.ErrSessionNotFound
	}
	return nil
}

func (s *SQLiteStore) ApplyFork(ctx context.Context, parent, child core.RunID, at core.SeqNum, createdAt int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var config []byte
	if err = tx.QueryRowContext(ctx, `SELECT config FROM sessions WHERE run_id=?`, parent.String()).Scan(&config); errors.Is(err, sql.ErrNoRows) {
		return core.ErrSessionNotFound
	} else if err != nil {
		return err
	}
	var exists int
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM turns WHERE run_id=? AND seq=?`, parent.String(), at).Scan(&exists); err != nil {
		return fmt.Errorf("fork sequence %d does not exist", at)
	}
	status, _ := json.Marshal(core.StatusIdle)
	if _, err = tx.ExecContext(ctx, `INSERT INTO sessions(run_id,parent_run,fork_seq,status,config,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, child.String(), parent.String(), at, status, config, createdAt, createdAt); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO turns SELECT ?,seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at FROM turns WHERE run_id=? AND seq<=?`, child.String(), parent.String(), at); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) Snapshot(ctx context.Context) ([]byte, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	snapshot := MaterializedSnapshot{Version: 1, Sessions: []SnapshotSession{}, Turns: []SnapshotTurn{}}
	rows, err := s.db.QueryContext(ctx, `SELECT run_id,parent_run,fork_seq,status,config,created_at,updated_at FROM sessions ORDER BY run_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v SnapshotSession
		var parent sql.NullString
		var fork sql.NullInt64
		if err = rows.Scan(&v.RunID, &parent, &fork, &v.Status, &v.Config, &v.CreatedAt, &v.UpdatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if parent.Valid {
			v.ParentRun = parent.String
		}
		if fork.Valid {
			n := uint64(fork.Int64)
			v.ForkSeq = &n
		}
		snapshot.Sessions = append(snapshot.Sessions, v)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	// Re-query by session through the verified reader to keep one decoding path.
	for _, session := range snapshot.Sessions {
		id, parseErr := uuid.Parse(session.RunID)
		if parseErr != nil {
			return nil, parseErr
		}
		log, readErr := s.ReadLog(ctx, id)
		if readErr != nil {
			return nil, readErr
		}
		for _, envelope := range log {
			snapshot.Turns = append(snapshot.Turns, SnapshotTurn{RunID: session.RunID, Envelope: envelope})
		}
	}
	return json.Marshal(snapshot)
}

func (s *SQLiteStore) Restore(ctx context.Context, encoded []byte) error {
	var snapshot MaterializedSnapshot
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return err
	}
	if snapshot.Version != 1 {
		return fmt.Errorf("unsupported materialized snapshot version %d", snapshot.Version)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DELETE FROM turns; DELETE FROM sessions`); err != nil {
		return err
	}
	for _, v := range snapshot.Sessions {
		var parent any
		if v.ParentRun != "" {
			parent = v.ParentRun
		}
		var fork any
		if v.ForkSeq != nil {
			fork = *v.ForkSeq
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO sessions(run_id,parent_run,fork_seq,status,config,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, v.RunID, parent, fork, []byte(v.Status), []byte(v.Config), v.CreatedAt, v.UpdatedAt); err != nil {
			return err
		}
	}
	for _, v := range snapshot.Turns {
		e := v.Envelope.Event
		kind, marshalErr := json.Marshal(e.Kind)
		if marshalErr != nil {
			return marshalErr
		}
		var payload any
		if e.Payload != nil {
			payload = string(e.Payload)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO turns(run_id,seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, v.RunID, v.Envelope.Seq, v.Envelope.PrevHash[:], kind, payload, e.PayloadCID, e.Model, e.TokensIn, e.TokensOut, e.WallMS, v.Envelope.CreatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stringsContainsUnique(err error) bool {
	return err != nil && (errors.Is(err, core.ErrSessionExists) || strings.Contains(err.Error(), "UNIQUE"))
}
