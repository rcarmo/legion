// Package store implements Legion persistence.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rcarmo/legion/internal/core"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type SQLiteStore struct {
	db      *sql.DB
	writeMu sync.Mutex
}

func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &SQLiteStore{db: db}
	if err = s.init(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func OpenMemory() (*SQLiteStore, error) {
	return Open("file:" + uuid.NewString() + "?mode=memory&cache=shared")
}
func (s *SQLiteStore) Close() error { return s.db.Close() }
func (s *SQLiteStore) init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;"); err != nil {
		return err
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		b, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, string(b)); err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (s *SQLiteStore) CreateSession(ctx context.Context, id core.RunID, c core.RunConfig) error {
	cfg, _ := json.Marshal(c)
	st, _ := json.Marshal(core.StatusIdle)
	now := core.NowMS()
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions(run_id,status,config,created_at,updated_at) VALUES(?,?,?,?,?)`, id.String(), st, cfg, now, now)
	if err != nil && strings.Contains(err.Error(), "UNIQUE") {
		return core.ErrSessionExists
	}
	return err
}
func (s *SQLiteStore) Append(ctx context.Context, id core.RunID, event core.TurnEvent) (core.SeqNum, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var exists int
	if err = tx.QueryRowContext(ctx, "SELECT 1 FROM sessions WHERE run_id=?", id.String()).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return 0, core.ErrSessionNotFound
	} else if err != nil {
		return 0, err
	}
	var seq uint64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq)+1,0) FROM turns WHERE run_id=?", id.String()).Scan(&seq); err != nil {
		return 0, err
	}
	var prev [32]byte
	if seq > 0 {
		last, err := loadTurn(ctx, tx, id, core.SeqNum(seq-1))
		if err != nil {
			return 0, err
		}
		prev = core.HashEnvelope(last)
	}
	kind, _ := json.Marshal(event.Kind)
	var payload any
	if event.Payload != nil {
		payload = string(event.Payload)
	}
	now := core.NowMS()
	_, err = tx.ExecContext(ctx, `INSERT INTO turns(run_id,seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id.String(), seq, prev[:], kind, payload, event.PayloadCID, event.Model, event.TokensIn, event.TokensOut, event.WallMS, now)
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sessions SET updated_at=? WHERE run_id=?", now, id.String()); err != nil {
		return 0, err
	}
	return core.SeqNum(seq), tx.Commit()
}
func (s *SQLiteStore) ReadLog(ctx context.Context, id core.RunID) ([]core.TurnEnvelope, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at FROM turns WHERE run_id=? ORDER BY seq`, id.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanRows(rows, id)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		var n int
		if e := s.db.QueryRowContext(ctx, "SELECT count(*) FROM sessions WHERE run_id=?", id.String()).Scan(&n); e != nil || n == 0 {
			return nil, core.ErrSessionNotFound
		}
	}
	return out, core.VerifyChain(out, id)
}
func (s *SQLiteStore) ReadRecent(ctx context.Context, id core.RunID, n int) ([]core.TurnEnvelope, error) {
	if n < 0 {
		n = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at FROM (SELECT * FROM turns WHERE run_id=? ORDER BY seq DESC LIMIT ?) ORDER BY seq`, id.String(), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows, id)
}
func (s *SQLiteStore) SessionStatus(ctx context.Context, id core.RunID) (core.SessionStatus, error) {
	var b []byte
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM sessions WHERE run_id=?", id.String()).Scan(&b); errors.Is(err, sql.ErrNoRows) {
		return core.SessionStatus{}, core.ErrSessionNotFound
	} else if err != nil {
		return core.SessionStatus{}, err
	}
	var st core.SessionStatus
	return st, json.Unmarshal(b, &st)
}
func (s *SQLiteStore) SetStatus(ctx context.Context, id core.RunID, st core.SessionStatus) error {
	b, _ := json.Marshal(st)
	r, err := s.db.ExecContext(ctx, "UPDATE sessions SET status=?,updated_at=? WHERE run_id=?", b, core.NowMS(), id.String())
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return core.ErrSessionNotFound
	}
	return nil
}
func (s *SQLiteStore) Fork(ctx context.Context, id core.RunID, at core.SeqNum) (core.RunID, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback()
	var cfg []byte
	if err = tx.QueryRowContext(ctx, "SELECT config FROM sessions WHERE run_id=?", id.String()).Scan(&cfg); errors.Is(err, sql.ErrNoRows) {
		return uuid.Nil, core.ErrSessionNotFound
	} else if err != nil {
		return uuid.Nil, err
	}
	var exists int
	if err = tx.QueryRowContext(ctx, "SELECT 1 FROM turns WHERE run_id=? AND seq=?", id.String(), at).Scan(&exists); err != nil {
		return uuid.Nil, fmt.Errorf("fork sequence %d does not exist", at)
	}
	nid := uuid.New()
	st, _ := json.Marshal(core.StatusIdle)
	now := core.NowMS()
	if _, err = tx.ExecContext(ctx, `INSERT INTO sessions(run_id,parent_run,fork_seq,status,config,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, nid.String(), id.String(), at, st, cfg, now, now); err != nil {
		return uuid.Nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO turns SELECT ?,seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at FROM turns WHERE run_id=? AND seq<=?`, nid.String(), id.String(), at); err != nil {
		return uuid.Nil, err
	}
	return nid, tx.Commit()
}
func (s *SQLiteStore) ListSessions(ctx context.Context, f core.SessionFilter) ([]core.SessionSummary, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.run_id,s.status,s.config,s.created_at,s.updated_at,(SELECT COUNT(*) FROM turns t WHERE t.run_id=s.run_id) FROM sessions s ORDER BY s.created_at DESC LIMIT ? OFFSET ?`, limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.SessionSummary
	for rows.Next() {
		var rid string
		var stb, cfgb []byte
		var c, u, turns int64
		if err = rows.Scan(&rid, &stb, &cfgb, &c, &u, &turns); err != nil {
			return nil, err
		}
		var st core.SessionStatus
		var cfg core.RunConfig
		if json.Unmarshal(stb, &st) != nil || json.Unmarshal(cfgb, &cfg) != nil {
			continue
		}
		if f.Status != "" && st.Status != f.Status {
			continue
		}
		id, _ := uuid.Parse(rid)
		out = append(out, core.SessionSummary{id, st, cfg.Model, uint64(turns), c, u})
	}
	return out, rows.Err()
}

type scanner interface{ Scan(...any) error }

func scanEnvelope(r scanner, id core.RunID) (core.TurnEnvelope, error) {
	var e core.TurnEnvelope
	e.RunID = id
	var prev, kind, payload []byte
	var cid, model sql.NullString
	var ti, to, wall sql.NullInt64
	if err := r.Scan(&e.Seq, &prev, &kind, &payload, &cid, &model, &ti, &to, &wall, &e.CreatedAt); err != nil {
		return e, err
	}
	copy(e.PrevHash[:], prev)
	if err := json.Unmarshal(kind, &e.Event.Kind); err != nil {
		return e, err
	}
	if len(payload) > 0 {
		e.Event.Payload = append(json.RawMessage(nil), payload...)
	}
	if cid.Valid {
		e.Event.PayloadCID = &cid.String
	}
	if model.Valid {
		e.Event.Model = &model.String
	}
	if ti.Valid {
		v := uint32(ti.Int64)
		e.Event.TokensIn = &v
	}
	if to.Valid {
		v := uint32(to.Int64)
		e.Event.TokensOut = &v
	}
	if wall.Valid {
		v := uint64(wall.Int64)
		e.Event.WallMS = &v
	}
	return e, nil
}
func loadTurn(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id core.RunID, seq core.SeqNum) (core.TurnEnvelope, error) {
	return scanEnvelope(q.QueryRowContext(ctx, `SELECT seq,prev_hash,kind,payload,payload_cid,model,tokens_in,tokens_out,wall_ms,created_at FROM turns WHERE run_id=? AND seq=?`, id.String(), seq), id)
}
func scanRows(rows *sql.Rows, id core.RunID) ([]core.TurnEnvelope, error) {
	var out []core.TurnEnvelope
	for rows.Next() {
		e, err := scanEnvelope(rows, id)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
