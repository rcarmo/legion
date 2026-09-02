package store

import (
	"context"
	"fmt"
	"strings"
)

// ApplyLoadRows is the narrow replicated-storage capacity primitive. It writes
// opaque rows into the derived SQLite state in bounded multi-value statements;
// the enclosing Raft command remains typed and versioned. This is intentionally
// separate from EventStore.Append, whose per-event hash chain is a durability
// and latency path rather than a bulk-ingest benchmark.
func (s *SQLiteStore) ApplyLoadRows(ctx context.Context, first uint64, payloads []string) error {
	if len(payloads) == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS load_events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		return err
	}
	const rowsPerStatement = 1_000
	for offset := 0; offset < len(payloads); offset += rowsPerStatement {
		end := min(offset+rowsPerStatement, len(payloads))
		var query strings.Builder
		query.WriteString(`INSERT INTO load_events(id,payload) VALUES `)
		args := make([]any, 0, (end-offset)*2)
		for index := offset; index < end; index++ {
			if index > offset {
				query.WriteByte(',')
			}
			query.WriteString(`(?,?)`)
			args = append(args, first+uint64(index), payloads[index])
		}
		if _, err = tx.ExecContext(ctx, query.String(), args...); err != nil {
			return fmt.Errorf("load rows: %w", err)
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) LoadRowCount(ctx context.Context) (uint64, error) {
	var count uint64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM load_events`).Scan(&count)
	return count, err
}
