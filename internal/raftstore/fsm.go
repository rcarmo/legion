package raftstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/store"
)

type fsm struct {
	materialized *store.SQLiteStore
	notify       chan<- Notification
}

func (f *fsm) Apply(log *raft.Log) any {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return applyResult{Err: err}
	}
	if cmd.Version != commandVersion {
		return applyResult{Err: fmt.Errorf("unsupported raft command version %d", cmd.Version)}
	}
	ctx := context.Background()
	result := applyResult{RunID: cmd.RunID}
	switch cmd.Type {
	case commandCreate:
		if cmd.Config == nil {
			result.Err = fmt.Errorf("create command missing config")
			break
		}
		result.Err = f.materialized.ApplyCreate(ctx, cmd.RunID, *cmd.Config, cmd.Timestamp)
	case commandAppend:
		if cmd.Event == nil {
			result.Err = fmt.Errorf("append command missing event")
			break
		}
		var log []core.TurnEnvelope
		log, result.Err = f.materialized.ReadLog(ctx, cmd.RunID)
		if result.Err != nil {
			break
		}
		result.Seq = core.SeqNum(len(log))
		var previous [32]byte
		if result.Seq > 0 {
			previous = core.HashEnvelope(log[len(log)-1])
		}
		envelope := core.TurnEnvelope{RunID: cmd.RunID, Seq: result.Seq, PrevHash: previous, Event: *cmd.Event, CreatedAt: cmd.Timestamp}
		result.Err = f.materialized.ApplyEnvelope(ctx, envelope)
	case commandStatus:
		if cmd.Status == nil {
			result.Err = fmt.Errorf("status command missing status")
			break
		}
		result.Err = f.materialized.ApplyStatus(ctx, cmd.RunID, *cmd.Status, cmd.Timestamp)
	case commandFork:
		if cmd.ChildID == nil || cmd.AtSeq == nil {
			result.Err = fmt.Errorf("fork command missing child or sequence")
			break
		}
		result.RunID = *cmd.ChildID
		result.Err = f.materialized.ApplyFork(ctx, cmd.RunID, *cmd.ChildID, *cmd.AtSeq, cmd.Timestamp)
	default:
		result.Err = fmt.Errorf("unknown raft command %q", cmd.Type)
	}
	if result.Err == nil && f.notify != nil {
		select {
		case f.notify <- Notification{Index: log.Index, Type: string(cmd.Type), RunID: result.RunID}:
		default:
		}
	}
	return result
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	encoded, err := f.materialized.Snapshot(context.Background())
	if err != nil {
		return nil, err
	}
	return snapshot(encoded), nil
}

func (f *fsm) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	encoded, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return f.materialized.Restore(context.Background(), encoded)
}

type snapshot []byte

func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (snapshot) Release() {}
