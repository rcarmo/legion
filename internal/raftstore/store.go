// Package raftstore replicates Legion EventStore commands with Hashicorp Raft.
package raftstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/store"
)

type Config struct {
	NodeID       string
	DataDir      string
	BindAddr     string
	Bootstrap    bool
	ApplyTimeout time.Duration
	RaftConfig   *raft.Config
}

type Notification struct {
	Index uint64
	Type  string
	RunID core.RunID
}

type Store struct {
	raft         *raft.Raft
	materialized *store.SQLiteStore
	transport    *raft.NetworkTransport
	bolt         io.Closer
	notify       chan Notification
	timeout      time.Duration
	nodeID       raft.ServerID
}

func Open(config Config) (*Store, error) {
	if config.NodeID == "" {
		return nil, fmt.Errorf("node id is required")
	}
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if config.BindAddr == "" {
		config.BindAddr = "127.0.0.1:0"
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = 10 * time.Second
	}
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		return nil, err
	}
	// SQLite is a derived query view. Rebuild it from the durable Raft
	// snapshot/log on every start so replay cannot double-apply commands.
	statePath := filepath.Join(config.DataDir, "state.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(statePath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	materialized, err := store.Open(statePath)
	if err != nil {
		return nil, err
	}
	if err = materialized.ConfigureDerived(context.Background()); err != nil {
		_ = materialized.Close()
		return nil, err
	}
	closeMaterialized := true
	defer func() {
		if closeMaterialized {
			_ = materialized.Close()
		}
	}()
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft.db"))
	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(config.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}
	address, err := net.ResolveTCPAddr("tcp", config.BindAddr)
	if err != nil {
		return nil, err
	}
	if address.Port == 0 {
		return nil, fmt.Errorf("raft bind address must use a stable non-zero port")
	}
	transport, err := raft.NewTCPTransport(config.BindAddr, address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	raftConfig := config.RaftConfig
	if raftConfig == nil {
		raftConfig = raft.DefaultConfig()
	} else {
		raftConfig = cloneRaftConfig(raftConfig)
	}
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	notify := make(chan Notification, 256)
	r, err := raft.NewRaft(raftConfig, &fsm{materialized: materialized, notify: notify}, bolt, bolt, snapshots, transport)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	result := &Store{raft: r, materialized: materialized, transport: transport, bolt: bolt, notify: notify, timeout: config.ApplyTimeout, nodeID: raftConfig.LocalID}
	closeMaterialized = false
	hasState, err := raft.HasExistingState(bolt, bolt, snapshots)
	if err != nil {
		result.Close()
		return nil, err
	}
	if config.Bootstrap && !hasState {
		future := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: raftConfig.LocalID, Address: transport.LocalAddr(), Suffrage: raft.Voter}}})
		if err = future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			result.Close()
			return nil, err
		}
	}
	return result, nil
}

func cloneRaftConfig(value *raft.Config) *raft.Config              { copy := *value; return &copy }
func (s *Store) Address() raft.ServerAddress                       { return s.transport.LocalAddr() }
func (s *Store) NodeID() raft.ServerID                             { return s.nodeID }
func (s *Store) LeaderWithID() (raft.ServerAddress, raft.ServerID) { return s.raft.LeaderWithID() }
func (s *Store) State() raft.RaftState                             { return s.raft.State() }
func (s *Store) Notifications() <-chan Notification                { return s.notify }
func (s *Store) Raft() *raft.Raft                                  { return s.raft }

func (s *Store) Close() error {
	var errs []error
	if s.raft != nil {
		if err := s.raft.Shutdown().Error(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.transport != nil {
		if err := s.transport.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.bolt != nil {
		if err := s.bolt.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.materialized != nil {
		if err := s.materialized.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Store) apply(ctx context.Context, command command) (applyResult, error) {
	encoded, err := encodeCommand(command)
	if err != nil {
		return applyResult{}, err
	}
	timeout := s.timeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	future := s.raft.Apply(encoded, timeout)
	if err = future.Error(); err != nil {
		return applyResult{}, err
	}
	result, ok := future.Response().(applyResult)
	if !ok {
		return applyResult{}, fmt.Errorf("unexpected raft apply response %T", future.Response())
	}
	return result, result.Err
}

func (s *Store) Barrier(ctx context.Context) error {
	timeout := s.timeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	return s.raft.Barrier(timeout).Error()
}
func (s *Store) VerifyLeader() error { return s.raft.VerifyLeader().Error() }
func (s *Store) AddNonvoter(id, address string) error {
	return s.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(address), 0, s.timeout).Error()
}
func (s *Store) Promote(id, address string) error {
	return s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, s.timeout).Error()
}
func (s *Store) RemoveServer(id string) error {
	return s.raft.RemoveServer(raft.ServerID(id), 0, s.timeout).Error()
}
func (s *Store) Snapshot() error { return s.raft.Snapshot().Error() }

func (s *Store) CreateSession(ctx context.Context, id core.RunID, config core.RunConfig) error {
	_, err := s.apply(ctx, command{Version: commandVersion, Type: commandCreate, RunID: id, Config: &config, Timestamp: core.NowMS()})
	return err
}
func (s *Store) Append(ctx context.Context, id core.RunID, event core.TurnEvent) (core.SeqNum, error) {
	result, err := s.apply(ctx, command{Version: commandVersion, Type: commandAppend, RunID: id, Event: &event, Timestamp: core.NowMS()})
	return result.Seq, err
}

// AppendBatch commits multiple typed events through one Raft entry and one
// SQLite transaction while preserving the ordinary per-session hash chain.
func (s *Store) AppendBatch(ctx context.Context, id core.RunID, events []core.TurnEvent) (core.SeqNum, error) {
	if len(events) == 0 {
		return 0, fmt.Errorf("append batch requires at least one event")
	}
	result, err := s.apply(ctx, command{Version: commandVersion, Type: commandAppendBatch, RunID: id, Events: append([]core.TurnEvent(nil), events...), Timestamp: core.NowMS()})
	return result.Seq, err
}
func (s *Store) appendLoadBatch(ctx context.Context, first uint64, rows []string) error {
	if len(rows) == 0 {
		return fmt.Errorf("load batch requires at least one row")
	}
	_, err := s.apply(ctx, command{Version: commandVersion, Type: commandLoadBatch, LoadFirst: first, LoadRows: append([]string(nil), rows...), Timestamp: core.NowMS()})
	return err
}
func (s *Store) ReadLog(ctx context.Context, id core.RunID) ([]core.TurnEnvelope, error) {
	if err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	return s.materialized.ReadLog(ctx, id)
}
func (s *Store) ReadRecent(ctx context.Context, id core.RunID, n int) ([]core.TurnEnvelope, error) {
	if err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	return s.materialized.ReadRecent(ctx, id, n)
}
func (s *Store) SessionStatus(ctx context.Context, id core.RunID) (core.SessionStatus, error) {
	if err := s.Barrier(ctx); err != nil {
		return core.SessionStatus{}, err
	}
	return s.materialized.SessionStatus(ctx, id)
}
func (s *Store) SetStatus(ctx context.Context, id core.RunID, status core.SessionStatus) error {
	_, err := s.apply(ctx, command{Version: commandVersion, Type: commandStatus, RunID: id, Status: &status, Timestamp: core.NowMS()})
	return err
}
func (s *Store) Fork(ctx context.Context, id core.RunID, at core.SeqNum) (core.RunID, error) {
	child := uuid.New()
	result, err := s.apply(ctx, command{Version: commandVersion, Type: commandFork, RunID: id, ChildID: &child, AtSeq: &at, Timestamp: core.NowMS()})
	return result.RunID, err
}
func (s *Store) ListSessions(ctx context.Context, filter core.SessionFilter) ([]core.SessionSummary, error) {
	if err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	return s.materialized.ListSessions(ctx, filter)
}
