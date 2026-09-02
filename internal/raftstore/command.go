package raftstore

import (
	"encoding/json"

	"github.com/rcarmo/legion/internal/core"
)

const commandVersion uint16 = 1

type commandType string

const (
	commandCreate      commandType = "create_session"
	commandAppend      commandType = "append_envelope"
	commandAppendBatch commandType = "append_envelopes"
	commandLoadBatch   commandType = "load_rows"
	commandStatus      commandType = "set_status"
	commandFork        commandType = "fork_session"
)

type command struct {
	Version   uint16              `json:"version"`
	Type      commandType         `json:"type"`
	RunID     core.RunID          `json:"run_id"`
	ChildID   *core.RunID         `json:"child_id,omitempty"`
	AtSeq     *core.SeqNum        `json:"at_seq,omitempty"`
	Config    *core.RunConfig     `json:"config,omitempty"`
	Event     *core.TurnEvent     `json:"event,omitempty"`
	Events    []core.TurnEvent    `json:"events,omitempty"`
	LoadFirst uint64              `json:"load_first,omitempty"`
	LoadRows  []string            `json:"load_rows,omitempty"`
	Status    *core.SessionStatus `json:"status,omitempty"`
	Timestamp int64               `json:"timestamp"`
}

type applyResult struct {
	Seq   core.SeqNum
	RunID core.RunID
	Err   error
}

func encodeCommand(value command) ([]byte, error) { return json.Marshal(value) }
