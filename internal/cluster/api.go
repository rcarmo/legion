package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/raftstore"
)

type Directory struct {
	mu        sync.RWMutex
	apiByRaft map[string]string
}

func NewDirectory() *Directory { return &Directory{apiByRaft: map[string]string{}} }
func apiURL(apiAddr string) string {
	if !strings.Contains(apiAddr, "://") {
		apiAddr = "http://" + apiAddr
	}
	return strings.TrimSuffix(apiAddr, "/")
}
func (d *Directory) Add(raftAddr, apiAddr string) {
	if raftAddr == "" || apiAddr == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.apiByRaft[raftAddr] = apiURL(apiAddr)
}
func (d *Directory) URL(raftAddr string) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	value, ok := d.apiByRaft[raftAddr]
	return value, ok
}
func (d *Directory) AddPeers(peers []DiscoveredPeer) {
	for _, peer := range peers {
		d.Add(peer.RaftAddr, peer.RaftAPIAddr)
	}
}

type rpcRequest struct {
	Op      string             `json:"op"`
	RunID   core.RunID         `json:"run_id"`
	Config  core.RunConfig     `json:"config"`
	Event   core.TurnEvent     `json:"event"`
	Status  core.SessionStatus `json:"status"`
	At      core.SeqNum        `json:"at"`
	N       int                `json:"n"`
	Filter  core.SessionFilter `json:"filter"`
	NodeID  uint64             `json:"node_id"`
	Address string             `json:"address"`
}
type rpcResponse struct {
	Seq      core.SeqNum           `json:"seq,omitempty"`
	RunID    core.RunID            `json:"run_id,omitempty"`
	Log      []core.TurnEnvelope   `json:"log,omitempty"`
	Status   core.SessionStatus    `json:"status,omitempty"`
	Sessions []core.SessionSummary `json:"sessions,omitempty"`
	Leader   string                `json:"leader,omitempty"`
	Error    string                `json:"error,omitempty"`
}

type ControlServer struct{ Store *raftstore.Store }

func (s ControlServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/join", s.handle)
	mux.HandleFunc("POST /store", s.handle)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}
func (s ControlServer) handle(w http.ResponseWriter, r *http.Request) {
	var request rpcRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil {
		writeRPC(w, http.StatusBadRequest, rpcResponse{Error: err.Error()})
		return
	}
	if s.Store.State() != raft.Leader {
		leader, _ := s.Store.LeaderWithID()
		writeRPC(w, http.StatusConflict, rpcResponse{Error: "not leader", Leader: string(leader)})
		return
	}
	ctx := r.Context()
	response := rpcResponse{}
	var err error
	switch request.Op {
	case "join":
		err = JoinNode(s.Store, request.NodeID, request.Address)
	case "create":
		err = s.Store.CreateSession(ctx, request.RunID, request.Config)
	case "append":
		response.Seq, err = s.Store.Append(ctx, request.RunID, request.Event)
	case "read_log":
		response.Log, err = s.Store.ReadLog(ctx, request.RunID)
	case "read_recent":
		response.Log, err = s.Store.ReadRecent(ctx, request.RunID, request.N)
	case "status":
		response.Status, err = s.Store.SessionStatus(ctx, request.RunID)
	case "set_status":
		err = s.Store.SetStatus(ctx, request.RunID, request.Status)
	case "fork":
		response.RunID, err = s.Store.Fork(ctx, request.RunID, request.At)
	case "list":
		response.Sessions, err = s.Store.ListSessions(ctx, request.Filter)
	default:
		err = fmt.Errorf("unknown operation %q", request.Op)
	}
	if err != nil {
		response.Error = err.Error()
		writeRPC(w, http.StatusUnprocessableEntity, response)
		return
	}
	writeRPC(w, http.StatusOK, response)
}
func writeRPC(w http.ResponseWriter, status int, response rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

type RoutedStore struct {
	Local     *raftstore.Store
	Directory *Directory
	Client    *http.Client
}

func NewRoutedStore(local *raftstore.Store, directory *Directory) *RoutedStore {
	return &RoutedStore{Local: local, Directory: directory, Client: &http.Client{Timeout: 10 * time.Second}}
}
func (s *RoutedStore) call(ctx context.Context, path string, request rpcRequest) (rpcResponse, error) {
	if s.Local.State() == raft.Leader {
		return rpcResponse{}, errors.New("routed call attempted on leader")
	}
	leader, _ := s.Local.LeaderWithID()
	base, ok := s.Directory.URL(string(leader))
	if !ok {
		return rpcResponse{}, fmt.Errorf("leader API for Raft address %q is unknown", leader)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return rpcResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(encoded))
	if err != nil {
		return rpcResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := s.Client.Do(req)
	if err != nil {
		return rpcResponse{}, err
	}
	defer response.Body.Close()
	var result rpcResponse
	if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
		return rpcResponse{}, err
	}
	if response.StatusCode/100 != 2 {
		return result, fmt.Errorf("cluster RPC: %s", result.Error)
	}
	return result, nil
}
func (s *RoutedStore) Join(ctx context.Context, nodeID uint64, address, targetAPI string) error {
	request := rpcRequest{Op: "join", NodeID: nodeID, Address: address}
	encoded, _ := json.Marshal(request)
	var last error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL(targetAPI)+"/raft/join", bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := s.Client.Do(req)
		if err == nil {
			var result rpcResponse
			decodeErr := json.NewDecoder(response.Body).Decode(&result)
			_ = response.Body.Close()
			if response.StatusCode/100 == 2 {
				return nil
			}
			if decodeErr == nil {
				last = fmt.Errorf("join: %s", result.Error)
			} else {
				last = decodeErr
			}
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func (s *RoutedStore) CreateSession(ctx context.Context, id core.RunID, c core.RunConfig) error {
	if s.Local.State() == raft.Leader {
		return s.Local.CreateSession(ctx, id, c)
	}
	_, err := s.call(ctx, "/store", rpcRequest{Op: "create", RunID: id, Config: c})
	return err
}
func (s *RoutedStore) Append(ctx context.Context, id core.RunID, event core.TurnEvent) (core.SeqNum, error) {
	if s.Local.State() == raft.Leader {
		return s.Local.Append(ctx, id, event)
	}
	r, e := s.call(ctx, "/store", rpcRequest{Op: "append", RunID: id, Event: event})
	return r.Seq, e
}
func (s *RoutedStore) ReadLog(ctx context.Context, id core.RunID) ([]core.TurnEnvelope, error) {
	if s.Local.State() == raft.Leader {
		return s.Local.ReadLog(ctx, id)
	}
	r, e := s.call(ctx, "/store", rpcRequest{Op: "read_log", RunID: id})
	return r.Log, e
}
func (s *RoutedStore) ReadRecent(ctx context.Context, id core.RunID, n int) ([]core.TurnEnvelope, error) {
	if s.Local.State() == raft.Leader {
		return s.Local.ReadRecent(ctx, id, n)
	}
	r, e := s.call(ctx, "/store", rpcRequest{Op: "read_recent", RunID: id, N: n})
	return r.Log, e
}
func (s *RoutedStore) SessionStatus(ctx context.Context, id core.RunID) (core.SessionStatus, error) {
	if s.Local.State() == raft.Leader {
		return s.Local.SessionStatus(ctx, id)
	}
	r, e := s.call(ctx, "/store", rpcRequest{Op: "status", RunID: id})
	return r.Status, e
}
func (s *RoutedStore) SetStatus(ctx context.Context, id core.RunID, status core.SessionStatus) error {
	if s.Local.State() == raft.Leader {
		return s.Local.SetStatus(ctx, id, status)
	}
	_, e := s.call(ctx, "/store", rpcRequest{Op: "set_status", RunID: id, Status: status})
	return e
}
func (s *RoutedStore) Fork(ctx context.Context, id core.RunID, at core.SeqNum) (core.RunID, error) {
	if s.Local.State() == raft.Leader {
		return s.Local.Fork(ctx, id, at)
	}
	r, e := s.call(ctx, "/store", rpcRequest{Op: "fork", RunID: id, At: at})
	return r.RunID, e
}
func (s *RoutedStore) ListSessions(ctx context.Context, filter core.SessionFilter) ([]core.SessionSummary, error) {
	if s.Local.State() == raft.Leader {
		return s.Local.ListSessions(ctx, filter)
	}
	r, e := s.call(ctx, "/store", rpcRequest{Op: "list", Filter: filter})
	return r.Sessions, e
}

var _ core.EventStore = (*RoutedStore)(nil)
