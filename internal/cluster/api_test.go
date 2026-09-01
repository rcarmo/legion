package cluster

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/raftstore"
)

func reserveRaftAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}
func clusterRaftConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 200 * time.Millisecond
	config.ElectionTimeout = 200 * time.Millisecond
	config.LeaderLeaseTimeout = 100 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	return config
}
func openClusterStore(t *testing.T, id, address string, bootstrap bool) *raftstore.Store {
	t.Helper()
	value, err := raftstore.Open(raftstore.Config{NodeID: id, DataDir: t.TempDir(), BindAddr: address, Bootstrap: bootstrap, ApplyTimeout: 5 * time.Second, RaftConfig: clusterRaftConfig()})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func awaitLeader(t *testing.T, nodes map[string]*raftstore.Store) *raftstore.Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node != nil && node.State() == raft.Leader {
				return node
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}
func awaitVoters(t *testing.T, node *raftstore.Store, count int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		future := node.Raft().GetConfiguration()
		if future.Error() == nil {
			voters := 0
			for _, server := range future.Configuration().Servers {
				if server.Suffrage == raft.Voter {
					voters++
				}
			}
			if voters == count {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("did not reach %d voters", count)
}

func TestLeaderJoinAndFollowerForwardingAcrossFailover(t *testing.T) {
	ctx := context.Background()
	addresses := map[string]string{"1": reserveRaftAddress(t), "2": reserveRaftAddress(t), "3": reserveRaftAddress(t)}
	nodes := map[string]*raftstore.Store{}
	servers := map[string]*httptest.Server{}
	routed := map[string]*RoutedStore{}
	defer func() {
		for _, server := range servers {
			if server != nil {
				server.Close()
			}
		}
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	}()
	for index := 1; index <= 3; index++ {
		id := fmt.Sprint(index)
		nodes[id] = openClusterStore(t, id, addresses[id], index == 1)
		servers[id] = httptest.NewServer(ControlServer{Store: nodes[id]}.Handler())
	}
	leader := awaitLeader(t, nodes)
	leaderID := string(leader.NodeID())
	directory := NewDirectory()
	for id := range nodes {
		directory.Add(addresses[id], servers[id].URL)
		routed[id] = NewRoutedStore(nodes[id], directory)
	}
	for _, id := range []string{"2", "3"} {
		if err := routed[id].Join(ctx, uint64(id[0]-'0'), addresses[id], servers[leaderID].URL); err != nil {
			t.Fatal(err)
		}
	}
	awaitVoters(t, leader, 3)
	runID := uuid.New()
	if err := routed["2"].CreateSession(ctx, runID, core.RunConfig{Model: "faux/forwarded"}); err != nil {
		t.Fatal(err)
	}
	if _, err := routed["3"].Append(ctx, runID, core.NewUserMessage("through follower")); err != nil {
		t.Fatal(err)
	}
	log, err := routed["2"].ReadLog(ctx, runID)
	if err != nil || len(log) != 1 {
		t.Fatalf("log=%#v err=%v", log, err)
	}
	servers[leaderID].Close()
	servers[leaderID] = nil
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	nodes[leaderID] = nil
	newLeader := awaitLeader(t, nodes)
	if newLeader.NodeID() == leader.NodeID() {
		t.Fatal("leader did not change")
	}
	liveFollower := "2"
	if string(newLeader.NodeID()) == liveFollower {
		liveFollower = "3"
	}
	if err := routed[liveFollower].SetStatus(ctx, runID, core.StatusCompleted); err != nil {
		t.Fatal(err)
	}
	status, err := routed[liveFollower].SessionStatus(ctx, runID)
	if err != nil || status != core.StatusCompleted {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}
