package raftstore

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/rcarmo/legion/internal/core"
)

func testRaftConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 200 * time.Millisecond
	config.ElectionTimeout = 200 * time.Millisecond
	config.LeaderLeaseTimeout = 100 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	config.SnapshotThreshold = 4
	config.SnapshotInterval = 100 * time.Millisecond
	return config
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func openTestStore(t *testing.T, id, dir, address string, bootstrap bool) *Store {
	t.Helper()
	store, err := Open(Config{NodeID: id, DataDir: dir, BindAddr: address, Bootstrap: bootstrap, ApplyTimeout: 5 * time.Second, RaftConfig: testRaftConfig()})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func waitLeader(t *testing.T, nodes map[string]*Store, excluded string) *Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range nodes {
			if id != excluded && node != nil && node.State() == raft.Leader {
				return node
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("cluster did not elect leader")
	return nil
}

func waitVoters(t *testing.T, leader *Store, count int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		future := leader.raft.GetConfiguration()
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
	t.Fatalf("cluster did not reach %d voters", count)
}

func waitStatus(t *testing.T, node *Store, id core.RunID, want core.SessionStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := node.materialized.SessionStatus(context.Background(), id)
		if err == nil && status == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %s did not materialize status %#v", node.NodeID(), want)
}

func TestThreeNodeSessionSurvivesLeaderKillAndRejoin(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dirs := map[string]string{}
	addresses := map[string]string{}
	nodes := map[string]*Store{}
	for index := 1; index <= 3; index++ {
		id := fmt.Sprint(index)
		dirs[id] = root + "/node-" + id
		addresses[id] = reserveAddress(t)
		nodes[id] = openTestStore(t, id, dirs[id], addresses[id], id == "1")
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	}()
	leader := waitLeader(t, nodes, "")
	for _, id := range []string{"2", "3"} {
		if err := leader.AddNonvoter(id, string(nodes[id].Address())); err != nil {
			t.Fatal(err)
		}
		if err := leader.Promote(id, string(nodes[id].Address())); err != nil {
			t.Fatal(err)
		}
	}
	waitVoters(t, leader, 3)
	runID := uuid.New()
	if err := leader.CreateSession(ctx, runID, core.RunConfig{Model: "faux/failover"}); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Append(ctx, runID, core.NewUserMessage("before failover")); err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		waitStatus(t, node, runID, core.StatusIdle)
	}

	deadID := string(leader.NodeID())
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	nodes[deadID] = nil
	leader = waitLeader(t, nodes, deadID)
	if err := leader.SetStatus(ctx, runID, core.StatusCompleted); err != nil {
		t.Fatal(err)
	}
	for id, node := range nodes {
		if id != deadID {
			waitStatus(t, node, runID, core.StatusCompleted)
		}
	}

	nodes[deadID] = openTestStore(t, deadID, dirs[deadID], addresses[deadID], false)
	waitStatus(t, nodes[deadID], runID, core.StatusCompleted)
	log, err := nodes[deadID].materialized.ReadLog(ctx, runID)
	if err != nil || len(log) != 1 {
		t.Fatalf("rejoined log=%#v err=%v", log, err)
	}
}

func TestNotificationsReportAppliedCommands(t *testing.T) {
	node := openTestStore(t, "1", t.TempDir(), reserveAddress(t), true)
	defer node.Close()
	waitLeader(t, map[string]*Store{"1": node}, "")
	id := uuid.New()
	if err := node.CreateSession(context.Background(), id, core.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	select {
	case notification := <-node.Notifications():
		if notification.Type != string(commandCreate) || notification.RunID != id {
			t.Fatalf("notification=%#v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("no local notification")
	}
}
