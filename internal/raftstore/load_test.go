//go:build loadtest

package raftstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

const (
	loadInserts         = 25_000
	loadBatchSize       = 500
	minInsertsPerSecond = 24_500.0
)

// TestThreeNodeReplicatedBatchLoad is an opt-in production capacity gate. It
// measures a typed Legion Raft command applied transactionally to pure-Go
// SQLite on every voter, matching the Rust hiqlite batch-insert workload.
func TestThreeNodeReplicatedBatchLoad(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	nodes := map[string]*Store{}
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 2 * time.Second
	config.ElectionTimeout = 2 * time.Second
	config.LeaderLeaseTimeout = time.Second
	config.CommitTimeout = 10 * time.Millisecond
	config.SnapshotThreshold = 100_000
	for index := 1; index <= 3; index++ {
		id := fmt.Sprint(index)
		var err error
		nodes[id], err = Open(Config{NodeID: id, DataDir: root + "/node-" + id, BindAddr: reserveAddress(t), Bootstrap: id == "1", ApplyTimeout: 30 * time.Second, RaftConfig: config})
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Close()
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
	payloads := make([]string, loadBatchSize)
	for index := range payloads {
		payloads[index] = "0123456789abcdef0123456789abcdef"
	}
	started := time.Now()
	for base := 0; base < loadInserts; base += loadBatchSize {
		if err := leader.appendLoadBatch(ctx, uint64(base), payloads); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	rate := loadInserts / elapsed.Seconds()
	t.Logf("Go Legion replicated load: %d inserts in %.3fs = %.0f inserts/s", loadInserts, elapsed.Seconds(), rate)
	if rate < minInsertsPerSecond {
		t.Fatalf("%.0f inserts/s is below %.0f target", rate, minInsertsPerSecond)
	}
	if err := leader.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		allApplied := true
		for _, node := range nodes {
			count, err := node.materialized.LoadRowCount(ctx)
			if err != nil || count != loadInserts {
				allApplied = false
				break
			}
		}
		if allApplied {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("followers did not apply all load rows")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for nodeID, node := range nodes {
		count, err := node.materialized.LoadRowCount(ctx)
		if err != nil || count != loadInserts {
			t.Fatalf("node %s rows=%d err=%v", nodeID, count, err)
		}
	}
}
