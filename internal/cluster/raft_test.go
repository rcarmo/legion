package cluster

import "testing"

func TestRaftJoinTargetRequiresCompleteAdvertisement(t *testing.T) {
	id := uint64(2)
	outcome := BootstrapOutcome{Kind: Join, Peers: []DiscoveredPeer{{EndpointID: "a"}, {EndpointID: "b", RaftID: &id, RaftAddr: "127.0.0.1:1", RaftAPIAddr: "127.0.0.1:2"}}}
	peer, err := RaftJoinTarget(outcome)
	if err != nil || peer.EndpointID != "b" {
		t.Fatalf("peer=%#v err=%v", peer, err)
	}
	outcome.Peers[1].RaftAPIAddr = ""
	if _, err = RaftJoinTarget(outcome); err == nil {
		t.Fatal("incomplete advertisement accepted")
	}
}
