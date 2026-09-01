package cluster

import (
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/rcarmo/legion/internal/raftstore"
)

// RaftJoinTarget selects an existing peer that can service a join request.
func RaftJoinTarget(outcome BootstrapOutcome) (*DiscoveredPeer, error) {
	if outcome.Kind != Join {
		return nil, nil
	}
	for index := range outcome.Peers {
		peer := &outcome.Peers[index]
		if peer.RaftID != nil && peer.RaftAddr != "" && peer.RaftAPIAddr != "" {
			return peer, nil
		}
	}
	return nil, fmt.Errorf("discovered peers do not advertise complete Raft coordinates")
}

// JoinNode is called on the current leader by the startup join endpoint. A new
// server is first added as a nonvoter so it catches up before gaining a vote.
func JoinNode(leader *raftstore.Store, nodeID uint64, address string) error {
	id := strconv.FormatUint(nodeID, 10)
	configuration := leader.Raft().GetConfiguration()
	if err := configuration.Error(); err != nil {
		return err
	}
	for _, server := range configuration.Configuration().Servers {
		sameID := string(server.ID) == id
		sameAddress := string(server.Address) == address
		if sameID && sameAddress {
			if server.Suffrage == raft.Voter {
				return nil
			}
			return leader.Promote(id, address)
		}
		if sameID || sameAddress {
			if err := leader.RemoveServer(string(server.ID)); err != nil {
				return err
			}
		}
	}
	if err := leader.AddNonvoter(id, address); err != nil {
		return err
	}
	return leader.Promote(id, address)
}
