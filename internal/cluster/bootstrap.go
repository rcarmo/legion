package cluster

import (
	"context"
	"sort"
	"time"
)

type BootstrapKind string

const (
	Bootstrap BootstrapKind = "bootstrap"
	Join      BootstrapKind = "join"
)

type BootstrapOutcome struct {
	Kind         BootstrapKind
	EndpointID   string
	Peers        []DiscoveredPeer
	Registration *BonjourRegistration
}

func RunBootstrap(ctx context.Context, node *Node, raft RaftAdvertisement, window time.Duration, version string) (BootstrapOutcome, error) {
	id := node.ID().String()
	if !node.Config.MDNS {
		return BootstrapOutcome{Kind: Bootstrap, EndpointID: id}, nil
	}
	ip := LocalIP()
	registration, err := RegisterBonjour(id, Hostname(node.ShortID()), ip, node.Config.APIPort, version, raft)
	if err != nil {
		return BootstrapOutcome{}, err
	}
	peers, err := BrowseBonjour(ctx, window, id)
	if err != nil {
		registration.Close()
		return BootstrapOutcome{}, err
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].EndpointID < peers[j].EndpointID })
	kind := Bootstrap
	// Simultaneous first starts all see one another. Elect the lexicographically
	// smallest stable endpoint ID as the sole bootstrap node; all others join it.
	if len(peers) > 0 && peers[0].EndpointID < id {
		kind = Join
	}
	return BootstrapOutcome{Kind: kind, EndpointID: id, Peers: peers, Registration: registration}, nil
}
