package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"lukechampine.com/blake3"
)

type NodePresence struct {
	EndpointID string `json:"endpoint_id"`
	ShortID    string `json:"short_id"`
	APIPort    uint16 `json:"api_port"`
	Timestamp  int64  `json:"timestamp"`
}

func ClusterTopic() gossip.TopicID {
	sum := blake3.Sum256([]byte("legion-cluster-v1"))
	return gossip.TopicID(sum)
}

type Membership struct {
	Gossip *gossip.Gossip
	Topic  *gossip.Topic
	Router *iroh.Router
	cancel context.CancelFunc
}

func StartMembership(ctx context.Context, node *Node, bootstrap []netaddr.EndpointAddr, heartbeat time.Duration, onJoined func(NodePresence), onLeft func(string)) (*Membership, error) {
	return StartMembershipWithProtocols(ctx, node, bootstrap, heartbeat, onJoined, onLeft, nil)
}

// StartMembershipWithProtocols serves gossip and application protocols through
// one router on the node's shared iroh endpoint.
func StartMembershipWithProtocols(ctx context.Context, node *Node, bootstrap []netaddr.EndpointAddr, heartbeat time.Duration, onJoined func(NodePresence), onLeft func(string), protocols map[string]iroh.ProtocolHandler) (*Membership, error) {
	g := gossip.NewGossip(node.Endpoint)
	handlers := make(map[string]iroh.ProtocolHandler, len(protocols)+1)
	for alpn, handler := range protocols {
		handlers[alpn] = handler
	}
	handlers[gossip.ALPN] = g.Handler()
	router, err := iroh.NewRouter(node.Endpoint, handlers, nil)
	if err != nil {
		return nil, err
	}
	topic, err := g.Subscribe(ctx, ClusterTopic(), bootstrap)
	if err != nil {
		router.Shutdown(ctx)
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	handle := &Membership{Gossip: g, Topic: topic, Router: router, cancel: cancel}
	go func() {
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case now := <-ticker.C:
				presence := NodePresence{EndpointID: node.ID().String(), ShortID: node.ShortID(), APIPort: node.Config.APIPort, Timestamp: now.UnixMilli()}
				if encoded, e := json.Marshal(presence); e == nil {
					_ = topic.Broadcast(runCtx, encoded)
				}
			}
		}
	}()
	go func() {
		for event, eventErr := range topic.Events() {
			if eventErr != nil {
				continue
			}
			switch event.Kind {
			case gossip.Received:
				var p NodePresence
				if json.Unmarshal(event.Content, &p) == nil && p.EndpointID != node.ID().String() && onJoined != nil {
					onJoined(p)
				}
			case gossip.NeighborDown:
				if onLeft != nil {
					onLeft(event.Peer.String())
				}
			}
		}
	}()
	return handle, nil
}
func (m *Membership) Close(ctx context.Context) error {
	m.cancel()
	err := m.Topic.Close()
	m.Gossip.Shutdown(ctx)
	m.Router.Shutdown(ctx)
	return err
}
