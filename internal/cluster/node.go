// Package cluster provides Legion's go-iroh transport and LAN discovery.
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/relay"
)

type NodeConfig struct {
	DataDir  string
	BindAddr string
	APIPort  uint16
	MDNS     bool
	Relay    bool
}

type NodeIdentity struct {
	SecretKey key.SecretKey
	ShortID   string
}

func LoadOrGenerateIdentity(dataDir string) (NodeIdentity, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return NodeIdentity{}, err
	}
	path := filepath.Join(dataDir, "node.key")
	encoded, err := os.ReadFile(path)
	var secret key.SecretKey
	if err == nil {
		secret, err = key.SecretKeyFromSlice(encoded)
	} else if os.IsNotExist(err) {
		secret, err = key.GenerateSecretKey()
		if err == nil {
			bytes := secret.Bytes()
			err = os.WriteFile(path, bytes[:], 0o600)
		}
	}
	if err != nil {
		return NodeIdentity{}, err
	}
	id := secret.Public().EndpointID().String()
	return NodeIdentity{SecretKey: secret, ShortID: id[:8]}, nil
}

func LoadOrGenerateRaftID(dataDir string) (uint64, error) {
	path := filepath.Join(dataDir, "raft.id")
	encoded, err := os.ReadFile(path)
	if err == nil {
		if len(encoded) != 8 {
			return 0, fmt.Errorf("invalid raft id file: expected 8 bytes")
		}
		id := binary.BigEndian.Uint64(encoded)
		if id == 0 {
			return 0, fmt.Errorf("invalid raft id file: zero id")
		}
		return id, nil
	}
	if !os.IsNotExist(err) {
		return 0, err
	}
	encoded = make([]byte, 8)
	for binary.BigEndian.Uint64(encoded) == 0 {
		if _, err = rand.Read(encoded); err != nil {
			return 0, err
		}
	}
	if err = os.WriteFile(path, encoded, 0o600); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(encoded), nil
}

type Node struct {
	Identity  NodeIdentity
	Endpoint  *iroh.Endpoint
	Config    NodeConfig
	discovery *mdns.Discovery
	cancel    context.CancelFunc
}

func StartNode(ctx context.Context, config NodeConfig) (*Node, error) {
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if config.BindAddr == "" {
		config.BindAddr = "[::]:0"
	}
	bind, err := netip.ParseAddrPort(config.BindAddr)
	if err != nil {
		return nil, err
	}
	identity, err := LoadOrGenerateIdentity(config.DataDir)
	if err != nil {
		return nil, err
	}
	var lookup iroh.AddressLookupServices
	var discovery *mdns.Discovery
	if config.MDNS {
		discovery = mdns.New(identity.SecretKey.Public().EndpointID())
		lookup.AddPublisher(discovery)
		lookup.AddResolver(discovery)
	}
	opts := []iroh.Option{iroh.WithSecretKey(identity.SecretKey), iroh.WithBindAddr(bind)}
	if config.MDNS {
		opts = append(opts, iroh.WithAddressLookup(&lookup))
	}
	if config.Relay {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()), iroh.WithNetReport())
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	node := &Node{Identity: identity, Endpoint: ep, Config: config, discovery: discovery, cancel: cancel}
	if discovery != nil {
		go discovery.Start(runCtx)
		addr := ep.Addr()
		discovery.Publish(dns.NewEndpointData(addr.Addrs()...))
	}
	return node, nil
}
func (n *Node) ID() key.EndpointID              { return n.Endpoint.ID() }
func (n *Node) ShortID() string                 { return n.Identity.ShortID }
func (n *Node) Close(ctx context.Context) error { n.cancel(); return n.Endpoint.Shutdown(ctx) }
