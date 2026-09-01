package cluster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"lukechampine.com/blake3"
)

func TestRaftIDPersists(t *testing.T) {
	dir := t.TempDir()
	first, err := LoadOrGenerateRaftID(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrGenerateRaftID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first == 0 || first != second {
		t.Fatalf("raft ids %d %d", first, second)
	}
	encoded, err := os.ReadFile(dir + "/raft.id")
	if err != nil || len(encoded) != 8 {
		t.Fatalf("raft id bytes=%d err=%v", len(encoded), err)
	}
}

func TestIdentityPersistsRustCompatibleSeed(t *testing.T) {
	dir := t.TempDir()
	first, err := LoadOrGenerateIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(dir + "/node.key")
	if err != nil || len(encoded) != 32 {
		t.Fatalf("key bytes=%d err=%v", len(encoded), err)
	}
	second, err := LoadOrGenerateIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first.SecretKey.Public() != second.SecretKey.Public() || first.ShortID != second.ShortID {
		t.Fatal("identity changed across restart")
	}
}
func TestClusterTopicMatchesRustBLAKE3(t *testing.T) {
	want := blake3.Sum256([]byte("legion-cluster-v1"))
	got := ClusterTopic()
	if hex.EncodeToString(got[:]) != hex.EncodeToString(want[:]) {
		t.Fatalf("topic=%x want=%x", got, want)
	}
}
func TestNodePresenceRustJSONShape(t *testing.T) {
	encoded, err := json.Marshal(NodePresence{EndpointID: "abc", ShortID: "abc", APIPort: 8080, Timestamp: 123})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"endpoint_id":"abc","short_id":"abc","api_port":8080,"timestamp":123}`
	if string(encoded) != want {
		t.Fatalf("got %s want %s", encoded, want)
	}
}
func TestBonjourTXTAndWildcardResolution(t *testing.T) {
	props := parseTXT([]string{"node_id=abc", "version=0.1.0", "raft_id=7", "raft_addr=0.0.0.0:7000"})
	if props["node_id"] != "abc" || props["raft_id"] != "7" {
		t.Fatal(props)
	}
	if got := resolveAdvertised(props["raft_addr"], "192.0.2.10"); got != "192.0.2.10:7000" {
		t.Fatal(got)
	}
	if ServiceType != "_durable-fn._udp.local." {
		t.Fatal(ServiceType)
	}
}

func TestBonjourRegistrationAndBrowse(t *testing.T) {
	ctx := context.Background()
	id := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	registration, err := RegisterBonjour(id, "legion-test.local.", "127.0.0.1", 18080, "0.1.0", RaftAdvertisement{NodeID: 7, RaftAddr: "0.0.0.0:17000", APIAddr: "0.0.0.0:17001"})
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close()
	peers, err := BrowseBonjour(ctx, 750*time.Millisecond, "other")
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range peers {
		if peer.EndpointID == id {
			if peer.RaftID == nil || *peer.RaftID != 7 || peer.RaftAddr != "127.0.0.1:17000" || peer.RaftAPIAddr != "127.0.0.1:17001" {
				t.Fatalf("peer=%#v", peer)
			}
			return
		}
	}
	t.Fatalf("registered service not discovered: %#v", peers)
}

func TestSimultaneousBootstrapElectsLowestEndpoint(t *testing.T) {
	low := "1000"
	high := "2000"
	peers := []DiscoveredPeer{{EndpointID: low}}
	kind := Bootstrap
	if len(peers) > 0 && peers[0].EndpointID < high {
		kind = Join
	}
	if kind != Join {
		t.Fatal("higher endpoint did not join")
	}
	kind = Bootstrap
	peers = []DiscoveredPeer{{EndpointID: high}}
	if len(peers) > 0 && peers[0].EndpointID < low {
		kind = Join
	}
	if kind != Bootstrap {
		t.Fatal("lowest endpoint did not bootstrap")
	}
}

func TestGoIrohDirectALPNEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, err := StartNode(ctx, NodeConfig{DataDir: t.TempDir(), BindAddr: "[::1]:0"})
	if err != nil {
		t.Fatal(err)
	}
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return err
		}
		if _, err = io.Copy(stream, stream); err != nil {
			return err
		}
		return stream.Close()
	})
	router, err := iroh.NewRouter(server.Endpoint, map[string]iroh.ProtocolHandler{"legion/test/1": handler}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Shutdown(ctx)
	client, err := StartNode(ctx, NodeConfig{DataDir: t.TempDir(), BindAddr: "[::1]:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	address := netaddr.NewEndpointAddr(server.ID()).WithIP(server.Endpoint.LocalAddr())
	conn, err := client.Endpoint.Connect(ctx, address, "legion/test/1")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stream)
	if err != nil || string(got) != "hello" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestGoIrohGossipPresenceExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	server, err := StartNode(ctx, NodeConfig{DataDir: t.TempDir(), BindAddr: "[::1]:0", APIPort: 8080})
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan NodePresence, 1)
	serverMembership, err := StartMembership(ctx, server, nil, 20*time.Millisecond, func(p NodePresence) { joined <- p }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverMembership.Close(ctx)
	client, err := StartNode(ctx, NodeConfig{DataDir: t.TempDir(), BindAddr: "[::1]:0", APIPort: 8081})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := []netaddr.EndpointAddr{netaddr.NewEndpointAddr(server.ID()).WithIP(server.Endpoint.LocalAddr())}
	clientMembership, err := StartMembership(ctx, client, bootstrap, 20*time.Millisecond, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientMembership.Close(ctx)
	select {
	case presence := <-joined:
		if presence.EndpointID != client.ID().String() || presence.APIPort != 8081 {
			t.Fatalf("presence=%#v", presence)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestNodeRequiresParseableBindAddress(t *testing.T) {
	_, err := StartNode(context.Background(), NodeConfig{DataDir: t.TempDir(), BindAddr: "bad"})
	if err == nil {
		t.Fatal("bad bind accepted")
	}
	if _, err = netip.ParseAddrPort("[::1]:0"); err != nil {
		t.Fatal(err)
	}
}
