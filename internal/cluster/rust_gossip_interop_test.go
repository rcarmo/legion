//go:build rustinterop

package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

type gossipFixtureEvent struct {
	Kind       string   `json:"kind"`
	EndpointID string   `json:"endpoint_id"`
	Peer       string   `json:"peer"`
	Content    string   `json:"content"`
	Addrs      []string `json:"addrs"`
}

func TestRustGoGossipPresenceInterop(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "target", "debug", "legion-gossip-interop-fixture")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()
	events := make(chan gossipFixtureEvent, 16)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var event gossipFixtureEvent
			if json.Unmarshal(scanner.Bytes(), &event) == nil {
				events <- event
			}
		}
	}()
	var ready gossipFixtureEvent
	select {
	case ready = <-events:
		if ready.Kind != "Ready" {
			t.Fatalf("first Rust event=%#v", ready)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	rustID, err := key.ParseEndpointID(ready.EndpointID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.Addrs) == 0 {
		t.Fatal("Rust gossip fixture has no direct address")
	}
	ap, err := netip.ParseAddrPort(ready.Addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	ep, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	goGossip := gossip.NewGossip(ep)
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{gossip.ALPN: goGossip.Handler()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Shutdown(context.Background())
	topic, err := goGossip.SubscribeAndJoin(ctx, ClusterTopic(), []netaddr.EndpointAddr{netaddr.NewEndpointAddr(rustID).WithIP(ap)})
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Close()
	goPresence := NodePresence{EndpointID: ep.ID().String(), ShortID: ep.ID().String()[:8], APIPort: 8080, Timestamp: 123}
	encoded, _ := json.Marshal(goPresence)
	if err = topic.Broadcast(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event := <-events:
			if event.Kind == "Received" {
				var got NodePresence
				if json.Unmarshal([]byte(event.Content), &got) != nil || got != goPresence {
					t.Fatalf("Rust received incompatible presence %q", event.Content)
				}
				goto rustBroadcast
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
rustBroadcast:
	rustPresence := NodePresence{EndpointID: ready.EndpointID, ShortID: ready.EndpointID[:8], APIPort: 8081, Timestamp: 456}
	payload, _ := json.Marshal(rustPresence)
	command, _ := json.Marshal(map[string]string{"cmd": "Broadcast", "content": string(payload)})
	if _, err = fmt.Fprintf(stdin, "%s\n", command); err != nil {
		t.Fatal(err)
	}
	for event, eventErr := range topic.Events() {
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		if event.Kind == gossip.Received {
			var got NodePresence
			if json.Unmarshal(event.Content, &got) != nil || got != rustPresence {
				t.Fatalf("Go received incompatible presence %q", event.Content)
			}
			break
		}
	}
}
