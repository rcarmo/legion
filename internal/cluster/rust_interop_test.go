//go:build rustinterop

package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

const interopALPN = "legion/interop/1"

type rustReady struct {
	EndpointID string   `json:"endpoint_id"`
	Addrs      []string `json:"addrs"`
	RelayURL   string   `json:"relay_url"`
	Transport  string   `json:"transport"`
}

type rustPeer struct {
	ready rustReady
	cmd   *exec.Cmd
	stop  context.CancelFunc
}

func startRustPeer(t *testing.T, transport string) *rustPeer {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "target", "debug", "legion-interop-fixture")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("Rust fixture missing (%s): run make go-rust-interop", bin)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, transport)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	peer := &rustPeer{cmd: cmd, stop: cancel}
	t.Cleanup(func() { peer.stop(); _ = peer.cmd.Wait() })
	lines := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "LEGION_INTEROP_READY ") {
				lines <- strings.TrimPrefix(scanner.Text(), "LEGION_INTEROP_READY ")
				return
			}
		}
		close(lines)
	}()
	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatal("Rust fixture exited before readiness")
		}
		if err := json.Unmarshal([]byte(line), &peer.ready); err != nil {
			t.Fatalf("decode Rust readiness %q: %v", line, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Rust fixture did not become ready")
	}
	return peer
}

func endpointAddr(t *testing.T, ready rustReady, relayOnly bool) netaddr.EndpointAddr {
	t.Helper()
	id, err := key.ParseEndpointID(ready.EndpointID)
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(id)
	if ready.RelayURL != "" {
		u, err := netaddr.ParseRelayURL(ready.RelayURL)
		if err != nil {
			t.Fatal(err)
		}
		addr = addr.WithRelayURL(u)
	}
	if !relayOnly {
		for _, value := range ready.Addrs {
			ap, err := netip.ParseAddrPort(value)
			if err != nil {
				t.Fatal(err)
			}
			addr = addr.WithIP(ap)
		}
	}
	return addr
}

func rustEcho(t *testing.T, transport string) {
	t.Helper()
	peer := startRustPeer(t, transport)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts := []iroh.Option{iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0"))}
	relayOnly := transport == "relay"
	if relayOnly {
		opts = []iroh.Option{iroh.WithRelayMode(relay.ModeDefault()), iroh.WithoutIPTransports()}
	}
	client, err := iroh.Bind(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	if relayOnly {
		if err = client.Online(ctx); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := client.Connect(ctx, endpointAddr(t, peer.ready, relayOnly), interopALPN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("hello from Go")
	if _, err = stream.Write(want); err != nil {
		t.Fatal(err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stream)
	if err != nil || string(got) != string(want) {
		t.Fatalf("echo=%q err=%v", got, err)
	}
	selectedRelayed := false
	for _, path := range conn.Paths() {
		if path.Selected {
			selectedRelayed = path.Relayed
		}
	}
	if selectedRelayed != relayOnly {
		t.Fatalf("transport=%s selectedRelayed=%t paths=%+v", transport, selectedRelayed, conn.Paths())
	}
	t.Logf("mixed Rust/Go %s echo verified: rust=%s go=%s paths=%s", transport, peer.ready.EndpointID, client.ID(), fmt.Sprint(conn.Paths()))
}

func TestRustGoDirectInterop(t *testing.T) { rustEcho(t, "direct") }
func TestRustGoRelayInterop(t *testing.T)  { rustEcho(t, "relay") }
