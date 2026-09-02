//go:build rustinterop

package namespace

import (
	"bufio"
	"context"
	"encoding/json"
	"net/netip"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

type rustNinePReady struct {
	EndpointID string   `json:"endpoint_id"`
	Addrs      []string `json:"addrs"`
}

func TestRustGoNinePInterop(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	cmd := exec.Command(filepath.Join(root, "target", "debug", "legion-ninep-interop-fixture"), "server")
	cmd.Env = append(cmd.Environ(), "LEGION_NAMESPACE_CAPABILITY=rust-go-ninep-secret")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	var ready rustNinePReady
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "LEGION_NINEP_READY ") {
			if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "LEGION_NINEP_READY ")), &ready); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if ready.EndpointID == "" {
		t.Fatal("fixture did not become ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ep, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Shutdown(context.Background())
	id, err := key.ParseEndpointID(ready.EndpointID)
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(id)
	for _, value := range ready.Addrs {
		addr = addr.WithIP(netip.MustParseAddrPort(value))
	}
	if client, dialErr := DialIroh(ctx, ep, addr, ""); dialErr == nil {
		client.Close()
		t.Fatal("Rust endpoint accepted missing 9P capability")
	}
	if client, dialErr := DialIroh(ctx, ep, addr, "wrong"); dialErr == nil {
		client.Close()
		t.Fatal("Rust endpoint accepted wrong 9P capability")
	}
	client, err := DialIroh(ctx, ep, addr, "rust-go-ninep-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	got, err := client.Read("/cluster/health")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"rust":true}` {
		t.Fatalf("got %s", got)
	}
}
