//go:build rustinterop

package deploy

import (
	"bufio"
	"context"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
)

func rustBlobBin(t *testing.T) string {
	t.Helper()
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	bin := filepath.Join(root, "target", "debug", "legion-blob-interop-fixture")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("missing %s; run make go-blob-interop", bin)
	}
	return bin
}
func TestRustServesGoFetchesBlob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, rustBlobBin(t), "serve", "rust-to-go")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	var ticket string
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "LEGION_BLOB_READY ") {
			ticket = strings.TrimPrefix(scanner.Text(), "LEGION_BLOB_READY ")
			break
		}
	}
	if ticket == "" {
		t.Fatal("no Rust ticket")
	}
	client, err := iroh.Bind(context.Background(), iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	callCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	got, err := FetchTicket(callCtx, client, ticket)
	if err != nil || string(got) != "rust-to-go" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}
func TestGoServesRustFetchesBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cas, _ := OpenCAS(t.TempDir())
	cid, _ := cas.Put(ctx, []byte("go-to-rust"))
	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{blobs.ALPN: cas.Handler()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Shutdown(context.Background())
	ticket, err := cas.Ticket(cid, server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.CommandContext(ctx, rustBlobBin(t), "fetch", ticket).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "LEGION_BLOB_DATA go-to-rust") {
		t.Fatalf("out=%s err=%v", out, err)
	}
	t.Log(fmt.Sprintf("Rust fetched Go ticket %s", ticket))
}
