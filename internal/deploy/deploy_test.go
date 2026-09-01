package deploy

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
)

func TestCASPersistsAndUsesIrohHash(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cas, err := OpenCAS(root)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("same artifact")
	first, err := cas.Put(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cas.Put(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first != blobs.NewHash(data).String() {
		t.Fatalf("cid %s %s", first, second)
	}
	reopened, err := OpenCAS(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get(ctx, first)
	if err != nil || string(got) != string(data) {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestRustCompatibleBlobTicketTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cas, _ := OpenCAS(t.TempDir())
	cid, _ := cas.Put(ctx, []byte("ticket payload"))
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
	parsed, err := blobs.ParseTicket(ticket)
	if err != nil || parsed.Hash().String() != cid {
		t.Fatalf("ticket=%s err=%v", ticket, err)
	}
	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	got, err := FetchTicket(ctx, client, ticket)
	if err != nil || string(got) != "ticket payload" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestRegisterRoutePromoteAndReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	r, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	base := r.Register(ctx, Job{Name: "hello", Runtime: legionruntime.Joker, Code: "base"})
	if base.Status != "success" {
		t.Fatal(base)
	}
	canary, _ := r.Push(ctx, []byte("canary"))
	if err = r.Route(Route{Name: "hello", ArtifactCID: canary, Weight: 10000}); err != nil {
		t.Fatal(err)
	}
	selected, _ := r.Resolve("hello", "call")
	if selected.ArtifactCID != canary {
		t.Fatal(selected)
	}
	if err = r.Promote("hello"); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, ok := reopened.Manifest("hello")
	if !ok || manifest.ArtifactCID != canary {
		t.Fatal(manifest)
	}
}

func TestWeightedRouteIsStableAndApproximate(t *testing.T) {
	r := Route{ArtifactCID: "cid", Weight: 2500}
	selected := 0
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("call-%d", i)
		if r.Select(id) != r.Select(id) {
			t.Fatal("unstable")
		}
		if r.Select(id) {
			selected++
		}
	}
	if selected < 2350 || selected > 2650 {
		t.Fatalf("selected %d", selected)
	}
}
