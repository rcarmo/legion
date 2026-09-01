package namespace

import (
	"context"
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

func TestAuthenticatedIrohNinePRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	tree := NewTree()
	_ = tree.SetJSON("/cluster/health", map[string]bool{"ok": true})
	ns := New(tree).WithCapability([]byte("secret"))
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{ALPN: ns.Handler()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Shutdown(context.Background())
	clientEP, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer clientEP.Shutdown(context.Background())
	addr := server.Addr()
	badConn, err := clientEP.Connect(ctx, addr, ALPN)
	if err != nil {
		t.Fatal(err)
	}
	badStream, err := badConn.OpenStreamConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bad, e := NewClient(badStream, "wrong"); e == nil {
		bad.Close()
		t.Fatal("wrong capability accepted")
	}
	client, err := DialIroh(ctx, clientEP, addr, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	b, err := client.Read("/cluster/health")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]bool
	_ = json.Unmarshal(b, &got)
	if !got["ok"] {
		t.Fatalf("got %s", b)
	}
}
