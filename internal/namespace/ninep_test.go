package namespace

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func tcpPair(t *testing.T, ns *LegionNamespace, cap string) *Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = ln.Close() })
	go ns.ServeTCP(ctx, ln)
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(conn, cap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func TestNinePRoundTripAndMetadata(t *testing.T) {
	tree := NewTree()
	_ = tree.SetJSON("/cluster/health", map[string]bool{"ok": true})
	_ = tree.SetJSON("/fn/hello/manifest.json", map[string]any{"version": "1.2.3", "parameters": map[string]string{"type": "object"}})
	c := tcpPair(t, New(tree), "")
	b, err := c.Read("/cluster/health")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]bool
	_ = json.Unmarshal(b, &got)
	if !got["ok"] {
		t.Fatalf("got %s", b)
	}
	b, err = c.Read("/fn/hello/schema")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"object"}` {
		t.Fatalf("got %s", b)
	}
	if _, err = c.Write("/cluster/health", []byte(`{"ok":false}`)); err != nil {
		t.Fatal(err)
	}
}
func TestCapabilityAttach(t *testing.T) {
	ns := New(NewTree()).WithCapability([]byte("secret"))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ns.ServeTCP(ctx, ln)
	for _, cap := range []string{"", "wrong"} {
		conn, _ := net.Dial("tcp", ln.Addr().String())
		if c, e := NewClient(conn, cap); e == nil {
			c.Close()
			t.Fatalf("accepted %q", cap)
		}
	}
	conn, _ := net.Dial("tcp", ln.Addr().String())
	c, e := NewClient(conn, "secret")
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
}
func TestBlockingTurnsRead(t *testing.T) {
	tree := NewTree()
	tree.SetBlob("/sessions/00000000-0000-0000-0000-000000000001/turns", nil)
	c := tcpPair(t, New(tree), "")
	done := make(chan []byte, 1)
	go func() { b, _ := c.Read("/sessions/00000000-0000-0000-0000-000000000001/turns"); done <- b }()
	select {
	case <-done:
		t.Fatal("read did not block")
	case <-time.After(100 * time.Millisecond):
	}
	tree.SetBlob("/sessions/00000000-0000-0000-0000-000000000001/turns", []byte("turn"))
	select {
	case b := <-done:
		if string(b) != "turn" {
			t.Fatalf("got %q", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read remained blocked")
	}
}
