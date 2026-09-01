package namespace

import (
	"context"
	"testing"
)

type localPeer struct{ tree *Tree }

func (p localPeer) Read(_ context.Context, path string) ([]byte, error) {
	n, ok := p.tree.Get(path)
	if !ok {
		return nil, context.Canceled
	}
	return n.Data, nil
}
func (p localPeer) Write(_ context.Context, path string, b []byte) ([]byte, error) {
	p.tree.SetBlob(path, b)
	return b, nil
}
func TestPeerPrefixReadWrite(t *testing.T) {
	remote := NewTree()
	remote.SetBlob("/cluster/self", []byte(`{"peer":"remote"}`))
	ns := New(NewTree())
	ns.RegisterPeer("key", localPeer{remote})
	c := tcpPair(t, ns, "")
	b, err := c.Read("/peers/key/cluster/self")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"peer":"remote"}` {
		t.Fatal(string(b))
	}
	b, err = c.Write("/peers/key/deploy/register", []byte(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"x":1}` {
		t.Fatal(string(b))
	}
}
