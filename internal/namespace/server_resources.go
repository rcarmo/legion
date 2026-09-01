package namespace

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/rcarmo/legion/internal/cluster"
	"github.com/rcarmo/legion/internal/raftstore"
)

type TreeDeploy struct{ Tree *Tree }

func (d TreeDeploy) Read(_ context.Context, p string) ([]byte, bool, error) {
	n, ok := d.Tree.Get(p)
	if !ok {
		return nil, false, nil
	}
	if n.Kind == Directory {
		b, e := json.Marshal(d.Tree.List(p))
		return b, true, e
	}
	return n.Data, true, nil
}
func (d TreeDeploy) Write(_ context.Context, p string, b []byte) ([]byte, bool, error) {
	switch {
	case p == "/deploy/register" || p == "/deploy/route" || p == "/deploy/promote":
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			return nil, false, err
		}
		_ = d.Tree.SetJSON(p, v)
		return b, true, nil
	case len(p) > len("/deploy/blobs/") && p[:len("/deploy/blobs/")] == "/deploy/blobs/":
		d.Tree.SetBlob(p, b)
		out, e := json.Marshal(map[string]any{"path": p, "size": len(b)})
		return out, true, e
	default:
		return nil, false, nil
	}
}

type LiveCluster struct {
	Node  *cluster.Node
	Store *raftstore.Store
	Tree  *Tree
}

func (c LiveCluster) Read(_ context.Context, p string) ([]byte, bool, error) {
	var v any
	switch p {
	case "/cluster/self":
		v = map[string]any{"endpoint_id": c.Node.ID().String(), "short_id": c.Node.ShortID(), "api_port": c.Node.Config.APIPort}
	case "/cluster/health":
		state := "unavailable"
		if c.Store != nil {
			state = c.Store.State().String()
		}
		v = map[string]any{"healthy": true, "peers": len(c.Tree.List("/cluster/peers")), "raft": state}
	case "/cluster/leader":
		if c.Store == nil {
			v = nil
		} else {
			addr, id := c.Store.LeaderWithID()
			v = map[string]any{"node_id": string(id), "address": string(addr), "leader": c.Store.State() == raft.Leader}
		}
	default:
		return nil, false, nil
	}
	b, e := json.Marshal(v)
	return b, true, e
}

var _ Deploy = TreeDeploy{}
var _ Cluster = LiveCluster{}
var _ = fmt.Sprintf
