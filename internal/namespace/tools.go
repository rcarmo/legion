package namespace

import (
	"context"
	"encoding/json"

	"github.com/rcarmo/legion/internal/core"
)

// ClusterTools exposes the live namespace view through the same tool path used
// by agents and workflows. Membership does not imply remote scheduling.
type ClusterTools struct {
	Namespace *LegionNamespace
	Base      core.ToolRegistry
}

func (t ClusterTools) Definitions(ctx context.Context) ([]core.ToolDefinition, error) {
	defs, err := t.Base.Definitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"cluster.status", "cluster.peers"} {
		defs = append(defs, core.ToolDefinition{Name: name, Description: "Read live cluster membership and health", Parameters: json.RawMessage(`{"type":"object"}`), Effect: core.EffectRead})
	}
	return defs, nil
}
func (t ClusterTools) Dispatch(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	switch name {
	case "cluster.status":
		return t.Namespace.read(ctx, "/cluster/health")
	case "cluster.peers":
		peers := []json.RawMessage{}
		for _, name := range t.Namespace.tree.List("/cluster/peers") {
			n, ok := t.Namespace.tree.Get("/cluster/peers/" + name)
			if ok && n.Kind == JSON {
				peers = append(peers, json.RawMessage(n.Data))
			}
		}
		return json.Marshal(peers)
	default:
		return t.Base.Dispatch(ctx, name, args)
	}
}
