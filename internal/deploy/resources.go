package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

// Resources adapts Registry to the deployment portion of the 9P namespace.
type Resources struct {
	Registry   *Registry
	OnRegister func(legionruntime.Manifest)
}

func (d Resources) Read(_ context.Context, p string) ([]byte, bool, error) {
	if d.Registry == nil {
		return nil, false, nil
	}
	if strings.HasPrefix(p, "/deploy/blobs/") {
		cid := strings.TrimPrefix(p, "/deploy/blobs/")
		b, err := d.Registry.cas.Get(context.Background(), cid)
		if err != nil {
			return nil, true, err
		}
		out, err := json.Marshal(map[string]any{"cid": cid, "size": len(b)})
		return out, true, err
	}
	if strings.HasPrefix(p, "/deploy/routes/") {
		name := strings.TrimPrefix(p, "/deploy/routes/")
		route, ok := d.Registry.RouteFor(name)
		if !ok {
			return nil, false, nil
		}
		b, err := json.Marshal(route)
		return b, true, err
	}
	if strings.HasPrefix(p, "/fn/") && strings.HasSuffix(p, "/manifest.json") {
		name := strings.TrimSuffix(strings.TrimPrefix(p, "/fn/"), "/manifest.json")
		m, ok := d.Registry.Manifest(name)
		if !ok {
			return nil, false, nil
		}
		b, err := json.Marshal(m)
		return b, true, err
	}
	return nil, false, nil
}
func (d Resources) Write(ctx context.Context, p string, b []byte) ([]byte, bool, error) {
	if d.Registry == nil {
		return nil, false, nil
	}
	switch p {
	case "/deploy/push":
		cid, err := d.Registry.Push(ctx, b)
		if err != nil {
			return nil, true, err
		}
		out, err := json.Marshal(map[string]any{"artifact_cid": cid, "size": len(b)})
		return out, true, err
	case "/deploy/register":
		var job Job
		if err := json.Unmarshal(b, &job); err != nil {
			return nil, true, err
		}
		outcome := d.Registry.Register(ctx, job)
		out, err := json.Marshal(outcome)
		if outcome.Status != "success" {
			return out, true, fmt.Errorf("%s", outcome.Error)
		}
		if d.OnRegister != nil {
			if manifest, ok := d.Registry.Manifest(job.Name); ok {
				d.OnRegister(manifest)
			}
		}
		return out, true, err
	case "/deploy/route":
		var route Route
		if err := json.Unmarshal(b, &route); err != nil {
			return nil, true, err
		}
		if err := d.Registry.Route(route); err != nil {
			return nil, true, err
		}
		if stored, ok := d.Registry.RouteFor(route.Name); ok {
			route = stored
		}
		out, err := json.Marshal(route)
		return out, true, err
	case "/deploy/promote":
		var req struct {
			Name        string `json:"name"`
			ArtifactCID string `json:"artifact_cid"`
		}
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, true, err
		}
		if req.ArtifactCID != "" {
			if err := d.Registry.Route(Route{Name: req.Name, ArtifactCID: req.ArtifactCID, Weight: 10000}); err != nil {
				return nil, true, err
			}
		}
		if err := d.Registry.Promote(req.Name); err != nil {
			return nil, true, err
		}
		m, _ := d.Registry.Manifest(req.Name)
		if d.OnRegister != nil {
			d.OnRegister(m)
		}
		out, err := json.Marshal(m)
		return out, true, err
	}
	if strings.HasPrefix(p, "/deploy/blobs/") {
		requested := strings.TrimPrefix(p, "/deploy/blobs/")
		cid, err := d.Registry.Push(ctx, b)
		if err != nil {
			return nil, true, err
		}
		if requested != "" && requested != "-" && requested != cid {
			return nil, true, fmt.Errorf("blob CID mismatch: got %s", cid)
		}
		out, err := json.Marshal(map[string]any{"artifact_cid": cid, "size": len(b)})
		return out, true, err
	}
	return nil, false, nil
}
