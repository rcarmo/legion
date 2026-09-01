package deploy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
	"lukechampine.com/blake3"
)

type Job struct {
	Name        string             `json:"name"`
	Runtime     legionruntime.Kind `json:"runtime"`
	Description string             `json:"description"`
	Parameters  json.RawMessage    `json:"parameters"`
	Code        string             `json:"code,omitempty"`
	WASMBase64  string             `json:"wasm_b64,omitempty"`
	Bytes       []byte             `json:"bytes,omitempty"`
	CID         string             `json:"cid,omitempty"`
	Idempotent  bool               `json:"idempotent"`
	Env         map[string]string  `json:"env,omitempty"`
}
type Outcome struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Path        string `json:"path,omitempty"`
	ArtifactCID string `json:"artifact_cid,omitempty"`
	Error       string `json:"error,omitempty"`
	WallMS      uint64 `json:"wall_ms"`
}
type Route struct {
	Name        string `json:"name"`
	ArtifactCID string `json:"artifact_cid"`
	Weight      uint16 `json:"weight"`
	UpdatedAt   int64  `json:"updated_at"`
}

func (r Route) Select(callID string) bool {
	if r.Weight == 0 {
		return false
	}
	if r.Weight >= 10000 {
		return true
	}
	h := blake3.Sum256([]byte(callID))
	bucket := uint32(h[0])<<8 | uint32(h[1])
	return bucket*10000/65536 < uint32(r.Weight)
}

type Registry struct {
	root      string
	cas       *CAS
	mu        sync.RWMutex
	manifests map[string]legionruntime.Manifest
	routes    map[string]Route
}

func Open(root string) (*Registry, error) {
	cas, e := OpenCAS(filepath.Join(root, "blobs"))
	if e != nil {
		return nil, e
	}
	r := &Registry{root: filepath.Join(root, "fn"), cas: cas, manifests: map[string]legionruntime.Manifest{}, routes: map[string]Route{}}
	if e = r.load(); e != nil {
		return nil, e
	}
	return r, nil
}
func (r *Registry) CAS() *CAS { return r.cas }
func validName(s string) bool {
	if s == "" || len(s) > 64 || strings.HasPrefix(s, "-") || strings.HasSuffix(s, "-") {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func (r *Registry) Push(ctx context.Context, b []byte) (string, error) { return r.cas.Put(ctx, b) }
func (r *Registry) Register(ctx context.Context, j Job) (out Outcome) {
	start := time.Now()
	fail := func(e error) Outcome {
		return Outcome{Name: j.Name, Status: "failed", Error: e.Error(), WallMS: uint64(time.Since(start).Milliseconds())}
	}
	if !validName(j.Name) {
		return fail(fmt.Errorf("name must match [a-z0-9-]+"))
	}
	artifact := j.Bytes
	if len(artifact) == 0 && j.WASMBase64 != "" {
		var decodeErr error
		artifact, decodeErr = base64.StdEncoding.DecodeString(j.WASMBase64)
		if decodeErr != nil {
			artifact, decodeErr = base64.RawURLEncoding.DecodeString(j.WASMBase64)
		}
		if decodeErr != nil {
			return fail(fmt.Errorf("wasm_b64: %w", decodeErr))
		}
	}
	if len(artifact) == 0 {
		artifact = []byte(j.Code)
	}
	if j.CID == "" && len(artifact) == 0 {
		return fail(fmt.Errorf("artifact required"))
	}
	if j.Runtime == "" {
		j.Runtime = legionruntime.Bun
	}
	if j.Runtime != legionruntime.WASM && j.Runtime != legionruntime.Bun && j.Runtime != legionruntime.Joker {
		return fail(fmt.Errorf("unsupported runtime %q", j.Runtime))
	}
	cid := j.CID
	var e error
	if cid == "" {
		cid, e = r.cas.Put(ctx, artifact)
	} else {
		artifact, e = r.cas.Get(ctx, cid)
	}
	if e != nil {
		return fail(e)
	}
	ext := "wasm"
	if j.Runtime == legionruntime.Joker {
		ext = "joke"
	}
	if j.Runtime == legionruntime.Bun {
		ext = "ts"
	}
	p := filepath.Join(r.root, j.Name, "index."+ext)
	if _, e = r.cas.Materialize(ctx, cid, p); e != nil {
		return fail(e)
	}
	params := j.Parameters
	if len(params) == 0 {
		params = []byte(`{"type":"object","properties":{}}`)
	}
	m := legionruntime.Manifest{Name: j.Name, Runtime: j.Runtime, Version: "1.0.0", ArtifactCID: cid, DeployedAt: time.Now().UnixMilli(), Parameters: params, Description: j.Description, Idempotent: j.Idempotent, Env: j.Env}
	r.mu.Lock()
	r.manifests[j.Name] = m
	r.mu.Unlock()
	if e = r.save(); e != nil {
		return fail(e)
	}
	return Outcome{Name: j.Name, Status: "success", Path: p, ArtifactCID: cid, WallMS: uint64(time.Since(start).Milliseconds())}
}
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.manifests))
	for name := range r.manifests {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
func (r *Registry) Manifest(name string) (legionruntime.Manifest, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.manifests[name]
	return m, ok
}
func (r *Registry) Route(route Route) error {
	if !validName(route.Name) || route.Weight > 10000 || route.ArtifactCID == "" {
		return fmt.Errorf("invalid route")
	}
	if _, err := r.cas.Get(context.Background(), route.ArtifactCID); err != nil {
		return fmt.Errorf("route artifact: %w", err)
	}
	if _, ok := r.Manifest(route.Name); !ok {
		return fmt.Errorf("function not found")
	}
	route.UpdatedAt = time.Now().UnixMilli()
	r.mu.Lock()
	r.routes[route.Name] = route
	r.mu.Unlock()
	return r.save()
}
func (r *Registry) RouteFor(name string) (Route, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, ok := r.routes[name]
	return route, ok
}
func (r *Registry) Promote(name string) error {
	r.mu.Lock()
	route, ok := r.routes[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("route not found")
	}
	m, ok := r.manifests[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("manifest not found")
	}
	m.ArtifactCID = route.ArtifactCID
	m.DeployedAt = time.Now().UnixMilli()
	r.manifests[name] = m
	delete(r.routes, name)
	r.mu.Unlock()
	return r.save()
}
func (r *Registry) Resolve(name, call string) (legionruntime.Manifest, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.manifests[name]
	if !ok {
		return m, fmt.Errorf("function %s not found", name)
	}
	if route, ok := r.routes[name]; ok && route.Select(call) {
		m.ArtifactCID = route.ArtifactCID
	}
	return m, nil
}
func (r *Registry) registryPath() string { return filepath.Join(filepath.Dir(r.root), "registry.json") }
func (r *Registry) save() error {
	r.mu.RLock()
	v := struct {
		Manifests map[string]legionruntime.Manifest `json:"manifests"`
		Routes    map[string]Route                  `json:"routes"`
	}{r.manifests, r.routes}
	r.mu.RUnlock()
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(r.registryPath()), 0755); e != nil {
		return e
	}
	tmp := r.registryPath() + ".tmp"
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	return os.Rename(tmp, r.registryPath())
}
func (r *Registry) load() error {
	b, e := os.ReadFile(r.registryPath())
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	v := struct {
		Manifests map[string]legionruntime.Manifest `json:"manifests"`
		Routes    map[string]Route                  `json:"routes"`
	}{}
	if e = json.Unmarshal(b, &v); e != nil {
		return e
	}
	if v.Manifests != nil {
		r.manifests = v.Manifests
	}
	if v.Routes != nil {
		r.routes = v.Routes
	}
	return nil
}
