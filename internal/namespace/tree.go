// Package namespace exposes Legion resources as a concurrent 9P namespace.
package namespace

import (
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

type NodeKind uint8

const (
	Directory NodeKind = iota
	Blob
	JSON
)

type Node struct {
	Kind      NodeKind
	Data      []byte
	UpdatedAt int64
}

type Tree struct {
	mu      sync.RWMutex
	nodes   map[string]Node
	changed chan struct{}
}

func NewTree() *Tree {
	t := &Tree{nodes: make(map[string]Node), changed: make(chan struct{})}
	for _, p := range []string{"/", "/fn", "/sessions", "/deploy", "/deploy/blobs", "/deploy/routes", "/cluster", "/cluster/peers", "/peers"} {
		t.nodes[p] = Node{Kind: Directory, UpdatedAt: time.Now().UnixMilli()}
	}
	for _, p := range []string{"/sessions/new", "/deploy/push", "/deploy/register", "/deploy/route", "/deploy/promote"} {
		t.nodes[p] = Node{Kind: Blob, UpdatedAt: time.Now().UnixMilli()}
	}
	for _, p := range []string{"/cluster/leader", "/cluster/health", "/cluster/self"} {
		b, _ := json.Marshal(nil)
		t.nodes[p] = Node{Kind: JSON, Data: b, UpdatedAt: time.Now().UnixMilli()}
	}
	return t
}

func clean(p string) string {
	if p == "" {
		return "/"
	}
	p = path.Clean("/" + strings.TrimPrefix(p, "/"))
	return p
}
func (t *Tree) Get(p string) (Node, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[clean(p)]
	n.Data = append([]byte(nil), n.Data...)
	return n, ok
}
func (t *Tree) SetBlob(p string, data []byte) { t.set(p, Blob, data) }
func (t *Tree) SetJSON(p string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	t.set(p, JSON, b)
	return nil
}
func (t *Tree) EnsureDir(p string) { t.set(p, Directory, nil) }
func (t *Tree) set(p string, kind NodeKind, data []byte) {
	p = clean(p)
	now := time.Now().UnixMilli()
	t.mu.Lock()
	for parent := path.Dir(p); ; parent = path.Dir(parent) {
		if _, ok := t.nodes[parent]; !ok {
			t.nodes[parent] = Node{Kind: Directory, UpdatedAt: now}
		}
		if parent == "/" {
			break
		}
	}
	t.nodes[p] = Node{Kind: kind, Data: append([]byte(nil), data...), UpdatedAt: now}
	close(t.changed)
	t.changed = make(chan struct{})
	t.mu.Unlock()
}
func (t *Tree) Delete(p string) {
	p = clean(p)
	prefix := p + "/"
	t.mu.Lock()
	for k := range t.nodes {
		if k == p || strings.HasPrefix(k, prefix) {
			delete(t.nodes, k)
		}
	}
	close(t.changed)
	t.changed = make(chan struct{})
	t.mu.Unlock()
}
func (t *Tree) List(dir string) []string {
	dir = clean(dir)
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	t.mu.RLock()
	seen := map[string]bool{}
	for k := range t.nodes {
		if rest := strings.TrimPrefix(k, prefix); rest != k && rest != "" {
			seen[strings.SplitN(rest, "/", 2)[0]] = true
		}
	}
	t.mu.RUnlock()
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func (t *Tree) Changed() <-chan struct{} { t.mu.RLock(); defer t.mu.RUnlock(); return t.changed }
