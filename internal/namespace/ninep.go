package namespace

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
	"lukechampine.com/blake3"
)

const ALPN = "9p"

type LegionNamespace struct {
	tree       *Tree
	mu         sync.RWMutex
	replies    map[string][]byte
	peers      map[string]Peer
	resources  Resources
	functions  Functions
	deploy     Deploy
	cluster    Cluster
	capability *[32]byte
}

func New(tree *Tree) *LegionNamespace {
	if tree == nil {
		tree = NewTree()
	}
	return &LegionNamespace{tree: tree, replies: map[string][]byte{}, peers: map[string]Peer{}}
}
func (n *LegionNamespace) Tree() *Tree { return n.tree }
func (n *LegionNamespace) Read(ctx context.Context, path string) ([]byte, error) {
	return n.read(ctx, path)
}
func (n *LegionNamespace) Write(ctx context.Context, path string, data []byte) ([]byte, error) {
	if err := n.write(ctx, path, data); err != nil {
		return nil, err
	}
	return n.read(ctx, path)
}
func (n *LegionNamespace) WithCapability(token []byte) *LegionNamespace {
	h := blake3.Sum256(token)
	n.capability = &h
	return n
}
func (n *LegionNamespace) WithResources(v Resources) *LegionNamespace { n.resources = v; return n }
func (n *LegionNamespace) WithFunctions(v Functions) *LegionNamespace { n.functions = v; return n }
func (n *LegionNamespace) WithDeploy(v Deploy) *LegionNamespace       { n.deploy = v; return n }
func (n *LegionNamespace) WithCluster(v Cluster) *LegionNamespace     { n.cluster = v; return n }
func (n *LegionNamespace) RegisterPeer(key string, p Peer) {
	n.mu.Lock()
	n.peers[key] = p
	n.mu.Unlock()
}
func (n *LegionNamespace) UnregisterPeer(key string) {
	n.mu.Lock()
	delete(n.peers, key)
	n.mu.Unlock()
}
func (n *LegionNamespace) Attach() (p9.File, error) { return n.attach("") }
func (n *LegionNamespace) AttachName(_, aname string, _ p9.UID) (p9.File, error) {
	return n.attach(aname)
}
func (n *LegionNamespace) attach(aname string) (p9.File, error) {
	if n.capability != nil {
		token, ok := strings.CutPrefix(aname, "cap=")
		actual := blake3.Sum256([]byte(token))
		if !ok || subtle.ConstantTimeCompare(n.capability[:], actual[:]) != 1 {
			return nil, linux.EACCES
		}
	}
	return &file{ns: n, path: "/"}, nil
}
func (n *LegionNamespace) exists(p string) bool {
	if p == "/peers" {
		return true
	}
	if peer, remote, ok := peerPath(p); ok {
		n.mu.RLock()
		_, yes := n.peers[peer]
		n.mu.RUnlock()
		return yes && (remote == "/" || virtual(remote) || strings.Count(remote, "/") == 1 || strings.HasPrefix(remote, "/sessions/") || strings.HasPrefix(remote, "/fn/") || strings.HasPrefix(remote, "/deploy/") || strings.HasPrefix(remote, "/cluster/"))
	}
	if virtual(p) {
		return true
	}
	_, ok := n.tree.Get(p)
	return ok
}
func (n *LegionNamespace) isDir(p string) bool {
	v := parts(p)
	if len(v) == 2 && v[0] == "fn" && n.functions != nil {
		return false
	}
	if p == "/peers" {
		return true
	}
	if _, remote, ok := peerPath(p); ok {
		return remote == "/" || remote == "/fn" || remote == "/sessions" || remote == "/deploy" || remote == "/cluster" || strings.Count(remote, "/") == 2 && (strings.HasPrefix(remote, "/sessions/") || strings.HasPrefix(remote, "/fn/") || strings.HasPrefix(remote, "/deploy/blobs"))
	}
	x, ok := n.tree.Get(p)
	return ok && x.Kind == Directory
}
func qid(p string, dir bool) p9.QID {
	h := blake3.Sum256([]byte(p))
	typ := p9.TypeRegular
	if dir {
		typ = p9.TypeDir
	}
	return p9.QID{Type: typ, Path: binary.LittleEndian.Uint64(h[:8])}
}
func peerPath(p string) (string, string, bool) {
	v := parts(p)
	if len(v) >= 2 && v[0] == "peers" {
		suffix := "/" + strings.Join(v[2:], "/")
		return v[1], suffix, true
	}
	return "", "", false
}
func virtual(p string) bool {
	v := parts(p)
	return len(v) == 2 && ((v[0] == "sessions" && v[1] == "new") || (v[0] == "deploy" && (v[1] == "push" || v[1] == "register" || v[1] == "route" || v[1] == "promote"))) || len(v) == 3 && ((v[0] == "sessions" && (v[2] == "turns" || v[2] == "status" || v[2] == "context" || v[2] == "fork" || v[2] == "config" || v[2] == "reconcile")) || (v[0] == "fn" && (v[2] == "schema" || v[2] == "versions" || v[2] == "default" || v[2] == "manifest.json")) || (v[0] == "deploy" && v[1] == "blobs") || (v[0] == "cluster" && (v[2] == "leader" || v[2] == "health" || v[2] == "self")))
}
func (n *LegionNamespace) read(ctx context.Context, p string) ([]byte, error) {
	n.mu.RLock()
	reply, ok := n.replies[p]
	n.mu.RUnlock()
	if ok {
		return append([]byte(nil), reply...), nil
	}
	v := parts(p)
	if len(v) == 3 && v[0] == "fn" && (v[2] == "schema" || v[2] == "versions" || v[2] == "default") {
		node, ok := n.tree.Get("/fn/" + v[1] + "/manifest.json")
		if !ok && n.deploy != nil {
			if data, handled, err := n.deploy.Read(ctx, "/fn/"+v[1]+"/manifest.json"); err != nil {
				return nil, err
			} else if handled {
				node, ok = Node{Kind: JSON, Data: data}, true
			}
		}
		if !ok {
			return nil, linux.ENOENT
		}
		var m map[string]any
		if json.Unmarshal(node.Data, &m) != nil {
			return nil, linux.EIO
		}
		var out any
		switch v[2] {
		case "schema":
			out = m["parameters"]
		case "versions":
			out = []any{m["version"]}
		case "default":
			out = m["version"]
		}
		return json.Marshal(out)
	}
	if strings.HasPrefix(p, "/cluster/") && n.cluster != nil {
		if b, ok, e := n.cluster.Read(ctx, p); e != nil {
			return nil, e
		} else if ok {
			return b, nil
		}
	}
	if (strings.HasPrefix(p, "/deploy/") || strings.HasPrefix(p, "/fn/")) && n.deploy != nil {
		if b, ok, e := n.deploy.Read(ctx, p); e != nil {
			return nil, e
		} else if ok {
			return b, nil
		}
	}
	if n.resources != nil {
		if b, ok, e := n.resources.Read(ctx, p); e != nil {
			return nil, e
		} else if ok {
			return b, nil
		}
	}
	if p == "/peers" {
		n.mu.RLock()
		keys := make([]string, 0, len(n.peers))
		for k := range n.peers {
			keys = append(keys, k)
		}
		n.mu.RUnlock()
		return json.Marshal(keys)
	}
	if key, remote, ok := peerPath(p); ok {
		n.mu.RLock()
		peer := n.peers[key]
		n.mu.RUnlock()
		if peer == nil {
			return nil, linux.ENOENT
		}
		return peer.Read(ctx, remote)
	}
	node, ok := n.tree.Get(p)
	if !ok {
		if virtual(p) {
			return []byte("null"), nil
		}
		return nil, linux.ENOENT
	}
	if node.Kind == Directory {
		return json.Marshal(n.tree.List(p))
	}
	return node.Data, nil
}
func (n *LegionNamespace) write(ctx context.Context, p string, data []byte) error {
	if strings.HasPrefix(p, "/deploy/") && n.deploy != nil {
		if b, ok, e := n.deploy.Write(ctx, p, data); e != nil {
			return e
		} else if ok {
			n.reply(p, b)
			return nil
		}
	}
	v := parts(p)
	if len(v) == 2 && v[0] == "fn" && n.functions != nil {
		b, e := n.functions.Invoke(ctx, v[1], data)
		if e != nil {
			return e
		}
		n.reply(p, b)
		return nil
	}
	if n.resources != nil {
		if b, ok, e := n.resources.Write(ctx, p, data); e != nil {
			return e
		} else if ok {
			n.reply(p, b)
			n.tree.SetBlob(p, b)
			return nil
		}
	}
	if key, remote, ok := peerPath(p); ok {
		n.mu.RLock()
		peer := n.peers[key]
		n.mu.RUnlock()
		if peer == nil {
			return linux.ENOENT
		}
		b, e := peer.Write(ctx, remote, data)
		if e == nil {
			n.reply(p, b)
		}
		return e
	}
	var x any
	if json.Unmarshal(data, &x) == nil {
		_ = n.tree.SetJSON(p, x)
	} else {
		n.tree.SetBlob(p, data)
	}
	n.reply(p, data)
	return nil
}
func (n *LegionNamespace) reply(p string, b []byte) {
	n.mu.Lock()
	n.replies[p] = append([]byte(nil), b...)
	n.mu.Unlock()
}

type file struct {
	templatefs.NoopFile
	ns   *LegionNamespace
	path string
}

func (f *file) Walk(names []string) ([]p9.QID, p9.File, error) {
	cur := f.path
	qs := make([]p9.QID, 0, len(names))
	for _, name := range names {
		switch name {
		case ".":
		case "..":
			cur = path.Dir(cur)
		default:
			cur = path.Join(cur, name)
		}
		if !f.ns.exists(cur) {
			return nil, nil, linux.ENOENT
		}
		qs = append(qs, qid(cur, f.ns.isDir(cur)))
	}
	return qs, &file{ns: f.ns, path: cur}, nil
}
func (f *file) GetAttr(p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	dir := f.ns.isDir(f.path)
	mode := p9.ModeRegular | 0644
	if dir {
		mode = p9.ModeDirectory | 0755
	}
	var size uint64
	if !dir {
		if b, e := f.ns.read(context.Background(), f.path); e == nil {
			size = uint64(len(b))
		}
	}
	return qid(f.path, dir), p9.AttrMaskAll, p9.Attr{Mode: mode, NLink: 1, Size: size, BlockSize: 4096, Blocks: (size + 511) / 512}, nil
}
func (f *file) StatFS() (p9.FSStat, error) {
	return p9.FSStat{Type: 0x01021994, BlockSize: 4096, NameLength: 255}, nil
}
func (f *file) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	functionCall := len(parts(f.path)) == 2 && parts(f.path)[0] == "fn"
	if f.ns.isDir(f.path) && mode.Mode() != p9.ReadOnly && !functionCall {
		return p9.QID{}, 0, linux.EISDIR
	}
	return qid(f.path, f.ns.isDir(f.path)), 0, nil
}
func (f *file) ReadAt(out []byte, off int64) (int, error) {
	for {
		changed := f.ns.tree.Changed()
		data, err := f.ns.read(context.Background(), f.path)
		if err != nil {
			return 0, err
		}
		streaming := strings.HasPrefix(f.path, "/sessions/") && strings.HasSuffix(f.path, "/turns")
		if off < int64(len(data)) || !streaming {
			if off >= int64(len(data)) {
				return 0, io.EOF
			}
			return copy(out, data[off:]), nil
		}
		<-changed
	}
}
func (f *file) WriteAt(data []byte, off int64) (int, error) {
	if off != 0 {
		return 0, linux.EINVAL
	}
	if err := f.ns.write(context.Background(), f.path, data); err != nil {
		return 0, err
	}
	return len(data), nil
}
func (f *file) Readdir(off uint64, count uint32) (p9.Dirents, error) {
	if !f.ns.isDir(f.path) {
		return nil, linux.ENOTDIR
	}
	names := f.ns.tree.List(f.path)
	if f.path == "/peers" {
		n, _ := f.ns.read(context.Background(), f.path)
		_ = json.Unmarshal(n, &names)
	}
	if off >= uint64(len(names)) {
		return nil, io.EOF
	}
	out := make(p9.Dirents, 0)
	for i := off; i < uint64(len(names)) && uint32(len(out)) < count; i++ {
		child := path.Join(f.path, names[i])
		q := qid(child, f.ns.isDir(child))
		out = append(out, p9.Dirent{QID: q, Offset: i + 1, Type: q.Type, Name: names[i]})
	}
	return out, nil
}
func (f *file) Create(name string, flags p9.OpenFlags, _ p9.FileMode, _ p9.UID, _ p9.GID) (p9.File, p9.QID, uint32, error) {
	p := path.Join(f.path, name)
	f.ns.tree.SetBlob(p, nil)
	q := qid(p, false)
	return &file{ns: f.ns, path: p}, q, 0, nil
}
func (f *file) Mkdir(name string, _ p9.FileMode, _ p9.UID, _ p9.GID) (p9.QID, error) {
	p := path.Join(f.path, name)
	f.ns.tree.EnsureDir(p)
	return qid(p, true), nil
}
func (f *file) UnlinkAt(name string, _ uint32) error {
	f.ns.tree.Delete(path.Join(f.path, name))
	return nil
}
func (f *file) FSync() error            { return nil }
func (f *file) Close() error            { return nil }
func (f *file) Renamed(p9.File, string) {}

var _ p9.Attacher = (*LegionNamespace)(nil)
var _ p9.NamedAttacher = (*LegionNamespace)(nil)
var _ p9.File = (*file)(nil)
var _ = errors.New
var _ = fmt.Sprintf
