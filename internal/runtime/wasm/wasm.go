// Package wasm executes Extism-compatible WebAssembly functions on wazero.
package wasm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	extism "github.com/extism/go-sdk"
	legionruntime "github.com/rcarmo/legion/internal/runtime"
	"github.com/tetratelabs/wazero"
)

type Runtime struct {
	source    legionruntime.ArtifactSource
	namespace legionruntime.Namespace
	budget    legionruntime.Budget
	limits    legionruntime.Limits
	mu        sync.Mutex
	cache     map[string]*extism.CompiledPlugin
}

func New(source legionruntime.ArtifactSource, namespace legionruntime.Namespace, budget legionruntime.Budget, limits legionruntime.Limits) *Runtime {
	if limits.Timeout <= 0 {
		limits = legionruntime.DefaultLimits()
	}
	return &Runtime{source: source, namespace: namespace, budget: budget, limits: limits, cache: map[string]*extism.CompiledPlugin{}}
}
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var first error
	for _, p := range r.cache {
		if err := p.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	r.cache = map[string]*extism.CompiledPlugin{}
	return first
}
func (r *Runtime) compiled(ctx context.Context, cid string) (*extism.CompiledPlugin, error) {
	r.mu.Lock()
	p := r.cache[cid]
	r.mu.Unlock()
	if p != nil {
		return p, nil
	}
	if r.source == nil {
		return nil, fmt.Errorf("artifact source unavailable")
	}
	b, err := r.source.Fetch(ctx, cid)
	if err != nil {
		return nil, err
	}
	pages := uint32((r.limits.MaxMemoryBytes + 65535) / 65536)
	if pages == 0 {
		pages = 1
	}
	manifest := extism.Manifest{Wasm: []extism.Wasm{extism.WasmData{Data: b}}, Memory: &extism.ManifestMemory{MaxPages: pages, MaxHttpResponseBytes: 0, MaxVarBytes: int64(r.limits.MaxOutputBytes)}, Timeout: uint64(r.limits.Timeout.Milliseconds())}
	config := extism.PluginConfig{RuntimeConfig: wazero.NewRuntimeConfig().WithCloseOnContextDone(true), EnableWasi: true}
	p, err = extism.NewCompiledPlugin(ctx, manifest, config, r.hostFunctions())
	if err != nil {
		return nil, fmt.Errorf("compile wasm: %w", err)
	}
	r.mu.Lock()
	if existing := r.cache[cid]; existing != nil {
		r.mu.Unlock()
		_ = p.Close(ctx)
		return existing, nil
	}
	r.cache[cid] = p
	r.mu.Unlock()
	return p, nil
}
func (r *Runtime) Invoke(ctx context.Context, req legionruntime.Request) (legionruntime.Result, error) {
	start := time.Now()
	cid := req.ArtifactCID
	if cid == "" {
		return legionruntime.Result{}, fmt.Errorf("artifact CID required")
	}
	compiled, err := r.compiled(ctx, cid)
	if err != nil {
		return legionruntime.Result{}, err
	}
	plugin, err := compiled.Instance(ctx, extism.PluginInstanceConfig{})
	if err != nil {
		return legionruntime.Result{}, fmt.Errorf("instantiate wasm: %w", err)
	}
	defer plugin.Close(context.Background())
	exit, out, err := plugin.CallWithContext(ctx, "run", req.Args)
	wall := uint64(time.Since(start).Milliseconds())
	if err != nil {
		return legionruntime.Result{CallID: req.CallID, WallMS: wall}, fmt.Errorf("call wasm: %w", err)
	}
	if exit != 0 {
		return legionruntime.Result{CallID: req.CallID, WallMS: wall}, fmt.Errorf("wasm exit %d", exit)
	}
	return legionruntime.Result{CallID: req.CallID, Output: normalizeJSON(out), WallMS: wall}, nil
}
func normalizeJSON(out []byte) json.RawMessage {
	var v any
	if json.Unmarshal(out, &v) == nil {
		b, _ := json.Marshal(v)
		return b
	}
	b, _ := json.Marshal(string(out))
	return b
}
func (r *Runtime) hostFunctions() []extism.HostFunction {
	logFn := extism.NewHostFunctionWithStack("log", func(_ context.Context, p *extism.CurrentPlugin, s []uint64) {
		message, err := p.ReadString(s[0])
		if err != nil {
			panic(err)
		}
		log.Printf("wasm: %s", message)
	}, []extism.ValueType{extism.ValueTypePTR}, nil)
	readFn := extism.NewHostFunctionWithStack("read", func(ctx context.Context, p *extism.CurrentPlugin, s []uint64) {
		key, err := p.ReadString(s[0])
		if err != nil {
			panic(err)
		}
		var b []byte
		if r.namespace != nil {
			b, err = r.namespace.Read(ctx, key)
		}
		if err != nil {
			panic(err)
		}
		ptr, err := p.WriteBytes(b)
		if err != nil {
			panic(err)
		}
		s[0] = ptr
	}, []extism.ValueType{extism.ValueTypePTR}, []extism.ValueType{extism.ValueTypePTR})
	writeFn := extism.NewHostFunctionWithStack("write", func(ctx context.Context, p *extism.CurrentPlugin, s []uint64) {
		key, err := p.ReadString(s[0])
		if err != nil {
			panic(err)
		}
		value, err := p.ReadBytes(s[1])
		if err != nil {
			panic(err)
		}
		if r.namespace != nil {
			_, err = r.namespace.Write(ctx, key, value)
		}
		if err != nil {
			panic(err)
		}
	}, []extism.ValueType{extism.ValueTypePTR, extism.ValueTypePTR}, nil)
	budgetFn := extism.NewHostFunctionWithStack("budget", func(_ context.Context, p *extism.CurrentPlugin, s []uint64) {
		input, err := p.ReadBytes(s[0])
		if err != nil || len(input) != 8 {
			panic(fmt.Errorf("decode budget request: %w", err))
		}
		requested := binary.LittleEndian.Uint64(input)
		var granted uint64
		if r.budget != nil {
			granted = r.budget.Take(requested)
		}
		output := make([]byte, 8)
		binary.LittleEndian.PutUint64(output, granted)
		ptr, err := p.WriteBytes(output)
		if err != nil {
			panic(err)
		}
		s[0] = ptr
	}, []extism.ValueType{extism.ValueTypePTR}, []extism.ValueType{extism.ValueTypePTR})
	return []extism.HostFunction{logFn, readFn, writeFn, budgetFn}
}
