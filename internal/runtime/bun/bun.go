// Package bun executes deployed JavaScript or TypeScript in supervised Bun workers.
package bun

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

//go:embed worker.ts
var workerSource []byte

type Runtime struct {
	Bin        string
	Source     legionruntime.ArtifactSource
	Limits     legionruntime.Limits
	mu         sync.Mutex
	workers    map[string]*worker
	workerPath string
}

type worker struct {
	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  *json.Encoder
	stdout *bufio.Scanner
}

type workerRequest struct {
	CallID string            `json:"call_id"`
	Args   json.RawMessage   `json:"args"`
	Env    map[string]string `json:"env,omitempty"`
}
type workerResponse struct {
	CallID string          `json:"call_id"`
	Output json.RawMessage `json:"output"`
	Error  string          `json:"error,omitempty"`
}

func New(bin string, source legionruntime.ArtifactSource, limits legionruntime.Limits) *Runtime {
	if bin == "" {
		bin = os.Getenv("LEGION_BUN_BIN")
	}
	if bin == "" {
		bin = "bun"
	}
	if limits.Timeout <= 0 {
		limits = legionruntime.DefaultLimits()
	}
	return &Runtime{Bin: bin, Source: source, Limits: limits, workers: map[string]*worker{}}
}

func (r *Runtime) Invoke(ctx context.Context, req legionruntime.Request) (legionruntime.Result, error) {
	started := time.Now()
	if req.ArtifactCID == "" {
		return legionruntime.Result{}, fmt.Errorf("artifact CID required")
	}
	if cache, ok := r.Source.(legionruntime.CachedArtifactSource); ok {
		script, err := cache.CachedPath(ctx, req.ArtifactCID, "ts")
		if err != nil {
			return legionruntime.Result{}, err
		}
		source, err := os.ReadFile(script)
		if err != nil {
			return legionruntime.Result{}, err
		}
		if persistentModule(source) {
			return r.invokeWorker(ctx, script, req, started)
		}
		return r.invokeScript(ctx, script, req, started)
	}
	return r.invokeOnce(ctx, req, started)
}

func (r *Runtime) invokeWorker(ctx context.Context, script string, req legionruntime.Request, started time.Time) (legionruntime.Result, error) {
	w, err := r.getWorker(script, req.FunctionName)
	if err != nil {
		return legionruntime.Result{}, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	type response struct {
		value workerResponse
		err   error
	}
	result := make(chan response, 1)
	go func() {
		if err := w.stdin.Encode(workerRequest{CallID: req.CallID, Args: req.Args, Env: req.Env}); err != nil {
			result <- response{err: err}
			return
		}
		if !w.stdout.Scan() {
			result <- response{err: fmt.Errorf("Bun worker ended: %s", w.stdout.Err())}
			return
		}
		var value workerResponse
		err := json.Unmarshal(w.stdout.Bytes(), &value)
		result <- response{value: value, err: err}
	}()
	select {
	case <-ctx.Done():
		r.dropWorker(script, w)
		return legionruntime.Result{}, fmt.Errorf("Bun timeout: %w", ctx.Err())
	case value := <-result:
		if value.err != nil {
			r.dropWorker(script, w)
			return legionruntime.Result{}, value.err
		}
		wall := uint64(time.Since(started).Milliseconds())
		return legionruntime.Result{CallID: req.CallID, Output: value.value.Output, Error: value.value.Error, WallMS: wall}, nil
	}
}

func (r *Runtime) getWorker(script, function string) (*worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w := r.workers[script]; w != nil {
		return w, nil
	}
	if r.workerPath == "" {
		dir, err := os.MkdirTemp("", "legion-bun-worker-")
		if err != nil {
			return nil, err
		}
		r.workerPath = filepath.Join(dir, "worker.ts")
		if err = os.WriteFile(r.workerPath, workerSource, 0600); err != nil {
			return nil, err
		}
	}
	cmd := exec.Command(r.Bin, "run", r.workerPath, script)
	cmd.Dir = filepath.Dir(script)
	cmd.Env = allowlistedEnv(nil, function, "worker")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr limitedBuffer
	stderr.max = 64 << 10
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("spawn Bun %s: %w", r.Bin, err)
	}
	w := &worker{cmd: cmd, stdin: json.NewEncoder(input), stdout: bufio.NewScanner(output)}
	w.stdout.Buffer(make([]byte, 64<<10), r.Limits.MaxOutputBytes)
	r.workers[script] = w
	return w, nil
}

func (r *Runtime) dropWorker(script string, w *worker) {
	r.mu.Lock()
	if r.workers[script] == w {
		delete(r.workers, script)
	}
	r.mu.Unlock()
	if w.cmd.Process != nil {
		_ = syscall.Kill(-w.cmd.Process.Pid, syscall.SIGKILL)
	}
	_ = w.cmd.Wait()
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	workers := r.workers
	r.workers = map[string]*worker{}
	path := r.workerPath
	r.workerPath = ""
	r.mu.Unlock()
	for _, w := range workers {
		if w.cmd.Process != nil {
			_ = syscall.Kill(-w.cmd.Process.Pid, syscall.SIGTERM)
		}
		_ = w.cmd.Wait()
	}
	if path != "" {
		return os.RemoveAll(filepath.Dir(path))
	}
	return nil
}

func persistentModule(source []byte) bool {
	text := string(source)
	return strings.Contains(text, "export function run") || strings.Contains(text, "export async function run") || strings.Contains(text, "export default") || strings.Contains(text, "export const run")
}

func (r *Runtime) invokeOnce(ctx context.Context, req legionruntime.Request, started time.Time) (legionruntime.Result, error) {
	source, err := r.Source.Fetch(ctx, req.ArtifactCID)
	if err != nil {
		return legionruntime.Result{}, err
	}
	dir, err := os.MkdirTemp("", "legion-bun-")
	if err != nil {
		return legionruntime.Result{}, err
	}
	defer os.RemoveAll(dir)
	script := filepath.Join(dir, "index.ts")
	if err = os.WriteFile(script, source, 0600); err != nil {
		return legionruntime.Result{}, err
	}
	return r.invokeScript(ctx, script, req, started)
}

func (r *Runtime) invokeScript(ctx context.Context, script string, req legionruntime.Request, started time.Time) (legionruntime.Result, error) {
	var err error
	callCtx, cancel := context.WithTimeout(ctx, r.Limits.Timeout)
	defer cancel()
	cmd := exec.CommandContext(callCtx, r.Bin, "run", script)
	cmd.Dir = filepath.Dir(script)
	cmd.Env = allowlistedEnv(req.Env, req.FunctionName, req.CallID)
	cmd.Stdin = bytes.NewReader(req.Args)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedBuffer
	stdout.max = r.Limits.MaxOutputBytes
	stderr.max = 64 << 10
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		return legionruntime.Result{}, fmt.Errorf("spawn Bun %s: %w", r.Bin, err)
	}
	pid := cmd.Process.Pid
	err = cmd.Wait()
	if callCtx.Err() != nil {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return legionruntime.Result{}, fmt.Errorf("Bun timeout: %w", callCtx.Err())
	}
	if stdout.exceeded {
		return legionruntime.Result{}, fmt.Errorf("Bun output exceeds %d bytes", r.Limits.MaxOutputBytes)
	}
	wall := uint64(time.Since(started).Milliseconds())
	if err != nil {
		return legionruntime.Result{CallID: req.CallID, WallMS: wall, Error: strings.TrimSpace(stderr.String())}, nil
	}
	output := bytes.TrimSpace(stdout.Bytes())
	var value any
	if json.Unmarshal(output, &value) != nil {
		value = map[string]any{"output": string(output)}
	}
	normalized, _ := json.Marshal(value)
	return legionruntime.Result{CallID: req.CallID, Output: normalized, WallMS: wall}, nil
}

type limitedBuffer struct {
	bytes.Buffer
	max      int
	exceeded bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.max - b.Len()
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		_, _ = b.Buffer.Write(p[:remaining])
	}
	if n > remaining {
		b.exceeded = true
	}
	return n, nil
}
func allowlistedEnv(env map[string]string, name, call string) []string {
	names := []string{"PATH", "HOME", "TMPDIR", "LANG", "TZ"}
	out := make([]string, 0, len(names)+len(env)+2)
	for _, n := range names {
		if v, ok := os.LookupEnv(n); ok {
			out = append(out, n+"="+v)
		}
	}
	for k, v := range env {
		if validEnvName(k) {
			out = append(out, k+"="+v)
		}
	}
	return append(out, "LEGION_FUNCTION_NAME="+name, "LEGION_CALL_ID="+call)
}
func validEnvName(k string) bool {
	if k == "" || !(k[0] == '_' || k[0] >= 'A' && k[0] <= 'Z') {
		return false
	}
	for i := 1; i < len(k); i++ {
		c := k[i]
		if !(c == '_' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}
