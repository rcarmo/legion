// Package bun executes deployed JavaScript or TypeScript in isolated Bun subprocesses.
package bun

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

type Runtime struct {
	Bin    string
	Source legionruntime.ArtifactSource
	Limits legionruntime.Limits
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
	return &Runtime{Bin: bin, Source: source, Limits: limits}
}
func (r *Runtime) Invoke(ctx context.Context, req legionruntime.Request) (legionruntime.Result, error) {
	start := time.Now()
	if req.ArtifactCID == "" {
		return legionruntime.Result{}, fmt.Errorf("artifact CID required")
	}
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
	callCtx, cancel := context.WithTimeout(ctx, r.Limits.Timeout)
	defer cancel()
	cmd := exec.CommandContext(callCtx, r.Bin, "run", script)
	cmd.Dir = dir
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
	wall := uint64(time.Since(start).Milliseconds())
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
