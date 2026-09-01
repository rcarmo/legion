// Package joker supervises isolated Joker worker processes.
package joker

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

const Revision = "edd0fe7fff7b2bae3a714a9918502f7dd3b21d5f"

type Runtime struct {
	Bin    string
	Source legionruntime.ArtifactSource
	Limits legionruntime.Limits
}
type envelope struct {
	FunctionName string            `json:"function_name"`
	CallID       string            `json:"call_id"`
	Args         json.RawMessage   `json:"args"`
	Env          map[string]string `json:"env,omitempty"`
}
type workerResponse struct {
	CallID string          `json:"call_id"`
	Output json.RawMessage `json:"output"`
	Error  string          `json:"error,omitempty"`
}

func New(bin string, source legionruntime.ArtifactSource, limits legionruntime.Limits) *Runtime {
	if bin == "" {
		bin = os.Getenv("LEGION_JOKER_BIN")
	}
	if bin == "" {
		bin = "joker"
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
	dir, err := os.MkdirTemp("", "legion-joker-")
	if err != nil {
		return legionruntime.Result{}, err
	}
	defer os.RemoveAll(dir)
	script := filepath.Join(dir, "function.joke")
	if err = os.WriteFile(script, source, 0600); err != nil {
		return legionruntime.Result{}, err
	}
	worker := filepath.Join(dir, "worker.joke")
	if err = os.WriteFile(worker, []byte(workerSource), 0600); err != nil {
		return legionruntime.Result{}, err
	}
	input, _ := json.Marshal(envelope{req.FunctionName, req.CallID, req.Args, req.Env})
	input = append(input, '\n')
	callCtx, cancel := context.WithTimeout(ctx, r.Limits.Timeout)
	defer cancel()
	cmd := exec.CommandContext(callCtx, r.Bin, worker, script)
	cmd.Dir = dir
	cmd.Env = allowlistedEnv(req.Env, req.FunctionName, req.CallID)
	cmd.Stdin = bytes.NewReader(input)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedBuffer
	stdout.max = r.Limits.MaxOutputBytes
	stderr.max = 64 << 10
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		return legionruntime.Result{}, fmt.Errorf("spawn Joker %s: %w", r.Bin, err)
	}
	pid := cmd.Process.Pid
	err = cmd.Wait()
	if callCtx.Err() != nil {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return legionruntime.Result{}, fmt.Errorf("Joker timeout: %w", callCtx.Err())
	}
	if stdout.exceeded {
		return legionruntime.Result{}, fmt.Errorf("Joker output exceeds %d bytes", r.Limits.MaxOutputBytes)
	}
	if err != nil {
		return legionruntime.Result{}, fmt.Errorf("Joker failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	line := bytes.TrimSpace(stdout.Bytes())
	var response workerResponse
	if err = json.Unmarshal(line, &response); err != nil {
		return legionruntime.Result{}, fmt.Errorf("invalid Joker NDJSON response: %w", err)
	}
	wall := uint64(time.Since(start).Milliseconds())
	if response.Error != "" {
		return legionruntime.Result{CallID: req.CallID, Output: response.Output, WallMS: wall, Error: response.Error}, nil
	}
	return legionruntime.Result{CallID: req.CallID, Output: response.Output, WallMS: wall}, nil
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
	out := make([]string, 0, len(names)+len(env)+3)
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
	out = append(out, "LEGION_FUNCTION_NAME="+name, "LEGION_CALL_ID="+call, "LEGION_JOKER_REVISION="+Revision)
	return out
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

const workerSource = `(require '[joker.json :as json])
(def request (json/read-string (read-line)))
(defn respond [value error]
  (println (json/write-string {"call_id" (get request "call_id") "output" value "error" error})))
(try
  (load-file (first *command-line-args*))
  (respond (apply (resolve (symbol "run")) [(get request "args")]) nil)
  (catch Error e (respond nil (str e))))
`
