package namespace

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/rcarmo/legion/internal/deploy"
	legionruntime "github.com/rcarmo/legion/internal/runtime"
	bunruntime "github.com/rcarmo/legion/internal/runtime/bun"
	"github.com/rcarmo/legion/internal/runtime/joker"
	wasmruntime "github.com/rcarmo/legion/internal/runtime/wasm"
)

func runtimeNamespace(t *testing.T) (*LegionNamespace, func()) {
	t.Helper()
	registry, err := deploy.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	limits := legionruntime.DefaultLimits()
	wasm := wasmruntime.New(registry.CAS(), nil, nil, limits)
	jokerBin := os.Getenv("LEGION_JOKER_TEST_BIN")
	if jokerBin == "" {
		jokerBin = "joker"
	}
	bun := bunruntime.New("", registry.CAS(), limits)
	functions := legionruntime.Functions{Registry: registry, WASM: legionruntime.NewBoundedInvoker(wasm, limits), Bun: legionruntime.NewBoundedInvoker(bun, limits), Joker: legionruntime.NewBoundedInvoker(joker.New(jokerBin, registry.CAS(), limits), limits)}
	tree := NewTree()
	resources := deploy.Resources{Registry: registry, OnRegister: func(manifest legionruntime.Manifest) {
		tree.EnsureDir("/fn/" + manifest.Name)
		_ = tree.SetJSON("/fn/"+manifest.Name+"/manifest.json", manifest)
	}}
	ns := New(tree).WithDeploy(resources).WithFunctions(functions)
	return ns, func() { _ = bun.Close(); _ = wasm.Close(context.Background()) }
}
func deployAndCall(t *testing.T, ns *LegionNamespace, job deploy.Job, want string) {
	t.Helper()
	body, _ := json.Marshal(job)
	if _, err := ns.WritePath(httptest.NewRequest("PUT", "/", nil), "/deploy/register", body); err != nil {
		t.Fatal(err)
	}
	out, err := ns.WritePath(httptest.NewRequest("PUT", "/", nil), "/fn/"+job.Name, []byte(`{"name":"Rui"}`))
	if err != nil {
		t.Fatal(err)
	}
	var result legionruntime.Result
	if err = json.Unmarshal(out, &result); err != nil {
		t.Fatal(err)
	}
	if string(result.Output) != want {
		t.Fatalf("output=%s", result.Output)
	}
	client := tcpPair(t, ns, "")
	ninepOut, err := client.Write("/fn/"+job.Name, []byte(`{"name":"Rui"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(ninepOut, &result); err != nil || string(result.Output) != want {
		t.Fatalf("9p output=%s err=%v", ninepOut, err)
	}
	manifest, err := ns.ReadPath(httptest.NewRequest("GET", "/", nil), "/fn/"+job.Name+"/manifest.json")
	if err != nil || !json.Valid(manifest) {
		t.Fatalf("manifest=%s err=%v", manifest, err)
	}
}
func TestDeployInvokeBunViaNamespace(t *testing.T) {
	if _, err := exec.LookPath("bun"); err != nil {
		t.Skip("Bun unavailable")
	}
	ns, closeRuntime := runtimeNamespace(t)
	defer closeRuntime()
	code := "const i=JSON.parse(await Bun.stdin.text()); console.log(JSON.stringify({greeting: 'Hello, '+i.name+'!'}))"
	deployAndCall(t, ns, deploy.Job{Name: "bun-hello", Runtime: legionruntime.Bun, Code: code}, `{"greeting":"Hello, Rui!"}`)
}
func TestDeployInvokeJokerViaNamespace(t *testing.T) {
	if os.Getenv("LEGION_JOKER_TEST_BIN") == "" {
		t.Skip("set LEGION_JOKER_TEST_BIN")
	}
	ns, closeRuntime := runtimeNamespace(t)
	defer closeRuntime()
	deployAndCall(t, ns, deploy.Job{Name: "joker-hello", Runtime: legionruntime.Joker, Code: `(defn run [args] {"greeting" (str "Hello, " (get args "name") "!")})`}, `{"greeting":"Hello, Rui!"}`)
}
func TestDeployInvokeWASMViaNamespace(t *testing.T) {
	p := filepath.Join("..", "..", "target", "wasm32-wasip1", "release", "wasm_hello.wasm")
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		t.Skip("make wasm-fixture")
	}
	if err != nil {
		t.Fatal(err)
	}
	ns, closeRuntime := runtimeNamespace(t)
	defer closeRuntime()
	deployAndCall(t, ns, deploy.Job{Name: "wasm-hello", Runtime: legionruntime.WASM, WASMBase64: base64.StdEncoding.EncodeToString(b)}, `{"greeting":"Hello, Rui!"}`)
}
