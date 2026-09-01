package joker

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

type source []byte

func (s source) Fetch(context.Context, string) ([]byte, error) { return append([]byte(nil), s...), nil }
func jokerBin(t *testing.T) string {
	t.Helper()
	if b := os.Getenv("LEGION_JOKER_TEST_BIN"); b != "" {
		return b
	}
	for _, b := range []string{"joker", "/tmp/joker"} {
		if p, err := exec.LookPath(b); err == nil {
			return p
		}
	}
	t.Skip("Joker test binary unavailable; set LEGION_JOKER_TEST_BIN")
	return ""
}
func TestInvokeNDJSONAndEnvironment(t *testing.T) {
	code := source(`(require '[joker.os :as os])
(defn run [args] {"name" (get args "name") "declared" (os/get-env "GREETING") "function" (os/get-env "LEGION_FUNCTION_NAME")})`)
	r := New(jokerBin(t), code, legionruntime.DefaultLimits())
	result, err := r.Invoke(context.Background(), legionruntime.Request{FunctionName: "hello", CallID: "call-1", ArtifactCID: "cid", Args: json.RawMessage(`{"name":"Rui"}`), Env: map[string]string{"GREETING": "hi", "bad-name": "leak"}})
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]string
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	if err = json.Unmarshal(result.Output, &output); err != nil {
		t.Fatal(err)
	}
	if output["name"] != "Rui" || output["declared"] != "hi" || output["function"] != "hello" {
		t.Fatal(output)
	}
}
func TestTimeoutKillsWorker(t *testing.T) {
	limits := legionruntime.DefaultLimits()
	limits.Timeout = 50 * time.Millisecond
	r := New(jokerBin(t), source(`(require '[joker.time :as time]) (defn run [args] (do (time/sleep 10000000000) args))`), limits)
	_, err := r.Invoke(context.Background(), legionruntime.Request{FunctionName: "slow", CallID: "call", ArtifactCID: "cid", Args: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected timeout")
	}
}
