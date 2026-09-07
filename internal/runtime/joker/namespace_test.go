package joker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	legionruntime "github.com/rcarmo/legion/internal/runtime"
)

func TestNamespaceHelpers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			http.Error(w, "denied", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /namespace/cluster/self":
			_, _ = io.WriteString(w, `{"node":"local"}`)
		case "PUT /namespace/sessions/new":
			b, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(b), "test/model") {
				http.Error(w, "bad config", 400)
				return
			}
			_, _ = io.WriteString(w, `{"run_id":"child"}`)
		case "POST /functions/echo/invoke":
			b, _ := io.ReadAll(r.Body)
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	code := source(`(defn run [args] {"self" (legion/read-json "/cluster/self") "child" (legion/write-json "/sessions/new" {"model" "test/model"}) "result" (legion/invoke "echo" args)})`)
	runtime := New(jokerBin(t), code, legionruntime.DefaultLimits())
	req := legionruntime.Request{FunctionName: "test", ArtifactCID: "cid", CallID: "test", Args: json.RawMessage(`{"v":42}`), Env: map[string]string{"LEGION_URL": server.URL, "LEGION_API_KEY": "test-key"}}
	result, err := runtime.Invoke(context.Background(), req)
	if err != nil || result.Error != "" {
		t.Fatal(err, result.Error)
	}
	var out map[string]json.RawMessage
	if err = json.Unmarshal(result.Output, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out["self"]), "local") || !strings.Contains(string(out["child"]), "child") || !strings.Contains(string(out["result"]), "42") {
		t.Fatal(string(result.Output))
	}
	req.Env["LEGION_API_KEY"] = "wrong"
	result, err = runtime.Invoke(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Error, "401") {
		t.Fatal("wrong credential accepted", result)
	}
}
