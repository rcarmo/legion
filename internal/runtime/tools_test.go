package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rcarmo/legion/internal/core"
)

type toolManifests struct{ m Manifest }

func (r *toolManifests) Names() []string                          { return []string{r.m.Name} }
func (r *toolManifests) Manifest(name string) (Manifest, bool)    { return r.m, name == r.m.Name }
func (r *toolManifests) Resolve(string, string) (Manifest, error) { return r.m, nil }

type toolInvoker struct {
	result Result
	calls  int
}

func (i *toolInvoker) Invoke(context.Context, Request) (Result, error) {
	i.calls++
	return i.result, nil
}
func TestFunctionTools(t *testing.T) {
	ctx := context.Background()
	r := &toolManifests{m: Manifest{Name: "echo", Runtime: Bun, Idempotent: true}}
	i := &toolInvoker{result: Result{Output: json.RawMessage(`{"value":42}`)}}
	tools := FunctionTools{Registry: r, Functions: Functions{Registry: r, Bun: i}, Base: core.EchoToolRegistry{}}
	defs, err := tools.Definitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := defs[len(defs)-1]
	if d.Name != "fn.echo" || d.Effect != core.EffectIdempotent {
		t.Fatal(d)
	}
	r.m.Idempotent = false
	defs, _ = tools.Definitions(ctx)
	if defs[len(defs)-1].Effect != core.EffectWrite {
		t.Fatal("stale manifest")
	}
	out, err := tools.Dispatch(ctx, "fn.echo", json.RawMessage(`{}`))
	if err != nil || string(out) != `{"value":42}` {
		t.Fatal(string(out), err)
	}
	i.result.Error = "runtime failed"
	if _, err = tools.Dispatch(ctx, "fn.echo", json.RawMessage(`{}`)); err == nil || !strings.Contains(err.Error(), "runtime failed") {
		t.Fatal(err)
	}
	if i.calls != 2 {
		t.Fatal("unexpected dispatch count", i.calls)
	}
	out, err = tools.Dispatch(ctx, "echo", json.RawMessage(`{"builtin":true}`))
	if err != nil || !strings.Contains(string(out), "builtin") {
		t.Fatal(string(out), err)
	}
}
