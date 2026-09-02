package telemetry

import (
	"context"
	"strings"
	"testing"

	"github.com/rcarmo/legion/internal/core"
)

func TestMetricsRenderDurableTokensLatencyAndFunctionQuantiles(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryEventStore()
	id := core.RunID([16]byte{1})
	if err := store.CreateSession(ctx, id, core.RunConfig{Model: "fixture/model"}); err != nil {
		t.Fatal(err)
	}
	for _, wall := range []uint64{10, 30} {
		if _, err := store.Append(ctx, id, core.AssistantMessage(map[string]string{"content": "ok"}, "fixture/model", 4, 2, wall)); err != nil {
			t.Fatal(err)
		}
	}
	functions := NewFunctionMetrics()
	functions.Record("echo", 5, false)
	functions.Record("echo", 15, true)
	body, err := (StoreMetrics{Store: store, Functions: functions}).Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`legion_function_duration_ms{function="echo",quantile="0.95"} 15`,
		`legion_session_turns_total{provider="fixture",model="fixture/model"} 2`,
		`legion_session_tokens_total{provider="fixture",model="fixture/model",direction="input"} 8`,
		`legion_session_turn_wall_ms_total{provider="fixture",model="fixture/model"} 40`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}
