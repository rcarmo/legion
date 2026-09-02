//go:build oteltest

package telemetry_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	goai "github.com/rcarmo/go-ai"
	"github.com/rcarmo/legion/internal/agent"
	"github.com/rcarmo/legion/internal/core"
	legiontelemetry "github.com/rcarmo/legion/internal/telemetry"
	collectormetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type usageInference struct{}

func (usageInference) Stream(context.Context, string, *goai.Context, *goai.StreamOptions) (<-chan goai.Event, error) {
	message := &goai.Message{
		Role:       goai.RoleAssistant,
		Content:    []goai.ContentBlock{{Type: "text", Text: "done"}},
		StopReason: goai.StopReasonStop,
		Usage:      &goai.Usage{Input: 11, Output: 7, CacheRead: 3, CacheWrite: 4, TotalTokens: 25},
	}
	out := make(chan goai.Event, 1)
	out <- &goai.DoneEvent{Reason: message.StopReason, Message: message}
	close(out)
	return out, nil
}

type capture struct {
	mu      sync.Mutex
	traces  [][]byte
	metrics [][]byte
}

func (c *capture) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	switch r.URL.Path {
	case "/v1/traces":
		c.traces = append(c.traces, body)
	case "/v1/metrics":
		c.metrics = append(c.metrics, body)
	}
	c.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func TestAgentLifecycleAndTokenUsageReachOTLP(t *testing.T) {
	captured := &capture{}
	server := httptest.NewServer(http.HandlerFunc(captured.handler))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	ctx := context.Background()
	providers, err := legiontelemetry.Init(ctx, "legion-otel-test", "node-test")
	if err != nil {
		t.Fatal(err)
	}
	store := core.NewMemoryEventStore()
	loop := agent.New(store, core.EchoToolRegistry{}, usageInference{})
	id, err := loop.Start(ctx, core.RunConfig{Model: "fixture/model", Budget: core.Budget{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = loop.Resume(ctx, id, core.UserMessage("exercise telemetry")); err != nil {
		t.Fatal(err)
	}
	if _, err = loop.Resolve(ctx, id); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = providers.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}

	captured.mu.Lock()
	traces := append([][]byte(nil), captured.traces...)
	metrics := append([][]byte(nil), captured.metrics...)
	captured.mu.Unlock()
	spanNames := map[string]bool{}
	for _, body := range traces {
		var request collectortrace.ExportTraceServiceRequest
		if err = proto.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode traces: %v", err)
		}
		for _, resource := range request.ResourceSpans {
			for _, scope := range resource.ScopeSpans {
				for _, span := range scope.Spans {
					spanNames[span.Name] = true
					for _, attribute := range span.Attributes {
						assertSafeAttribute(t, attribute.Key)
					}
				}
			}
		}
	}
	for _, want := range []string{"agent.start", "agent.resume", "agent.resolve"} {
		if !spanNames[want] {
			t.Fatalf("OTLP traces missing %q; got %v", want, spanNames)
		}
	}

	values := map[string]int64{}
	for _, body := range metrics {
		var request collectormetric.ExportMetricsServiceRequest
		if err = proto.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode metrics: %v", err)
		}
		for _, resource := range request.ResourceMetrics {
			for _, scope := range resource.ScopeMetrics {
				for _, metric := range scope.Metrics {
					var latest int64
					for _, point := range metric.GetSum().DataPoints {
						for _, attribute := range point.Attributes {
							assertSafeAttribute(t, attribute.Key)
						}
						latest += point.GetAsInt()
					}
					if latest > values[metric.Name] {
						values[metric.Name] = latest
					}
				}
			}
		}
	}
	wantMetrics := map[string]int64{
		"legion.agent.tokens.input":       11,
		"legion.agent.tokens.output":      7,
		"legion.agent.tokens.cache_read":  3,
		"legion.agent.tokens.cache_write": 4,
	}
	for name, want := range wantMetrics {
		if values[name] != want {
			t.Fatalf("OTLP metric %s=%d, want %d (all=%v)", name, values[name], want, values)
		}
	}
}

func assertSafeAttribute(t *testing.T, key string) {
	t.Helper()
	lower := strings.ToLower(key)
	for _, forbidden := range []string{"session", "run_id", "prompt", "content", "user"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("high-cardinality or content attribute exported: %q", key)
		}
	}
}
