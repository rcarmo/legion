package telemetry

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rcarmo/legion/internal/core"
)

type FunctionMetrics struct {
	mu             sync.Mutex
	values         map[string]*functionValue
	rateRejections atomic.Uint64
}
type functionValue struct {
	calls, errors, wall uint64
	durations           []uint64
}

func NewFunctionMetrics() *FunctionMetrics {
	return &FunctionMetrics{values: map[string]*functionValue{}}
}
func (m *FunctionMetrics) Record(name string, wall uint64, failed bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	v := m.values[name]
	if v == nil {
		v = &functionValue{}
		m.values[name] = v
	}
	v.calls++
	v.wall += wall
	const retainedDurations = 4096
	if len(v.durations) < retainedDurations {
		v.durations = append(v.durations, wall)
	} else {
		v.durations[v.calls%retainedDurations] = wall
	}
	if failed {
		v.errors++
	}
	m.mu.Unlock()
}
func (m *FunctionMetrics) Reject() {
	if m != nil {
		m.rateRejections.Add(1)
	}
}
func (m *FunctionMetrics) Render() string {
	if m == nil {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.values))
	for n := range m.values {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("# TYPE legion_function_invocations_total counter\n# TYPE legion_function_wall_ms_total counter\n# TYPE legion_function_duration_ms gauge\n")
	for _, n := range names {
		v := m.values[n]
		label := promLabel(n)
		fmt.Fprintf(&b, "legion_function_invocations_total{function=\"%s\",outcome=\"success\"} %d\n", label, v.calls-v.errors)
		fmt.Fprintf(&b, "legion_function_invocations_total{function=\"%s\",outcome=\"error\"} %d\n", label, v.errors)
		fmt.Fprintf(&b, "legion_function_wall_ms_total{function=\"%s\"} %d\n", label, v.wall)
		ordered := append([]uint64(nil), v.durations...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
		if len(ordered) > 0 {
			fmt.Fprintf(&b, "legion_function_duration_ms{function=\"%s\",quantile=\"0.50\"} %d\n", label, quantile(ordered, 0.50))
			fmt.Fprintf(&b, "legion_function_duration_ms{function=\"%s\",quantile=\"0.95\"} %d\n", label, quantile(ordered, 0.95))
			fmt.Fprintf(&b, "legion_function_duration_ms{function=\"%s\",quantile=\"0.99\"} %d\n", label, quantile(ordered, 0.99))
		}
	}
	fmt.Fprintf(&b, "legion_function_rate_limit_rejections_total %d\n", m.rateRejections.Load())
	return b.String()
}
func quantile(values []uint64, q float64) uint64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values))*q+0.999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
func promLabel(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n").Replace(s)
}

type StoreMetrics struct {
	Store             core.EventStore
	Functions         *FunctionMetrics
	SessionRejections func() uint64
}

func (m StoreMetrics) Render(ctx context.Context) (string, error) {
	var b strings.Builder
	b.WriteString(m.Functions.Render())
	type totals struct{ turns, input, output, wall uint64 }
	byModel := map[string]*totals{}
	offset := 0
	for {
		sessions, err := m.Store.ListSessions(ctx, core.SessionFilter{Limit: 100, Offset: offset})
		if err != nil {
			return "", err
		}
		if len(sessions) == 0 {
			break
		}
		offset += len(sessions)
		for _, s := range sessions {
			v := byModel[s.Model]
			if v == nil {
				v = &totals{}
				byModel[s.Model] = v
			}
			log, err := m.Store.ReadLog(ctx, s.RunID)
			if err != nil {
				return "", err
			}
			for _, e := range log {
				if e.Event.Kind.Kind != "assistant_message" {
					continue
				}
				v.turns++
				if e.Event.TokensIn != nil {
					v.input += uint64(*e.Event.TokensIn)
				}
				if e.Event.TokensOut != nil {
					v.output += uint64(*e.Event.TokensOut)
				}
				if e.Event.WallMS != nil {
					v.wall += *e.Event.WallMS
				}
			}
		}
	}
	names := make([]string, 0, len(byModel))
	for n := range byModel {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		v := byModel[n]
		provider := "unknown"
		if value, _, ok := strings.Cut(n, "/"); ok {
			provider = value
		}
		label := promLabel(n)
		providerLabel := promLabel(provider)
		fmt.Fprintf(&b, "legion_session_turns_total{provider=\"%s\",model=\"%s\"} %d\n", providerLabel, label, v.turns)
		fmt.Fprintf(&b, "legion_session_tokens_total{provider=\"%s\",model=\"%s\",direction=\"input\"} %d\n", providerLabel, label, v.input)
		fmt.Fprintf(&b, "legion_session_tokens_total{provider=\"%s\",model=\"%s\",direction=\"output\"} %d\n", providerLabel, label, v.output)
		fmt.Fprintf(&b, "legion_session_turn_wall_ms_total{provider=\"%s\",model=\"%s\"} %d\n", providerLabel, label, v.wall)
	}
	if m.SessionRejections != nil {
		fmt.Fprintf(&b, "legion_session_rate_limit_rejections_total %d\n", m.SessionRejections())
	}
	return b.String(), nil
}
