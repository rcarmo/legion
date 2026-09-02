package telemetry

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
)

type Providers struct {
	traces  *sdktrace.TracerProvider
	metrics *sdkmetric.MeterProvider
}

func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}
func Init(ctx context.Context, service, node string) (*Providers, error) {
	if !Enabled() {
		return &Providers{}, nil
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service), attribute.String("service.instance.id", node)))
	if err != nil {
		return nil, err
	}
	te, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	me, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(te), sdktrace.WithResource(res))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me, sdkmetric.WithInterval(250*time.Millisecond))), sdkmetric.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	return &Providers{traces: tp, metrics: mp}, nil
}
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	if p == nil {
		return nil
	}
	if p.metrics != nil {
		errs = append(errs, p.metrics.ForceFlush(ctx), p.metrics.Shutdown(ctx))
	}
	if p.traces != nil {
		errs = append(errs, p.traces.ForceFlush(ctx), p.traces.Shutdown(ctx))
	}
	return errors.Join(errs...)
}
func Tracer() trace.Tracer { return otel.Tracer("legion.agent-loop") }

type Instruments struct{ input, output, cacheRead, cacheWrite metric.Int64Counter }

func NewInstruments() (Instruments, error) {
	m := otel.Meter("legion.agent-loop")
	input, e := m.Int64Counter("legion.agent.tokens.input")
	if e != nil {
		return Instruments{}, e
	}
	output, e := m.Int64Counter("legion.agent.tokens.output")
	if e != nil {
		return Instruments{}, e
	}
	cr, e := m.Int64Counter("legion.agent.tokens.cache_read")
	if e != nil {
		return Instruments{}, e
	}
	cw, e := m.Int64Counter("legion.agent.tokens.cache_write")
	return Instruments{input, output, cr, cw}, e
}
func (i Instruments) RecordUsage(ctx context.Context, model, outcome string, input, output, cacheRead, cacheWrite int) {
	provider := "unknown"
	if value, _, ok := strings.Cut(model, "/"); ok {
		provider = value
	}
	attrs := metric.WithAttributes(attribute.String("gen_ai.provider.name", provider), attribute.String("gen_ai.request.model", model), attribute.String("outcome", outcome))
	i.input.Add(ctx, int64(max(input, 0)), attrs)
	i.output.Add(ctx, int64(max(output, 0)), attrs)
	i.cacheRead.Add(ctx, int64(max(cacheRead, 0)), attrs)
	i.cacheWrite.Add(ctx, int64(max(cacheWrite, 0)), attrs)
}
