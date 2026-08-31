//! Optional OTLP/HTTP traces and metrics configured through standard OTEL env vars.

use anyhow::Result;
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::{MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::SdkMeterProvider,
    trace::{SdkTracerProvider, Tracer},
};

pub struct TelemetryProviders {
    pub tracer: Tracer,
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl TelemetryProviders {
    pub fn init(service_name: &str, node: &str) -> Result<Option<Self>> {
        if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none()
            && std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_none()
            && std::env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_none()
        {
            return Ok(None);
        }
        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .with_attribute(opentelemetry::KeyValue::new(
                "service.instance.id",
                node.to_owned(),
            ))
            .build();
        let span_exporter = SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(span_exporter)
            .build();
        let tracer = tracer_provider.tracer("legion-server");
        global::set_tracer_provider(tracer_provider.clone());

        let metric_exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(metric_exporter)
            .build();
        global::set_meter_provider(meter_provider.clone());

        Ok(Some(Self {
            tracer,
            tracer_provider,
            meter_provider,
        }))
    }
}

impl Drop for TelemetryProviders {
    fn drop(&mut self) {
        let _ = self.meter_provider.force_flush();
        let _ = self.tracer_provider.force_flush();
        let _ = self.meter_provider.shutdown();
        let _ = self.tracer_provider.shutdown();
    }
}
