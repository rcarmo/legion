#[path = "../telemetry.rs"]
mod telemetry;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> anyhow::Result<()> {
    let providers = telemetry::TelemetryProviders::init("legion-otel-probe", "test-node")?
        .expect("OTEL endpoint must be configured");
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(providers.tracer.clone()))
        .init();
    let span = tracing::info_span!("agent.resolve", model = "test/faux");
    let entered = span.enter();
    legion_loop::telemetry::record_token_usage("test/faux", 11, 7, 5, 3);
    drop(entered);
    drop(span);
    drop(providers);
    Ok(())
}
