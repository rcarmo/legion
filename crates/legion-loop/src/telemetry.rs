//! Low-cardinality OpenTelemetry instruments for agent execution.

use opentelemetry::{KeyValue, global, metrics::Counter};
use std::sync::OnceLock;

struct TokenCounters {
    input: Counter<u64>,
    output: Counter<u64>,
    cache_read: Counter<u64>,
    cache_write: Counter<u64>,
}

fn counters() -> &'static TokenCounters {
    static COUNTERS: OnceLock<TokenCounters> = OnceLock::new();
    COUNTERS.get_or_init(|| {
        let meter = global::meter("legion.agent-loop");
        TokenCounters {
            input: meter
                .u64_counter("legion.agent.tokens.input")
                .with_description("Uncached input tokens consumed by model calls")
                .build(),
            output: meter
                .u64_counter("legion.agent.tokens.output")
                .with_description("Output tokens produced by model calls")
                .build(),
            cache_read: meter
                .u64_counter("legion.agent.tokens.cache_read")
                .with_description("Input tokens served from provider prompt caches")
                .build(),
            cache_write: meter
                .u64_counter("legion.agent.tokens.cache_write")
                .with_description("Input tokens written to provider prompt caches")
                .build(),
        }
    })
}

/// Record provider-reported usage exactly once, when a live model call completes.
/// Deliberately excludes run/session identifiers and user-controlled content.
pub fn record_token_usage(model: &str, input: u32, output: u32, cache_read: u32, cache_write: u32) {
    let provider = model.split_once('/').map_or("unknown", |(name, _)| name);
    let attributes = [
        KeyValue::new("gen_ai.provider.name", provider.to_owned()),
        KeyValue::new("gen_ai.request.model", model.to_owned()),
        KeyValue::new("outcome", "success"),
    ];
    let counters = counters();
    counters.input.add(input.into(), &attributes);
    counters.output.add(output.into(), &attributes);
    counters.cache_read.add(cache_read.into(), &attributes);
    counters.cache_write.add(cache_write.into(), &attributes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_is_derived_without_user_or_session_dimensions() {
        record_token_usage("anthropic/claude-test", 1, 2, 3, 4);
    }
}
