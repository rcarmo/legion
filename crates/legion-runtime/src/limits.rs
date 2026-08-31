//! Shared invocation limits and metrics for all runtime backends.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use legion_core::error::{LegionError, Result};

use crate::invoke::{InvokeRequest, InvokeResult, Invoker};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InvocationLimits {
    pub timeout_ms: u64,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_concurrent_per_function: usize,
    pub max_requests_per_window: u32,
    pub rate_window_ms: u64,
}

impl Default for InvocationLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_input_bytes: 1_048_576,
            max_output_bytes: 4_194_304,
            max_concurrent_per_function: 8,
            max_requests_per_window: 120,
            rate_window_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct InvocationMetric {
    pub count: u64,
    pub wall_ms_total: u64,
}

#[derive(Default)]
pub struct InvocationMetrics {
    values: Mutex<BTreeMap<(String, String, String), InvocationMetric>>,
}

impl InvocationMetrics {
    fn record(&self, function: &str, runtime: &str, outcome: &str, wall_ms: u64) {
        let mut values = self
            .values
            .lock()
            .expect("invocation metrics lock poisoned");
        let metric = values
            .entry((function.into(), runtime.into(), outcome.into()))
            .or_default();
        metric.count += 1;
        metric.wall_ms_total += wall_ms;
    }

    pub fn render_prometheus(&self) -> String {
        let values = self
            .values
            .lock()
            .expect("invocation metrics lock poisoned");
        let mut output = String::from(
            "# HELP legion_function_invocations_total Function invocations by outcome.\n\
             # TYPE legion_function_invocations_total counter\n",
        );
        for ((function, runtime, outcome), metric) in values.iter() {
            let labels = format!(
                "function=\"{}\",runtime=\"{}\",outcome=\"{}\"",
                escape_label(function),
                escape_label(runtime),
                escape_label(outcome),
            );
            output.push_str(&format!(
                "legion_function_invocations_total{{{labels}}} {}\n",
                metric.count
            ));
        }
        output.push_str(
            "# HELP legion_function_invocation_wall_ms_total Total function invocation wall time.\n\
             # TYPE legion_function_invocation_wall_ms_total counter\n",
        );
        for ((function, runtime, outcome), metric) in values.iter() {
            let labels = format!(
                "function=\"{}\",runtime=\"{}\",outcome=\"{}\"",
                escape_label(function),
                escape_label(runtime),
                escape_label(outcome),
            );
            output.push_str(&format!(
                "legion_function_invocation_wall_ms_total{{{labels}}} {}\n",
                metric.wall_ms_total
            ));
        }
        output
    }
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub struct BoundedInvoker {
    inner: Arc<dyn Invoker>,
    runtime: String,
    limits: InvocationLimits,
    metrics: Arc<InvocationMetrics>,
    semaphores: Mutex<BTreeMap<String, Arc<Semaphore>>>,
    rate_windows: Mutex<BTreeMap<String, RateWindow>>,
}

struct RateWindow {
    started: Instant,
    count: u32,
}

impl BoundedInvoker {
    pub fn new(
        inner: Arc<dyn Invoker>,
        runtime: impl Into<String>,
        limits: InvocationLimits,
        metrics: Arc<InvocationMetrics>,
    ) -> Self {
        Self {
            inner,
            runtime: runtime.into(),
            limits,
            metrics,
            semaphores: Mutex::new(BTreeMap::new()),
            rate_windows: Mutex::new(BTreeMap::new()),
        }
    }

    fn check_rate(&self, function: &str) -> Result<()> {
        if self.limits.max_requests_per_window == 0 || self.limits.rate_window_ms == 0 {
            return Ok(());
        }
        let now = Instant::now();
        let duration = Duration::from_millis(self.limits.rate_window_ms);
        let mut windows = self.rate_windows.lock().expect("invocation rate lock poisoned");
        windows.retain(|_, window| now.duration_since(window.started) < duration);
        let window = windows.entry(function.into()).or_insert(RateWindow {
            started: now,
            count: 0,
        });
        let elapsed = now.duration_since(window.started);
        if elapsed >= duration {
            window.started = now;
            window.count = 0;
        }
        if window.count >= self.limits.max_requests_per_window {
            let retry_after_ms = duration.saturating_sub(now.duration_since(window.started))
                .as_millis().max(1) as u64;
            self.metrics.record(function, &self.runtime, "rate_limited", 0);
            return Err(LegionError::InvocationRateLimited {
                function: function.into(),
                retry_after_ms,
            });
        }
        window.count += 1;
        Ok(())
    }

    fn semaphore(&self, function: &str) -> Arc<Semaphore> {
        let mut semaphores = self
            .semaphores
            .lock()
            .expect("invocation semaphore lock poisoned");
        semaphores
            .entry(function.into())
            .or_insert_with(|| Arc::new(Semaphore::new(self.limits.max_concurrent_per_function)))
            .clone()
    }
}

#[async_trait]
impl Invoker for BoundedInvoker {
    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResult> {
        let function = request.function_name.clone();
        let input_bytes = serde_json::to_vec(&request.args)?.len();
        if input_bytes > self.limits.max_input_bytes {
            self.metrics
                .record(&function, &self.runtime, "input_limit", 0);
            return Err(LegionError::InvocationLimitExceeded {
                function,
                field: "input_bytes",
                actual: input_bytes,
                limit: self.limits.max_input_bytes,
            });
        }
        self.check_rate(&function)?;

        let permit = self.semaphore(&function).try_acquire_owned().map_err(|_| {
            self.metrics.record(&function, &self.runtime, "busy", 0);
            LegionError::InvocationBusy(function.clone())
        })?;

        let start = Instant::now();
        let invocation = tokio::time::timeout(
            Duration::from_millis(self.limits.timeout_ms),
            self.inner.invoke(request),
        )
        .await;
        drop(permit);
        let wall_ms = start.elapsed().as_millis() as u64;

        let result = match invocation {
            Err(_) => {
                self.metrics
                    .record(&function, &self.runtime, "timeout", wall_ms);
                return Err(LegionError::InvocationTimeout {
                    function,
                    timeout_ms: self.limits.timeout_ms,
                });
            }
            Ok(Err(error)) => {
                self.metrics
                    .record(&function, &self.runtime, "error", wall_ms);
                return Err(error);
            }
            Ok(Ok(result)) => result,
        };

        let output_bytes = serde_json::to_vec(&result.output)?.len();
        if output_bytes > self.limits.max_output_bytes {
            self.metrics
                .record(&function, &self.runtime, "output_limit", wall_ms);
            return Err(LegionError::InvocationLimitExceeded {
                function,
                field: "output_bytes",
                actual: output_bytes,
                limit: self.limits.max_output_bytes,
            });
        }

        let outcome = if result.error.is_some() {
            "error"
        } else {
            "success"
        };
        self.metrics
            .record(&function, &self.runtime, outcome, wall_ms);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    struct FakeInvoker {
        delay_ms: u64,
        output: Value,
    }

    #[async_trait]
    impl Invoker for FakeInvoker {
        async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResult> {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            Ok(InvokeResult {
                call_id: request.call_id,
                output: self.output.clone(),
                wall_ms: self.delay_ms,
                error: None,
            })
        }
    }

    fn request(args: Value) -> InvokeRequest {
        InvokeRequest {
            function_name: "test".into(),
            call_id: "call".into(),
            artifact_cid: None,
            args,
        }
    }

    #[tokio::test]
    async fn rejects_oversized_input_and_records_metric() {
        let metrics = Arc::new(InvocationMetrics::default());
        let invoker = BoundedInvoker::new(
            Arc::new(FakeInvoker {
                delay_ms: 0,
                output: Value::Null,
            }),
            "test",
            InvocationLimits {
                max_input_bytes: 4,
                ..Default::default()
            },
            metrics.clone(),
        );
        let error = invoker
            .invoke(request(json!({"long":"value"})))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            LegionError::InvocationLimitExceeded {
                field: "input_bytes",
                ..
            }
        ));
        assert!(
            metrics
                .render_prometheus()
                .contains("outcome=\"input_limit\"")
        );
    }

    #[tokio::test]
    async fn enforces_timeout() {
        let metrics = Arc::new(InvocationMetrics::default());
        let invoker = BoundedInvoker::new(
            Arc::new(FakeInvoker {
                delay_ms: 50,
                output: Value::Null,
            }),
            "test",
            InvocationLimits {
                timeout_ms: 5,
                ..Default::default()
            },
            metrics,
        );
        let error = invoker.invoke(request(Value::Null)).await.unwrap_err();
        assert!(matches!(error, LegionError::InvocationTimeout { .. }));
    }

    #[tokio::test]
    async fn rate_limits_each_function_independently() {
        let metrics = Arc::new(InvocationMetrics::default());
        let invoker = BoundedInvoker::new(
            Arc::new(FakeInvoker { delay_ms: 0, output: Value::Null }),
            "test",
            InvocationLimits {
                max_requests_per_window: 1,
                rate_window_ms: 60_000,
                ..Default::default()
            },
            metrics.clone(),
        );
        invoker.invoke(request(Value::Null)).await.unwrap();

        let error = invoker.invoke(request(Value::Null)).await.unwrap_err();
        assert!(matches!(error, LegionError::InvocationRateLimited { .. }));
        assert!(metrics.render_prometheus().contains("outcome=\"rate_limited\""));

        let mut other = request(Value::Null);
        other.function_name = "other".into();
        invoker.invoke(other).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_concurrent_call_for_same_function() {
        let invoker = Arc::new(BoundedInvoker::new(
            Arc::new(FakeInvoker { delay_ms: 50, output: Value::Null }),
            "test",
            InvocationLimits { max_concurrent_per_function: 1, ..Default::default() },
            Arc::new(InvocationMetrics::default()),
        ));
        let first = {
            let invoker = invoker.clone();
            tokio::spawn(async move { invoker.invoke(request(Value::Null)).await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;

        let error = invoker.invoke(request(Value::Null)).await.unwrap_err();
        assert!(matches!(error, LegionError::InvocationBusy(_)));
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejects_oversized_output() {
        let invoker = BoundedInvoker::new(
            Arc::new(FakeInvoker {
                delay_ms: 0,
                output: json!({"long":"value"}),
            }),
            "test",
            InvocationLimits {
                max_output_bytes: 4,
                ..Default::default()
            },
            Arc::new(InvocationMetrics::default()),
        );
        let error = invoker.invoke(request(Value::Null)).await.unwrap_err();
        assert!(matches!(
            error,
            LegionError::InvocationLimitExceeded {
                field: "output_bytes",
                ..
            }
        ));
    }
}
