//! Dynamic Legion resource handlers backing the 9P namespace.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use legion_core::{
    error::{LegionError, Result},
    traits::{AgentLoopTrait, EventStore},
    types::{ExternalEvent, RunConfig, RunId, SeqNum, SessionStatus},
};

/// Optional application hooks for dynamic namespace paths.
#[async_trait]
pub trait NamespaceResources: Send + Sync {
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>>;
    async fn write(&self, path: &str, data: &[u8]) -> Result<Option<Vec<u8>>>;
}

/// Transport-neutral peer namespace used by `/peers/<key>/...`.
#[async_trait]
pub trait PeerNamespace: Send + Sync {
    async fn read(&self, path: &str) -> std::io::Result<Vec<u8>>;
    async fn write(&self, path: &str, data: &[u8]) -> std::io::Result<Vec<u8>>;
}

/// Application hook for `/fn/<name>` invocation paths.
#[async_trait]
pub trait FunctionNamespace: Send + Sync {
    async fn invoke(&self, name: &str, data: &[u8]) -> Result<Vec<u8>>;
}

/// Application hook for dynamic deployment namespace paths.
#[async_trait]
pub trait DeployNamespace: Send + Sync {
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>>;
    async fn write(&self, path: &str, data: &[u8]) -> Result<Option<Vec<u8>>>;
}

/// Application hook for live cluster status paths.
#[async_trait]
pub trait ClusterNamespace: Send + Sync {
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>>;
}

/// Session path implementation shared by 9P and tests.
pub struct SessionResources<L> {
    store: Arc<dyn EventStore>,
    agent_loop: Arc<L>,
}

impl<L> SessionResources<L> {
    pub fn new(store: Arc<dyn EventStore>, agent_loop: Arc<L>) -> Self {
        Self { store, agent_loop }
    }

    fn run_id(segment: &str) -> Result<RunId> {
        segment
            .parse()
            .map_err(|error| LegionError::Store(format!("invalid run id: {error}")))
    }
}

#[async_trait]
impl<L> NamespaceResources for SessionResources<L>
where
    L: AgentLoopTrait + 'static,
{
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
        let value = match parts.as_slice() {
            ["sessions", run_id, "turns"] => {
                serde_json::to_value(self.store.read_log(Self::run_id(run_id)?).await?)?
            }
            ["sessions", run_id, "status"] => {
                serde_json::to_value(self.store.session_status(Self::run_id(run_id)?).await?)?
            }
            ["sessions", run_id, "context"] => {
                serde_json::to_value(self.store.read_recent(Self::run_id(run_id)?, 64).await?)?
            }
            ["sessions", run_id, "config"] => self
                .store
                .read_log(Self::run_id(run_id)?)
                .await?
                .into_iter()
                .find_map(|entry| {
                    matches!(
                        entry.event.kind,
                        legion_core::types::TurnEventKind::SessionStarted
                    )
                    .then_some(entry.event.payload)
                    .flatten()
                })
                .unwrap_or(Value::Null),
            _ => return Ok(None),
        };
        Ok(Some(serde_json::to_vec(&value)?))
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<Option<Vec<u8>>> {
        let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
        let response = match parts.as_slice() {
            ["sessions", "new"] => {
                let config: RunConfig = serde_json::from_slice(data)?;
                let run_id = self.agent_loop.start(config).await?;
                json!({"run_id": run_id})
            }
            ["sessions", run_id, "turns"] => {
                let run_id = Self::run_id(run_id)?;
                let value: Value = serde_json::from_slice(data)?;
                let content = value
                    .get("content")
                    .and_then(Value::as_str)
                    .or_else(|| value.as_str())
                    .ok_or_else(|| LegionError::Store("turn content required".into()))?;
                self.agent_loop
                    .resume(run_id, ExternalEvent::user_message(content))
                    .await?;
                let envelope = self.agent_loop.resolve(run_id).await?;
                json!({"seq": envelope.seq, "event": envelope.event})
            }
            ["sessions", run_id, "status"] => {
                let run_id = Self::run_id(run_id)?;
                let command = std::str::from_utf8(data)
                    .map_err(|error| LegionError::Store(error.to_string()))?
                    .trim_matches(|character: char| character == '"' || character.is_whitespace());
                let status = match command {
                    "abort" => SessionStatus::Aborted,
                    "resume" => SessionStatus::Resuming,
                    _ => {
                        return Err(LegionError::Store(
                            "status command must be abort or resume".into(),
                        ));
                    }
                };
                self.store.set_status(run_id, status.clone()).await?;
                serde_json::to_value(status)?
            }
            ["sessions", run_id, "fork"] => {
                let run_id = Self::run_id(run_id)?;
                let value: Value = serde_json::from_slice(data)?;
                let at_seq: SeqNum = value
                    .get("at_seq")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| LegionError::Store("at_seq required".into()))?;
                json!({"run_id": self.store.fork(run_id, at_seq).await?})
            }
            _ => return Ok(None),
        };
        Ok(Some(serde_json::to_vec(&response)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_core::{
        test_doubles::{EchoToolRegistry, MemoryEventStore},
        types::Budget,
    };
    use legion_loop::driver::LegionLoop;

    #[tokio::test]
    async fn session_new_status_and_fork_paths_work() {
        let store = Arc::new(MemoryEventStore::new());
        let agent_loop = Arc::new(LegionLoop::new(
            store.clone(),
            Arc::new(EchoToolRegistry::new()),
        ));
        let resources = SessionResources::new(store.clone(), agent_loop);
        let config = RunConfig {
            system_prompt: None,
            model: "faux/test".into(),
            budget: Budget::default(),
            tools: vec![],
            metadata: None,
        };
        let created = resources
            .write("/sessions/new", &serde_json::to_vec(&config).unwrap())
            .await
            .unwrap()
            .unwrap();
        let run_id = serde_json::from_slice::<Value>(&created).unwrap()["run_id"]
            .as_str()
            .unwrap()
            .to_string();
        let status = resources
            .read(&format!("/sessions/{run_id}/status"))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8(status).unwrap().contains("idle"));
        let forked = resources
            .write(&format!("/sessions/{run_id}/fork"), br#"{"at_seq":0}"#)
            .await
            .unwrap()
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&forked).unwrap()["run_id"].is_string());
    }
}
