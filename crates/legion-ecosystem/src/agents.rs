use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use legion_core::{
    error::{LegionError, Result},
    traits::{AgentLoopTrait, EventStore, ToolRegistry},
    types::{EffectClass, ExternalEvent, RunConfig, RunId, SeqNum, SessionStatus, ToolDefinition},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;

/// A named agent configuration exposed as `agent.<name>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProfile {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub config: RunConfig,
}

/// The durable result of running an agent, optionally as a forked child.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildRun {
    pub run_id: RunId,
    pub parent_run_id: Option<RunId>,
    pub status: SessionStatus,
    pub output: Value,
}

/// Late-bound registry that avoids a construction cycle between the agent loop
/// and the tool registry consumed by that loop.
pub struct AgentToolRegistry {
    store: Arc<dyn EventStore>,
    executor: RwLock<Option<Arc<dyn AgentLoopTrait>>>,
    profiles: RwLock<BTreeMap<String, AgentProfile>>,
}

impl AgentToolRegistry {
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self {
            store,
            executor: RwLock::new(None),
            profiles: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn bind(&self, executor: Arc<dyn AgentLoopTrait>) {
        *self.executor.write().await = Some(executor);
    }

    pub async fn register(&self, profile: AgentProfile) -> Result<()> {
        validate_name(&profile.name)?;
        self.profiles
            .write()
            .await
            .insert(profile.name.clone(), profile);
        Ok(())
    }

    pub async fn unregister(&self, name: &str) -> bool {
        self.profiles.write().await.remove(name).is_some()
    }

    pub async fn profiles(&self) -> Vec<AgentProfile> {
        self.profiles.read().await.values().cloned().collect()
    }

    /// Start a fresh profile or fork a parent, inject the assignment, and run
    /// the child to its next durable terminal/parked state.
    pub async fn run(
        &self,
        profile_name: &str,
        prompt: String,
        parent: Option<(RunId, SeqNum)>,
    ) -> Result<ChildRun> {
        let profile = self
            .profiles
            .read()
            .await
            .get(profile_name)
            .cloned()
            .ok_or_else(|| LegionError::ToolNotFound(format!("agent.{profile_name}")))?;
        let executor = self
            .executor
            .read()
            .await
            .clone()
            .ok_or_else(|| LegionError::ToolError("agent executor is not bound".into()))?;

        let (run_id, parent_run_id) = if let Some((parent_run_id, at_seq)) = parent {
            let parent_log = self.store.read_log(parent_run_id).await?;
            if !parent_log.iter().any(|entry| entry.seq == at_seq) {
                return Err(LegionError::ToolError(format!(
                    "parent sequence {at_seq} does not exist for {parent_run_id}"
                )));
            }
            (
                self.store.fork(parent_run_id, at_seq).await?,
                Some(parent_run_id),
            )
        } else {
            (executor.start(profile.config).await?, None)
        };

        executor
            .resume(run_id, ExternalEvent::UserMessage(prompt))
            .await?;
        let envelope = executor.resolve(run_id).await?;
        let status = self.store.session_status(run_id).await?;
        Ok(ChildRun {
            run_id,
            parent_run_id,
            status,
            output: envelope.event.payload.unwrap_or(Value::Null),
        })
    }
}

#[async_trait]
impl ToolRegistry for AgentToolRegistry {
    async fn definitions(&self) -> Vec<ToolDefinition> {
        self.profiles()
            .await
            .into_iter()
            .map(|profile| ToolDefinition {
                name: format!("agent.{}", profile.name),
                description: profile.description,
                parameters: json!({
                    "type": "object",
                    "required": ["prompt"],
                    "properties": {
                        "prompt": { "type": "string" },
                        "parent_run_id": { "type": "string", "format": "uuid" },
                        "at_seq": { "type": "integer", "minimum": 0 }
                    }
                }),
                effect: EffectClass::Write,
            })
            .collect()
    }

    async fn dispatch(&self, name: &str, args: Value) -> Result<Value> {
        let profile = name
            .strip_prefix("agent.")
            .ok_or_else(|| LegionError::ToolNotFound(name.into()))?;
        let prompt = args
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or_else(|| LegionError::ToolError("prompt is required".into()))?
            .to_string();
        let parent = match args.get("parent_run_id") {
            Some(value) => {
                let parent_run_id = value
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    .ok_or_else(|| LegionError::ToolError("parent_run_id must be a UUID".into()))?;
                let at_seq = args.get("at_seq").and_then(Value::as_u64).ok_or_else(|| {
                    LegionError::ToolError("at_seq is required with parent_run_id".into())
                })?;
                Some((parent_run_id, at_seq))
            }
            None => None,
        };
        serde_json::to_value(self.run(profile, prompt, parent).await?)
            .map_err(LegionError::Serialization)
    }
}

fn validate_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if valid {
        Ok(())
    } else {
        Err(LegionError::ToolError(
            "agent name must be a lowercase DNS label".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_core::{
        test_doubles::MemoryEventStore,
        types::{Budget, TurnEnvelope, TurnEvent},
    };
    use tokio::sync::Mutex;
    use uuid::Uuid;

    struct StubLoop {
        store: Arc<MemoryEventStore>,
        starts: Mutex<u32>,
    }

    #[async_trait]
    impl AgentLoopTrait for StubLoop {
        async fn start(&self, config: RunConfig) -> Result<RunId> {
            *self.starts.lock().await += 1;
            let id = Uuid::new_v4();
            self.store.create_session(id, &config).await?;
            self.store
                .append(id, TurnEvent::session_started(&config))
                .await?;
            Ok(id)
        }
        async fn recover(&self, _: RunId) -> Result<()> {
            Ok(())
        }
        async fn resume(&self, run_id: RunId, event: ExternalEvent) -> Result<()> {
            if let ExternalEvent::UserMessage(message) = event {
                self.store
                    .append(run_id, TurnEvent::user_message(message))
                    .await?;
            }
            Ok(())
        }
        async fn resolve(&self, run_id: RunId) -> Result<TurnEnvelope> {
            let seq = self
                .store
                .append(
                    run_id,
                    TurnEvent::assistant_message(json!({"content":"done"}), "stub/model", 1, 1, 1),
                )
                .await?;
            self.store
                .set_status(run_id, SessionStatus::Completed)
                .await?;
            Ok(self.store.read_log(run_id).await?.remove(seq as usize))
        }
    }

    fn profile() -> AgentProfile {
        AgentProfile {
            name: "researcher".into(),
            description: "Research a question".into(),
            config: RunConfig {
                system_prompt: Some("research".into()),
                model: "stub/model".into(),
                budget: Budget::default(),
                tools: vec![],
                metadata: None,
            },
        }
    }

    #[tokio::test]
    async fn profile_is_a_callable_tool() {
        let store = Arc::new(MemoryEventStore::new());
        let registry = AgentToolRegistry::new(store.clone());
        registry.register(profile()).await.unwrap();
        registry
            .bind(Arc::new(StubLoop {
                store,
                starts: Mutex::new(0),
            }))
            .await;
        let value = registry
            .dispatch("agent.researcher", json!({"prompt":"find it"}))
            .await
            .unwrap();
        assert_eq!(value["output"]["content"], "done");
        assert_eq!(value["status"]["status"], "completed");
    }

    #[tokio::test]
    async fn supervised_run_forks_parent() {
        let store = Arc::new(MemoryEventStore::new());
        let loop_ = Arc::new(StubLoop {
            store: store.clone(),
            starts: Mutex::new(0),
        });
        let parent = loop_.start(profile().config.clone()).await.unwrap();
        store
            .append(parent, TurnEvent::user_message("parent"))
            .await
            .unwrap();
        let registry = AgentToolRegistry::new(store.clone());
        registry.register(profile()).await.unwrap();
        registry.bind(loop_).await;
        let child = registry
            .run("researcher", "child".into(), Some((parent, 1)))
            .await
            .unwrap();
        assert_eq!(child.parent_run_id, Some(parent));
        assert_ne!(child.run_id, parent);
        assert_eq!(store.read_log(child.run_id).await.unwrap().len(), 4);
    }
}
