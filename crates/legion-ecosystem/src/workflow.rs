use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use futures::future::join_all;
use legion_core::{
    error::{LegionError, Result},
    traits::ToolRegistry,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One node in a durable-tool workflow graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub id: String,
    pub tool: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub nodes: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowResult {
    pub outputs: BTreeMap<String, Value>,
    /// Parallel execution waves, retained as useful supervision evidence.
    pub waves: Vec<Vec<String>>,
}

/// Validates and executes a directed acyclic graph through Legion tools.
pub struct WorkflowRunner {
    tools: Arc<dyn ToolRegistry>,
}

impl WorkflowRunner {
    pub fn new(tools: Arc<dyn ToolRegistry>) -> Self {
        Self { tools }
    }

    pub async fn run(&self, workflow: Workflow) -> Result<WorkflowResult> {
        let nodes = validate(workflow)?;
        let mut pending: BTreeSet<String> = nodes.keys().cloned().collect();
        let mut outputs: BTreeMap<String, Value> = BTreeMap::new();
        let mut waves = Vec::new();

        while !pending.is_empty() {
            let ready: Vec<String> = pending
                .iter()
                .filter(|id| {
                    nodes[*id]
                        .depends_on
                        .iter()
                        .all(|dep| outputs.contains_key(dep))
                })
                .cloned()
                .collect();
            if ready.is_empty() {
                return Err(LegionError::ToolError(
                    "workflow graph contains a cycle".into(),
                ));
            }

            let jobs = ready.iter().map(|id| {
                let node = nodes[id].clone();
                let tools = self.tools.clone();
                let dependencies: Map<String, Value> = node
                    .depends_on
                    .iter()
                    .map(|dep| (dep.clone(), outputs[dep].clone()))
                    .collect();
                async move {
                    let mut args = match node.args {
                        Value::Object(args) => args,
                        Value::Null => Map::new(),
                        _ => {
                            return Err(LegionError::ToolError(format!(
                                "workflow node '{}' args must be an object",
                                node.id
                            )));
                        }
                    };
                    if !dependencies.is_empty() {
                        args.insert("dependencies".into(), Value::Object(dependencies));
                    }
                    let output = tools.dispatch(&node.tool, Value::Object(args)).await?;
                    Ok::<_, LegionError>((node.id, output))
                }
            });

            for result in join_all(jobs).await {
                let (id, value) = result?;
                pending.remove(&id);
                outputs.insert(id, value);
            }
            waves.push(ready);
        }
        Ok(WorkflowResult { outputs, waves })
    }
}

fn validate(workflow: Workflow) -> Result<BTreeMap<String, WorkflowNode>> {
    if workflow.nodes.is_empty() {
        return Err(LegionError::ToolError(
            "workflow must contain at least one node".into(),
        ));
    }
    let mut nodes = BTreeMap::new();
    for node in workflow.nodes {
        if node.id.is_empty() {
            return Err(LegionError::ToolError(
                "workflow node id cannot be empty".into(),
            ));
        }
        if nodes.insert(node.id.clone(), node).is_some() {
            return Err(LegionError::ToolError(
                "workflow node ids must be unique".into(),
            ));
        }
    }
    for node in nodes.values() {
        for dependency in &node.depends_on {
            if dependency == &node.id || !nodes.contains_key(dependency) {
                return Err(LegionError::ToolError(format!(
                    "workflow node '{}' has invalid dependency '{dependency}'",
                    node.id
                )));
            }
        }
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use legion_core::types::{EffectClass, ToolDefinition};
    use serde_json::json;

    struct TestTools;
    #[async_trait]
    impl ToolRegistry for TestTools {
        async fn definitions(&self) -> Vec<ToolDefinition> {
            vec![ToolDefinition {
                name: "echo".into(),
                description: String::new(),
                parameters: json!({}),
                effect: EffectClass::Read,
            }]
        }
        async fn dispatch(&self, name: &str, args: Value) -> Result<Value> {
            if name != "echo" {
                return Err(LegionError::ToolNotFound(name.into()));
            }
            Ok(args)
        }
    }

    #[tokio::test]
    async fn executes_parallel_waves_and_passes_dependencies() {
        let result = WorkflowRunner::new(Arc::new(TestTools))
            .run(Workflow {
                nodes: vec![
                    WorkflowNode {
                        id: "a".into(),
                        tool: "echo".into(),
                        args: json!({"v":1}),
                        depends_on: vec![],
                    },
                    WorkflowNode {
                        id: "b".into(),
                        tool: "echo".into(),
                        args: json!({"v":2}),
                        depends_on: vec![],
                    },
                    WorkflowNode {
                        id: "merge".into(),
                        tool: "echo".into(),
                        args: json!({}),
                        depends_on: vec!["a".into(), "b".into()],
                    },
                ],
            })
            .await
            .unwrap();
        assert_eq!(result.waves, vec![vec!["a", "b"], vec!["merge"]]);
        assert_eq!(result.outputs["merge"]["dependencies"]["a"]["v"], 1);
    }

    #[tokio::test]
    async fn rejects_cycles_and_missing_dependencies() {
        let runner = WorkflowRunner::new(Arc::new(TestTools));
        let cycle = Workflow {
            nodes: vec![
                WorkflowNode {
                    id: "a".into(),
                    tool: "echo".into(),
                    args: json!({}),
                    depends_on: vec!["b".into()],
                },
                WorkflowNode {
                    id: "b".into(),
                    tool: "echo".into(),
                    args: json!({}),
                    depends_on: vec!["a".into()],
                },
            ],
        };
        assert!(
            runner
                .run(cycle)
                .await
                .unwrap_err()
                .to_string()
                .contains("cycle")
        );
        let missing = Workflow {
            nodes: vec![WorkflowNode {
                id: "a".into(),
                tool: "echo".into(),
                args: json!({}),
                depends_on: vec!["missing".into()],
            }],
        };
        assert!(
            runner
                .run(missing)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid dependency")
        );
    }
}
