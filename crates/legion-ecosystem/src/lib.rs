//! Multi-agent composition primitives for Legion.
//!
//! Agent profiles become ordinary tools, supervised children reuse durable
//! session forks, and workflows execute validated dependency graphs through a
//! shared `ToolRegistry`.

mod agents;
mod workflow;

pub use agents::{AgentProfile, AgentToolRegistry, ChildRun};
pub use workflow::{Workflow, WorkflowNode, WorkflowResult, WorkflowRunner};
