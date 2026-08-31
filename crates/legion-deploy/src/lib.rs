//! legion-deploy — function deployment pipeline.
//!
//! Accepts deploy requests (source code + manifest), validates them,
//! persists code to the data directory, and registers the manifest in
//! the namespace so `RegistryBridge` can expose them as agent tools.

pub mod blob_store;
pub mod pipeline;
pub mod job;

pub use blob_store::DeployBlobStore;
pub use pipeline::DeployPipeline;
pub use job::{DeployJob, DeployOutcome, DeployStatus};
