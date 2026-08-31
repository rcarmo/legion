//! legion-runtime — function execution engine.
//!
//! Executes deployed functions from the namespace.  Two runtimes are supported:
//!
//! - **WASM** — via extism/wasmtime (guarded by the optional `extism` feature)
//! - **Bun** — subprocess execution of JS/TS functions (Milestone 3)
//!
//! For Milestone 2, this crate provides:
//! - The `FunctionManifest` type (shared between deploy + runtime)
//! - The `FunctionRuntime` enum and `InvokeRequest`/`InvokeResult` types
//! - A `BunRuntime` skeleton that shell-invokes Bun and returns stdout as JSON
//! - A `RegistryBridge` — exposes deployed functions as `legion_core::ToolDefinition`s
//!   so the agent can call them via the standard tool dispatch path

pub mod manifest;
pub mod invoke;
pub mod limits;
pub mod routing;
pub mod bun;
#[cfg(feature = "extism")]
pub mod wasm;
pub mod registry_bridge;

pub use manifest::{FunctionManifest, FunctionRuntime};
pub use invoke::{InvokeRequest, InvokeResult, Invoker};
pub use limits::{BoundedInvoker, InvocationLimits, InvocationMetrics};
pub use routing::FunctionRoute;
pub use registry_bridge::RegistryBridge;
