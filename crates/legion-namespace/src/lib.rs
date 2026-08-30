//! legion-namespace — a virtual 9P-style tree for Legion resources.
//!
//! The namespace is a hierarchical key-value store that organises Legion
//! resources under conventional paths. It is intentionally decoupled from
//! the actual 9P wire protocol (jetstream); this crate owns the in-process
//! tree and exposes async read/write/watch operations. A later `legion-9p`
//! crate will project it over the wire.
//!
//! ## Tree layout
//!
//! ```text
//! /fn/
//!   <name>/         — deployed function bundle
//!     manifest.json — name, runtime, version, deployed_at
//!     code          — WASM bytes or JS source (opaque blob)
//! /sessions/
//!   <run_id>/       — live session tree
//!     status        — current SessionStatus as JSON
//!     config        — RunConfig as JSON
//!     log           — recent turn log as newline-delimited JSON
//! /deploy/
//!   queue           — pending deploy jobs (FIFO JSON array)
//!   history         — completed/failed jobs
//! /cluster/
//!   self            — this node's identity JSON
//!   peers/          — one entry per known iroh peer
//! ```

pub mod tree;
pub mod watch;

pub use tree::{Namespace, Node, NodeKind};
