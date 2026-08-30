//! In-process namespace tree: concurrent, path-addressed, watched.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde_json::Value;
use tokio::sync::{broadcast, RwLock};
use tracing::debug;

use crate::watch::WatchEvent;

// ── NodeKind ──────────────────────────────────────────────────────────────────

/// A namespace node is either a directory or a leaf value.
#[derive(Debug, Clone)]
pub enum NodeKind {
    /// A directory containing child nodes.
    Dir,
    /// A raw byte blob (WASM, compiled artifacts, etc.)
    Blob(Bytes),
    /// A JSON value (config, status, manifests, etc.)
    Json(Value),
}

/// A single node in the namespace tree.
#[derive(Debug, Clone)]
pub struct Node {
    pub kind:       NodeKind,
    pub updated_at: i64,
}

// ── Namespace ─────────────────────────────────────────────────────────────────

/// Thread-safe in-process namespace tree.
///
/// Paths are `/`-separated strings (e.g. `/fn/hello/manifest.json`).
/// Parent directories are created implicitly on write.
/// Watchers receive events for paths they are subscribed to.
#[derive(Clone)]
pub struct Namespace {
    inner: Arc<RwLock<NamespaceInner>>,
    tx:    broadcast::Sender<WatchEvent>,
}

struct NamespaceInner {
    nodes: HashMap<String, Node>,
}

impl Namespace {
    /// Create an empty namespace with the conventional top-level directories.
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        let mut nodes = HashMap::new();
        for dir in ["/", "/fn", "/sessions", "/deploy", "/cluster", "/cluster/peers"] {
            nodes.insert(dir.to_string(), Node {
                kind:       NodeKind::Dir,
                updated_at: now_ms(),
            });
        }
        Self {
            inner: Arc::new(RwLock::new(NamespaceInner { nodes })),
            tx,
        }
    }

    /// Read a node by path.
    pub async fn get(&self, path: &str) -> Option<Node> {
        self.inner.read().await.nodes.get(path).cloned()
    }

    /// Write a JSON value to a path, creating parent dirs as needed.
    pub async fn set_json(&self, path: &str, value: Value) {
        self.write_node(path, NodeKind::Json(value)).await;
    }

    /// Write a raw blob to a path.
    pub async fn set_blob(&self, path: &str, data: Bytes) {
        self.write_node(path, NodeKind::Blob(data)).await;
    }

    /// Delete a node (and its children).
    pub async fn delete(&self, path: &str) {
        let mut inner = self.inner.write().await;
        let prefix = format!("{}/", path.trim_end_matches('/'));
        inner.nodes.retain(|k, _| k != path && !k.starts_with(&prefix));
        drop(inner);
        let _ = self.tx.send(WatchEvent::Deleted { path: path.to_string() });
    }

    /// List direct children of a directory path.
    pub async fn ls(&self, dir: &str) -> Vec<String> {
        let dir   = dir.trim_end_matches('/');
        let inner = self.inner.read().await;
        inner.nodes.keys()
            .filter(|k| {
                if *k == dir { return false; }
                let rest = k.strip_prefix(dir).unwrap_or("");
                rest.starts_with('/') && !rest[1..].contains('/')
            })
            .map(|k| k[dir.len() + 1..].to_string())
            .collect()
    }

    /// Subscribe to all watch events.
    pub fn watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.tx.subscribe()
    }

    // ── internal ─────────────────────────────────────────────────────────────

    async fn write_node(&self, path: &str, kind: NodeKind) {
        let ts = now_ms();
        let mut inner = self.inner.write().await;

        // Ensure parent directories exist
        let mut parts: Vec<&str> = path.split('/').collect();
        parts.pop();
        let mut acc = String::new();
        for part in parts {
            if part.is_empty() { acc.push('/'); continue; }
            acc = format!("{}/{}", acc.trim_end_matches('/'), part);
            inner.nodes.entry(acc.clone()).or_insert_with(|| Node {
                kind:       NodeKind::Dir,
                updated_at: ts,
            });
        }

        let node = Node { kind, updated_at: ts };
        inner.nodes.insert(path.to_string(), node);
        drop(inner);

        debug!(path, "namespace write");
        let _ = self.tx.send(WatchEvent::Updated { path: path.to_string() });
    }
}

impl Default for Namespace {
    fn default() -> Self { Self::new() }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn namespace_write_and_read() {
        let ns = Namespace::new();
        ns.set_json("/fn/hello/manifest.json", json!({ "name": "hello", "runtime": "wasm" })).await;
        let node = ns.get("/fn/hello/manifest.json").await.unwrap();
        assert!(matches!(node.kind, NodeKind::Json(_)));
    }

    #[tokio::test]
    async fn namespace_ls_children() {
        let ns = Namespace::new();
        ns.set_json("/fn/alpha/manifest.json", json!({})).await;
        ns.set_json("/fn/beta/manifest.json",  json!({})).await;
        let mut children = ns.ls("/fn").await;
        children.sort();
        assert!(children.contains(&"alpha".to_string()));
        assert!(children.contains(&"beta".to_string()));
    }

    #[tokio::test]
    async fn namespace_watch_event() {
        let ns  = Namespace::new();
        let mut rx = ns.watch();
        ns.set_json("/cluster/self", json!({ "node": "test" })).await;
        let ev = rx.try_recv().unwrap();
        assert!(matches!(ev, WatchEvent::Updated { path } if path == "/cluster/self"));
    }

    #[tokio::test]
    async fn namespace_delete() {
        let ns = Namespace::new();
        ns.set_json("/fn/temp/manifest.json", json!({})).await;
        ns.delete("/fn/temp").await;
        assert!(ns.get("/fn/temp").await.is_none());
        assert!(ns.get("/fn/temp/manifest.json").await.is_none());
    }
}
