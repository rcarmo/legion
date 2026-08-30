//! Watch events emitted when the namespace tree changes.

/// An event fired when a namespace node changes.
#[derive(Debug, Clone)]
pub enum WatchEvent {
    /// A node was created or updated.
    Updated { path: String },
    /// A node (and its children) was deleted.
    Deleted { path: String },
}
