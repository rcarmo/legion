//! Persistent content-addressed storage for deployment artifacts.

use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use iroh_blobs::{Hash, store::fs::FsStore};
use tokio::io::AsyncReadExt;

/// Persistent iroh-blobs store used by the deploy pipeline.
#[derive(Debug, Clone)]
pub struct DeployBlobStore {
    store: FsStore,
}

impl DeployBlobStore {
    /// Open or create a blob store rooted at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let store = FsStore::load(path)
            .await
            .context("open deployment blob store")?;
        Ok(Self { store })
    }

    /// Add an artifact and return its BLAKE3 content identifier.
    pub async fn put(&self, data: impl Into<bytes::Bytes>) -> Result<String> {
        let tag = self
            .store
            .blobs()
            .add_bytes(data)
            .await
            .context("store deployment artifact")?;
        Ok(tag.hash.to_string())
    }

    /// Read and verify an artifact by content identifier.
    pub async fn get(&self, cid: &str) -> Result<Vec<u8>> {
        let hash = Hash::from_str(cid).context("parse deployment artifact CID")?;
        let mut reader = self.store.blobs().reader(hash);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .await
            .context("read deployment artifact")?;
        Ok(bytes)
    }

    /// Flush metadata and stop the store actor.
    pub async fn shutdown(&self) -> Result<()> {
        self.store
            .sync_db()
            .await
            .context("sync deployment blob store")?;
        self.store
            .shutdown()
            .await
            .context("shutdown deployment blob store")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_and_deduplicates_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let store = DeployBlobStore::open(dir.path()).await.unwrap();

        let first = store.put("same artifact").await.unwrap();
        let second = store.put("same artifact").await.unwrap();
        assert_eq!(first, second);
        assert_eq!(store.get(&first).await.unwrap(), b"same artifact");

        store.shutdown().await.unwrap();

        let reopened = DeployBlobStore::open(dir.path()).await.unwrap();
        assert_eq!(reopened.get(&first).await.unwrap(), b"same artifact");
        reopened.shutdown().await.unwrap();
    }
}
