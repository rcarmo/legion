//! jetstream 9P2000.L projection of the in-process namespace tree.

use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::Arc,
};

use jetstream_9p::{
    DEFAULT_MSIZE, P9_GETATTR_BASIC, P9_LOCK_SUCCESS, P9_QTDIR, P9_QTFILE, messages::*,
    ninep_2000_l::NineP200L,
};
use jetstream_wireformat::{Data, WireFormat};
use tokio::sync::RwLock;

use crate::{Namespace, NodeKind};

/// Mutable state for one 9P connection. Clones share fid and pending-write state.
#[derive(Clone)]
pub struct LegionNamespace {
    namespace: Namespace,
    fids: Arc<RwLock<HashMap<u32, String>>>,
    replies: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    remote_peers: Arc<RwLock<HashMap<String, Arc<dyn crate::resources::PeerNamespace>>>>,
    resources: Option<Arc<dyn crate::resources::NamespaceResources>>,
    functions: Option<Arc<dyn crate::resources::FunctionNamespace>>,
    deploy: Option<Arc<dyn crate::resources::DeployNamespace>>,
    cluster: Option<Arc<dyn crate::resources::ClusterNamespace>>,
    capability_hash: Option<[u8; 32]>,
}

impl LegionNamespace {
    pub fn new(namespace: Namespace) -> Self {
        Self {
            namespace,
            fids: Arc::new(RwLock::new(HashMap::new())),
            replies: Arc::new(RwLock::new(HashMap::new())),
            remote_peers: Arc::new(RwLock::new(HashMap::new())),
            resources: None,
            functions: None,
            deploy: None,
            cluster: None,
            capability_hash: None,
        }
    }

    /// Require callers to present `cap=<token>` as the 9P attach name.
    pub fn with_capability_token(mut self, token: impl AsRef<[u8]>) -> Self {
        self.capability_hash = Some(*blake3::hash(token.as_ref()).as_bytes());
        self
    }

    fn authorize_attach(&self, aname: &str) -> io::Result<()> {
        let Some(expected) = self.capability_hash else {
            return Ok(());
        };
        let supplied = aname.strip_prefix("cap=").ok_or_else(|| {
            io::Error::new(io::ErrorKind::PermissionDenied, "capability required")
        })?;
        let actual = blake3::hash(supplied.as_bytes());
        let mismatch = expected
            .iter()
            .zip(actual.as_bytes())
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            });
        if mismatch == 0 {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid capability",
            ))
        }
    }

    pub fn with_resources(
        mut self,
        resources: Arc<dyn crate::resources::NamespaceResources>,
    ) -> Self {
        self.resources = Some(resources);
        self
    }

    pub fn with_functions(
        mut self,
        functions: Arc<dyn crate::resources::FunctionNamespace>,
    ) -> Self {
        self.functions = Some(functions);
        self
    }

    pub fn with_deploy(mut self, deploy: Arc<dyn crate::resources::DeployNamespace>) -> Self {
        self.deploy = Some(deploy);
        self
    }

    pub fn with_cluster(mut self, cluster: Arc<dyn crate::resources::ClusterNamespace>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Register a peer namespace for transparent `/peers/<key>/...` proxying.
    pub async fn register_peer(
        &self,
        key: impl Into<String>,
        namespace: Arc<dyn crate::resources::PeerNamespace>,
    ) {
        self.remote_peers
            .write()
            .await
            .insert(key.into(), namespace);
    }

    fn peer_path(path: &str) -> Option<(&str, String)> {
        let components = path.trim_matches('/').split('/').collect::<Vec<_>>();
        if components.first() == Some(&"peers") && components.len() >= 2 {
            let suffix = format!("/{}", components[2..].join("/"));
            Some((
                components[1],
                if suffix == "/" { "/".into() } else { suffix },
            ))
        } else {
            None
        }
    }

    async fn target(&self, path: &str) -> io::Result<(Namespace, String)> {
        if Self::peer_path(path).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "remote peer path requires RPC forwarding",
            ));
        }
        Ok((self.namespace.clone(), path.to_string()))
    }

    async fn path_for(&self, fid: u32) -> io::Result<String> {
        self.fids
            .read()
            .await
            .get(&fid)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown fid"))
    }

    async fn exists(&self, path: &str) -> bool {
        if path == "/peers" || is_virtual_dir(path) {
            return true;
        }
        if let Some((peer, _)) = Self::peer_path(path) {
            return self.remote_peers.read().await.contains_key(peer);
        }
        if is_virtual_path(path) {
            return true;
        }
        if let Ok((ns, path)) = self.target(path).await {
            return ns.get(&path).await.is_some();
        }
        false
    }

    async fn is_dir(&self, path: &str) -> bool {
        if path == "/peers" || is_virtual_dir(path) {
            return true;
        }
        match self.target(path).await {
            Ok((ns, path)) => ns
                .get(&path)
                .await
                .is_some_and(|node| matches!(node.kind, NodeKind::Dir)),
            Err(_) => false,
        }
    }

    async fn qid(&self, path: &str) -> Qid {
        let digest = blake3::hash(path.as_bytes());
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest.as_bytes()[..8]);
        Qid {
            ty: if self.is_dir(path).await {
                P9_QTDIR
            } else {
                P9_QTFILE
            },
            version: 0,
            path: u64::from_le_bytes(bytes),
        }
    }

    async fn read_path(&self, path: &str) -> io::Result<Vec<u8>> {
        if let Some(reply) = self.replies.read().await.get(path).cloned() {
            return Ok(reply);
        }
        if let Some((name, field)) = function_metadata_path(path) {
            let manifest = self
                .namespace
                .get(&format!("/fn/{name}/manifest.json"))
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "function not found"))?;
            let NodeKind::Json(manifest) = manifest.kind else {
                return Err(io::Error::other("invalid function manifest"));
            };
            let value = match field {
                "schema" => manifest.get("parameters").cloned().unwrap_or_default(),
                "versions" => {
                    serde_json::json!([manifest.get("version").cloned().unwrap_or_default()])
                }
                "default" => manifest.get("version").cloned().unwrap_or_default(),
                _ => unreachable!(),
            };
            return serde_json::to_vec(&value).map_err(io::Error::other);
        }
        if path.starts_with("/cluster/")
            && let Some(cluster) = self.cluster.clone()
        {
            let path = path.to_string();
            let response = tokio::spawn(async move { cluster.read(&path).await })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
            if let Some(data) = response {
                return Ok(data);
            }
        }
        if path.starts_with("/deploy/")
            && let Some(deploy) = self.deploy.clone()
        {
            let path = path.to_string();
            let response = tokio::spawn(async move { deploy.read(&path).await })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
            if let Some(data) = response {
                return Ok(data);
            }
        }
        if let Some(resources) = self.resources.clone() {
            let path = path.to_string();
            let response = tokio::spawn(async move { resources.read(&path).await })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
            if let Some(data) = response {
                return Ok(data);
            }
        }
        if path == "/peers" {
            let mut peers = self
                .remote_peers
                .read()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            peers.sort();
            return serde_json::to_vec(&peers).map_err(io::Error::other);
        }
        if let Some((peer, remote_path)) = Self::peer_path(path) {
            let remote = self
                .remote_peers
                .read()
                .await
                .get(peer)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
            return tokio::spawn(async move { remote.read(&remote_path).await })
                .await
                .map_err(io::Error::other)?;
        }
        let (ns, path) = self.target(path).await?;
        let node = ns
            .get(&path)
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "path not found"))?;
        match node.kind {
            NodeKind::Blob(bytes) => Ok(bytes.to_vec()),
            NodeKind::Json(value) => serde_json::to_vec(&value).map_err(io::Error::other),
            NodeKind::Dir => serde_json::to_vec(&ns.ls(&path).await).map_err(io::Error::other),
        }
    }

    async fn write_path(&self, path: &str, data: &[u8]) -> io::Result<()> {
        if path.starts_with("/deploy/")
            && let Some(deploy) = self.deploy.clone()
        {
            let path_owned = path.to_string();
            let data = data.to_vec();
            let response = tokio::spawn(async move { deploy.write(&path_owned, &data).await })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
            if let Some(response) = response {
                self.replies
                    .write()
                    .await
                    .insert(path.to_string(), response);
                return Ok(());
            }
        }
        if let Some(name) = function_name(path)
            && let Some(functions) = self.functions.clone()
        {
            let name = name.to_string();
            let data = data.to_vec();
            let response = tokio::spawn(async move { functions.invoke(&name, &data).await })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
            self.replies
                .write()
                .await
                .insert(path.to_string(), response);
            return Ok(());
        }
        let response = if let Some(resources) = self.resources.clone() {
            let path = path.to_string();
            let data = data.to_vec();
            tokio::spawn(async move { resources.write(&path, &data).await })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?
        } else {
            None
        };
        if let Some(response) = response {
            self.replies
                .write()
                .await
                .insert(path.to_string(), response);
            return Ok(());
        }
        if let Some((peer, remote_path)) = Self::peer_path(path) {
            let remote = self
                .remote_peers
                .read()
                .await
                .get(peer)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
            let data = data.to_vec();
            let response = tokio::spawn(async move { remote.write(&remote_path, &data).await })
                .await
                .map_err(io::Error::other)??;
            self.replies
                .write()
                .await
                .insert(path.to_string(), response);
            return Ok(());
        }
        let (ns, target_path) = self.target(path).await?;
        if let Ok(value) = serde_json::from_slice(data) {
            ns.set_json(&target_path, value).await;
        } else {
            ns.set_blob(&target_path, data.to_vec().into()).await;
        }
        self.replies
            .write()
            .await
            .insert(path.to_string(), data.to_vec());
        Ok(())
    }

    async fn directory_data(&self, path: &str, offset: u64) -> io::Result<Vec<u8>> {
        let names = if path == "/peers" {
            self.remote_peers
                .read()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            let (ns, target_path) = self.target(path).await?;
            ns.ls(&target_path).await
        };
        let mut entries = BTreeMap::new();
        for name in names {
            let child = join_path(path, &name);
            entries.insert(name, child);
        }
        let mut data = Vec::new();
        for (index, (name, child)) in entries.into_iter().enumerate().skip(offset as usize) {
            let entry = Dirent {
                qid: self.qid(&child).await,
                offset: (index + 1) as u64,
                ty: if self.is_dir(&child).await {
                    P9_QTDIR
                } else {
                    P9_QTFILE
                },
                name,
            };
            entry.encode(&mut data)?;
        }
        Ok(data)
    }
}

fn function_name(path: &str) -> Option<&str> {
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["fn", name] => Some(name),
        _ => None,
    }
}

fn function_metadata_path(path: &str) -> Option<(&str, &str)> {
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["fn", name, field @ ("schema" | "versions" | "default")] => Some((name, field)),
        _ => None,
    }
}

fn is_virtual_dir(path: &str) -> bool {
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    matches!(parts.as_slice(), ["sessions", run_id] if run_id.parse::<legion_core::types::RunId>().is_ok())
}

fn is_virtual_path(path: &str) -> bool {
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    matches!(
        parts.as_slice(),
        ["sessions", "new"]
            | [
                "sessions",
                _,
                "turns" | "status" | "context" | "fork" | "config"
            ]
            | ["fn", _, "schema" | "versions" | "default"]
            | ["deploy", "register" | "route" | "promote"]
            | ["deploy", "blobs", _]
            | ["cluster", "leader" | "health" | "self"]
    )
}

fn join_path(base: &str, name: &str) -> String {
    if base == "/" {
        format!("/{name}")
    } else {
        format!("{}/{name}", base.trim_end_matches('/'))
    }
}

fn unsupported() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "operation not supported")
}

impl NineP200L for LegionNamespace {
    async fn version(&mut self, _tag: u16, message: &Tversion) -> io::Result<Rversion> {
        Ok(Rversion {
            msize: message.msize.min(DEFAULT_MSIZE),
            version: "9P2000.L".into(),
        })
    }

    async fn auth(&mut self, _tag: u16, _message: &Tauth) -> io::Result<Rauth> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "authenticate with a capability in Tattach.aname",
        ))
    }

    async fn flush(&mut self, _tag: u16, _message: &Tflush) -> io::Result<()> {
        Ok(())
    }

    async fn walk(&mut self, _tag: u16, message: &Twalk) -> io::Result<Rwalk> {
        let mut path = self.path_for(message.fid).await?;
        let mut qids = Vec::new();
        for component in &message.wnames {
            path = match component.as_str() {
                "." => path,
                ".." => path
                    .rsplit_once('/')
                    .map(|(parent, _)| {
                        if parent.is_empty() {
                            "/".into()
                        } else {
                            parent.into()
                        }
                    })
                    .unwrap_or_else(|| "/".into()),
                name => join_path(&path, name),
            };
            let exists = self.exists(&path).await;
            tracing::debug!(%path, exists, "9P walk component");
            if !exists {
                break;
            }
            qids.push(self.qid(&path).await);
        }
        if qids.len() != message.wnames.len() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "walk target not found",
            ));
        }
        self.fids.write().await.insert(message.newfid, path);
        Ok(Rwalk { wqids: qids })
    }

    async fn read(&mut self, _tag: u16, message: &Tread) -> io::Result<Rread> {
        let path = self.path_for(message.fid).await?;
        let data = if path.starts_with("/sessions/") && path.ends_with("/turns") {
            match self.read_path(&path).await {
                Ok(data) if message.offset < data.len() as u64 => data,
                Ok(_) | Err(_) => {
                    let mut watch = self.namespace.watch();
                    loop {
                        match watch.recv().await {
                            Ok(crate::watch::WatchEvent::Updated { path: updated })
                                if updated == path =>
                            {
                                break self.read_path(&path).await?;
                            }
                            Ok(_) => {}
                            Err(error) => return Err(io::Error::other(error)),
                        }
                    }
                }
            }
        } else {
            self.read_path(&path).await?
        };
        let start = (message.offset as usize).min(data.len());
        let end = start.saturating_add(message.count as usize).min(data.len());
        Ok(Rread {
            data: Data(data[start..end].to_vec()),
        })
    }

    async fn write(&mut self, _tag: u16, message: &Twrite) -> io::Result<Rwrite> {
        let path = self.path_for(message.fid).await?;
        if message.offset != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only offset-zero writes are supported",
            ));
        }
        self.write_path(&path, &message.data).await?;
        Ok(Rwrite {
            count: message.data.len() as u32,
        })
    }

    async fn clunk(&mut self, _tag: u16, message: &Tclunk) -> io::Result<()> {
        self.fids.write().await.remove(&message.fid);
        Ok(())
    }

    async fn remove(&mut self, _tag: u16, message: &Tremove) -> io::Result<()> {
        let path = self.path_for(message.fid).await?;
        let (ns, path) = self.target(&path).await?;
        ns.delete(&path).await;
        self.fids.write().await.remove(&message.fid);
        Ok(())
    }

    async fn attach(&mut self, _tag: u16, message: &Tattach) -> io::Result<Rattach> {
        self.authorize_attach(&message.aname)?;
        self.fids.write().await.insert(message.fid, "/".into());
        Ok(Rattach {
            qid: self.qid("/").await,
        })
    }

    async fn statfs(&mut self, _tag: u16, _message: &Tstatfs) -> io::Result<Rstatfs> {
        Ok(Rstatfs {
            ty: 0x0102_1994,
            bsize: 4096,
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            fsid: 0,
            namelen: 255,
        })
    }

    async fn lopen(&mut self, _tag: u16, message: &Tlopen) -> io::Result<Rlopen> {
        let path = self.path_for(message.fid).await?;
        Ok(Rlopen {
            qid: self.qid(&path).await,
            iounit: 0,
        })
    }

    async fn lcreate(&mut self, _tag: u16, message: &Tlcreate) -> io::Result<Rlcreate> {
        let parent = self.path_for(message.fid).await?;
        let path = join_path(&parent, &message.name);
        let (ns, target) = self.target(&path).await?;
        ns.set_blob(&target, Vec::new().into()).await;
        self.fids.write().await.insert(message.fid, path.clone());
        Ok(Rlcreate {
            qid: self.qid(&path).await,
            iounit: 0,
        })
    }

    async fn symlink(&mut self, _tag: u16, _message: &Tsymlink) -> io::Result<Rsymlink> {
        Err(unsupported())
    }
    async fn mknod(&mut self, _tag: u16, _message: &Tmknod) -> io::Result<Rmknod> {
        Err(unsupported())
    }
    async fn rename(&mut self, _tag: u16, _message: &Trename) -> io::Result<()> {
        Err(unsupported())
    }
    async fn readlink(&mut self, _tag: u16, _message: &Treadlink) -> io::Result<Rreadlink> {
        Err(unsupported())
    }

    async fn get_attr(&mut self, _tag: u16, message: &Tgetattr) -> io::Result<Rgetattr> {
        let path = self.path_for(message.fid).await?;
        let bytes = self.read_path(&path).await.unwrap_or_default();
        let qid = self.qid(&path).await;
        Ok(Rgetattr {
            valid: P9_GETATTR_BASIC,
            qid,
            mode: if qid.ty == P9_QTDIR {
                0o040755
            } else {
                0o100644
            },
            uid: 0,
            gid: 0,
            nlink: 1,
            rdev: 0,
            size: bytes.len() as u64,
            blksize: 4096,
            blocks: bytes.len().div_ceil(512) as u64,
            atime_sec: 0,
            atime_nsec: 0,
            mtime_sec: 0,
            mtime_nsec: 0,
            ctime_sec: 0,
            ctime_nsec: 0,
            btime_sec: 0,
            btime_nsec: 0,
            r#gen: 0,
            data_version: 0,
        })
    }

    async fn set_attr(&mut self, _tag: u16, _message: &Tsetattr) -> io::Result<()> {
        Err(unsupported())
    }
    async fn xattr_walk(&mut self, _tag: u16, _message: &Txattrwalk) -> io::Result<Rxattrwalk> {
        Err(unsupported())
    }
    async fn xattr_create(&mut self, _tag: u16, _message: &Txattrcreate) -> io::Result<()> {
        Err(unsupported())
    }

    async fn readdir(&mut self, _tag: u16, message: &Treaddir) -> io::Result<Rreaddir> {
        let path = self.path_for(message.fid).await?;
        let mut data = self.directory_data(&path, message.offset).await?;
        data.truncate(message.count as usize);
        Ok(Rreaddir { data: Data(data) })
    }

    async fn fsync(&mut self, _tag: u16, _message: &Tfsync) -> io::Result<()> {
        Ok(())
    }
    async fn lock(&mut self, _tag: u16, _message: &Tlock) -> io::Result<Rlock> {
        Ok(Rlock {
            status: P9_LOCK_SUCCESS,
        })
    }
    async fn get_lock(&mut self, _tag: u16, message: &Tgetlock) -> io::Result<Rgetlock> {
        Ok(Rgetlock {
            type_: 2,
            start: message.start,
            length: message.length,
            proc_id: message.proc_id,
            client_id: message.client_id.clone(),
        })
    }
    async fn link(&mut self, _tag: u16, _message: &Tlink) -> io::Result<()> {
        Err(unsupported())
    }

    async fn mkdir(&mut self, _tag: u16, message: &Tmkdir) -> io::Result<Rmkdir> {
        let parent = self.path_for(message.dfid).await?;
        let path = join_path(&parent, &message.name);
        let (ns, target) = self.target(&path).await?;
        ns.ensure_dir(&target).await;
        Ok(Rmkdir {
            qid: self.qid(&path).await,
        })
    }

    async fn rename_at(&mut self, _tag: u16, _message: &Trenameat) -> io::Result<()> {
        Err(unsupported())
    }
    async fn unlink_at(&mut self, _tag: u16, message: &Tunlinkat) -> io::Result<()> {
        let parent = self.path_for(message.dirfd).await?;
        let path = join_path(&parent, &message.name);
        let (ns, target) = self.target(&path).await?;
        ns.delete(&target).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    struct LocalPeer(Namespace);

    #[async_trait]
    impl crate::resources::PeerNamespace for LocalPeer {
        async fn read(&self, path: &str) -> io::Result<Vec<u8>> {
            let node = self
                .0
                .get(path)
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "path not found"))?;
            match node.kind {
                NodeKind::Json(value) => serde_json::to_vec(&value).map_err(io::Error::other),
                NodeKind::Blob(bytes) => Ok(bytes.to_vec()),
                NodeKind::Dir => {
                    serde_json::to_vec(&self.0.ls(path).await).map_err(io::Error::other)
                }
            }
        }

        async fn write(&self, path: &str, data: &[u8]) -> io::Result<Vec<u8>> {
            self.0.set_blob(path, data.to_vec().into()).await;
            Ok(data.to_vec())
        }
    }

    #[tokio::test]
    async fn attach_walk_read_and_write() {
        let ns = Namespace::new();
        ns.set_json("/cluster/health", json!({"ok": true})).await;
        let mut fs = LegionNamespace::new(ns.clone());
        fs.attach(
            1,
            &Tattach {
                fid: 1,
                afid: u32::MAX,
                uname: "test".into(),
                aname: "".into(),
                n_uname: 0,
            },
        )
        .await
        .unwrap();
        fs.walk(
            2,
            &Twalk {
                fid: 1,
                newfid: 2,
                wnames: vec!["cluster".into(), "health".into()],
            },
        )
        .await
        .unwrap();
        let read = fs
            .read(
                3,
                &Tread {
                    fid: 2,
                    offset: 0,
                    count: 4096,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&read.data).unwrap(),
            json!({"ok": true})
        );
        fs.write(
            4,
            &Twrite {
                fid: 2,
                offset: 0,
                data: Data(br#"{"ok":false}"#.to_vec()),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            ns.get("/cluster/health").await.unwrap().kind.as_json(),
            Some(&json!({"ok": false}))
        );
    }

    #[tokio::test]
    async fn attach_requires_matching_capability_when_configured() {
        let mut fs = LegionNamespace::new(Namespace::new()).with_capability_token("secret");
        let attach = |aname: &str| Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: "test".into(),
            aname: aname.into(),
            n_uname: 0,
        };
        assert_eq!(
            fs.attach(1, &attach("")).await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied,
        );
        assert_eq!(
            fs.attach(2, &attach("cap=wrong")).await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied,
        );
        fs.attach(3, &attach("cap=secret")).await.unwrap();
    }

    #[tokio::test]
    async fn function_metadata_paths_project_manifest_fields() {
        let ns = Namespace::new();
        ns.set_json(
            "/fn/hello/manifest.json",
            json!({
                "version": "1.2.3",
                "parameters": {"type": "object"}
            }),
        )
        .await;
        let fs = LegionNamespace::new(ns);

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs.read_path("/fn/hello/schema").await.unwrap()
            )
            .unwrap(),
            json!({"type": "object"})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs.read_path("/fn/hello/versions").await.unwrap()
            )
            .unwrap(),
            json!(["1.2.3"])
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs.read_path("/fn/hello/default").await.unwrap()
            )
            .unwrap(),
            json!("1.2.3")
        );
    }

    #[tokio::test]
    async fn proxies_registered_peer_paths() {
        let local = Namespace::new();
        let remote = Namespace::new();
        remote
            .set_json("/cluster/self", json!({"peer":"remote"}))
            .await;
        let mut fs = LegionNamespace::new(local);
        fs.register_peer("peer-key", Arc::new(LocalPeer(remote)))
            .await;
        fs.attach(
            1,
            &Tattach {
                fid: 1,
                afid: u32::MAX,
                uname: "test".into(),
                aname: "".into(),
                n_uname: 0,
            },
        )
        .await
        .unwrap();
        fs.walk(
            2,
            &Twalk {
                fid: 1,
                newfid: 2,
                wnames: vec![
                    "peers".into(),
                    "peer-key".into(),
                    "cluster".into(),
                    "self".into(),
                ],
            },
        )
        .await
        .unwrap();
        let read = fs
            .read(
                3,
                &Tread {
                    fid: 2,
                    offset: 0,
                    count: 4096,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&read.data).unwrap(),
            json!({"peer":"remote"})
        );
    }
}
