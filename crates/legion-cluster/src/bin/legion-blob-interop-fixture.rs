use std::{env, net::SocketAddr};

use anyhow::{Context, Result};
use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobFormat, BlobsProtocol, store::mem::MemStore, ticket::BlobTicket};

#[tokio::main]
async fn main() -> Result<()> {
    let mode = env::args().nth(1).unwrap_or_else(|| "serve".into());
    match mode.as_str() {
        "serve" => {
            let payload = env::args()
                .nth(2)
                .unwrap_or_else(|| "rust blob payload".into());
            let bind: SocketAddr = "127.0.0.1:0".parse()?;
            let endpoint = Endpoint::builder(presets::N0)
                .relay_mode(RelayMode::Disabled)
                .bind_addr(bind)?
                .bind()
                .await?;
            let store = MemStore::new();
            let tag = store.blobs().add_bytes(payload.into_bytes()).await?;
            let ticket = BlobTicket::new(endpoint.addr(), tag.hash, BlobFormat::Raw);
            let router = Router::builder(endpoint)
                .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
                .spawn();
            println!("LEGION_BLOB_READY {ticket}");
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        "fetch" => {
            let ticket: BlobTicket = env::args().nth(2).context("ticket required")?.parse()?;
            let endpoint = Endpoint::builder(presets::N0)
                .relay_mode(RelayMode::Disabled)
                .bind()
                .await?;
            let store = MemStore::new();
            let connection = endpoint
                .connect(ticket.addr().clone(), iroh_blobs::ALPN)
                .await?;
            store.remote().fetch(connection, ticket.hash()).await?;
            let bytes = store.blobs().get_bytes(ticket.hash()).await?;
            println!("LEGION_BLOB_DATA {}", String::from_utf8(bytes.to_vec())?);
            endpoint.close().await;
        }
        other => anyhow::bail!("unknown mode {other}"),
    }
    Ok(())
}
