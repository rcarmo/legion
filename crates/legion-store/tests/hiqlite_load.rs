use std::{path::Path, time::Instant};

use hiqlite::{Node, NodeConfig, params};
use tempfile::TempDir;

const INSERTS: usize = 25_000;
const BATCH_SIZE: usize = 500;
const MIN_INSERTS_PER_SECOND: f64 = 24_500.0;

fn nodes() -> Vec<Node> {
    (1..=3)
        .map(|id| Node {
            id,
            addr_raft: format!("127.0.0.1:{}", 38100 + id),
            addr_api: format!("127.0.0.1:{}", 38200 + id),
        })
        .collect()
}

fn config(id: u64, data_dir: &Path) -> NodeConfig {
    NodeConfig {
        node_id: id,
        nodes: nodes(),
        data_dir: data_dir.to_string_lossy().to_string().into(),
        secret_raft: "legion-load-raft-secret".into(),
        secret_api: "legion-load-api-secret".into(),
        health_check_delay_secs: 0,
        ..Default::default()
    }
}

/// Reproducible three-node replicated write gate. Ignored in the ordinary test
/// suite because it is a capacity test, not a unit test.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run with `make load-test-hiqlite` on dedicated local ports"]
async fn replicated_batch_inserts_exceed_target() {
    let root = TempDir::new().unwrap();
    let (one, two, three) = tokio::join!(
        hiqlite::start_node(config(1, &root.path().join("node-1"))),
        hiqlite::start_node(config(2, &root.path().join("node-2"))),
        hiqlite::start_node(config(3, &root.path().join("node-3"))),
    );
    let clients = vec![one.unwrap(), two.unwrap(), three.unwrap()];
    clients[0].wait_until_healthy_db().await;
    clients[0]
        .execute(
            "CREATE TABLE IF NOT EXISTS load_events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            params!(),
        )
        .await
        .unwrap();

    let started = Instant::now();
    for base in (0..INSERTS).step_by(BATCH_SIZE) {
        let batch = (base..base + BATCH_SIZE).map(|id| {
            (
                "INSERT INTO load_events (id, payload) VALUES ($1, $2)",
                params!(id as i64, "0123456789abcdef0123456789abcdef"),
            )
        });
        for result in clients[0].txn(batch).await.unwrap() {
            assert_eq!(result.unwrap(), 1);
        }
    }
    let elapsed = started.elapsed();
    let rate = INSERTS as f64 / elapsed.as_secs_f64();
    eprintln!(
        "hiqlite replicated load: {INSERTS} inserts in {:.3}s = {:.0} inserts/s",
        elapsed.as_secs_f64(),
        rate,
    );
    assert!(
        rate >= MIN_INSERTS_PER_SECOND,
        "{rate:.0} inserts/s is below {MIN_INSERTS_PER_SECOND:.0} target"
    );

    for client in clients {
        client.shutdown().await.unwrap();
    }
}
