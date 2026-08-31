use std::{path::Path, time::Duration};

use hiqlite::{Node, NodeConfig};
use legion_core::{
    traits::EventStore,
    types::{RunConfig, SessionStatus, TurnEvent},
};
use legion_store::HiqliteStore;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

fn nodes() -> Vec<Node> {
    vec![
        Node {
            id: 1,
            addr_raft: "127.0.0.1:37101".into(),
            addr_api: "127.0.0.1:37201".into(),
        },
        Node {
            id: 2,
            addr_raft: "127.0.0.1:37102".into(),
            addr_api: "127.0.0.1:37202".into(),
        },
        Node {
            id: 3,
            addr_raft: "127.0.0.1:37103".into(),
            addr_api: "127.0.0.1:37203".into(),
        },
    ]
}

fn config(id: u64, data_dir: &Path) -> NodeConfig {
    NodeConfig {
        node_id: id,
        nodes: nodes(),
        data_dir: data_dir.to_string_lossy().to_string().into(),
        secret_raft: "legion-test-raft-secret".into(),
        secret_api: "legion-test-api-secret".into(),
        health_check_delay_secs: 0,
        ..Default::default()
    }
}

async fn wait_for_leader(stores: &[Option<HiqliteStore>], excluded: Option<u64>) -> u64 {
    timeout(Duration::from_secs(30), async {
        loop {
            for store in stores.iter().flatten() {
                if let Some(leader) = store.raft_leader().await
                    && Some(leader) != excluded
                {
                    return leader;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("cluster did not elect a leader")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn session_survives_leader_kill_and_node_rejoin() {
    let root = TempDir::new().unwrap();
    let (node_1, node_2, node_3) = tokio::join!(
        HiqliteStore::connect(config(1, &root.path().join("node-1"))),
        HiqliteStore::connect(config(2, &root.path().join("node-2"))),
        HiqliteStore::connect(config(3, &root.path().join("node-3"))),
    );
    let mut stores = vec![
        Some(node_1.unwrap()),
        Some(node_2.unwrap()),
        Some(node_3.unwrap()),
    ];

    let first_leader = wait_for_leader(&stores, None).await;
    let remote = HiqliteStore::remote(
        nodes().into_iter().map(|node| node.addr_api).collect(),
        "legion-test-api-secret".into(),
    )
    .await
    .unwrap();
    let writer = stores
        .iter()
        .enumerate()
        .find(|(index, store)| store.is_some() && (*index as u64 + 1) != first_leader)
        .map(|(_, store)| store.as_ref().unwrap().clone())
        .unwrap();
    let run_id = Uuid::new_v4();
    writer
        .create_session(
            run_id,
            &RunConfig {
                system_prompt: None,
                model: "faux/failover".into(),
                budget: Default::default(),
                tools: vec![],
                metadata: None,
            },
        )
        .await
        .unwrap();
    writer
        .append(run_id, TurnEvent::user_message("before failover"))
        .await
        .unwrap();

    let stopped_index = (first_leader - 1) as usize;
    let stopped = stores[stopped_index].take().unwrap();
    stopped.shutdown().await.unwrap();
    drop(stopped);

    let second_leader = wait_for_leader(&stores, Some(first_leader)).await;
    for store in stores.iter().flatten() {
        eprintln!("after leader stop: {}", store.raft_diagnostics().await);
    }
    assert_ne!(first_leader, second_leader);
    let survivor = stores[(second_leader - 1) as usize].as_ref().unwrap();
    assert_eq!(survivor.read_log(run_id).await.unwrap().len(), 1);
    timeout(Duration::from_secs(30), async {
        loop {
            match remote.set_status(run_id, SessionStatus::Completed).await {
                Ok(()) => break,
                Err(error) => {
                    eprintln!("post-failover write not ready: {error}");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .await
    .expect("surviving quorum did not accept writes after leader loss");

    stores[stopped_index] = Some(
        HiqliteStore::connect(config(
            first_leader,
            &root.path().join(format!("node-{first_leader}")),
        ))
        .await
        .unwrap(),
    );
    timeout(Duration::from_secs(30), async {
        loop {
            let rejoined = stores[stopped_index].as_ref().unwrap();
            if matches!(rejoined.read_log(run_id).await, Ok(log) if log.len() == 1)
                && matches!(
                    rejoined.session_status(run_id).await,
                    Ok(SessionStatus::Completed)
                )
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("rejoined node did not auto-heal replicated session state");

    for store in stores.into_iter().flatten() {
        let _ = store.shutdown().await;
    }
}
