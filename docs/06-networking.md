# Networking & Discovery

Legion uses iroh as its P2P transport layer, with mDNS/Bonjour for LAN bootstrapping and an optional DHT path for WAN connectivity. iron is far too cool not to be used more broadly, but I actually resisted adding it (largely because I am not a big fan of DHTs) until I realized I could get mDNS/Bonjour peer discovery as a first step.

## Core Principle: Identity Is a Keypair

Every Legion node is identified by its **iroh public key**, not by its IP address or hostname. This means:

- A node's identity survives IP changes, DHCP reassignment, and network topology changes
- Peers reconnect to a node by its key after restart, without manual reconfiguration
- The cluster membership list is a list of public keys + last-known addresses; addresses are hints, not identities

## iroh

iroh provides QUIC-based P2P connections addressed by public key, with NAT traversal and relay fallback.

```rust
// Bind an iroh endpoint (generates or loads keypair from disk)
let endpoint = iroh::Endpoint::builder()
    .secret_key(load_or_generate_key())
    .discovery(discovery_chain)
    .bind()
    .await?;

// Connect to a peer by key (no IP needed)
let conn = endpoint.connect(peer_addr, LEGION_ALPN).await?;
```

### Features used

| Feature | Use |
|---|---|
| QUIC transport | All cluster communication |
| Authenticated encryption | Built-in; no TLS config needed |
| NAT hole-punching | Traverse home routers, VPNs |
| Relay fallback | Connectivity through strict NAT; relay URL stored in mDNS TXT |
| Multiple streams | Raft replication, 9P sessions, blob transfers — multiplexed on one connection |

---

## Discovery Chain

iroh supports stacked discovery backends. Legion uses all three tiers:

### Tier 1: mDNS (LAN, zero-config)

`iroh-mdns-address-lookup` advertises the node's public key and direct IP addresses over mDNS. On the same LAN, nodes find each other in seconds with no configuration.

```rust
let mdns = MdnsAddressLookup::builder()
    .build(endpoint.id());
endpoint.address_lookup().unwrap().add(mdns.clone());

// Subscribe to cluster events
let mut events = mdns.subscribe().await;
// → DiscoveryEvent::Discovered { peer_id, addrs }
// → DiscoveryEvent::Expired { peer_id }
```

### Tier 2: BitTorrent DHT (WAN, optional)

`iroh-mainline-address-lookup` publishes the node's key and addresses to the BitTorrent Mainline DHT via pkarr. Enables discovery across the internet without a dedicated signaling server.

Enable via config:
```toml
[discovery]
dht = true
```

### Tier 3: Relay (fallback)

iroh's built-in relay servers ensure connectivity even through symmetric NAT. The relay URL is included in the mDNS TXT record so LAN peers can use it as a fallback channel.

---

## Bonjour / DNS-SD Registration

In addition to iroh-mdns-address-lookup (which uses iroh's internal discovery protocol), each Legion node registers a standard DNS-SD service using `mdns-sd`. This makes the cluster visible to any Bonjour browser, `dns-sd` CLI, or Avahi client on the LAN.

### Service type

```
_legion._udp.local.
```

### TXT record fields

```
iroh-key=<base58-encoded-public-key>
raft-role=leader|follower|candidate
version=0.1.0
9p-port=7777
api-port=8080
```

### Example (Avahi CLI)

```
$ avahi-browse -t _legion._udp
+ eth0 IPv4  legion-node-1  _legion._udp  local
+ eth0 IPv4  legion-node-2  _legion._udp  local
```

---

## Cluster Bootstrap

The bootstrap sequence uses mDNS discovery events to form or join the Raft cluster:

```
Node starts
  │
  ├─ iroh endpoint binds (stable keypair)
  ├─ MdnsAddressLookup starts advertising
  ├─ Wait up to 5s for DiscoveryEvent::Discovered
  │
  ├─ Peers found?
  │   YES: Request Raft cluster join via iroh QUIC → AddLearner → AddMember
  │   NO:  Start single-node Raft as leader; record self in hiqlite peers table
  │
  └─ Start hiqlite with discovered peer addresses
```

The bootstrap glue is ~100 lines in `legion-cluster`:

```rust
pub async fn bootstrap_cluster(
    endpoint: &Endpoint,
    mdns: &MdnsAddressLookup,
    raft: &hiqlite::Client,
) -> Result<()> {
    let mut events = mdns.subscribe().await;
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    let mut discovered = vec![];

    loop {
        tokio::select! {
            Some(event) = events.next() => {
                if let DiscoveryEvent::Discovered { peer_id, addrs } = event {
                    discovered.push((peer_id, addrs));
                }
            }
            _ = &mut timeout => break,
        }
    }

    if discovered.is_empty() {
        // Bootstrap single-node cluster
        raft.start_single_node().await?;
    } else {
        // Join existing cluster via first discovered leader
        let leader = elect_leader_from_peers(&discovered, raft).await?;
        raft.join_cluster(leader).await?;
    }
    Ok(())
}
```

---

## iroh-gossip: Membership and Health

`iroh-gossip` runs a gossip protocol over the iroh transport to broadcast cluster health and membership events. This supplements Raft's built-in heartbeats with:

- Faster node failure detection (gossip period < Raft election timeout)
- Lightweight liveness broadcasts without Raft log entries
- Peer-to-peer health checks independent of the leader

```toml
[gossip]
fanout = 3          # peers to forward each gossip message to
interval_ms = 1000  # gossip round interval
```

---

## Network Topology Examples

### Three NUCs on a LAN

```
Node A (192.168.1.10) — iroh key: Ki...
Node B (192.168.1.11) — iroh key: mX...
Node C (192.168.1.12) — iroh key: pQ...

Discovery: mDNS (tier 1)
Raft: Node A elected leader on first boot
9P: Clients connect to any node via iroh QUIC
```

### Mixed LAN + remote node

```
Nodes A, B (LAN) — discover via mDNS
Node C (remote)  — discovers via DHT (tier 2)

iroh relay: fallback for A↔C, B↔C connections through NAT
Raft: quorum requires 2 of 3; LAN partition does not isolate C from quorum
```

### Single developer machine

```
Node A (localhost) — single-node Raft, no peers
Useful for: local dev, testing, function authoring
Full API available; no replication overhead
```
