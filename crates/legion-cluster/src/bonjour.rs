//! Bonjour / mDNS service registration via mdns-sd.
//!
//! Registers `_durable-fn._udp.local.` so Legion nodes can bootstrap
//! on a LAN without any internet connectivity.

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use tracing::{info, warn};

/// Bonjour service type for Legion cluster members.
pub const SERVICE_TYPE: &str = "_durable-fn._udp.local.";

/// A running Bonjour registration that unregisters on drop.
pub struct BonjourRegistration {
    daemon:        ServiceDaemon,
    instance_name: String,
}

impl BonjourRegistration {
    /// Register this node with mDNS.
    ///
    /// `node_id_hex` must be unique per node (iroh public key in hex form).
    /// `port` is the port to advertise (typically the REST API port).
    pub fn register(
        node_id_hex:  &str,
        hostname:     &str,
        ip_addr:      &str,
        port:         u16,
    ) -> Result<Self> {
        let daemon = ServiceDaemon::new()
            .context("create mdns-sd ServiceDaemon")?;

        // Instance name = first 16 chars of node_id for compactness
        let instance_name = format!("legion-{}", &node_id_hex[..node_id_hex.len().min(16)]);

        let host_name = if hostname.ends_with('.') {
            hostname.to_string()
        } else {
            format!("{hostname}.local.")
        };

        let properties = [
            ("node_id",  node_id_hex),
            ("version",  env!("CARGO_PKG_VERSION")),
        ];

        let service = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            ip_addr,
            port,
            &properties[..],
        ).context("build ServiceInfo")?;

        daemon.register(service)
            .context("register mdns service")?;

        info!(
            instance = %instance_name,
            ip       = %ip_addr,
            port,
            "Bonjour service registered"
        );

        Ok(Self { daemon, instance_name })
    }

    /// Browse for other Legion nodes on the LAN.
    ///
    /// Returns a channel that receives resolved `ServiceInfo` records.
    pub fn browse(&self) -> Result<mdns_sd::Receiver<mdns_sd::ServiceEvent>> {
        self.daemon
            .browse(SERVICE_TYPE)
            .context("browse mdns service type")
    }
}

impl Drop for BonjourRegistration {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.unregister(&self.instance_name) {
            warn!("mdns unregister error: {e}");
        }
        let _ = self.daemon.shutdown();
    }
}
