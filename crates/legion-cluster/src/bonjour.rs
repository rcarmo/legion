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
    daemon: ServiceDaemon,
    instance_name: String,
}

impl std::fmt::Debug for BonjourRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BonjourRegistration")
            .field("instance_name", &self.instance_name)
            .finish_non_exhaustive()
    }
}

impl BonjourRegistration {
    /// Register this node with mDNS.
    ///
    /// `node_id_hex` must be unique per node (iroh public key in hex form).
    /// `port` is the port to advertise (typically the REST API port).
    pub fn register(
        node_id_hex: &str,
        hostname: &str,
        ip_addr: &str,
        port: u16,
        properties: &[(&str, String)],
    ) -> Result<Self> {
        let daemon = ServiceDaemon::new().context("create mdns-sd ServiceDaemon")?;

        // Instance name = first 16 chars of node_id for compactness
        let instance_name = format!("legion-{}", &node_id_hex[..node_id_hex.len().min(16)]);

        let host_name = if hostname.ends_with('.') {
            hostname.to_string()
        } else {
            format!("{hostname}.local.")
        };

        let mut service_properties = vec![
            ("node_id", node_id_hex.to_string()),
            ("version", env!("CARGO_PKG_VERSION").to_string()),
        ];
        service_properties.extend_from_slice(properties);
        let property_refs = service_properties
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect::<Vec<_>>();

        let service = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            ip_addr,
            port,
            &property_refs[..],
        )
        .context("build ServiceInfo")?;

        daemon.register(service).context("register mdns service")?;

        info!(
            instance = %instance_name,
            ip       = %ip_addr,
            port,
            "Bonjour service registered"
        );

        Ok(Self {
            daemon,
            instance_name,
        })
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

impl BonjourRegistration {
    /// Resolve the service addresses, replacing wildcard hosts in Raft TXT values.
    pub fn resolve_advertised_addr(
        info: &mdns_sd::ResolvedService,
        property: &str,
    ) -> Option<String> {
        let advertised = info.get_properties().get(property)?.val_str();
        let (host, port) = advertised.rsplit_once(':')?;
        if host != "0.0.0.0" && host != "::" && host != "[::]" {
            return Some(advertised.to_string());
        }
        let ip = info.get_addresses().iter().next()?;
        Some(format!("{ip}:{port}"))
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
