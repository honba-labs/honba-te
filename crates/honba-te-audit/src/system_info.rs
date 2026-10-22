//! Best-effort system identity capture (plan §3.6 field 6).
//!
//! Local IP / MAC / hostname are probed through the OS surface; public IP
//! must be supplied by the caller when known (an egress probe would make
//! journal writes network-dependent). Every probe is best-effort: absent
//! surfaces degrade to `None` rather than failing appends.

use std::net::UdpSocket;
use std::path::Path;

use crate::entry::SystemMeta;

/// Capture the capture-host identity snapshot.
#[must_use]
pub fn capture_system_meta(public_ip: Option<String>, port: Option<u16>) -> SystemMeta {
    SystemMeta::new(public_ip, local_ip(), mac_address(), port, hostname())
}

/// Local interface IP via a connected UDP socket.
///
/// UDP `connect` performs no I/O against the peer (it only pins the local
/// route), so this never sends packets.
fn local_ip() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    Some(socket.local_addr().ok()?.ip().to_string())
}

/// MAC address of the first non-loopback NIC (Linux `/sys/class/net`).
fn mac_address() -> Option<String> {
    let net_dir = Path::new("/sys/class/net");
    let mut interfaces: Vec<_> = std::fs::read_dir(net_dir).ok()?.flatten().collect();
    interfaces.sort_by_key(std::fs::DirEntry::file_name);
    for interface in interfaces {
        if interface.file_name() == "lo" {
            continue;
        }
        let address_path = interface.path().join("address");
        let address = std::fs::read_to_string(&address_path).ok()?;
        let trimmed = address.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    None
}

/// Capture-host hostname (env, then common proc files).
fn hostname() -> Option<String> {
    if let Ok(name) = std::env::var("HOSTNAME") {
        let name = name.trim();
        if !name.is_empty() {
            return Some(name.to_owned());
        }
    }
    for candidate in ["/etc/hostname", "/proc/sys/kernel/hostname"] {
        if let Ok(name) = std::fs::read_to_string(candidate) {
            let name = name.trim();
            if !name.is_empty() {
                return Some(name.to_owned());
            }
        }
    }
    None
}
