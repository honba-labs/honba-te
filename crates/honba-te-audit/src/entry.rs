//! Audit record schema: one SEBI-compliance order-lifecycle event
//! (plan §3.6 mandatory fields 1–7).
//!
//! Records deliberately carry canonical *strings* for money/quantity and
//! identifiers — the audit crate must journal exactly what crossed the
//! wire (and survive across broker adapters), not re-interpret it. All
//! time is injected: the journal stamps the capture instant explicitly.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hash_chain::{ChainLink, chain_hash};

/// System identity of the capture host (plan §3.6 field 6).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemMeta {
    /// Egress public IP when known to the caller.
    public_ip: Option<String>,
    /// Local interface IP (resolved by route probing).
    local_ip: Option<String>,
    /// First non-loopback NIC MAC address.
    mac_address: Option<String>,
    /// Local socket port of the capture process.
    port: Option<u16>,
    /// Hostname of the capture host.
    hostname: Option<String>,
}

impl SystemMeta {
    /// Table-constructor for tests and CLI wiring.
    pub fn new(
        public_ip: Option<String>,
        local_ip: Option<String>,
        mac_address: Option<String>,
        port: Option<u16>,
        hostname: Option<String>,
    ) -> Self {
        Self {
            public_ip,
            local_ip,
            mac_address,
            port,
            hostname,
        }
    }

    /// Public IP accessor.
    pub fn public_ip(&self) -> Option<&str> {
        self.public_ip.as_deref()
    }

    /// Local IP accessor.
    pub fn local_ip(&self) -> Option<&str> {
        self.local_ip.as_deref()
    }

    /// MAC address accessor.
    pub fn mac_address(&self) -> Option<&str> {
        self.mac_address.as_deref()
    }

    /// Port accessor.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Hostname accessor.
    pub fn hostname(&self) -> Option<&str> {
        self.hostname.as_deref()
    }
}

/// Order-lifecycle event class being journaled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEvent {
    /// Parent order accepted by the exchange gateway.
    #[default]
    Placed,
    /// Order modification accepted.
    Modified,
    /// Order cancellation accepted.
    Cancelled,
    /// Broker or exchange business rejection.
    Rejected,
    /// Execution fill reported.
    Filled,
}

impl AuditEvent {
    /// Stable lowercase label for CSV cells.
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Placed => "placed",
            Self::Modified => "modified",
            Self::Cancelled => "cancelled",
            Self::Rejected => "rejected",
            Self::Filled => "filled",
        }
    }
}

/// Microsecond-precision RFC 3339 rendering used by the chain hash and the
/// CSV exporter — a single source of timestamp truth for verification.
#[must_use]
pub fn timestamp_rfc3339_us(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Per-entry order context the journal needs, sans chain bookkeeping.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuditSpec {
    /// Client order ID (plan §3.6 field 2).
    pub client_order_id: Uuid,
    /// Exchange-assigned order ID, when the gateway echoed one.
    pub exchange_order_id: Option<String>,
    /// Registered SEBI algo ID (field 3).
    pub algo_id: String,
    /// Client account identifier (field 4).
    pub client_account_id: String,
    /// Client PAN / Demat ID (field 4).
    pub client_pan: String,
    /// Order side label, e.g. `BUY` (field 5).
    pub side: String,
    /// Product label, e.g. `MIS` / `CNC` (field 5).
    pub product: String,
    /// Order type label, e.g. `LIMIT` / `MARKET` (field 5).
    pub order_type: String,
    /// Quantity as written on the wire (field 5).
    pub quantity: String,
    /// Price as written on the wire (field 5).
    pub price: String,
    /// Event class being journaled.
    pub event: AuditEvent,
    /// Raw HTTP/WebSocket wire payload bound into the hash (field 7).
    pub raw_payload: String,
}

/// One sealed journal entry: mandatory SEBI fields + chain bookkeeping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// Monotonic 1-based journal sequence (chain position).
    pub seq: u64,
    /// Microsecond UTC capture timestamp (field 1).
    pub captured_at: DateTime<Utc>,
    // -- mandatory §3.6 fields (2–7) -----------------------------------
    pub client_order_id: Uuid,
    pub exchange_order_id: Option<String>,
    pub algo_id: String,
    pub client_account_id: String,
    pub client_pan: String,
    pub side: String,
    pub product: String,
    pub order_type: String,
    pub quantity: String,
    pub price: String,
    pub event: AuditEvent,
    pub system: SystemMeta,
    pub raw_payload: String,
    // -- chain bookkeeping ---------------------------------------------
    /// Digest of the preceding entry (genesis for the first).
    pub prev_hash: String,
    /// This entry's sealed digest.
    pub hash: String,
}

impl AuditRecord {
    /// Seal the record: bind lineage into `prev_hash` and compute this
    /// entry's digest over the canonical chain inputs.
    #[must_use]
    pub fn finalize(mut self, prev_hash: String) -> Self {
        self.prev_hash = prev_hash;
        self.hash = chain_hash(
            &self.prev_hash,
            &self.captured_at_text(),
            &self.client_order_id_text(),
            &self.raw_payload,
        );
        self
    }

    /// Canonical capture timestamp text (chain input).
    #[must_use]
    pub fn captured_at_text(&self) -> String {
        timestamp_rfc3339_us(self.captured_at)
    }

    /// Canonical client order id text (chain input).
    #[must_use]
    pub fn client_order_id_text(&self) -> String {
        self.client_order_id.to_string()
    }

    /// The pure verification view of this record.
    #[must_use]
    pub fn chain_link(&self) -> ChainLink {
        ChainLink {
            prev_hash: self.prev_hash.clone(),
            timestamp: self.captured_at_text(),
            client_order_id: self.client_order_id_text(),
            payload: self.raw_payload.clone(),
            hash: self.hash.clone(),
        }
    }
}
