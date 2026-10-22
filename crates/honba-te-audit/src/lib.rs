//! `honba-te-audit` — hash-chained regulatory audit journal and SEBI
//! Annexure exporter (plan §3.6, M5).
//!
//! Module map (built up across this phase's commits):
//! - [`hash_chain`] — the SHA-256 `H_n = SHA-256(H_{n-1} |Timestamp|
//!   ClientOrdId |Payload)` chain and pure verification.
//! - [`entry`] — `AuditRecord`/`AuditSpec`/`SystemMeta` compliance schema.
//!
//! The fsynced segment WAL journal, Parquet compaction, system-identity
//! enrichment, the Annexure exporter, and the `honba-audit-export` CLI
//! follow in subsequent phase-6 commits.

pub mod entry;
pub mod hash_chain;

pub use entry::{AuditEvent, AuditRecord, AuditSpec, SystemMeta, timestamp_rfc3339_us};
pub use hash_chain::{ChainBreak, ChainLink, chain_hash, genesis, hash_hex, verify_chain};
