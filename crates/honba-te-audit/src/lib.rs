//! `honba-te-audit` — hash-chained regulatory audit journal and SEBI
//! Annexure exporter (plan §3.6, M5).
//!
//! Module map:
//! - [`hash_chain`] — the SHA-256 `H_n = SHA-256(H_{n-1} |Timestamp|
//!   ClientOrdId |Payload)` chain and pure verification.
//! - [`entry`] — `AuditRecord`/`AuditSpec`/`SystemMeta` compliance schema.
//! - [`journal`] — fsynced append-only JSONL segment WAL + Parquet
//!   compaction + tamper-evidence verification.
//! - [`system_info`] — best-effort capture-host identity (IP/MAC/hostname).
//! - [`exporter`] — fixed-column SEBI Annexure CSV rendering.
//!
//! The `honba-audit-export` binary ties verify → export → (optional)
//! Parquet compaction together for inspection workflows.

pub mod entry;
pub mod exporter;
pub mod hash_chain;
pub mod journal;
pub mod system_info;

pub use entry::{AuditEvent, AuditRecord, AuditSpec, SystemMeta, timestamp_rfc3339_us};
pub use exporter::{ANNEXURE_COLUMNS, escape_field, to_annexure_csv};
pub use hash_chain::{ChainBreak, ChainLink, chain_hash, genesis, hash_hex, verify_chain};
pub use journal::{AuditJournal, JournalError, SEGMENT_ROTATION};
