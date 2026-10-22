//! Tamper-evident SHA-256 hash chain (plan §3.6).
//!
//! Every audit entry carries the cryptographic digest of its lineage:
//!
//! ```text
//! H_n = SHA-256(H_{n-1} | Timestamp | ClientOrdId | Payload)
//! ```
//!
//! The `|` separator is the plan's `‖` concat realized as an unambiguous
//! framing: each field is escaped (`\` → `\\`, `|` → `\|`) before joining,
//! so no rebalancing of delimiters across field boundaries can produce two
//! different field triples with the same digest input. The chain root is
//! [`genesis`], a fixed well-known digest so the first entry can never
//! pretend continuity.

use sha2::{Digest, Sha256};

/// Well-known ASCII seed hashed to produce the chain root.
pub const GENESIS_SEED: &str = "honba-audit-genesis-v1";

/// SHA-256 of `input`, hex-encoded.
#[must_use]
pub fn hash_hex(input: &str) -> String {
    hex::encode(Sha256::digest(input.as_bytes()))
}

/// The fixed chain root every journal starts from.
#[must_use]
pub fn genesis() -> String {
    hash_hex(GENESIS_SEED)
}

/// One entry's digest input plus its stored digest — the minimum a
/// verifier needs to recompute and confirm the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainLink {
    /// Digest carried over from the previous entry (`H_{n-1}`).
    pub prev_hash: String,
    /// Capture timestamp, RFC 3339 with microsecond precision.
    pub timestamp: String,
    /// Client order identifier text as it appeared in the entry.
    pub client_order_id: String,
    /// Raw wire payload bound into the digest.
    pub payload: String,
    /// The stored digest under test.
    pub hash: String,
}

/// Where and why chain verification failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainBreak {
    /// The first entry does not anchor to the fixed genesis digest.
    GenesisMismatch {
        /// Position of the offending entry (1-based sequence).
        seq: u64,
        /// Digest expected (`genesis()`).
        expected: String,
        /// Digest actually stored.
        actual: String,
    },
    /// A recomputed digest differs from the stored one: the bytes changed
    /// after the entry was sealed.
    BrokenLink {
        /// Position of the offending entry (1-based sequence).
        seq: u64,
        /// Digest recomputed from the entry's canonical inputs.
        expected: String,
        /// Digest actually stored.
        actual: String,
    },
}

/// Recompute the digest for one entry.
///
/// Fields are escaped (`\` → `\\`, `|` → `\|`) so the four-field join is
/// injective: no two distinct `(prev, timestamp, id, payload)` tuples can
/// map to the same digest input.
#[must_use]
pub fn chain_hash(
    prev_hash: &str,
    timestamp: &str,
    client_order_id: &str,
    payload: &str,
) -> String {
    hash_hex(&format!(
        "{}|{}|{}|{}",
        escape_field(prev_hash),
        escape_field(timestamp),
        escape_field(client_order_id),
        escape_field(payload)
    ))
}

/// Backslash-pipe escaping keeping the framed digest input injective.
fn escape_field(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '|' => escaped.push_str("\\|"),
            other => escaped.push(other),
        }
    }
    escaped
}

impl std::fmt::Display for ChainBreak {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GenesisMismatch {
                seq,
                expected,
                actual,
            } => write!(
                formatter,
                "genesis anchor mismatch at sequence {seq}: expected {expected}, found {actual}"
            ),
            Self::BrokenLink {
                seq,
                expected,
                actual,
            } => write!(
                formatter,
                "hash mismatch at sequence {seq}: expected {expected}, found {actual}"
            ),
        }
    }
}

/// Verify a full chain slice (in journal order). Empty slices are valid.
///
/// # Errors
/// Returns the first [`ChainBreak`] encountered, tagged with the 1-based
/// sequence number of the offending entry.
pub fn verify_chain(links: &[ChainLink]) -> Result<(), ChainBreak> {
    let root = genesis();
    for (index, link) in links.iter().enumerate() {
        let seq = index as u64 + 1;
        if index == 0 && link.prev_hash != root {
            return Err(ChainBreak::GenesisMismatch {
                seq,
                expected: root,
                actual: link.prev_hash.clone(),
            });
        }
        let recomputed = chain_hash(
            &link.prev_hash,
            &link.timestamp,
            &link.client_order_id,
            &link.payload,
        );
        if recomputed != link.hash {
            return Err(ChainBreak::BrokenLink {
                seq,
                expected: recomputed,
                actual: link.hash.clone(),
            });
        }
    }
    Ok(())
}
