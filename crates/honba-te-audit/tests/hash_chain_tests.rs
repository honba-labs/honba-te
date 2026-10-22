//! M5 hash-chain tests (plan §3.6): genesis anchor, digest determinism,
//! chain linking, and tamper detection.

use honba_te_audit::{ChainBreak, ChainLink, chain_hash, genesis, hash_hex, verify_chain};

fn sealed_link(prev: &str, timestamp: &str, id: &str, payload: &str) -> ChainLink {
    ChainLink {
        prev_hash: prev.to_owned(),
        timestamp: timestamp.to_owned(),
        client_order_id: id.to_owned(),
        payload: payload.to_owned(),
        hash: chain_hash(prev, timestamp, id, payload),
    }
}

#[test]
fn genesis_is_deterministic_hex_root() {
    assert_eq!(
        genesis(),
        genesis(),
        "chain root must be a fixed well-known value"
    );
    let root = genesis();
    assert_eq!(root.len(), 64, "SHA-256 renders as 64 hex chars");
    assert!(root.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_ne!(
        genesis(),
        hash_hex("honba-audit-genesis-v2"),
        "seed matters"
    );
}

#[test]
fn chain_hash_is_deterministic_and_field_sensitive() {
    let first = chain_hash(&genesis(), "ts-1", "client-1", "raw");
    assert_eq!(first, chain_hash(&genesis(), "ts-1", "client-1", "raw"));
    assert_ne!(
        first,
        chain_hash(&genesis(), "ts-1", "client-1", "RAW"),
        "payload order"
    );
    assert_ne!(
        first,
        chain_hash(&genesis(), "ts-2", "client-1", "raw"),
        "timestamp order"
    );
    assert_ne!(
        first,
        chain_hash(&genesis(), "ts-1", "client-2", "raw"),
        "order id order"
    );
    // The '|' delimiter keeps field boundaries unambiguous: "a|b" + "c" can
    // never collide with "a" + "b|c" inside the four-field line.
    assert_ne!(
        chain_hash(&genesis(), "x|y", "z", "p"),
        chain_hash(&genesis(), "x", "y|z", "p")
    );
}

#[test]
fn sealed_chain_passes_verification() {
    let link1 = sealed_link(
        &genesis(),
        "2026-01-01T00:00:00.000001Z",
        "id-1",
        "payload-1",
    );
    let link2 = sealed_link(
        &link1.hash,
        "2026-01-01T00:00:00.000002Z",
        "id-2",
        "payload-2",
    );
    let link3 = sealed_link(
        &link2.hash,
        "2026-01-01T00:00:00.000003Z",
        "id-3",
        "payload-3",
    );
    assert_ne!(link3.hash, link2.hash, "every entry links forward");
    verify_chain(&[link1, link2, link3]).expect("sealed chain must verify");
    verify_chain(&[]).expect("empty chain is trivially valid");
}

#[test]
fn tampered_payload_breaks_the_chain_at_the_right_seq() {
    let link1 = sealed_link(&genesis(), "t1", "id-1", "payload-1");
    let link2 = sealed_link(&link1.hash, "t2", "id-2", "payload-2");
    let mut forged = link2.clone();
    forged.payload = "payload-2-TAMPERED".into();
    match verify_chain(&[link1, forged]) {
        Err(ChainBreak::BrokenLink { seq, .. }) => assert_eq!(seq, 2, "first break reported"),
        other => panic!("expected BrokenLink, got {other:?}"),
    }
}

#[test]
fn forged_genesis_anchor_is_flagged() {
    let mut link = sealed_link(&genesis(), "t1", "id-1", "payload-1");
    link.prev_hash = hash_hex("forged-root");
    match verify_chain(&[link]) {
        Err(ChainBreak::GenesisMismatch { seq, .. }) => assert_eq!(seq, 1),
        other => panic!("expected GenesisMismatch, got {other:?}"),
    }
}
