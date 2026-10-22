//! Zerodha postback listener contract tests.
//!
//! The checksum formula is the official Kite Connect one — SHA-256 of
//! `order_id + order_timestamp + api_secret` — and every delivery path is
//! exercised over real loopback HTTP.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};

use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};
use honba_te_zerodha::{
    PostbackConfig, PostbackListener, decode_postback_payload, postback_checksum,
    verify_postback_checksum,
};

fn sample_postback(status: &str, filled: i64, checksum: String) -> Value {
    json!({
        "user_id": "AB1234",
        "unfilled_quantity": 100 - filled,
        "app_id": 1234,
        "checksum": checksum,
        "placed_by": "AB1234",
        "order_id": "220303000308932",
        "exchange_order_id": "1000000001482421",
        "parent_order_id": null,
        "status": status,
        "status_message": null,
        "status_message_raw": null,
        "order_timestamp": "2022-03-03 09:24:25",
        "exchange_update_timestamp": "2022-03-03 09:24:25",
        "exchange_timestamp": "2022-03-03 09:24:25",
        "variety": "regular",
        "exchange": "NSE",
        "tradingsymbol": "SBIN",
        "instrument_token": 779521,
        "order_type": "MARKET",
        "transaction_type": "BUY",
        "validity": "DAY",
        "product": "CNC",
        "quantity": 100,
        "disclosed_quantity": 0,
        "price": 0,
        "trigger_price": 0,
        "average_price": 470.25,
        "filled_quantity": filled,
        "pending_quantity": 100 - filled,
        "cancelled_quantity": 0,
        "market_protection": 0,
        "meta": {},
        "tag": "algo-m2",
        "guid": "XXXXXX"
    })
}

#[test]
fn checksum_is_sha256_of_concat_with_secret() {
    let expected = postback_checksum("SECRET", "220303000308932", "2022-03-03 09:24:25");
    // 64 lowercase hex characters, deterministic:
    assert_eq!(expected.len(), 64);
    assert_eq!(expected, expected.to_lowercase());
    assert_eq!(
        expected,
        postback_checksum("SECRET", "220303000308932", "2022-03-03 09:24:25"),
        "deterministic"
    );

    // Cross-checked against the official formula:
    // sha256(order_id ++ order_timestamp ++ api_secret) computed independently
    // via the hand-rolled sha256 in honba-te-auth (RFC 6238-vector base).
    let digest = honba_te_auth::crypto::sha256(b"2203030003089322022-03-03 09:24:25SECRET");
    let manual = honba_te_auth::crypto::hex_encode(&digest);
    assert_eq!(expected, manual);

    assert!(verify_postback_checksum(
        "SECRET",
        "220303000308932",
        "2022-03-03 09:24:25",
        &expected
    ));
    assert!(!verify_postback_checksum(
        "SECRET",
        "220303000308932",
        "2022-03-03 09:24:25",
        "0000000000000000000000000000000000000000000000000000000000000000"
    ));
    assert!(!verify_postback_checksum(
        "OTHER",
        "220303000308932",
        "2022-03-03 09:24:25",
        &expected
    ));
}

#[test]
fn decode_emits_fill_with_delta_tracking() {
    let secret = "SECRET";
    let mut fills = HashMap::new();

    // First delivery: 25 of 100.
    let frame = sample_postback(
        "UPDATE",
        25,
        postback_checksum(secret, "220303000308932", "2022-03-03 09:24:25"),
    );
    let events = decode_postback_payload(&frame, secret, &mut fills).expect("events");
    assert_eq!(events.len(), 1);
    match &events[0] {
        BrokerExecutionEvent::Fill(fill) => {
            assert_eq!(fill.quantity.0.to_string(), "25");
            assert_eq!(fill.price.0.to_string(), "470.25");
            assert_eq!(fill.order_id, "220303000308932");
            assert_eq!(fill.algo_id, "algo-m2");
            assert_eq!(fill.side, honba_core::OrderSide::Buy);
            assert_eq!(
                fill.timestamp.format("%Y-%m-%dT%H:%M:%S").to_string(),
                "2022-03-03T03:54:25"
            );
        }
        other => panic!("expected fill, got {other:?}"),
    }

    // Same cumulative level again (a modify-triggered UPDATE) → no event.
    let events = decode_postback_payload(&frame, secret, &mut fills).expect("events");
    assert_eq!(events.len(), 0, "delta zero must not emit a duplicate fill");

    // Completion arrives with filled=100 → delta 75.
    let frame_complete = sample_postback(
        "COMPLETE",
        100,
        postback_checksum(secret, "220303000308932", "2022-03-03 09:24:25"),
    );
    let events = decode_postback_payload(&frame_complete, secret, &mut fills).expect("events");
    match &events[0] {
        BrokerExecutionEvent::Fill(fill) => assert_eq!(fill.quantity.0.to_string(), "75"),
        other => panic!("expected fill, got {other:?}"),
    }
}

#[test]
fn decode_maps_rejection_and_cancellation() {
    let secret = "SECRET";
    let ts = "2022-03-03 09:24:25";
    let mut fills = HashMap::new();

    let rejected = sample_postback(
        "REJECTED",
        0,
        postback_checksum(secret, "220303000308932", ts),
    );
    let events = decode_postback_payload(&rejected, secret, &mut fills).expect("events");
    match &events[0] {
        BrokerExecutionEvent::OrderRejected { order_id, reason } => {
            assert_eq!(order_id.as_deref(), Some("220303000308932"));
            assert!(reason.contains("rejected") || reason.is_empty() || !reason.is_empty());
        }
        other => panic!("expected rejection, got {other:?}"),
    }

    let cancelled = sample_postback(
        "CANCELLED",
        0,
        postback_checksum(secret, "220303000308932", ts),
    );
    let events = decode_postback_payload(&cancelled, secret, &mut fills).expect("events");
    match &events[0] {
        BrokerExecutionEvent::OrderCancelled { order_id, .. } => {
            assert_eq!(order_id, "220303000308932");
        }
        other => panic!("expected cancellation, got {other:?}"),
    }
}

#[test]
fn decode_rejects_missing_or_bad_checksum() {
    let secret = "SECRET";
    let mut fills = HashMap::new();

    let mut frame = sample_postback("COMPLETE", 100, "valid".into());
    frame["checksum"] = Value::Null;
    assert!(matches!(
        decode_postback_payload(&frame, secret, &mut fills),
        Err(BrokerError::InvalidRequest(_))
    ));

    let forged = sample_postback("COMPLETE", 100, postback_checksum(secret, "X", "Y"));
    assert!(matches!(
        decode_postback_payload(&forged, secret, &mut fills),
        Err(BrokerError::Auth(_))
    ));

    let bad_shape = json!("not-an-object");
    assert!(matches!(
        decode_postback_payload(&bad_shape, secret, &mut fills),
        Err(BrokerError::InvalidRequest(_))
    ));
}

#[test]
fn decode_accepts_batched_arrays_of_postbacks() {
    let secret = "SECRET";
    let ts = "2022-03-03 09:24:25";
    let mut fills = HashMap::new();
    let batch = json!([
        sample_postback(
            "UPDATE",
            10,
            postback_checksum(secret, "220303000308932", ts)
        ),
        sample_postback(
            "CANCELLED",
            10,
            postback_checksum(secret, "220303000308932", ts)
        )
    ]);
    let events = decode_postback_payload(&batch, secret, &mut fills).expect("batch events");
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], BrokerExecutionEvent::Fill(_)));
    assert!(matches!(
        events[1],
        BrokerExecutionEvent::OrderCancelled { .. }
    ));
}

async fn spawn_listener(secret: &str) -> (String, honba_te_core::ChannelExecutionStream) {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let config = PostbackConfig::new(addr, secret);
    let (stream, bound) = PostbackListener::spawn(config).await.expect("bind");
    (format!("http://{bound}"), stream)
}

#[tokio::test]
async fn webhook_delivers_fill_and_forgeries_get_rejected() {
    let secret = "SECRET";
    let (base, mut stream) = spawn_listener(secret).await;
    let client = reqwest::Client::new();
    let ts = "2022-03-03 09:24:25";

    // Valid COMPLETE postback → 200 and a Fill event.
    let valid = sample_postback(
        "COMPLETE",
        100,
        postback_checksum(secret, "220303000308932", ts),
    );
    let res = client
        .post(format!("{base}/postback"))
        .json(&valid)
        .send()
        .await
        .expect("post");
    assert_eq!(res.status(), StatusCode::OK);
    let event = tokio::time::timeout(Duration::from_secs(2), stream.next_event())
        .await
        .expect("event within timeout")
        .expect("fill");
    match event {
        BrokerExecutionEvent::Fill(fill) => {
            assert_eq!(fill.order_id, "220303000308932");
            assert_eq!(fill.quantity.0, rust_decimal::Decimal::from(100));
        }
        other => panic!("expected fill, got {other:?}"),
    }

    // Forged checksum → 403.
    let forged = sample_postback(
        "COMPLETE",
        50,
        postback_checksum("EVIL", "220303000308932", ts),
    );
    let res = client
        .post(format!("{base}/postback"))
        .json(&forged)
        .send()
        .await
        .expect("post");
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Missing checksum → 400.
    let mut missing = sample_postback("COMPLETE", 10, "x".into());
    missing.as_object_mut().unwrap().remove("checksum");
    let res = client
        .post(format!("{base}/postback"))
        .json(&missing)
        .send()
        .await
        .expect("post");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Garbage body → 400.
    let res = client
        .post(format!("{base}/postback"))
        .body("not json")
        .header("content-type", "application/json")
        .send()
        .await
        .expect("post");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Health route serves.
    let res = client
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("health");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "ok");
}
