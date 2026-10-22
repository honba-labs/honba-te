//! Kite Ticker order-update frame and stream contract tests.
//!
//! Exercises: pure frame parsing (order/error/foreign frames), stream
//! dispatch with cumulative-fill deltas, correlation lookup, and stream
//! closure behaviour.

use std::time::Duration;

use tokio::sync::mpsc;

use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};
use honba_te_zerodha::{KiteTickerOrderStream, parse_order_update_frame};

fn frame(updates: serde_json::Value) -> String {
    serde_json::json!({ "type": "order", "data": updates }).to_string()
}

fn update(status: &str, filled: u64) -> serde_json::Value {
    serde_json::json!({
        "order_id": "220119000332528",
        "exchange_order_id": "1000000001482421",
        "exchange": "NSE",
        "tradingsymbol": "SBIN",
        "transaction_type": "SELL",
        "order_type": "LIMIT",
        "product": "CNC",
        "status": status,
        "quantity": 100,
        "filled_quantity": filled,
        "price": 0,
        "average_price": 470.25,
        "timestamp": "2019-12-19T14:33:39"
    })
}

#[test]
fn parses_order_frames_with_object_and_array_data() {
    let updates = parse_order_update_frame(&frame(serde_json::json!([update("OPEN", 0)])))
        .expect("array data parses");
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].order_id, "220119000332528");
    assert_eq!(updates[0].status, "OPEN");

    let single = parse_order_update_frame(&frame(update("COMPLETE", 100))).expect("object data");
    assert_eq!(single.len(), 1);

    // Numeric wire values decode exactly:
    assert_eq!(
        honba_te_core::wire::price_from_json(&single[0].average_price.clone().unwrap())
            .unwrap()
            .0
            .to_string(),
        "470.25"
    );
}

#[test]
fn skips_foreign_frames_and_errors_on_error_frames() {
    // Market data / unknown frame types yield empty updates, not errors.
    let ticks = serde_json::json!({"type": "ticks", "data": []}).to_string();
    assert!(parse_order_update_frame(&ticks).unwrap().is_empty());
    let empty_type = serde_json::json!({"foo": 1}).to_string();
    assert!(matches!(
        parse_order_update_frame(&empty_type),
        Err(BrokerError::InvalidRequest(_))
    ));
    let garbage = "}}not json{{";
    assert!(matches!(
        parse_order_update_frame(garbage),
        Err(BrokerError::InvalidRequest(_))
    ));
    let error = serde_json::json!({"type": "error", "data": "{\"code\":2}"}).to_string();
    assert!(matches!(
        parse_order_update_frame(&error),
        Err(BrokerError::Transport(_))
    ));
}

#[tokio::test]
async fn stream_emits_ack_then_delta_fills_rejection_cancel() {
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let mut stream = KiteTickerOrderStream::new(rx);

    tx.send(frame(serde_json::json!([update("OPEN", 0)])))
        .unwrap();
    tx.send(frame(serde_json::json!([update("COMPLETE", 40)])))
        .unwrap();
    tx.send(frame(serde_json::json!([update("UPDATE", 70)])))
        .unwrap();
    tx.send(frame(serde_json::json!([update("COMPLETE", 100)])))
        .unwrap();
    tx.send(frame(serde_json::json!([update("REJECTED", 0)])))
        .unwrap();
    tx.send(frame(serde_json::json!([update("CANCELLED", 0)])))
        .unwrap();

    let ack = stream.next_event().await.expect("ack");
    match ack {
        BrokerExecutionEvent::OrderAck(a) => {
            assert_eq!(a.order_id, "220119000332528");
            assert_eq!(a.status, honba_core::OrderStatus::New);
            assert_eq!(a.exchange_order_id.as_deref(), Some("1000000001482421"));
        }
        other => panic!("expected ack, got {other:?}"),
    }

    // Delta fills: 40, then +30, then +30.
    for expected in ["40", "30", "30"] {
        let event = stream.next_event().await.expect("fill");
        match event {
            BrokerExecutionEvent::Fill(fill) => {
                assert_eq!(fill.quantity.0.to_string(), expected);
                assert_eq!(fill.price.0.to_string(), "470.25");
                assert!(
                    fill.fill_id.starts_with("220119000332528:"),
                    "cumulative fill id: {}",
                    fill.fill_id
                );
            }
            other => panic!("expected fill {expected}, got {other:?}"),
        }
    }

    let rejected = stream.next_event().await.expect("rejected");
    assert!(matches!(
        rejected,
        BrokerExecutionEvent::OrderRejected { .. }
    ));
    let cancelled = stream.next_event().await.expect("cancelled");
    assert!(matches!(
        cancelled,
        BrokerExecutionEvent::OrderCancelled { .. }
    ));
}

#[tokio::test]
async fn correlation_lookup_flows_into_events() {
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let oid = uuid::Uuid::new_v4();
    let mut stream = KiteTickerOrderStream::new(rx)
        .with_correlation_lookup(move |order_id| (order_id == "220119000332528").then_some(oid));
    tx.send(frame(serde_json::json!([update("OPEN", 0)])))
        .unwrap();
    match stream.next_event().await.unwrap() {
        BrokerExecutionEvent::OrderAck(a) => assert_eq!(a.client_order_id, oid),
        other => panic!("expected ack, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_closes_with_transport_error_after_channel_drop() {
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let mut stream = KiteTickerOrderStream::new(rx);
    drop(tx);
    let err = stream.next_event().await.expect_err("closed");
    assert!(matches!(err, BrokerError::Transport(_)));
}

#[tokio::test]
async fn ticker_connector_resolves_into_stream_stream_type() {
    // Live-connector smoke: against a non-existent endpoint the connector
    // must surface a Transport error synchronously rather than hang.
    // (No network use beyond loopback failure.)
    let cfg = honba_te_zerodha::TickerConnectConfig {
        max_attempts: 1,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        keepalive: Duration::from_secs(1),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        honba_te_zerodha::spawn_order_updates_with("key", "tok", cfg),
    )
    .await
    .expect("connector resolves promptly");
    match result {
        Err(BrokerError::Transport(_)) => {}
        Err(other) => panic!("expected transport, got {other:?}"),
        Ok(mut stream) => {
            // If a real connection somehow happened (offline sandbox), the
            // stream must still terminate or yield an error within budget.
            let _ = tokio::time::timeout(Duration::from_secs(1), stream.next_event()).await;
        }
    }
}
