//! Upstox portfolio-feed frame and stream contract tests.

use serde_json::Value;
use serde_json::json;
use tokio::sync::mpsc;

use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};
use honba_te_upstox::{UpstoxOrderStream, parse_portfolio_feed_frame};

fn frame() -> String {
    serde_json::json!({
        "update_type": "order",
        "user_id": "S4156",
        "exchange": "NSE",
        "instrument_token": "NSE_EQ|INE848E01016",
        "trading_symbol": "NHPC-EQ",
        "product": "D",
        "order_type": "MARKET",
        "average_price": 37.25,
        "price": 0,
        "trigger_price": 0,
        "quantity": 10,
        "disclosed_quantity": 0,
        "pending_quantity": 0,
        "transaction_type": "BUY",
        "order_ref_id": "d38b37de-404a-4767-bc72-5e2b2a0b2e4a",
        "exchange_order_id": "151102000542877",
        "parent_order_id": "",
        "validity": "DAY",
        "status": "complete",
        "is_amo": false,
        "variety": "NORMAL",
        "tag": "algo-m2",
        "exchange_timestamp": "2024-02-21 14:40:02",
        "status_message": "",
        "order_id": "240221000010011",
        "order_request_id": "105201",
        "filled_quantity": 10,
        "guid": "abcde"
    })
    .to_string()
}

#[tokio::test]
async fn complete_update_emits_full_fill_with_exact_price() {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut stream = UpstoxOrderStream::new(rx);
    tx.send(frame()).unwrap();
    let event = stream.next_event().await.expect("fill");
    match event {
        BrokerExecutionEvent::Fill(fill) => {
            assert_eq!(fill.order_id, "240221000010011");
            assert_eq!(fill.quantity.0.to_string(), "10");
            assert_eq!(fill.price.0.to_string(), "37.25");
            assert_eq!(fill.instrument.exchange, honba_core::Exchange::Nse);
            assert_eq!(fill.instrument.symbol, "NHPC");
            assert_eq!(fill.algo_id, "algo-m2");
            assert_eq!(
                fill.timestamp.format("%Y-%m-%dT%H:%M:%S").to_string(),
                "2024-02-21T09:10:02"
            );
        }
        other => panic!("expected fill, got {other:?}"),
    }
}

#[tokio::test]
async fn open_rejected_cancelled_map_and_deltas_track() {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut stream = UpstoxOrderStream::new(rx);

    let base = serde_json::json!({
        "update_type": "order",
        "status": "open",
        "order_id": "240221000010012",
        "quantity": 5,
        "filled_quantity": 0,
        "exchange": "BSE",
        "instrument_token": "BSE_EQ|INE002A00714",
        "transaction_type": "SELL",
        "exchange_timestamp": "2024-02-21 10:00:00"
    });
    tx.send(base.to_string()).unwrap();

    // Partial fill 2 of 5 then complete 5 → deltas 2 then 3.
    tx.send(patch(
        &base,
        json!({"status": "partial fill", "filled_quantity": 2}),
    ))
    .unwrap();
    tx.send(patch(
        &base,
        json!({"status": "complete", "filled_quantity": 5}),
    ))
    .unwrap();

    let ack = stream.next_event().await.expect("ack");
    match ack {
        BrokerExecutionEvent::OrderAck(a) => {
            assert_eq!(a.order_id, "240221000010012");
            assert_eq!(a.status, honba_core::OrderStatus::New);
        }
        other => panic!("expected ack, got {other:?}"),
    }
    for expected in ["2", "3"] {
        match stream.next_event().await.expect("fill") {
            BrokerExecutionEvent::Fill(fill) => {
                assert_eq!(fill.quantity.0.to_string(), expected);
                assert_eq!(fill.instrument.exchange, honba_core::Exchange::Bse);
            }
            other => panic!("expected fill {expected}, got {other:?}"),
        }
    }

    tx.send(patch(
        &base,
        json!({"status": "rejected", "status_message": "SLTP not valid"}),
    ))
    .unwrap();
    let rejected = stream.next_event().await.expect("rejected");
    match rejected {
        BrokerExecutionEvent::OrderRejected { order_id, reason } => {
            assert_eq!(order_id.as_deref(), Some("240221000010012"));
            assert_eq!(reason, "SLTP not valid");
        }
        other => panic!("expected rejected, got {other:?}"),
    }

    tx.send(patch(&base, json!({"status": "cancelled"})))
        .unwrap();
    let cancelled = stream.next_event().await.expect("cancelled");
    assert!(matches!(
        cancelled,
        BrokerExecutionEvent::OrderCancelled { .. }
    ));
}

fn patch(base: &Value, overlay: Value) -> String {
    let mut merged = base.clone();
    if let (Some(b), Some(o)) = (merged.as_object_mut(), overlay.as_object()) {
        for (k, v) in o {
            b.insert(k.clone(), v.clone());
        }
    }
    merged.to_string()
}

#[test]
fn foreign_and_malformed_frames_behave() {
    // Foreign update types are dropped, not fatal:
    let foreign = serde_json::json!({"update_type": "position", "order_id": "x", "status": "complete", "filled_quantity": 9}).to_string();
    assert!(parse_portfolio_feed_frame(&foreign).unwrap().is_empty());
    // Empty / whitespace frames skip:
    assert!(parse_portfolio_feed_frame("").unwrap().is_empty());
    assert!(parse_portfolio_feed_frame("   \n").unwrap().is_empty());
    // Malformed JSON fails loudly:
    assert!(matches!(
        parse_portfolio_feed_frame("}{{"),
        Err(BrokerError::InvalidRequest(_))
    ));
    // Batched arrays filter to order updates:
    let batch = serde_json::json!([
        {"update_type": "order", "order_id": "240221000010011", "status": "open", "filled_quantity": 0},
        {"update_type": "position", "order_id": "ign", "status": "complete"}
    ])
    .to_string();
    assert_eq!(parse_portfolio_feed_frame(&batch).unwrap().len(), 1);
}

#[tokio::test]
async fn correlation_lookup_flows_into_events() {
    let (tx, rx) = mpsc::unbounded_channel();
    let cid = uuid::Uuid::new_v4();
    let mut stream = UpstoxOrderStream::new(rx)
        .with_correlation_lookup(move |order_id| (order_id == "240221000010011").then_some(cid));
    tx.send(frame()).unwrap();
    match stream.next_event().await.unwrap() {
        BrokerExecutionEvent::OrderAck(_) => panic!("complete frame must be a fill"),
        BrokerExecutionEvent::Fill(fill) => assert_eq!(fill.client_order_id, cid),
        BrokerExecutionEvent::OrderCancelled { .. }
        | BrokerExecutionEvent::OrderRejected { .. } => {
            panic!("unexpected terminal event")
        }
    }
    let _ = tx;
}

#[tokio::test]
async fn stream_closes_with_transport_after_channel_drop() {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut stream = UpstoxOrderStream::new(rx);
    drop(tx);
    assert!(matches!(
        stream.next_event().await,
        Err(BrokerError::Transport(_))
    ));
}
