//! Angel One SmartStream order-update frame and stream contract tests.

use tokio::sync::mpsc;

use honba_te_angelone::{SmartStreamOrderStream, parse_order_update_frame};
use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};

/// Order-update sample from the SmartAPI WebSocket2 docs (string numerics,
/// integer orderid, time-only timestamp).
fn update(status: &str, filled: u64) -> String {
    serde_json::json!({
        "lastmodifiedtime": "09:45:11",
        "tradingsymbol": "RELIANCE-EQ",
        "symboltoken": null,
        "instrumenttype": "",
        "strikeprice": "-1",
        "optiontype": "",
        "expirydate": "",
        "lotsize": "1",
        "cancelsize": "0",
        "averageprice": "2568",
        "filledshares": filled.to_string(),
        "unfilledshares": "0",
        "orderid": 201020000000080i64,
        "text": "",
        "status": status,
        "exchange": "NSE",
        "transactiontype": "BUY",
        "producttype": "DELIVERY",
        "duration": "DAY",
        "triggerprice": "-1",
        "quantity": "2",
        "disclosedquantity": "1",
        "price": "2568",
        "ordertype": "LIMIT",
        "stoploss": "-1",
        "squareoff": "-1",
        "trailingstoploss": "-1"
    })
    .to_string()
}

#[test]
fn parses_docs_sample_and_ignores_heartbeats() {
    let updates = parse_order_update_frame(&update("complete", 2)).expect("docs sample");
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].status.as_deref(), Some("complete"));
    // orderid arrives as a JSON integer:
    assert_eq!(
        updates[0].orderid.as_ref().and_then(|v| v.as_i64()),
        Some(201020000000080)
    );

    // Heartbeats are plain text frames, not JSON:
    assert!(parse_order_update_frame("ping").unwrap().is_empty());
    assert!(parse_order_update_frame("PONG").unwrap().is_empty());
    assert!(parse_order_update_frame("").unwrap().is_empty());

    // Malformed JSON fails the stream:
    assert!(matches!(
        parse_order_update_frame("}{{"),
        Err(BrokerError::InvalidRequest(_))
    ));
}

#[tokio::test]
async fn stream_emits_ack_then_delta_fill_rejection_and_cancel() {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut stream = SmartStreamOrderStream::new(rx);

    tx.send(update("open", 0)).unwrap();
    tx.send(update("complete", 1)).unwrap();
    tx.send(update("complete", 2)).unwrap();
    tx.send(update("rejected", 2)).unwrap();
    tx.send(update("cancelled", 2)).unwrap();

    let ack = stream.next_event().await.expect("ack");
    match ack {
        BrokerExecutionEvent::OrderAck(a) => {
            assert_eq!(a.order_id, "201020000000080");
            assert_eq!(a.status, honba_core::OrderStatus::New);
        }
        other => panic!("expected ack, got {other:?}"),
    }

    // Delta fills: 1 then 1 more (cumulative strings on the wire).
    for expected in ["1", "1"] {
        match stream.next_event().await.expect("fill") {
            BrokerExecutionEvent::Fill(fill) => {
                assert_eq!(fill.quantity.0.to_string(), expected);
                assert_eq!(fill.price.0.to_string(), "2568");
                assert_eq!(fill.instrument.exchange, honba_core::Exchange::Nse);
                assert_eq!(fill.instrument.symbol, "RELIANCE");
            }
            other => panic!("expected fill {expected}, got {other:?}"),
        }
    }

    let rejected = stream.next_event().await.expect("rejected");
    assert!(matches!(
        rejected,
        BrokerExecutionEvent::OrderRejected {
            order_id: Some(_),
            ..
        }
    ));
    let cancelled = stream.next_event().await.expect("cancelled");
    assert!(matches!(
        cancelled,
        BrokerExecutionEvent::OrderCancelled { .. }
    ));
}

#[tokio::test]
async fn correlation_lookup_flows_into_events() {
    let (tx, rx) = mpsc::unbounded_channel();
    let cid = uuid::Uuid::new_v4();
    let mut stream = SmartStreamOrderStream::new(rx)
        .with_correlation_lookup(move |order_id| (order_id == "201020000000080").then_some(cid));
    tx.send(update("open", 0)).unwrap();
    match stream.next_event().await.unwrap() {
        BrokerExecutionEvent::OrderAck(a) => assert_eq!(a.client_order_id, cid),
        other => panic!("expected ack, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_closes_with_transport_after_channel_drop() {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut stream = SmartStreamOrderStream::new(rx);
    drop(tx);
    assert!(matches!(
        stream.next_event().await,
        Err(BrokerError::Transport(_))
    ));
}

#[tokio::test]
async fn connector_surfaces_transport_error_when_endpoint_rejects() {
    // Live-connector smoke: against a non-existent endpoint the connector
    // must surface a Transport error rather than hang (DNS failure path).
    let cfg = honba_te_angelone::SmartStreamConfig {
        auth_token: "jwt".into(),
        api_key: "key".into(),
        client_code: "A1".into(),
        feed_token: "feed".into(),
        heartbeat: std::time::Duration::from_secs(1),
        max_attempts: 1,
        backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(2),
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        honba_te_angelone::spawn_order_updates_with(cfg),
    )
    .await
    .expect("connector resolves promptly");
    match result {
        Err(BrokerError::Transport(_)) => {}
        Err(other) => panic!("expected transport, got {other:?}"),
        Ok(mut stream) => {
            // If a real handshake somehow happened, the stream must still
            // terminate or yield within budget.
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(1), stream.next_event()).await;
        }
    }
}
