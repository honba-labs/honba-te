//! M3 iceberg engine tests: native legging rules (Kite Connect v3 caps) and
//! the synthetic runner's pacing/termination policy.

use std::time::Duration;

use honba_core::Quantity;
use honba_te_core::BrokerError;
use honba_te_core::ExecutionStream;

use honba_te_slicer::{MAX_NATIVE_ICEBERG_LEGS, native_leg_count, run_synthetic_iceberg};

mod common;
use common::{
    ScriptedBroker, ScriptedStream, ack_event, cancelled_event, fill_event, parent_request,
    rejected_event,
};

#[test]
fn native_leg_count_follows_kite_contract() {
    // Exact division inside 2..=50 → derived legs.
    assert_eq!(
        native_leg_count(&Quantity::from_u64(2500), &Quantity::from_u64(250)).unwrap(),
        10
    );
    // Lower bound: exactly two legs.
    assert_eq!(
        native_leg_count(&Quantity::from_u64(2000), &Quantity::from_u64(1000)).unwrap(),
        2
    );
    // Upper bound breach: 51 legs.
    assert!(native_leg_count(&Quantity::from_u64(2550), &Quantity::from_u64(50)).is_err());
    // Exactly at the 50-leg cap passes.
    assert_eq!(
        native_leg_count(&Quantity::from_u64(5000), &Quantity::from_u64(100)).unwrap(),
        50
    );
    assert_eq!(MAX_NATIVE_ICEBERG_LEGS, 50);
    // Non-exact division (2000 / 300) is rejected.
    assert!(native_leg_count(&Quantity::from_u64(2000), &Quantity::from_u64(300)).is_err());
    // Display must be strictly smaller than the parent and positive.
    assert!(native_leg_count(&Quantity::from_u64(2000), &Quantity::from_u64(2000)).is_err());
    assert!(native_leg_count(&Quantity::from_u64(2000), &Quantity::from_u64(0)).is_err());
}

#[tokio::test]
async fn synthetic_runner_advances_leg_by_leg_on_fill_confirmation() {
    // Parent 700 @ display 250 → legs 250 + 250 + 200.
    let parent = parent_request(700);
    let expected_correlation = parent.client_order_id;
    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into(), "L3".into()]);
    let stream = ScriptedStream::new(vec![
        ack_event("L1"),
        fill_event("L1", 250),
        ack_event("L2"),
        fill_event("L2", 250),
        ack_event("L3"),
        fill_event("L3", 200),
    ]);
    let mut stream: Box<dyn ExecutionStream> = Box::new(stream);

    let outcome = run_synthetic_iceberg(
        &broker,
        stream.as_mut(),
        parent,
        Quantity::from_u64(250),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    assert_eq!(outcome.filled_legs, ["L1", "L2", "L3"]);
    assert!(outcome.unfinished_legs.is_empty());
    assert_eq!(outcome.filled.0.to_string(), "700");

    // Leg isolation: exactly one child of display quantity at a time.
    let placed = broker.placed();
    assert_eq!(placed.len(), 3);
    assert_eq!(placed[0].quantity.0.to_string(), "250");
    assert_eq!(placed[1].quantity.0.to_string(), "250");
    assert_eq!(
        placed[2].quantity.0.to_string(),
        "200",
        "remainder leg smaller"
    );
    for child in &placed {
        assert_eq!(
            child.correlation_id,
            Some(expected_correlation),
            "child stays on the parent audit chain"
        );
    }
}

#[tokio::test]
async fn synthetic_runner_records_unfinished_legs_and_continues() {
    let parent = parent_request(600);
    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into()]);
    // L1 confirms; L2 stays silent → double quiet window (cancel + abandoned).
    let stream = ScriptedStream::new(vec![ack_event("L1"), fill_event("L1", 300)]).hanging();
    let mut stream: Box<dyn ExecutionStream> = Box::new(stream);

    let outcome = run_synthetic_iceberg(
        &broker,
        stream.as_mut(),
        parent,
        Quantity::from_u64(300),
        Duration::from_millis(25),
    )
    .await
    .unwrap();

    assert_eq!(outcome.filled_legs, ["L1"]);
    assert_eq!(outcome.unfinished_legs, ["L2"]);
    assert_eq!(
        outcome.filled.0.to_string(),
        "300",
        "L2 contributed nothing"
    );
    assert_eq!(broker.cancelled(), ["L2"], "pacing cancel issued");
}

#[tokio::test]
async fn synthetic_runner_aborts_when_active_leg_is_rejected() {
    let parent = parent_request(400);
    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into()]);
    let stream = ScriptedStream::new(vec![ack_event("L1"), rejected_event("L1", "RMS margin")]);
    let mut stream: Box<dyn ExecutionStream> = Box::new(stream);

    let err = run_synthetic_iceberg(
        &broker,
        stream.as_mut(),
        parent,
        Quantity::from_u64(400),
        Duration::from_millis(500),
    )
    .await
    .unwrap_err();
    match err {
        BrokerError::Rejected(reason) => assert!(reason.contains("RMS margin")),
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert_eq!(broker.placed().len(), 1, "run stops at the rejected leg");
    assert!(broker.cancelled().is_empty());
}

#[tokio::test]
async fn synthetic_runner_counts_fills_on_abandoned_legs() {
    // Leg's fill lands before its cancel → the partial count is honored and
    // the abandoned remainder is exactly leg_quantity − filled.
    let parent = parent_request(600);
    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into()]);
    let stream = ScriptedStream::new(vec![
        ack_event("L1"),
        fill_event("L1", 250),
        cancelled_event("L1"),
        ack_event("L2"),
        fill_event("L2", 300),
    ]);
    let mut stream: Box<dyn ExecutionStream> = Box::new(stream);

    let outcome = run_synthetic_iceberg(
        &broker,
        stream.as_mut(),
        parent,
        Quantity::from_u64(300),
        Duration::from_millis(500),
    )
    .await
    .unwrap();
    assert_eq!(outcome.filled_legs, ["L2"]);
    assert_eq!(outcome.unfinished_legs, ["L1"]);
    assert_eq!(
        outcome.filled.0.to_string(),
        "550",
        "250 abandoned-leg fill + 300"
    );
}
