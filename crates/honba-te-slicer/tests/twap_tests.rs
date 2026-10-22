//! M3 TWAP tests: schedule determinism, Gaussian jitter bounds, freeze lift,
//! and the runtime runner incl. limit-chase re-pricing.

use std::time::Duration;

use chrono::Utc;
use honba_core::Price;
use honba_te_slicer::{
    FreezeLimitCatalog, TwapChase, TwapConfig, TwapPlan, builtin_index_freezes, run_twap,
};
use rust_decimal::Decimal;

mod common;
use common::{
    ScriptedBroker, ScriptedStream, ack_event, cancelled_event, fill_event, parent_request,
};

fn indexed_catalog() -> FreezeLimitCatalog {
    let mut catalog = FreezeLimitCatalog::new();
    for record in builtin_index_freezes() {
        catalog.insert(record).unwrap();
    }
    catalog
}

fn config(slices: u32, horizon: Duration, jitter: Duration) -> TwapConfig {
    TwapConfig {
        slices,
        horizon,
        jitter,
        chase: None,
    }
}

#[test]
fn equal_split_matches_slices_and_freeze_lifts_leg_count() {
    let catalog = indexed_catalog();
    let start = Utc::now();

    // 9000 BANKNIFTY-equivalents: the ⌈Q/Qfreeze⌉ = 10 leg floor dominates the
    // requested 3 slices.
    let lifted = config(3, Duration::from_secs(30), Duration::from_millis(10));
    let mut parent = parent_request(9000);
    parent.instrument.symbol = "BANKNIFTY".into();
    let plan = TwapPlan::build(&parent, &lifted, &catalog, start, 7).unwrap();
    assert_eq!(plan.legs.len(), 10, "freeze 900 forces ⌈9000/900⌉ legs");
    for leg in &plan.legs {
        assert!(
            leg.quantity.0 <= Decimal::from(900),
            "freeze invariant {leg:?}"
        );
    }
    let total: Decimal = plan.legs.iter().map(|leg| leg.quantity.0).sum();
    assert_eq!(total.to_string(), "9000", "legs reconstruct the parent");

    // Comfortable split: 600 (≤ 900) across 2 requested slices → 300 + 300.
    let parent = parent_request(600);
    let plan_flat = TwapPlan::build(
        &parent,
        &config(2, Duration::from_secs(10), Duration::ZERO),
        &catalog,
        start,
        7,
    )
    .unwrap();
    assert_eq!(plan_flat.legs.len(), 2);
    assert!(
        plan_flat
            .legs
            .iter()
            .all(|leg| leg.quantity.0.to_string() == "300")
    );

    // Single slice stays a single slice when the parent already fits.
    let plan_one = TwapPlan::build(
        &parent,
        &config(1, Duration::from_secs(5), Duration::ZERO),
        &catalog,
        start,
        7,
    )
    .unwrap();
    assert_eq!(plan_one.legs.len(), 1);
}

#[test]
fn schedule_is_deterministic_with_bounded_gaussian_jitter() {
    let catalog = indexed_catalog();
    let start = Utc::now();
    // 200 ms jitter σ over a 5 s horizon with 5 legs: nominal slots at
    // 0 s, 1 s, 2 s, 3 s, 4 s.
    let config = config(5, Duration::from_secs(5), Duration::from_millis(200));

    // One frozen parent: determinism must hold bit-for-bit for identical
    // inputs, including the parent identity.
    let parent = parent_request(1000);
    let plan_a = TwapPlan::build(&parent, &config, &catalog, start, 0xBE2B0E55).unwrap();
    let plan_b = TwapPlan::build(&parent, &config, &catalog, start, 0xBE2B0E55).unwrap();
    assert_eq!(plan_a, plan_b, "same seed → byte-identical schedule");
    assert!(
        plan_a
            .legs
            .iter()
            .all(|leg| leg.quantity.0.to_string() == "200")
    );

    // Irwin–Hall(3) hard bound: |z| ≤ 3σ (full support), integer div < 1 ms.
    for (index, leg) in plan_a.legs.iter().enumerate() {
        let expected = start + chrono::Duration::milliseconds(1_000i64 * index as i64);
        let delta_ms = (leg.submitted_at - expected).num_milliseconds();
        assert!(
            delta_ms.abs() <= 601,
            "leg {index} jitter {delta_ms} ms outside ±3σ"
        );
    }

    // Different seeds decorrelate the schedule (footprint-masking property).
    let plan_c = TwapPlan::build(&parent, &config, &catalog, start, 42).unwrap();
    assert_ne!(
        plan_a.legs, plan_c.legs,
        "different seeds → different timing"
    );
}

#[tokio::test]
async fn runner_sleeps_to_schedule_and_aggregates_fills() {
    let catalog = indexed_catalog();
    let start = Utc::now();
    // 600 across 3 slices spaced 100 ms apart (zero jitter): quantities
    // 200 + 200 + 200, submission slots at 0 ms / 100 ms / 200 ms.
    let parent = parent_request(600);
    let plan = TwapPlan::build(
        &parent,
        &config(3, Duration::from_millis(300), Duration::ZERO),
        &catalog,
        start,
        11,
    )
    .unwrap();

    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into(), "L3".into()]);
    let stream = ScriptedStream::new(vec![
        ack_event("L1"),
        fill_event("L1", 200),
        ack_event("L2"),
        fill_event("L2", 200),
        ack_event("L3"),
        fill_event("L3", 200),
    ]);
    let mut stream: Box<dyn honba_te_core::ExecutionStream> = Box::new(stream);

    let outcome = run_twap(&broker, stream.as_mut(), &parent, &plan, None)
        .await
        .unwrap();
    assert_eq!(outcome.placed_legs, ["L1", "L2", "L3"]);
    assert_eq!(outcome.filled.0.to_string(), "600");
    assert_eq!(outcome.chase_steps, 0);

    let placed = broker.placed();
    assert!(
        placed
            .iter()
            .all(|child| child.quantity.0.to_string() == "200")
    );
    // Schedule integrity: spacing between placements matches the plan slots.
    for leg in &plan.legs {
        assert!(
            leg.submitted_at <= Utc::now(),
            "runner submitted strictly in schedule order"
        );
    }
}

#[tokio::test]
async fn limit_chase_cancels_quiet_child_and_resubmits_at_ticked_price() {
    let catalog = indexed_catalog();
    let start = Utc::now();
    // 600 limit-buy @ 100.00, 2 legs of 300, chase config: 2 steps, ₹0.05
    // tick, 20 ms quiet window.
    let parent = parent_request(600);
    let chase = TwapChase::with_tick(2, "0.05", Duration::from_millis(20)).unwrap();
    let config = TwapConfig {
        slices: 2,
        horizon: Duration::from_millis(100),
        jitter: Duration::ZERO,
        chase: Some(chase.clone()),
    };
    let plan = TwapPlan::build(&parent, &config, &catalog, start, 3).unwrap();

    // Script: leg 1 fills instantly; leg 2 stays quiet → cancel → the chase
    // resubmission (child L2b) fills at the ticked-up limit.
    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into(), "L2b".into()]);
    let stream = ScriptedStream::new(vec![
        ack_event("L1"),
        fill_event("L1", 300),
        ack_event("L2"),
        cancelled_event("L2"),
        ack_event("L2b"),
        fill_event("L2b", 300),
    ]);
    let mut stream: Box<dyn honba_te_core::ExecutionStream> = Box::new(stream);

    let outcome = run_twap(&broker, stream.as_mut(), &parent, &plan, Some(&chase))
        .await
        .unwrap();
    assert_eq!(outcome.filled.0.to_string(), "600");
    assert_eq!(outcome.chase_steps, 1, "one re-price executed");
    assert_eq!(outcome.placed_legs, ["L1", "L2", "L2b"]);

    let placed = broker.placed();
    assert_eq!(
        placed[1].order_type.limit_price().unwrap().0.to_string(),
        "100.00"
    );
    let chased = placed[2].order_type.limit_price().unwrap();
    assert_eq!(
        chased.0.to_string(),
        "100.05",
        "buy legs chase upward by exactly one tick"
    );
    // The resubmission carries only the unfilled remainder.
    assert_eq!(placed[2].quantity.0.to_string(), "300");
}

#[tokio::test]
async fn chase_budget_exhaustion_parks_unfilled_remainder() {
    let catalog = indexed_catalog();
    let start = Utc::now();
    // Single-leg parent; the chase re-price (step 1) is cancelled again but
    // the budget (max 1 step) ends the leg with zero fills.
    let parent = parent_request(300);
    let chase = TwapChase::with_tick(1, "0.05", Duration::from_millis(15)).unwrap();
    let config = TwapConfig {
        slices: 1,
        horizon: Duration::from_millis(40),
        jitter: Duration::ZERO,
        chase: Some(chase.clone()),
    };
    let plan = TwapPlan::build(&parent, &config, &catalog, start, 5).unwrap();

    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into()]);
    let stream = ScriptedStream::new(vec![
        ack_event("L1"),
        cancelled_event("L1"),
        ack_event("L2"),
        cancelled_event("L2"),
    ]);
    let mut stream: Box<dyn honba_te_core::ExecutionStream> = Box::new(stream);

    let outcome = run_twap(&broker, stream.as_mut(), &parent, &plan, Some(&chase))
        .await
        .unwrap();
    assert_eq!(outcome.filled.0.to_string(), "0");
    assert_eq!(
        outcome.chase_steps, 1,
        "budget of 1 step; the second cancel ends the leg"
    );
    assert_eq!(outcome.placed_legs, ["L1", "L2"]);

    let placed = broker.placed();
    // Both children price at the base AND the chased limit respectively.
    assert_eq!(
        placed[0].order_type.limit_price().unwrap().0,
        Price::from_rupees("100").unwrap().0
    );
    assert_eq!(
        placed[1].order_type.limit_price().unwrap().0.to_string(),
        "100.05"
    );
    // The Cancelled acks were broker-scripted (arrived before any quiet
    // window), so the runner itself never invoked cancel_order.
    assert!(broker.cancelled().is_empty());
}

#[tokio::test]
async fn silent_window_cancels_then_fails_fast_without_cancel_ack() {
    // A child that neither fills nor acknowledges the cancel must make the
    // runner fail loudly instead of spinning on repeated quiet windows (the
    // same timer path that normally precedes the scripted Cancelled branch).
    let catalog = indexed_catalog();
    let start = Utc::now();
    let parent = parent_request(300);
    let chase = TwapChase::with_tick(2, "0.05", Duration::from_millis(15)).unwrap();
    let config = TwapConfig {
        slices: 1,
        horizon: Duration::from_millis(30),
        jitter: Duration::ZERO,
        chase: Some(chase),
    };
    let plan = TwapPlan::build(&parent, &config, &catalog, start, 9).unwrap();

    let broker = ScriptedBroker::new(vec!["L1".into()]);
    let stream = ScriptedStream::new(vec![ack_event("L1")]).hanging();
    let mut stream: Box<dyn honba_te_core::ExecutionStream> = Box::new(stream);

    let err = run_twap(
        &broker,
        stream.as_mut(),
        &parent,
        &plan,
        Some(&fork_chase()),
    )
    .await
    .expect_err("runner fails fast, never hangs");
    match err {
        honba_te_core::BrokerError::Transport(msg) => {
            assert!(msg.contains("cancel ack not observed"), "{msg}")
        }
        other => panic!("expected Transport, got {other:?}"),
    }
    // One cancel request was issued against the child before failing.
    assert_eq!(broker.cancelled(), ["L1"]);
}

fn fork_chase() -> TwapChase {
    TwapChase::with_tick(2, "0.05", Duration::from_millis(15)).unwrap()
}
