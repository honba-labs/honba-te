//! M3 freeze-limit slicing contract tests (plan §3.4 items 3 + property §5.1).

use std::sync::Arc;

use honba_core::OrderSide;
use honba_te_core::BrokerError;
use honba_te_slicer::{
    FreezeLimitCatalog, FreezeLimitSlicer, builtin_index_freezes, place_slice_plan,
    plan_against_limit,
};

mod common;
use common::{ScriptedBroker, parent_request};

fn indexed_catalog() -> FreezeLimitCatalog {
    let mut catalog = FreezeLimitCatalog::new();
    for record in builtin_index_freezes() {
        catalog.insert(record).unwrap();
    }
    catalog
}

#[test]
fn parent_within_freeze_passes_through_as_single_child() {
    let slicer = FreezeLimitSlicer::new(Arc::new(indexed_catalog()));
    let plan = slicer.plan(&parent_request(1800)).unwrap();
    assert_eq!(plan.legs.len(), 1, "waiting parent fits exactly one leg");
    assert_eq!(plan.legs[0].quantity.0.to_string(), "1800");
    assert_eq!(plan.total_quantity().0.to_string(), "1800");
}

#[test]
fn greedy_chunks_match_ceil_formula_and_freeze_cap() {
    let slicer = FreezeLimitSlicer::new(Arc::new(indexed_catalog()));
    // 2500 NIFTY equivalents against freeze 1800 → 1800 + 700.
    let mut parent = parent_request(2500);
    parent.instrument.symbol = "NIFTY".into();
    let plan = slicer.plan(&parent).unwrap();
    assert_eq!(plan.legs.len(), 2);
    assert_eq!(
        plan.legs[0].quantity.0.to_string(),
        "1800",
        "greedy full freeze chunk"
    );
    assert_eq!(plan.legs[1].quantity.0.to_string(), "700", "remainder leg");
    // BANKNIFTY familiar case: 5000 @ 900 → 900*5 + 500.
    let mut parent = parent_request(5000);
    parent.instrument.symbol = "BANKNIFTY".into();
    let plan = slicer.plan(&parent).unwrap();
    assert_eq!(plan.legs.len(), 6);
    assert!(
        plan.legs[0..5]
            .iter()
            .all(|leg| leg.quantity.0.to_string() == "900")
    );
    assert_eq!(plan.legs[5].quantity.0.to_string(), "500");
    for leg in &plan.legs {
        assert!(
            leg.quantity.0 <= rust_decimal::Decimal::from(900),
            "freeze invariant {leg:?}"
        );
    }
}

#[test]
fn unknown_instrument_uses_default_or_passes_through() {
    let mut catalog = FreezeLimitCatalog::new().with_default_freeze(Some(400));
    let slicer = FreezeLimitSlicer::new(Arc::new(catalog.clone()));
    let plan = slicer.plan(&parent_request(1000)).unwrap();
    assert_eq!(plan.legs.len(), 3, "400 + 400 + 200");
    assert_eq!(plan.total_quantity().0.to_string(), "1000");

    catalog = catalog.with_default_freeze(None);
    let slicer = FreezeLimitSlicer::new(Arc::new(catalog));
    let plan = slicer.plan(&parent_request(9_999_999)).unwrap();
    assert_eq!(plan.legs.len(), 1, "no limit → unsliced standard leg");
}

#[tokio::test]
async fn orchestrator_places_children_with_synchronized_ids() {
    let catalog = Arc::new(indexed_catalog());
    let slicer = FreezeLimitSlicer::new(catalog.clone());
    let mut parent = parent_request(2500);
    parent.instrument.symbol = "NIFTY".into();
    let plan = slicer.plan(&parent).unwrap();

    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into()]);
    let acks = place_slice_plan(&broker, &parent, &plan).await.unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!(
        acks.iter().map(|a| a.order_id.as_str()).collect::<Vec<_>>(),
        ["L1", "L2"]
    );

    let placed = broker.placed();
    assert_eq!(placed.len(), 2);
    for (index, child) in placed.iter().enumerate() {
        // Synchronized SEBI trail: distinct child UUIDs, shared parent correlation.
        assert_ne!(child.client_order_id, parent.client_order_id);
        assert_eq!(
            child.correlation_id,
            Some(parent.client_order_id),
            "child {index}"
        );
        // Every parent leg is inherited verbatim.
        assert_eq!(child.instrument, parent.instrument);
        assert_eq!(child.side, parent.side);
        assert_eq!(child.order_type, parent.order_type);
        assert_eq!(child.tif, parent.tif);
        assert_eq!(child.algo_id, parent.algo_id);
        assert_eq!(child.client_id, parent.client_id);
    }
    assert_eq!(
        placed[0].quantity.0.to_string(),
        "1800",
        "children submit in plan order"
    );
    assert_eq!(placed[1].quantity.0.to_string(), "700");
}

#[tokio::test]
async fn orchestrator_aborts_on_first_failed_leg() {
    let slicer = FreezeLimitSlicer::new(Arc::new(indexed_catalog()));
    let mut parent = parent_request(3000);
    parent.instrument.symbol = "NIFTY".into();
    let plan = slicer.plan(&parent).unwrap();
    assert_eq!(plan.legs.len(), 2);

    // First leg fails before anything is risked.
    let broker = ScriptedBroker::new(vec!["L1".into()]).fail_at(0);
    let err = place_slice_plan(&broker, &parent, &plan).await.unwrap_err();
    assert!(format!("{err:?}").contains("scripted failure #0"), "{err}");
    assert!(broker.placed().is_empty());

    // Second leg fails after the first is live → error propagates, first leg
    // stays on the exchange (callers reconcile via the execution stream).
    let broker = ScriptedBroker::new(vec!["L1".into(), "L2".into()]).fail_at(1);
    let err = place_slice_plan(&broker, &parent, &plan).await.unwrap_err();
    assert!(format!("{err:?}").contains("scripted failure #1"), "{err}");
    assert_eq!(broker.placed().len(), 1, "prefix leg remains placed");
}

#[tokio::test]
async fn sell_side_children_inherit_side_and_symbol_exactly() {
    let slicer = FreezeLimitSlicer::new(Arc::new(indexed_catalog()));
    let mut parent = parent_request(400);
    parent.side = OrderSide::Sell;
    parent.instrument.symbol = "NIFTY".into();
    let plan = slicer.plan(&parent).unwrap();
    assert_eq!(plan.legs.len(), 1);
    let broker = ScriptedBroker::new(vec!["CHILD".into()]);
    place_slice_plan(&broker, &parent, &plan).await.unwrap();
    let placed = broker.placed();
    assert_eq!(placed[0].side, OrderSide::Sell);
    assert_eq!(placed[0].instrument.symbol, "NIFTY");
    assert_eq!(placed[0].quantity.0.to_string(), "400");
}

#[test]
fn free_function_plan_against_explicit_limit() {
    // No limit → single leg carrying the full quantity.
    let plan = plan_against_limit(&parent_request(7777), None).unwrap();
    assert_eq!(plan.legs.len(), 1);
    assert_eq!(plan.legs[0].quantity.0.to_string(), "7777");

    // Cap smaller than the parent → greedy deterministic chunks.
    let plan = plan_against_limit(&parent_request(10), Some(4)).unwrap();
    assert_eq!(plan.legs.len(), 3);
    assert_eq!(plan.legs[0].quantity.0.to_string(), "4");
    assert_eq!(plan.legs[2].quantity.0.to_string(), "2");

    // Zero parent quantity is a domain violation, not a silent empty plan.
    let err = plan_against_limit(&parent_request(0), Some(4)).unwrap_err();
    assert!(matches!(err, BrokerError::InvalidRequest(_)));
}
