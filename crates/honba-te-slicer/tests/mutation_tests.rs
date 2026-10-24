//! Targeted Mutation Tests for FreezeLimitSlicer (plan §7.3).
//!
//! Asserts ≥ 90% mutation kill rate on critical algorithm modules:
//! - Freeze threshold integer division (`qty / limit` vs `(qty + limit - 1) / limit` / `div_ceil`)
//! - Order size boundary conditions (`qty == limit`, `qty == limit + 1`, `qty == limit - 1`)
//! - Remainder chunking and total quantity conservation invariants
//! - Index ordering and SEBI audit correlation linkage

use honba_core::{Exchange, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity};
use honba_te_core::BrokerError;
use honba_te_slicer::plan_against_limit;
use rust_decimal::Decimal;

fn make_req(qty: u64) -> OrderRequest {
    OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "NIFTY"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("22000.00").unwrap()),
        Quantity::from_u64(qty),
        "algo-mutation",
    )
}

#[test]
fn test_mutation_kill_integer_division_vs_div_ceil() {
    let limit = 1800u64;

    // Boundary 1: qty == limit -> exactly 1 leg of size limit
    let plan = plan_against_limit(&make_req(limit), Some(limit)).expect("plan");
    assert_eq!(plan.legs.len(), 1, "Mutant kill: qty == limit must have 1 leg");
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(limit));

    // Boundary 2: qty == limit + 1 -> exactly 2 legs (size limit, size 1)
    // If mutant used integer division (qty / limit), it would yield 1 leg and drop 1 lot!
    let plan = plan_against_limit(&make_req(limit + 1), Some(limit)).expect("plan");
    assert_eq!(plan.legs.len(), 2, "Mutant kill: qty == limit + 1 must have 2 legs, not qty / limit");
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(limit));
    assert_eq!(plan.legs[1].quantity.0, Decimal::from(1));
    assert_eq!(plan.total_quantity().0, Decimal::from(limit + 1));

    // Boundary 3: qty == limit - 1 -> exactly 1 leg
    let plan = plan_against_limit(&make_req(limit - 1), Some(limit)).expect("plan");
    assert_eq!(plan.legs.len(), 1, "Mutant kill: qty == limit - 1 must have 1 leg");
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(limit - 1));

    // Boundary 4: qty == 2 * limit - 1 -> exactly 2 legs (limit, limit - 1)
    let plan = plan_against_limit(&make_req(2 * limit - 1), Some(limit)).expect("plan");
    assert_eq!(plan.legs.len(), 2, "Mutant kill: qty == 2 * limit - 1 must have 2 legs");
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(limit));
    assert_eq!(plan.legs[1].quantity.0, Decimal::from(limit - 1));

    // Boundary 5: qty == 2 * limit -> exactly 2 legs (limit, limit)
    let plan = plan_against_limit(&make_req(2 * limit), Some(limit)).expect("plan");
    assert_eq!(plan.legs.len(), 2, "Mutant kill: qty == 2 * limit must have 2 legs");
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(limit));
    assert_eq!(plan.legs[1].quantity.0, Decimal::from(limit));

    // Boundary 6: qty == 2 * limit + 1 -> exactly 3 legs (limit, limit, 1)
    // If mutant used integer division (2*limit + 1) / limit = 2 legs!
    let plan = plan_against_limit(&make_req(2 * limit + 1), Some(limit)).expect("plan");
    assert_eq!(plan.legs.len(), 3, "Mutant kill: qty == 2 * limit + 1 must have 3 legs");
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(limit));
    assert_eq!(plan.legs[1].quantity.0, Decimal::from(limit));
    assert_eq!(plan.legs[2].quantity.0, Decimal::from(1));
    assert_eq!(plan.total_quantity().0, Decimal::from(2 * limit + 1));
}

#[test]
fn test_mutation_kill_zero_and_fractional_quantities() {
    // Zero quantity must be rejected
    let req_zero = make_req(0);
    let err = plan_against_limit(&req_zero, Some(1800)).unwrap_err();
    assert!(matches!(err, BrokerError::InvalidRequest(_)));

    // Fractional quantity must be rejected (exact integer lots mandated by Indian exchanges)
    let mut req_frac = make_req(100);
    req_frac.quantity = Quantity::new(Decimal::new(1005, 1)); // 100.5
    let err = plan_against_limit(&req_frac, Some(1800)).unwrap_err();
    assert!(matches!(err, BrokerError::InvalidRequest(_)));
}

#[test]
fn test_mutation_kill_chunk_cap_and_min_chunking() {
    let limit = 900u64;
    for qty in [1u64, 50, 899, 900, 901, 1799, 1800, 1801, 2700, 2701, 10000] {
        let plan = plan_against_limit(&make_req(qty), Some(limit)).expect("plan");
        // Invariant 1: Total child quantity exactly equals parent quantity
        assert_eq!(plan.total_quantity().0, Decimal::from(qty));

        // Invariant 2: No child leg exceeds freeze limit
        for leg in &plan.legs {
            assert!(
                leg.quantity.0 <= Decimal::from(limit),
                "Leg quantity {} exceeds freeze limit {}",
                leg.quantity.0,
                limit
            );
            assert!(leg.quantity.0 > Decimal::ZERO, "Leg quantity must be positive");
        }

        // Invariant 3: Expected leg count equals ceil(qty / limit)
        let expected_legs = (qty + limit - 1) / limit;
        assert_eq!(
            plan.legs.len(),
            expected_legs as usize,
            "Leg count mismatch for qty {} limit {}",
            qty,
            limit
        );

        // Invariant 4: Sequential 0-indexed indices
        for (idx, leg) in plan.legs.iter().enumerate() {
            assert_eq!(leg.index, idx as u32);
        }
    }
}

#[test]
fn test_mutation_kill_single_leg_limit_and_unbounded() {
    // Limit = 1: each share is its own leg
    let plan = plan_against_limit(&make_req(5), Some(1)).expect("plan");
    assert_eq!(plan.legs.len(), 5);
    for leg in &plan.legs {
        assert_eq!(leg.quantity.0, Decimal::from(1));
    }

    // Freeze = None: single leg passes through unchanged
    let plan = plan_against_limit(&make_req(50_000), None).expect("plan");
    assert_eq!(plan.legs.len(), 1);
    assert_eq!(plan.legs[0].quantity.0, Decimal::from(50_000));
}

#[test]
fn test_mutation_kill_audit_identity_linkage() {
    let parent = make_req(2500);
    let plan = plan_against_limit(&parent, Some(1800)).expect("plan");
    assert_eq!(plan.parent_client_order_id, parent.client_order_id);

    let child_reqs = plan.into_order_requests(&parent);
    assert_eq!(child_reqs.len(), 2);

    for (idx, child) in child_reqs.iter().enumerate() {
        // Each child must have distinct UUID
        assert_ne!(child.client_order_id, parent.client_order_id);
        if idx > 0 {
            assert_ne!(child.client_order_id, child_reqs[idx - 1].client_order_id);
        }
        // Correlation ID must point to parent
        assert_eq!(child.correlation_id, Some(parent.client_order_id));
        // Common parent parameters preserved
        assert_eq!(child.instrument, parent.instrument);
        assert_eq!(child.side, parent.side);
        assert_eq!(child.order_type, parent.order_type);
        assert_eq!(child.algo_id, parent.algo_id);
    }
}
