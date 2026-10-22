//! M4 SEBI OTR balancing tests (plan §3.5 item 4): steering away from
//! accounts at the ceiling, cold-start protection, and modification/fill
//! accounting.

use chrono::Utc;
use honba_core::Price;
use honba_te_core::ProductType;
use honba_te_router::{BrokerCostModel, OtrBalanceConfig, ResilientRouter, RoutePolicy, SorConfig};

mod common;
use common::{MockBroker, delivery_request};

fn config() -> SorConfig {
    SorConfig {
        policy: RoutePolicy::CostOptimized,
        ..SorConfig::default()
    }
}

fn router_with(max_otr: rust_decimal::Decimal, enforcement_min: u64) -> ResilientRouter {
    let mut config = config();
    config.otr = OtrBalanceConfig {
        max_otr,
        min_operations_before_enforcement: enforcement_min,
    };
    ResilientRouter::new(config)
}

/// `busy` carries a zero fee so CostOptimized always ranks it first (cost
/// 0 < fresh's 100): pressure deterministically accumulates on `busy` until
/// the OTR ceiling forces the router elsewhere.
fn two_accounts(router: &ResilientRouter) {
    router
        .register(
            "busy",
            MockBroker::healthy("busy"),
            BrokerCostModel::FlatFee { fee: 0.into() },
        )
        .unwrap();
    router
        .register(
            "fresh",
            MockBroker::healthy("fresh"),
            BrokerCostModel::FlatFee { fee: 100.into() },
        )
        .unwrap();
}

#[tokio::test]
async fn saturated_account_is_routed_around() {
    let router = router_with(rust_decimal::Decimal::new(50, 0), 25);
    two_accounts(&router);

    // 50 order actions on `busy` (ties break alphabetically): OTR = 50/1 = 50
    // hits the ceiling; enforcement armed past 25 operations.
    for _ in 0..50 {
        router
            .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
            .await
            .unwrap();
    }
    let next = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert_eq!(next.broker, "fresh", "saturated `busy` routed around");

    let order = router
        .plan_route(&delivery_request(1), ProductType::CncDelivery, Utc::now())
        .unwrap();
    assert_eq!(order, ["fresh"], "only `fresh` remains OTR-eligible");

    // Two fills grow the denominator: 50 / 2 = 25 < 50 → `busy` re-enters
    // the eligible ranking and, with cost 0 < 100, retakes rank #1.
    assert!(router.record_fill("busy"));
    assert!(router.record_fill("busy"));
    let order = router
        .plan_route(&delivery_request(1), ProductType::CncDelivery, Utc::now())
        .unwrap();
    assert_eq!(
        order,
        ["busy", "fresh"],
        "fills rebalance the account into eligibility"
    );
}

#[tokio::test]
async fn cold_start_protection_before_enforcement_arms() {
    // max_otr 5 with enforcement arming only after 25 actions: the first 10
    // orders keep `busy` eligible despite ratio 10 ≥ 5.
    let router = router_with(rust_decimal::Decimal::new(5, 0), 25);
    two_accounts(&router);

    for _ in 0..10 {
        router
            .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
            .await
            .unwrap();
    }
    let order = router
        .plan_route(&delivery_request(1), ProductType::CncDelivery, Utc::now())
        .unwrap();
    assert_eq!(
        order.len(),
        2,
        "cold-start protection: no premature lockout"
    );

    // Past 25 actions the ceiling enforces for real.
    for _ in 0..25 {
        router
            .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
            .await
            .unwrap();
    }
    let order = router
        .plan_route(&delivery_request(1), ProductType::CncDelivery, Utc::now())
        .unwrap();
    assert_eq!(
        order,
        ["fresh"],
        "35 actions ≥ 25-enforced, ratio 35 ≥ 5 → busy excluded"
    );
}

#[tokio::test]
async fn modifications_and_cancellations_count_toward_otr() {
    let router = router_with(rust_decimal::Decimal::new(50, 0), 25);
    two_accounts(&router);

    let routed = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();

    router
        .route_modify(
            &routed.ack.order_id,
            Some(Price::from_rupees("501.00").unwrap()),
            None,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(
        router
            .broker_states(Utc::now())
            .into_iter()
            .find(|state| state.name == routed.broker)
            .unwrap()
            .otr_ratio
            .to_string(),
        "2",
        "order + modification = 2 numerator actions (trades 0 → denominator 1)"
    );

    router
        .route_cancel(&routed.ack.order_id, Utc::now())
        .await
        .unwrap();
    let ratio = router
        .broker_states(Utc::now())
        .into_iter()
        .find(|state| state.name == routed.broker)
        .unwrap()
        .otr_ratio;
    assert_eq!(
        ratio.to_string(),
        "3",
        "cancellation counts as an order action"
    );
}

#[tokio::test]
async fn fills_credit_the_owning_account_via_order_id() {
    let router = router_with(rust_decimal::Decimal::new(50, 0), 25);
    two_accounts(&router);

    let routed = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert!(
        router.record_fill_for_order(&routed.ack.order_id),
        "ownership ledger resolves the account"
    );
    let trades = router
        .broker_states(Utc::now())
        .into_iter()
        .find(|state| state.name == routed.broker)
        .unwrap()
        .trades;
    assert_eq!(trades, 1);

    assert!(
        !router.record_fill_for_order("never-routed"),
        "unknown order id is not credited"
    );
}
