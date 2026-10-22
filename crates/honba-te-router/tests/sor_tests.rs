//! M4 SOR v2 contract tests: registration, routing strategies (plan §3.5
//! items 1 + 3), and the §5.2 SOR decision-latency sanity benchmark.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use honba_te_core::{BrokerError, ProductType};
use honba_te_router::{BrokerCostModel, ResilientRouter, RoutePolicy, SorConfig};

mod common;
use common::{MockBroker, delivery_request, intraday_request};

fn config(policy: RoutePolicy) -> SorConfig {
    SorConfig {
        policy,
        ..SorConfig::default()
    }
}

#[test]
fn duplicate_registration_is_refused() {
    let router = ResilientRouter::default();
    router
        .register(
            "zero-delivery",
            MockBroker::healthy("zero-delivery"),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    let err = router
        .register(
            "zero-delivery",
            MockBroker::healthy("zero-delivery"),
            BrokerCostModel::FlatFee { fee: 50.into() },
        )
        .unwrap_err();
    assert!(matches!(err, BrokerError::InvalidRequest(_)), "{err}");
}

#[tokio::test]
async fn delivery_orders_route_to_zero_brokerage() {
    // Plan §3.5: zero-brokerage accounts win equity delivery (cost 0 < fee 50).
    let router = ResilientRouter::new(config(RoutePolicy::CostOptimized));
    router
        .register(
            "zero-delivery",
            MockBroker::healthy("zero-delivery"),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    router
        .register(
            "flat-access",
            MockBroker::healthy("flat-access"),
            BrokerCostModel::FlatFee { fee: 50.into() },
        )
        .unwrap();

    let routed = router
        .route_order(delivery_request(100), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        routed.broker, "zero-delivery",
        "delivery rides the zero-brokerage account"
    );
}

#[tokio::test]
async fn intraday_orders_route_to_flat_fee() {
    // Plan §3.5: intraday/derivatives prefer flat-fee direct-access brokers.
    // Notional 100 × 500.00 = 50_000 → zero-delivery intraday cost
    // 50_000 × 10 bps / 10_000 = 50 > flat 20.
    let router = ResilientRouter::new(config(RoutePolicy::CostOptimized));
    router
        .register(
            "zero-delivery",
            MockBroker::healthy("zero-delivery"),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    router
        .register(
            "flat-access",
            MockBroker::healthy("flat-access"),
            BrokerCostModel::FlatFee { fee: 20.into() },
        )
        .unwrap();

    let routed = router
        .route_order(intraday_request(100), ProductType::MisIntraday, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        routed.broker, "flat-access",
        "intraday rides the flat-fee broker"
    );
}

#[tokio::test]
async fn pinned_policy_routes_to_named_account_first() {
    let router = ResilientRouter::new(config(RoutePolicy::Pinned("flat-access".into())));
    router
        .register(
            "zero-delivery",
            MockBroker::healthy("zero-delivery"),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    router
        .register(
            "flat-access",
            MockBroker::healthy("flat-access"),
            BrokerCostModel::FlatFee { fee: 50.into() },
        )
        .unwrap();

    let routed = router
        .route_order(delivery_request(10), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        routed.broker, "flat-access",
        "pinned account overrides cost"
    );
}

#[tokio::test]
async fn unregistered_pinned_account_falls_through_to_others() {
    let router = ResilientRouter::new(config(RoutePolicy::Pinned("ghost-account".into())));
    router
        .register(
            "only-broker",
            MockBroker::healthy("only-broker"),
            BrokerCostModel::FlatFee { fee: 30.into() },
        )
        .unwrap();

    let routed = router
        .route_order(delivery_request(10), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        routed.broker, "only-broker",
        "pin miss still routes to a live account"
    );
}

#[tokio::test]
async fn lowest_latency_discovers_the_fast_account_and_sticks() {
    // Three real-delayed brokers; RTT is measured on the placement path by
    // the router itself. Optimistic discovery: order #1 probes `alpha` and
    // #2/#3 probe the remaining unsampled accounts (name order), and from
    // #4 the measured-fast account sticks.
    let router = ResilientRouter::new(config(RoutePolicy::LowestLatency));
    router
        .register(
            "alpha",
            Arc::new(MockBroker::new("alpha").with_latency(Duration::from_millis(60))),
            BrokerCostModel::FlatFee { fee: 0.into() },
        )
        .unwrap();
    router
        .register(
            "beta",
            Arc::new(MockBroker::new("beta").with_latency(Duration::from_millis(5))),
            BrokerCostModel::FlatFee { fee: 0.into() },
        )
        .unwrap();
    router
        .register(
            "gamma",
            Arc::new(MockBroker::new("gamma").with_latency(Duration::from_millis(80))),
            BrokerCostModel::FlatFee { fee: 0.into() },
        )
        .unwrap();
    let now = Utc::now();
    let req = delivery_request(1);

    let first = router
        .route_order(req.clone(), ProductType::CncDelivery, now)
        .await
        .unwrap();
    assert_eq!(
        first.broker, "alpha",
        "cold probe order follows account names"
    );

    let second = router
        .route_order(req.clone(), ProductType::CncDelivery, now)
        .await
        .unwrap();
    assert_eq!(second.broker, "beta", "next unsampled account is probed");

    let third = router
        .route_order(req.clone(), ProductType::CncDelivery, now)
        .await
        .unwrap();
    assert_eq!(
        third.broker, "gamma",
        "last unsampled account completes discovery"
    );

    assert_eq!(
        router
            .route_order(req, ProductType::CncDelivery, now)
            .await
            .unwrap()
            .broker,
        "beta",
        "measured-fast account wins once discovery completes"
    );
}

#[tokio::test]
async fn sor_decision_latency_budget() {
    // Plan §5.2: router overhead τ_SOR < 100 µs on the decision path.
    // Debug-mode CI headroom: average over 500 decisions must stay far under
    // 1 ms; release-mode measurement targets the 100 µs figure itself.
    let router = ResilientRouter::new(config(RoutePolicy::CostOptimized));
    for name in ["zero-delivery", "flat-access", "triple-dot"] {
        router
            .register(
                name,
                MockBroker::healthy(name),
                BrokerCostModel::ZeroDelivery {
                    intraday_bps: 10.into(),
                },
            )
            .unwrap();
    }
    let now = Utc::now();
    let req = delivery_request(10);
    let started = Instant::now();
    for _ in 0..500 {
        let order = router
            .plan_route(&req, ProductType::CncDelivery, now)
            .unwrap();
        assert_eq!(order.len(), 3);
    }
    let average = started.elapsed() / 500;
    assert!(
        average.as_micros() < 1_000,
        "decision latency {} µs must stay under the 1 ms budget",
        average.as_micros()
    );
}
