//! M4 automated failover engine tests (plan §3.5 item 5): transparent
//! re-routing on transport faults, no failover on business rejections, hop
//! budget, breaker interaction, and stateful modify/cancel ownership.

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use honba_te_core::{BrokerError, ProductType};
use honba_te_router::{BrokerCostModel, CircuitState, ResilientRouter, RoutePolicy, SorConfig};

mod common;
use common::{MockBroker, Script, delivery_request};

fn config() -> SorConfig {
    SorConfig {
        policy: RoutePolicy::CostOptimized,
        ..SorConfig::default()
    }
}

/// zero-delivery always ranks first for delivery orders (cost 0 < 50).
fn accounts(router: &ResilientRouter) {
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
}

#[tokio::test]
async fn transport_failure_fails_over_to_secondary() {
    let router = ResilientRouter::new(config());
    // The primary answers with a connection fault; the router transparently
    // re-routes the SAME request to the next-ranked account.
    let primary = Arc::new(MockBroker::new("zero-delivery").push(Script::Transport));
    router
        .register(
            "zero-delivery",
            primary.clone(),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    let secondary = Arc::new(MockBroker::new("flat-access"));
    router
        .register(
            "flat-access",
            secondary.clone(),
            BrokerCostModel::FlatFee { fee: 50.into() },
        )
        .unwrap();

    let routed = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert_eq!(routed.broker, "flat-access", "failover to the secondary");
    assert!(primary.placed().is_empty(), "primary never accepted a leg");
    assert_eq!(secondary.placed().len(), 1, "secondary carries the request");
}

#[tokio::test]
async fn timeout_is_failover_eligible() {
    let router = ResilientRouter::new(config());
    router
        .register(
            "zero-delivery",
            Arc::new(MockBroker::new("zero-delivery").push(Script::Timeout)),
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
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();
    assert_eq!(routed.broker, "flat-access", "timeout is failover-eligible");
}

#[tokio::test]
async fn broker_rejection_never_fails_over() {
    let router = ResilientRouter::new(config());
    router
        .register(
            "zero-delivery",
            Arc::new(MockBroker::new("zero-delivery").push(Script::Reject)),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    let secondary = Arc::new(MockBroker::new("flat-access"));
    router
        .register(
            "flat-access",
            secondary.clone(),
            BrokerCostModel::FlatFee { fee: 50.into() },
        )
        .unwrap();

    let result = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await;
    match result {
        Err(BrokerError::Rejected(reason)) => assert!(
            reason.contains("RMS"),
            "the business rejection itself must surface: {reason}"
        ),
        other => panic!("expected Rejected without failover, got {other:?}"),
    }
    // Double-execution guard: nothing was placed on the secondary.
    assert!(
        secondary.placed().is_empty(),
        "rejected order is not re-routed"
    );
    // The gateway was reached → the rejection counts as a SEBI order action.
    let primary_ratio = router
        .broker_states(Utc::now())
        .iter()
        .find(|state| state.name == "zero-delivery")
        .unwrap()
        .otr_ratio;
    assert_eq!(primary_ratio.to_string(), "1");
}

#[tokio::test]
async fn zero_hop_budget_surfaces_the_transport_error_directly() {
    let mut config = config();
    config.max_failover_hops = 0;
    let router = ResilientRouter::new(config);
    router
        .register(
            "zero-delivery",
            Arc::new(MockBroker::new("zero-delivery").push(Script::Transport)),
            BrokerCostModel::ZeroDelivery {
                intraday_bps: 10.into(),
            },
        )
        .unwrap();
    let secondary = Arc::new(MockBroker::new("flat-access"));
    router
        .register(
            "flat-access",
            secondary.clone(),
            BrokerCostModel::FlatFee { fee: 50.into() },
        )
        .unwrap();

    let result = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await;
    assert!(
        matches!(result, Err(BrokerError::Transport(_))),
        "no hops left: the transport error must surface"
    );
    assert!(secondary.placed().is_empty());
}

#[tokio::test]
async fn three_consecutive_transport_failures_trip_the_breaker() {
    let router = ResilientRouter::new(config());
    // Three queued transport faults on the primary.
    let primary = MockBroker::new("zero-delivery")
        .push(Script::Transport)
        .push(Script::Transport)
        .push(Script::Transport);
    router
        .register(
            "zero-delivery",
            Arc::new(primary),
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

    let now = Utc::now();
    for index in 0..3 {
        let routed = router
            .route_order(delivery_request(1), ProductType::CncDelivery, now)
            .await
            .unwrap();
        assert_eq!(
            routed.broker, "flat-access",
            "request #{index} transparently failed over"
        );
    }
    assert_eq!(
        router
            .broker_states(now)
            .iter()
            .find(|state| state.name == "zero-delivery")
            .unwrap()
            .circuit,
        CircuitState::Open,
        "3 consecutive transport faults trip the breaker"
    );

    // Open breaker shortens the chain: the 4th request never touches the
    // primary at all.
    let routed = router
        .route_order(delivery_request(1), ProductType::CncDelivery, now)
        .await
        .unwrap();
    assert_eq!(routed.broker, "flat-access");
    assert_eq!(
        router
            .broker_states(now)
            .iter()
            .find(|state| state.name == "zero-delivery")
            .unwrap()
            .otr_ratio,
        rust_decimal::Decimal::ZERO,
        "transport failures never count as gateway-reached OTR actions"
    );
}

#[tokio::test]
async fn halfopen_recovery_through_the_router() {
    let router = ResilientRouter::new(config());
    let primary = MockBroker::new("zero-delivery")
        .push(Script::Transport)
        .push(Script::Transport)
        .push(Script::Transport)
        .push(Script::Ok);
    router
        .register(
            "zero-delivery",
            Arc::new(primary),
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

    let now = Utc::now();
    for _ in 0..3 {
        router
            .route_order(delivery_request(1), ProductType::CncDelivery, now)
            .await
            .unwrap();
    }
    assert_eq!(
        router
            .broker_states(now)
            .iter()
            .find(|state| state.name == "zero-delivery")
            .unwrap()
            .circuit,
        CircuitState::Open
    );

    // After the cooldown the breaker HalfOpen-probes with the 4th order.
    let later = now + ChronoDuration::seconds(31);
    let routed = router
        .route_order(delivery_request(1), ProductType::CncDelivery, later)
        .await
        .unwrap();
    assert_eq!(
        routed.broker, "zero-delivery",
        "probe rides the recovering primary first"
    );
    assert_eq!(
        router
            .broker_states(later)
            .iter()
            .find(|state| state.name == "zero-delivery")
            .unwrap()
            .circuit,
        CircuitState::Closed,
        "successful probe re-closes the breaker"
    );
}

#[tokio::test]
async fn modify_and_cancel_target_the_owning_account() {
    let router = ResilientRouter::new(config());
    accounts(&router);

    let routed = router
        .route_order(delivery_request(1), ProductType::CncDelivery, Utc::now())
        .await
        .unwrap();

    router
        .route_modify(&routed.ack.order_id, None, None, Utc::now())
        .await
        .unwrap();
    router
        .route_cancel(&routed.ack.order_id, Utc::now())
        .await
        .unwrap();

    // Ownership ledger: the account holding the child order serves its bonds.
    let states = router.broker_states(Utc::now());
    let holder = states
        .iter()
        .find(|state| state.name == routed.broker)
        .unwrap();
    assert_eq!(
        holder.otr_ratio.to_string(),
        "3",
        "order + modify + cancel = 3 SEBI order actions"
    );

    let missing = router
        .route_modify("never-routed-id", None, None, Utc::now())
        .await
        .unwrap_err();
    assert!(
        matches!(missing, BrokerError::OrderNotFound(_)),
        "{missing}"
    );
}
