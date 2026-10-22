//! Smart order router tests (moved from in-file unit tests per AGENTS.md).

use std::sync::Arc;

use chrono::Utc;
use honba_core::{
    Exchange, InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price, Quantity,
};
use honba_mon::{OtrConfig, RiskLimits, RiskRefusal};
use honba_te_core::MockBrokerExecutor;
use honba_te_router::SmartOrderRouter;

#[tokio::test]
async fn smart_order_router_flow() {
    let mut router = SmartOrderRouter::new(RiskLimits::default(), OtrConfig::default());
    {
        let engine = &mut router.risk_engine;
        engine.register_algo("algo-sor-1");
    }
    let mock = Arc::new(MockBrokerExecutor::new(false));
    router.register_broker(mock, true);

    let now = Utc::now();
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let req = OrderRequest::new(
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("1500.00").unwrap()),
        Quantity::from_u64(10),
        "algo-sor-1",
    );

    // Routing works
    let ack = router.route_order(req.clone(), None, now).await.unwrap();
    assert_eq!(ack.status, OrderStatus::New);

    // Global kill switch triggers rejection before touching broker
    router.risk_engine.halt_global("emergency halt", now);
    let err = router.route_order(req, None, now).await.unwrap_err();
    match err {
        honba_te_core::BrokerError::RiskRefusal(inner) => {
            assert!(matches!(&*inner, RiskRefusal::GlobalHalted { .. }));
        }
        other => panic!("expected global-halt risk refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn unregistered_algo_is_refused_before_dispatch() {
    let mut router = SmartOrderRouter::new(RiskLimits::default(), OtrConfig::default());
    let mock = Arc::new(MockBrokerExecutor::new(false));
    router.register_broker(mock, true);

    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "TATAMOTORS"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(5),
        "algo-never-registered",
    );
    let err = router.route_order(req, None, Utc::now()).await.unwrap_err();
    match err {
        honba_te_core::BrokerError::RiskRefusal(inner) => {
            assert!(matches!(&*inner, RiskRefusal::AlgoNotRegistered { .. }));
        }
        other => panic!("expected algo-registration risk refusal, got {other:?}"),
    }
}

#[test]
fn exec_router_notional_mathematics_are_exact() {
    let mut router = ExecRouterAlias::new(50);
    let p = rust_decimal::Decimal::from_str_exact("2914.85").unwrap();
    assert_eq!(
        router.notional_exact(Price::new(p), Quantity::from_u64(7)),
        p * rust_decimal::Decimal::from(7)
    );
    // OTR budget: 50 accepted, then refused
    let day = chrono::NaiveDate::from_ymd_opt(2026, 10, 22).unwrap();
    for _ in 0..50 {
        assert!(router.check_otr("INFY", day));
    }
    assert!(!router.check_otr("INFY", day));

    // Next trading day starts with a fresh budget.
    let next_day = day.succ_opt().unwrap();
    assert!(router.check_otr("INFY", next_day));

    // reset_day prunes counters belonging to days other than `next_day`
    // (the live trading day is untouched).
    router.reset_day(next_day);
    for _ in 0..49 {
        assert!(router.check_otr("INFY", next_day));
    }
    assert!(!router.check_otr("INFY", next_day));
}

// alias import for readability in tests
use honba_te_router::ExecRouter as ExecRouterAlias;
