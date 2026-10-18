//! OTR-aware smart order router integrating with deterministic risk engine.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;

use honba_core::{FillReport, OrderAck, OrderRequest, Price, Quantity};
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_te_core::{BrokerError, BrokerExecutor};

/// Backward-compatible router calculating notional with Decimal precision.
pub struct ExecRouter {
    pub otr_limit_per_scrip: u32,
    counters: HashMap<(NaiveDate, String), u32>,
}

impl ExecRouter {
    pub fn new(limit: u32) -> Self {
        Self {
            otr_limit_per_scrip: limit,
            counters: HashMap::new(),
        }
    }

    pub fn check_otr(&mut self, scrip: &str, day: NaiveDate) -> bool {
        let c = self.counters.entry((day, scrip.to_string())).or_insert(0);
        *c += 1;
        *c <= self.otr_limit_per_scrip
    }

    pub fn reset_day(&mut self, day: NaiveDate) {
        self.counters.retain(|(d, _), _| *d == day);
    }

    /// Calculate exact notional value in INR.
    pub fn notional(&self, p: Decimal, q: u64) -> Decimal {
        p * Decimal::from(q)
    }

    /// Calculate exact notional from typed Price and Quantity.
    pub fn notional_exact(&self, p: Price, q: Quantity) -> Decimal {
        p.0 * q.0
    }
}

/// Advanced Smart Order Router (SOR) with integrated SEBI-mandated stateful risk engine.
pub struct SmartOrderRouter {
    /// Synchronous deterministic risk engine
    pub risk_engine: RiskEngine,
    /// Registered broker executors by name
    brokers: HashMap<String, Arc<dyn BrokerExecutor>>,
    /// Default primary broker name
    primary_broker: Option<String>,
}

impl SmartOrderRouter {
    /// Create a new Smart Order Router with risk limits and OTR config.
    pub fn new(limits: RiskLimits, otr_config: OtrConfig) -> Self {
        Self {
            risk_engine: RiskEngine::new(limits, otr_config),
            brokers: HashMap::new(),
            primary_broker: None,
        }
    }

    /// Register a broker executor adapter.
    pub fn register_broker(&mut self, broker: Arc<dyn BrokerExecutor>, is_primary: bool) {
        let name = broker.name().to_string();
        if is_primary || self.primary_broker.is_none() {
            self.primary_broker = Some(name.clone());
        }
        self.brokers.insert(name, broker);
    }

    /// Route an order through pre-trade risk evaluation and dispatch to broker.
    pub async fn route_order(
        &mut self,
        req: OrderRequest,
        preferred_broker: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<OrderAck, BrokerError> {
        // 1. Mandatory Pre-Trade Risk Evaluation (SEBI OTR, price bands, limits, halts)
        self.risk_engine.check_order(&req, now)?;

        // 2. Select Broker
        let broker_name = preferred_broker
            .or(self.primary_broker.as_deref())
            .ok_or_else(|| BrokerError::InvalidRequest("no broker available for routing".into()))?;

        let broker = self
            .brokers
            .get(broker_name)
            .ok_or_else(|| {
                BrokerError::InvalidRequest(format!("broker '{broker_name}' not registered"))
            })?
            .clone();

        // 3. Dispatch to Broker
        broker.place_order(req).await
    }

    /// Record execution fill report into the risk engine.
    pub fn record_fill(&mut self, fill: &FillReport) {
        self.risk_engine.record_fill(fill);
    }

    /// Record cancellation into the risk engine for OTR accounting.
    pub fn record_cancellation(&mut self, algo_id: &str) {
        self.risk_engine.record_cancellation(algo_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use honba_core::*;
    use honba_mon::RiskRefusal;
    use honba_te_core::MockBrokerExecutor;

    #[tokio::test]
    async fn test_smart_order_router_flow() {
        let mut router = SmartOrderRouter::new(RiskLimits::default(), OtrConfig::default());
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
        assert!(matches!(
            err,
            BrokerError::RiskRefusal(RiskRefusal::GlobalHalted { .. })
        ));
    }
}
