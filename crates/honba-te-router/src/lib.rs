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

    /// Record an order cancellation into the risk engine for OTR accounting.
    pub fn record_cancellation(&mut self, algo_id: &str) {
        self.risk_engine.record_cancellation(algo_id, Utc::now());
    }
}
