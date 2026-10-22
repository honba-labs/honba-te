//! Resilient Smart Order Router v2 (plan §3.5, M4).
//!
//! [`ResilientRouter`] is the multi-broker SOR: cost-optimized and
//! latency-aware routing strategies over registered broker accounts, gated
//! by per-account health circuit breakers ([`crate::health`]) and SEBI OTR
//! balancing ([`crate::otr`]), with transparent automated failover.
//!
//! # Routing decision per request
//! 1. Filter registered accounts by health (`Closed`/`HalfOpen`) and OTR
//!    eligibility (ceiling not breached once enforcement has armed).
//! 2. Rank by the configured [`RoutePolicy`]:
//!    - [`RoutePolicy::CostOptimized`] — modeled brokerage ascending, OTR
//!      ratio as the tiebreak (delivery flows land on zero-brokerage
//!      accounts, intraday/derivative flows on flat-fee brokers).
//!    - [`RoutePolicy::LowestLatency`] — rolling RTT ascending.
//!    - [`RoutePolicy::Pinned`] — the pinned account first, everyone else
//!      behind it by account name.
//! 3. Place through the top candidate. On `Transport`/`Timeout` failure the
//!    hop advances to the next-ranked account (transparent automated
//!    failover, plan §4 M4 item 5), up to `max_failover_hops`.
//! 4. Business-level outcomes (broker rejections) count as gateway-reached
//!    OTR actions and never trigger failover — re-routing a broker-rejected
//!    order would risk double execution.
//!
//! Modifications and cancellations are stateful: they always target the
//! account that owns the child order (failover only applies to new orders).
//! The ownership ledger maps every broker order ID back to its account for
//! [`ResilientRouter::record_fill_for_order`],
//! [`ResilientRouter::route_modify`], and [`ResilientRouter::route_cancel`].
//!
//! Pre-trade SEBI risk gating stays upstream of this layer: wrap registry
//! executors in [`honba_te_core::RiskGatedExecutor`], or use the legacy
//! [`crate::SmartOrderRouter`] for the integrated risk-engine flow.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use honba_core::{OrderAck, OrderRequest, Price, Quantity};
use honba_te_core::{BrokerError, BrokerExecutor, ProductType};

use crate::cost::BrokerCostModel;
use crate::health::{CircuitState, HealthTracker};
use crate::otr::{OtrBalanceConfig, OtrLedger};

/// Routing strategy applied across eligible accounts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutePolicy {
    /// Rank by modeled brokerage ascending; ties break to the account with
    /// the most OTR headroom. Delivers the plan's cost-optimized split:
    /// delivery → zero-brokerage, intraday/derivatives → flat-fee.
    CostOptimized,
    /// Rank by rolling RTT ascending; unsampled accounts probe first
    /// (optimistic discovery, then measured-fast sticky ranking).
    LowestLatency,
    /// Pin the named account first; remaining accounts follow by name.
    Pinned(String),
}

/// Resilient-router configuration bundle.
#[derive(Debug, Clone, PartialEq)]
pub struct SorConfig {
    /// Account-ranking strategy.
    pub policy: RoutePolicy,
    /// Health circuit-breaker thresholds.
    pub health: crate::health::HealthConfig,
    /// SEBI OTR balancing thresholds.
    pub otr: OtrBalanceConfig,
    /// Failover budget for transport-level failures on new orders
    /// (plan default: 1 hop — primary faulted → next-ranked secondary).
    pub max_failover_hops: u32,
}

impl Default for SorConfig {
    fn default() -> Self {
        Self {
            policy: RoutePolicy::CostOptimized,
            health: crate::health::HealthConfig::default(),
            otr: OtrBalanceConfig::default(),
            max_failover_hops: 1,
        }
    }
}

/// One registered broker account.
pub struct BrokerRegistration {
    /// Routing-stable account label.
    pub name: String,
    /// Broker executor adapter (may already be rate-limited / risk-gated).
    pub executor: Arc<dyn BrokerExecutor>,
    /// Static brokerage model used by cost-based ranking.
    pub cost: BrokerCostModel,
    /// Rolling health circuit breaker.
    pub health: HealthTracker,
    /// Live SEBI OTR accounting.
    pub ledger: Mutex<OtrLedger>,
}

/// Operational snapshot for dashboards and ops tooling.
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerStatus {
    /// Account label.
    pub name: String,
    /// Derived circuit-breaker state.
    pub circuit: CircuitState,
    /// Current OTR ratio (order actions / trades, provisional denominator 1).
    pub otr_ratio: Decimal,
    /// Executed trades on the account.
    pub trades: u64,
}

/// Placement result tagged with the serving account.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutedAck {
    /// Account label the order was placed through.
    pub broker: String,
    /// Broker acknowledgement.
    pub ack: OrderAck,
}

/// Resilient Smart Order Router v2: multi-broker registration, health
/// circuit breakers, cost-optimized routing, OTR balancing, and automated
/// transport-level failover.
pub struct ResilientRouter {
    config: SorConfig,
    registered: Mutex<Vec<Arc<BrokerRegistration>>>,
    /// Broker order ID → owning account (stateful modify/cancel/fill paths).
    ownership: Mutex<HashMap<String, String>>,
}

impl Default for ResilientRouter {
    fn default() -> Self {
        Self::new(SorConfig::default())
    }
}

impl ResilientRouter {
    /// Router under `config`.
    pub fn new(config: SorConfig) -> Self {
        Self {
            config,
            registered: Mutex::new(Vec::new()),
            ownership: Mutex::new(HashMap::new()),
        }
    }

    /// Router-wide configuration.
    pub fn config(&self) -> &SorConfig {
        &self.config
    }

    /// Register one broker account. Duplicate names are rejected.
    pub fn register(
        &self,
        name: impl Into<String>,
        executor: Arc<dyn BrokerExecutor>,
        cost: BrokerCostModel,
    ) -> Result<(), BrokerError> {
        let registration = Arc::new(BrokerRegistration {
            name: name.into(),
            executor,
            cost,
            health: HealthTracker::new(self.config.health.clone()),
            ledger: Mutex::new(OtrLedger::default()),
        });
        let mut registered = self.registered.lock().unwrap();
        if registered
            .iter()
            .any(|existing| existing.name == registration.name)
        {
            return Err(BrokerError::InvalidRequest(format!(
                "broker account '{}' is already registered",
                registration.name
            )));
        }
        registered.push(registration);
        Ok(())
    }

    /// Health/OTR snapshot of every registered account.
    pub fn broker_states(&self, now: DateTime<Utc>) -> Vec<BrokerStatus> {
        self.registered
            .lock()
            .unwrap()
            .iter()
            .map(|registration| {
                let ledger = *registration.ledger.lock().unwrap();
                BrokerStatus {
                    name: registration.name.clone(),
                    circuit: registration.health.state(now),
                    otr_ratio: ledger.otr_ratio(),
                    trades: ledger.trades,
                }
            })
            .collect()
    }

    /// Pure routing decision: ranks every eligible account for `req`
    /// (modeled `product`) under the configured policy and returns the
    /// ordered account names. Exposed to carry the plan §5.2 microsecond
    /// SOR decision-latency benchmark without touching broker transports.
    pub fn plan_route(
        &self,
        req: &OrderRequest,
        product: ProductType,
        now: DateTime<Utc>,
    ) -> Result<Vec<String>, BrokerError> {
        let ranked = self.rank_candidates(req, product, now)?;
        Ok(ranked
            .iter()
            .map(|registration| registration.name.clone())
            .collect())
    }

    /// Eligible accounts filtered by breaker state + OTR gating, ranked by
    /// policy. Empty → [`BrokerError::InvalidRequest`].
    fn rank_candidates(
        &self,
        req: &OrderRequest,
        product: ProductType,
        now: DateTime<Utc>,
    ) -> Result<Vec<Arc<BrokerRegistration>>, BrokerError> {
        let registered = self.registered.lock().unwrap();
        let notional = req
            .order_type
            .limit_price()
            .map(|price: Price| price.0 * req.quantity.0)
            .unwrap_or(Decimal::ZERO);
        let mut eligible: Vec<Arc<BrokerRegistration>> = registered
            .iter()
            .filter(|registration| {
                registration.health.is_routable(now)
                    && !registration
                        .ledger
                        .lock()
                        .unwrap()
                        .breaches(&self.config.otr)
            })
            .cloned()
            .collect();
        drop(registered);
        if eligible.is_empty() {
            return Err(BrokerError::InvalidRequest(
                "no eligible broker account: all candidates breaker-tripped or over OTR ceiling"
                    .into(),
            ));
        }
        let otr_config = self.config.otr.clone();
        match &self.config.policy {
            RoutePolicy::CostOptimized => {
                eligible.sort_by(|a, b| {
                    let cost_a = a.cost.order_cost(product, notional);
                    let cost_b = b.cost.order_cost(product, notional);
                    cost_a
                        .cmp(&cost_b)
                        .then_with(|| {
                            b.ledger
                                .lock()
                                .unwrap()
                                .headroom(&otr_config)
                                .cmp(&a.ledger.lock().unwrap().headroom(&otr_config))
                        })
                        .then_with(|| a.name.cmp(&b.name))
                });
            }
            RoutePolicy::LowestLatency => {
                // Optimistic discovery: unsampled accounts probe first (one
                // placement yields a sample), then known-RTT accounts rank
                // by their rolling measurement ascending.
                eligible.sort_by(|a, b| {
                    let rtt_a = a.health.avg_rtt();
                    let rtt_b = b.health.avg_rtt();
                    match (rtt_a, rtt_b) {
                        (Some(measured_a), Some(measured_b)) => measured_a
                            .cmp(&measured_b)
                            .then_with(|| a.name.cmp(&b.name)),
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        (None, None) => a.name.cmp(&b.name),
                    }
                });
            }
            RoutePolicy::Pinned(pinned) => {
                eligible.sort_by(|a, b| {
                    let pinned_a = a.name == *pinned;
                    let pinned_b = b.name == *pinned;
                    pinned_b.cmp(&pinned_a).then_with(|| a.name.cmp(&b.name))
                });
            }
        }
        Ok(eligible)
    }

    /// Route a new parent order: rank, place, and transparently fail over on
    /// transport-level faults within the configured hop budget.
    pub async fn route_order(
        &self,
        req: OrderRequest,
        product: ProductType,
        now: DateTime<Utc>,
    ) -> Result<RoutedAck, BrokerError> {
        let candidates = self.rank_candidates(&req, product, now)?;
        let attempts = (self.config.max_failover_hops as usize + 1).min(candidates.len());
        let mut last_error: Option<BrokerError> = None;
        for registration in candidates.into_iter().take(attempts) {
            let started = Instant::now();
            match registration.executor.place_order(req.clone()).await {
                Ok(ack) => {
                    registration.health.record_latency(started.elapsed(), now);
                    registration.health.record_success(now);
                    registration.ledger.lock().unwrap().orders += 1;
                    self.ownership
                        .lock()
                        .unwrap()
                        .insert(ack.order_id.clone(), registration.name.clone());
                    return Ok(RoutedAck {
                        broker: registration.name.clone(),
                        ack,
                    });
                }
                // Transport-level failure: gateway unreachable → trip-
                // eligible health failure and transparent failover hop.
                Err(error @ BrokerError::Transport(_) | error @ BrokerError::Timeout) => {
                    registration.health.record_failure(now);
                    last_error = Some(error);
                }
                // Business-level outcome (broker/exchange rejection): the
                // gateway was reached, so this counts as a SEBI order action
                // and must NOT be re-routed (double-execution risk).
                Err(other) => {
                    registration.ledger.lock().unwrap().orders += 1;
                    registration.health.record_success(now);
                    return Err(other);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| BrokerError::InvalidRequest("no broker accounts registered".into())))
    }

    /// Route a modification to the account that owns `order_id`.
    ///
    /// Stateful routing: no cross-account failover exists for an order that
    /// already lives on its owning broker. OTR modifications count toward
    /// the account's numerator (SEBI order actions).
    pub async fn route_modify(
        &self,
        order_id: &str,
        new_price: Option<Price>,
        new_quantity: Option<Quantity>,
        now: DateTime<Utc>,
    ) -> Result<OrderAck, BrokerError> {
        let registration = self.owner(order_id)?;
        registration.ledger.lock().unwrap().modifications += 1;
        let started = Instant::now();
        let result = registration
            .executor
            .modify_order(order_id, new_price, new_quantity)
            .await;
        self.record_request_outcome(&registration, started.elapsed(), &result, now);
        result
    }

    /// Route a cancellation to the account that owns `order_id`.
    ///
    /// Cancellations count toward the OTR numerator on the owning account.
    pub async fn route_cancel(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), BrokerError> {
        let registration = self.owner(order_id)?;
        registration.ledger.lock().unwrap().cancellations += 1;
        let started = Instant::now();
        let result = registration.executor.cancel_order(order_id).await;
        self.record_request_outcome(&registration, started.elapsed(), &result, now);
        result
    }

    /// Credit a fill to a named account's OTR denominator.
    pub fn record_fill(&self, broker: &str) -> bool {
        let registered = self.registered.lock().unwrap();
        match registered.iter().find(|r| r.name == broker) {
            Some(registration) => {
                registration.ledger.lock().unwrap().trades += 1;
                true
            }
            None => false,
        }
    }

    /// Credit a fill via the ownership ledger (broker order ID → account).
    pub fn record_fill_for_order(&self, order_id: &str) -> bool {
        self.ownership
            .lock()
            .unwrap()
            .get(order_id)
            .cloned()
            .is_some_and(|broker| self.record_fill(&broker))
    }

    fn owner(&self, order_id: &str) -> Result<Arc<BrokerRegistration>, BrokerError> {
        let broker = self
            .ownership
            .lock()
            .unwrap()
            .get(order_id)
            .cloned()
            .ok_or_else(|| {
                BrokerError::OrderNotFound(format!(
                    "order {order_id} is not owned by any routed account"
                ))
            })?;
        self.registered
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.name == broker)
            .cloned()
            .ok_or_else(|| BrokerError::OrderNotFound(format!("account {broker} deregistered")))
    }

    fn record_request_outcome<T>(
        &self,
        registration: &BrokerRegistration,
        elapsed: std::time::Duration,
        result: &Result<T, BrokerError>,
        now: DateTime<Utc>,
    ) {
        match result {
            Ok(_) => {
                registration.health.record_latency(elapsed, now);
                registration.health.record_success(now);
            }
            Err(error) => {
                if matches!(error, BrokerError::Transport(_) | BrokerError::Timeout) {
                    registration.health.record_failure(now);
                } else {
                    registration.health.record_success(now);
                }
            }
        }
    }
}
