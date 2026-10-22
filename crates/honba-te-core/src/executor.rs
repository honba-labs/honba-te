//! Canonical broker execution trait v2, risk-gated wrapper, and mock adapter.

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use honba_core::{FillReport, Money, OrderAck, OrderRequest, OrderStatus, Price, Quantity};

use crate::decimal::quantity_to_u32_exact;
use crate::error::BrokerError;
use crate::types::{BrokerHealth, BrokerMargins, BrokerPosition, OrderSnapshot, ProductType};

/// Gateway contract for native iceberg legging: `display` must divide the
/// parent quantity exactly into 2..=50 legs (Kite Connect v3 documented caps).
pub fn validate_native_legs(
    parent_quantity: Quantity,
    display_quantity: Quantity,
) -> Result<u32, BrokerError> {
    use rust_decimal::prelude::ToPrimitive;
    let parent = quantity_to_u32_exact(parent_quantity.0)? as u64;
    let display = quantity_to_u32_exact(display_quantity.0)? as u64;
    if display == 0 || display >= parent {
        return Err(BrokerError::InvalidRequest(format!(
            "iceberg display quantity {display} must be non-zero and smaller than parent {parent}"
        )));
    }
    if !parent.is_multiple_of(display) {
        return Err(BrokerError::InvalidRequest(format!(
            "iceberg display quantity {display} must divide parent {parent} exactly"
        )));
    }
    let legs = parent / display;
    if !(2..=50).contains(&legs) {
        return Err(BrokerError::InvalidRequest(format!(
            "iceberg leg count {legs} outside the gateway range 2..=50"
        )));
    }
    legs.to_u32()
        .ok_or_else(|| BrokerError::InvalidRequest("iceberg leg count overflows".into()))
}

/// Unified trait implemented by broker execution adapters (Zerodha, Upstox,
/// Angel One, Paper/Sim).
///
/// # Trait v2 Surface
/// Compared with v1, this adds:
/// - [`BrokerExecutor::health_check`] — session/connection probing
/// - [`BrokerExecutor::fetch_order_book`] — full active order book snapshot
/// - [`BrokerExecutor::fetch_positions`] — Demat and intraday positions
/// - [`BrokerExecutor::fetch_margins`] — real-time cash and margin balances
///
/// Pull-based [`BrokerExecutor::get_order_status`] is retained strictly as a
/// polling fallback; order lifecycles must be driven primarily by push
/// streams ([`crate::ExecutionStream`]).
#[async_trait]
pub trait BrokerExecutor: Send + Sync {
    /// Broker identifier (e.g. "zerodha", "upstox", "angelone", "sim").
    fn name(&self) -> &str;

    /// Check current connection and session health.
    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let _ = self;
        Err(BrokerError::InvalidRequest(
            "health_check is not implemented by this broker adapter".into(),
        ))
    }

    /// Place a new order with the broker.
    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError>;

    /// Cancel an active order by broker order ID.
    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError>;

    /// Modify an existing open order with updated price or quantity.
    async fn modify_order(
        &self,
        order_id: &str,
        new_price: Option<Price>,
        new_quantity: Option<Quantity>,
    ) -> Result<OrderAck, BrokerError> {
        let _ = (order_id, new_price, new_quantity);
        Err(BrokerError::InvalidRequest(
            "modify_order is not implemented by this broker adapter".into(),
        ))
    }

    /// Fetch full active order book snapshot from broker.
    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        let _ = self;
        Err(BrokerError::InvalidRequest(
            "fetch_order_book is not implemented by this broker adapter".into(),
        ))
    }

    /// Fetch current Demat and intraday positions.
    async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError> {
        let _ = self;
        Err(BrokerError::InvalidRequest(
            "fetch_positions is not implemented by this broker adapter".into(),
        ))
    }

    /// Fetch real-time available cash and margin balances.
    async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError> {
        let _ = self;
        Err(BrokerError::InvalidRequest(
            "fetch_margins is not implemented by this broker adapter".into(),
        ))
    }

    /// Query the latest order status by broker order ID (polling fallback).
    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError>;
}

/// Optional broker-native iceberg placement (Kite Connect v3 variety
/// `iceberg`).
///
/// Brokers that meter iceberg legs themselves (currently Zerodha) implement
/// this by routing the parent through their native iceberg variety; the leg
/// count is derived from `parent.quantity / display_quantity`. Adapters for
/// venues without a native iceberg route (all but Zerodha today) fall back to
/// the slicer's synthetic iceberg runner, so the default implementation
/// returns [`BrokerError::InvalidRequest`].
#[async_trait]
pub trait IcebergOrderExecutor: Send + Sync {
    /// Place a parent order with broker-native iceberg legging.
    ///
    /// `display_quantity` is the per-leg visible quantity. The broker derives
    /// the leg count as `parent.quantity / display_quantity`, which per the
    /// gateway contract must divide exactly into 2..=50 legs.
    async fn place_iceberg_order(
        &self,
        parent: OrderRequest,
        display_quantity: Quantity,
    ) -> Result<OrderAck, BrokerError> {
        let _ = (parent, display_quantity);
        Err(BrokerError::InvalidRequest(
            "broker-native iceberg placement is not supported by this adapter".into(),
        ))
    }

    /// Derived native leg count for a parent/display pair, validated against
    /// the gateway contract (exact division, 2..=50 legs).
    fn native_iceberg_legs(
        &self,
        parent_quantity: Quantity,
        display_quantity: Quantity,
    ) -> Result<u32, BrokerError> {
        validate_native_legs(parent_quantity, display_quantity)
    }
}

/// A wrapper around any [`BrokerExecutor`] that strictly enforces pre-trade risk checks
/// via [`honba_mon::RiskEngine`] prior to forwarding orders to the broker.
///
/// This fulfills the SEBI mandate requiring a deterministic, stateful risk gate
/// between automated strategy decisions and broker execution.
pub struct RiskGatedExecutor<E: BrokerExecutor> {
    inner: E,
    risk_engine: Arc<Mutex<honba_mon::RiskEngine>>,
}

impl<E: BrokerExecutor> RiskGatedExecutor<E> {
    /// Create a new risk-gated broker executor.
    pub fn new(inner: E, risk_engine: Arc<Mutex<honba_mon::RiskEngine>>) -> Self {
        Self { inner, risk_engine }
    }

    /// Access the underlying broker executor.
    pub fn inner(&self) -> &E {
        &self.inner
    }

    /// Access the shared risk engine.
    pub fn risk_engine(&self) -> &Arc<Mutex<honba_mon::RiskEngine>> {
        &self.risk_engine
    }

    /// Place an order through the mandatory risk gate.
    ///
    /// The order is synchronously evaluated against SEBI OTR limits, circuit price bands,
    /// max notional, daily loss, and platform kill switches. If the risk engine refuses
    /// the order, it is rejected immediately without touching the broker network.
    pub async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let now = Utc::now();

        // 1. Mandatory Pre-Trade Risk Gate Evaluation
        {
            let mut engine = self.risk_engine.lock().await;
            engine.check_order(&req, now).map_err(BrokerError::from)?;
        }

        // 2. Dispatch to Broker
        self.inner.place_order(req).await
    }

    /// Cancel an order and record cancellation in the risk engine for OTR tracking.
    pub async fn cancel_order(&self, order_id: &str, algo_id: &str) -> Result<(), BrokerError> {
        self.inner.cancel_order(order_id).await?;
        let mut engine = self.risk_engine.lock().await;
        engine.record_cancellation(algo_id, Utc::now());
        Ok(())
    }

    /// Notify the risk engine of an execution fill report to update OTR and positions.
    pub async fn on_fill(&self, fill: &FillReport) {
        let mut engine = self.risk_engine.lock().await;
        engine.record_fill(fill);
    }
}

#[derive(Debug, Clone)]
struct MockOrderState {
    request: OrderRequest,
    product: ProductType,
    status: OrderStatus,
    filled_quantity: Quantity,
}

/// Simulated default cash balance: INR 1,000,000.00
fn simulated_cash() -> Money {
    Money::inr(Decimal::from(1_000_000u64))
}

/// A thread-safe mock broker executor for integration testing and deterministic simulation.
#[derive(Debug)]
pub struct MockBrokerExecutor {
    orders: Arc<Mutex<HashMap<String, MockOrderState>>>,
    auto_fill: bool,
    positions: Arc<Mutex<Vec<BrokerPosition>>>,
    margins: Arc<Mutex<BrokerMargins>>,
    health_ok: bool,
}

impl MockBrokerExecutor {
    /// Create a new mock broker executor.
    pub fn new(auto_fill: bool) -> Self {
        Self {
            orders: Arc::new(Mutex::new(HashMap::new())),
            auto_fill,
            positions: Arc::new(Mutex::new(Vec::new())),
            margins: Arc::new(Mutex::new(BrokerMargins {
                available_cash: simulated_cash(),
                used_margin: Money::inr(Decimal::ZERO),
                collateral: Money::inr(Decimal::ZERO),
            })),
            health_ok: true,
        }
    }

    /// Configure the simulated cash/margin balances returned by `fetch_margins`.
    pub fn with_margins(self, margins: BrokerMargins) -> Self {
        let margins = Arc::new(Mutex::new(margins));
        Self { margins, ..self }
    }

    /// Configure the simulated holdings returned by `fetch_positions`.
    pub fn with_positions(self, positions: Vec<BrokerPosition>) -> Self {
        let positions = Arc::new(Mutex::new(positions));
        Self { positions, ..self }
    }

    /// Simulate a broker session failure (health_check reports unhealthy).
    pub fn with_session_down(mut self) -> Self {
        self.health_ok = false;
        self
    }
}

#[async_trait]
impl BrokerExecutor for MockBrokerExecutor {
    fn name(&self) -> &str {
        "mock"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        Ok(BrokerHealth {
            broker: "mock".into(),
            session_valid: self.health_ok,
            authenticated_user: if self.health_ok {
                Some("mock-user".into())
            } else {
                None
            },
            latency: None,
            detail: if self.health_ok {
                "simulated session active".into()
            } else {
                "simulated session down".into()
            },
        })
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let order_id = format!("MOCK-{}", Uuid::new_v4().simple());
        let initial_status = if self.auto_fill {
            OrderStatus::Filled
        } else {
            OrderStatus::New
        };

        let ack = OrderAck {
            order_id: order_id.clone(),
            client_order_id: req.client_order_id,
            exchange_order_id: Some(format!("EX-{order_id}")),
            status: initial_status,
            timestamp: Utc::now(),
            correlation_id: req.correlation_id,
            client_id: req.client_id.clone(),
        };

        let mut lock = self.orders.lock().await;
        let filled_quantity = if self.auto_fill {
            req.quantity
        } else {
            Quantity::ZERO
        };
        lock.insert(
            order_id,
            MockOrderState {
                request: req,
                product: ProductType::CncDelivery,
                status: initial_status,
                filled_quantity,
            },
        );

        Ok(ack)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let mut lock = self.orders.lock().await;
        if let Some(state) = lock.get_mut(order_id) {
            if state.status.is_terminal() {
                return Err(BrokerError::InvalidRequest(format!(
                    "cannot cancel order {} in terminal state {:?}",
                    order_id, state.status
                )));
            }
            state.status = OrderStatus::Cancelled;
            Ok(())
        } else {
            Err(BrokerError::OrderNotFound(order_id.to_string()))
        }
    }

    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        let lock = self.orders.lock().await;
        let mut book = Vec::new();
        for (order_id, state) in lock.iter() {
            let req = &state.request;
            book.push(OrderSnapshot {
                order_id: order_id.clone(),
                client_order_id: Some(req.client_order_id),
                exchange_order_id: Some(format!("EX-{order_id}")),
                instrument: req.instrument.clone(),
                side: req.side,
                order_type: req.order_type,
                quantity: req.quantity,
                filled_quantity: state.filled_quantity,
                price: req.order_type.limit_price(),
                trigger_price: req.order_type.trigger_price(),
                status: state.status,
                product: Some(state.product),
                timestamp: Utc::now(),
            });
        }
        Ok(book)
    }

    async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError> {
        let lock = self.positions.lock().await;
        Ok(lock.clone())
    }

    async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError> {
        let lock = self.margins.lock().await;
        Ok(*lock)
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let lock = self.orders.lock().await;
        if let Some(state) = lock.get(order_id) {
            Ok(state.status)
        } else {
            Err(BrokerError::OrderNotFound(order_id.to_string()))
        }
    }
}
