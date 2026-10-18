//! # 本場・手 honba-te-core
//!
//! Canonical broker execution interfaces, error taxonomy, and deterministic pre-trade risk gating.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub use honba_core::{
    Exchange, FillReport, InstrumentId, Money, OrderAck, OrderRequest, OrderSide, OrderStatus,
    OrderType, Price, PriceBand, Quantity, TimeInForce,
};
pub use honba_mon::{RiskEngine, RiskRefusal};

/// Errors encountered during order execution across broker adapters.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Authentication or session token failure.
    #[error("auth failure: {0}")]
    Auth(String),

    /// Order rejected by broker or exchange.
    #[error("order rejected by broker: {0}")]
    Rejected(String),

    /// Order rejected by the synchronous pre-trade risk gate.
    #[error("pre-trade risk refusal: {0}")]
    RiskRefusal(#[from] honba_mon::RiskRefusal),

    /// Network or transport error communicating with broker API.
    #[error("network or transport error: {0}")]
    Transport(String),

    /// Operation timed out.
    #[error("broker request timeout")]
    Timeout,

    /// Order ID not found at broker.
    #[error("order not found: {0}")]
    OrderNotFound(String),

    /// Invalid request or parameters.
    #[error("invalid request or parameters: {0}")]
    InvalidRequest(String),
}

/// Unified trait implemented by broker execution adapters (Zerodha, Upstox, Angel One, Paper/Sim).
#[async_trait]
pub trait BrokerExecutor: Send + Sync {
    /// Broker identification name (e.g., "zerodha", "upstox", "angelone", "sim").
    fn name(&self) -> &str;

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

    /// Query the latest order status by broker order ID.
    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError>;
}

/// A wrapper around any [`BrokerExecutor`] that strictly enforces pre-trade risk checks
/// via [`RiskEngine`] prior to forwarding orders to the broker.
///
/// This fulfills the SEBI mandate requiring a deterministic, stateful risk gate
/// between automated strategy decisions and broker execution.
pub struct RiskGatedExecutor<E: BrokerExecutor> {
    inner: E,
    risk_engine: Arc<Mutex<RiskEngine>>,
}

impl<E: BrokerExecutor> RiskGatedExecutor<E> {
    /// Create a new risk-gated broker executor.
    pub fn new(inner: E, risk_engine: Arc<Mutex<RiskEngine>>) -> Self {
        Self { inner, risk_engine }
    }

    /// Access the underlying broker executor.
    pub fn inner(&self) -> &E {
        &self.inner
    }

    /// Access the shared risk engine.
    pub fn risk_engine(&self) -> &Arc<Mutex<RiskEngine>> {
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
            engine
                .check_order(&req, now)
                .map_err(BrokerError::RiskRefusal)?;
        }

        // 2. Dispatch to Broker
        self.inner.place_order(req).await
    }

    /// Cancel an order and record cancellation in the risk engine for OTR tracking.
    pub async fn cancel_order(&self, order_id: &str, algo_id: &str) -> Result<(), BrokerError> {
        self.inner.cancel_order(order_id).await?;
        let mut engine = self.risk_engine.lock().await;
        engine.record_cancellation(algo_id);
        Ok(())
    }

    /// Notify the risk engine of an execution fill report to update OTR and positions.
    pub async fn on_fill(&self, fill: &FillReport) {
        let mut engine = self.risk_engine.lock().await;
        engine.record_fill(fill);
    }
}

/// A thread-safe mock broker executor for integration testing and deterministic simulation.
#[derive(Debug, Default)]
pub struct MockBrokerExecutor {
    orders: Arc<Mutex<HashMap<String, (OrderRequest, OrderStatus)>>>,
    auto_fill: bool,
}

impl MockBrokerExecutor {
    /// Create a new mock broker executor.
    pub fn new(auto_fill: bool) -> Self {
        Self {
            orders: Arc::new(Mutex::new(HashMap::new())),
            auto_fill,
        }
    }
}

#[async_trait]
impl BrokerExecutor for MockBrokerExecutor {
    fn name(&self) -> &str {
        "mock"
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
            exchange_order_id: Some(format!("EX-{}", order_id)),
            status: initial_status,
            timestamp: Utc::now(),
        };

        let mut lock = self.orders.lock().await;
        lock.insert(order_id, (req, initial_status));

        Ok(ack)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let mut lock = self.orders.lock().await;
        if let Some((_, status)) = lock.get_mut(order_id) {
            if status.is_terminal() {
                return Err(BrokerError::InvalidRequest(format!(
                    "cannot cancel order {} in terminal state {:?}",
                    order_id, status
                )));
            }
            *status = OrderStatus::Cancelled;
            Ok(())
        } else {
            Err(BrokerError::OrderNotFound(order_id.to_string()))
        }
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let lock = self.orders.lock().await;
        if let Some((_, status)) = lock.get(order_id) {
            Ok(*status)
        } else {
            Err(BrokerError::OrderNotFound(order_id.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_risk_gated_executor_blocks_rejected_orders() {
        let mock = MockBrokerExecutor::new(false);
        let risk_engine = Arc::new(Mutex::new(RiskEngine::default()));
        let gated = RiskGatedExecutor::new(mock, risk_engine.clone());

        let inst = InstrumentId::new(Exchange::Nse, "TCS");
        let valid_req = OrderRequest::new(
            inst.clone(),
            OrderSide::Buy,
            OrderType::Market,
            Quantity::from_u64(10),
            "algo-approved",
        );

        // 1. Order passes
        let ack = gated.place_order(valid_req).await.unwrap();
        assert_eq!(ack.status, OrderStatus::New);

        // 2. Trip global halt
        {
            let mut engine = risk_engine.lock().await;
            engine.halt_global("Manual kill-switch", Utc::now());
        }

        // 3. New order immediately refused without dispatching to broker
        let req2 = OrderRequest::new(
            inst,
            OrderSide::Buy,
            OrderType::Market,
            Quantity::from_u64(10),
            "algo-approved",
        );
        let err = gated.place_order(req2).await.unwrap_err();
        assert!(matches!(
            err,
            BrokerError::RiskRefusal(RiskRefusal::GlobalHalted { .. })
        ));
    }
}
