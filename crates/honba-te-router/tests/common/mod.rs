//! Shared scripted mocks for router v2 tests.
//!
//! Each test binary compiles its own copy of this module, so items used by
//! only some binaries would trip dead-code warnings there.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, Price, Quantity};
use honba_te_core::{BrokerError, BrokerExecutor};

/// Scripted per-call outcomes replayed in order.
#[derive(Debug, Clone, Copy)]
pub enum Script {
    /// Success (finite simulated latency when the mock carries a delay).
    Ok,
    /// Broker/exchange business rejection (gateway reached).
    Reject,
    /// Network/transport failure (gateway NOT reached).
    Transport,
    /// Request timeout (failover-eligible).
    Timeout,
}

/// In-memory broker executor with scripted outcomes and a configurable
/// simulated latency applied to successful placement paths.
pub struct MockBroker {
    name: String,
    behaviors: Mutex<VecDeque<Script>>,
    latency: Duration,
    seq: Mutex<u64>,
    pub placements: Arc<Mutex<Vec<OrderRequest>>>,
    pub modifications: Arc<Mutex<Vec<String>>>,
    pub cancellations: Arc<Mutex<Vec<String>>>,
}

impl MockBroker {
    /// Broker that always places successfully.
    pub fn healthy(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::named(name))
    }

    fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            behaviors: Mutex::new(VecDeque::new()),
            latency: Duration::ZERO,
            seq: Mutex::new(0),
            placements: Arc::new(Mutex::new(Vec::new())),
            modifications: Arc::new(Mutex::new(Vec::new())),
            cancellations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Owned builder (for custom chains).
    pub fn new(name: impl Into<String>) -> Self {
        Self::named(name)
    }

    /// Queue a scripted outcome for the next place_order call.
    pub fn push(self, script: Script) -> Self {
        self.behaviors.lock().unwrap().push_back(script);
        self
    }

    /// Simulated wall-clock latency of each successful placement (drives
    /// the rolling-RTT breaker through the real router path).
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    pub fn placed(&self) -> Vec<OrderRequest> {
        self.placements.lock().unwrap().clone()
    }

    pub fn modified(&self) -> Vec<String> {
        self.modifications.lock().unwrap().clone()
    }

    pub fn cancelled(&self) -> Vec<String> {
        self.cancellations.lock().unwrap().clone()
    }
}

#[async_trait]
impl BrokerExecutor for MockBroker {
    fn name(&self) -> &str {
        &self.name
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let script = self
            .behaviors
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Script::Ok);
        match script {
            Script::Reject => return Err(BrokerError::Rejected("RMS margin shortfall".into())),
            Script::Transport => return Err(BrokerError::Transport("gateway dial timeout".into())),
            Script::Timeout => return Err(BrokerError::Timeout),
            Script::Ok => {}
        }
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        let order_id = format!("{}-{}", self.name, *seq);
        self.placements.lock().unwrap().push(req.clone());
        Ok(OrderAck {
            order_id,
            client_order_id: req.client_order_id,
            exchange_order_id: None,
            status: OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: req.correlation_id,
            client_id: req.client_id.clone(),
            trace_context: None,
        })
    }

    async fn modify_order(
        &self,
        order_id: &str,
        _new_price: Option<Price>,
        _new_quantity: Option<Quantity>,
    ) -> Result<OrderAck, BrokerError> {
        self.modifications
            .lock()
            .unwrap()
            .push(order_id.to_string());
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        Ok(OrderAck {
            order_id: format!("MOD-{}-{}", self.name, *seq),
            client_order_id: Uuid::nil(),
            exchange_order_id: None,
            status: OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: None,
            client_id: None,
            trace_context: None,
        })
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        self.cancellations
            .lock()
            .unwrap()
            .push(order_id.to_string());
        Ok(())
    }

    async fn get_order_status(&self, _order_id: &str) -> Result<OrderStatus, BrokerError> {
        Ok(OrderStatus::New)
    }
}

/// Canonical delivery (CNC) notional-aware parent: 100 shares @ 500.00.
pub fn delivery_request(qty: u64) -> OrderRequest {
    OrderRequest::new(
        honba_core::InstrumentId::new(honba_core::Exchange::Nse, "TATASTEEL"),
        OrderSide::Buy,
        honba_core::OrderType::Limit(Price::from_rupees("500.00").unwrap()),
        Quantity::from_u64(qty),
        "algo-m4",
    )
}

/// Canonical intraday parent: same economics, MIS product semantics.
pub fn intraday_request(qty: u64) -> OrderRequest {
    let mut req = delivery_request(qty);
    req.algo_id = "algo-m4-intraday".into();
    req
}
