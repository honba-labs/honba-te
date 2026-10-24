//! Shared scripted mocks for slicer orchestrator tests.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use honba_core::{Exchange, FillReport, InstrumentId, Money, OrderRequest, OrderSide, OrderType, Price};
use honba_te_core::{BrokerError, BrokerExecutionEvent, BrokerExecutor, ExecutionStream, Quantity};

/// Broker executor replaying a script of order IDs and optional failure
/// points; records every placed child for assertions.
#[derive(Debug, Default)]
pub struct ScriptedBroker {
    pub placed: Arc<Mutex<Vec<OrderRequest>>>,
    order_ids: Arc<Mutex<VecDeque<String>>>,
    pub cancelled: Arc<Mutex<Vec<String>>>,
    fail_at: Arc<Mutex<Option<usize>>>,
}

impl ScriptedBroker {
    pub fn new(order_ids: Vec<String>) -> Self {
        Self {
            placed: Arc::new(Mutex::new(Vec::new())),
            cancelled: Arc::new(Mutex::new(Vec::new())),
            order_ids: Arc::new(Mutex::new(VecDeque::from(order_ids))),
            fail_at: Arc::new(Mutex::new(None)),
        }
    }

    /// Fail placement of the Nth child (zero-based index).
    pub fn fail_at(self, zero_based: usize) -> Self {
        *self.fail_at.lock().unwrap() = Some(zero_based);
        self
    }

    pub fn placed(&self) -> Vec<OrderRequest> {
        self.placed.lock().unwrap().clone()
    }

    pub fn cancelled(&self) -> Vec<String> {
        self.cancelled.lock().unwrap().clone()
    }
}

#[async_trait]
impl BrokerExecutor for ScriptedBroker {
    fn name(&self) -> &str {
        "scripted"
    }

    async fn place_order(&self, req: OrderRequest) -> Result<honba_core::OrderAck, BrokerError> {
        let mut placed = self.placed.lock().unwrap();
        let index = placed.len();
        if let Some(fail) = *self.fail_at.lock().unwrap()
            && index == fail
        {
            return Err(BrokerError::Rejected(format!("scripted failure #{index}")));
        }
        let order_id = self
            .order_ids
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| format!("SCR-{index}"));
        Ok(honba_core::OrderAck {
            order_id,
            client_order_id: req.client_order_id,
            exchange_order_id: None,
            status: honba_core::OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: req.correlation_id,
            client_id: req.client_id.clone(),
            trace_context: None,
        })
        .inspect(|_| placed.push(req))
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        self.cancelled.lock().unwrap().push(order_id.to_string());
        Ok(())
    }

    async fn get_order_status(
        &self,
        _order_id: &str,
    ) -> Result<honba_core::OrderStatus, BrokerError> {
        Ok(honba_core::OrderStatus::New)
    }
}

/// Deterministic in-memory [`ExecutionStream`]; drained → optionally hang
/// (simulating silence, so runner timeout paths fire) or Transport error.
#[derive(Debug)]
pub struct ScriptedStream {
    events: Arc<Mutex<VecDeque<BrokerExecutionEvent>>>,
    hang_when_drained: bool,
}

impl ScriptedStream {
    pub fn new(events: Vec<BrokerExecutionEvent>) -> Self {
        Self {
            events: Arc::new(Mutex::new(VecDeque::from(events))),
            hang_when_drained: false,
        }
    }

    /// Drained stream waits forever instead of erroring — used to exercise
    /// runner timeout/abandonment paths predicated on a silent wire.
    pub fn hanging(mut self) -> Self {
        self.hang_when_drained = true;
        self
    }
}

#[async_trait]
impl ExecutionStream for ScriptedStream {
    async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError> {
        let popped = self.events.lock().unwrap().pop_front();
        match popped {
            Some(event) => Ok(event),
            None if self.hang_when_drained => std::future::pending().await,
            None => Err(BrokerError::Transport("scripted stream drained".into())),
        }
    }
}

/// Canonical parent request for slicing scenarios (N shares @ 100.00).
pub fn parent_request(qty: u64) -> OrderRequest {
    OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "RELIANCE"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("100.00").unwrap()),
        Quantity::from_u64(qty),
        "algo-m3",
    )
}

/// Canonical fill event for `order_id` with the given delta quantity.
pub fn fill_event(order_id: &str, quantity: u64) -> BrokerExecutionEvent {
    BrokerExecutionEvent::Fill(FillReport {
        fill_id: format!("{order_id}:F"),
        order_id: order_id.to_string(),
        client_order_id: uuid::Uuid::nil(),
        instrument: InstrumentId::new(Exchange::Nse, "RELIANCE"),
        side: OrderSide::Buy,
        price: Price::from_rupees("100.00").unwrap(),
        quantity: Quantity::from_u64(quantity),
        algo_id: "algo-m3".into(),
        timestamp: Utc::now(),
        correlation_id: None,
        client_id: None,
        fee: Money::zero_inr(),
        fee_breakdown: None,
        trace_context: None,
    })
}

/// Canonical ack event for a scripted order id.
pub fn ack_event(order_id: &str) -> BrokerExecutionEvent {
    BrokerExecutionEvent::OrderAck(honba_core::OrderAck {
        order_id: order_id.to_string(),
        client_order_id: Uuid::nil(),
        exchange_order_id: None,
        status: honba_core::OrderStatus::New,
        timestamp: Utc::now(),
        correlation_id: None,
        client_id: None,
        trace_context: None,
    })
}

/// Rejected event helper.
pub fn rejected_event(order_id: &str, reason: &str) -> BrokerExecutionEvent {
    BrokerExecutionEvent::OrderRejected {
        order_id: Some(order_id.to_string()),
        reason: reason.to_string(),
    }
}

/// Cancelled event helper.
pub fn cancelled_event(order_id: &str) -> BrokerExecutionEvent {
    BrokerExecutionEvent::OrderCancelled {
        order_id: order_id.to_string(),
        reason: None,
    }
}
