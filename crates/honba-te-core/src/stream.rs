//! Push-based execution event streaming.
//!
//! Order lifecycles must be driven primarily by real-time push streams
//! (Kite Connect postbacks / ticker order updates, Upstox v2 WebSocket order
//! feed, Angel One SmartStream), never by status polling alone. This module
//! defines the canonical [`ExecutionStream`] interface and the
//! [`BrokerExecutionEvent`] taxonomy shared by all broker transport adapters.

use async_trait::async_trait;
use honba_core::OrderAck;
use tokio::sync::mpsc;

use crate::error::BrokerError;

/// Asynchronous real-time stream of order updates and trade fills pushed by broker.
#[async_trait]
pub trait ExecutionStream: Send + Sync {
    /// Subscribe to the broker's real-time order update channel.
    ///
    /// The future resolves with the next event in broker-arrival order.
    /// Transport-level reconnect handling is owned by the concrete adapter;
    /// on unrecoverable stream failure the method returns a [`BrokerError`].
    async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError>;
}

/// Real-time order lifecycle event pushed by a broker transport.
#[derive(Debug, Clone)]
pub enum BrokerExecutionEvent {
    /// Broker accepted the order (acknowledgement).
    OrderAck(OrderAck),
    /// Partial or complete trade execution.
    Fill(honba_core::FillReport),
    /// Order cancelled on exchange (user, algo, or square-off).
    OrderCancelled {
        /// Broker order ID
        order_id: String,
        /// Optional broker-provided cancellation reason
        reason: Option<String>,
    },
    /// Order rejected by broker or exchange.
    OrderRejected {
        /// Broker order ID (may be absent pre-assignment, hence optional here)
        order_id: Option<String>,
        /// Rejection reason from broker
        reason: String,
    },
}

impl BrokerExecutionEvent {
    /// Broker order id associated with the event, when present.
    pub fn order_id(&self) -> Option<&str> {
        match self {
            Self::OrderAck(ack) => Some(&ack.order_id),
            Self::Fill(fill) => Some(&fill.order_id),
            Self::OrderCancelled { order_id, .. } => Some(order_id),
            Self::OrderRejected { order_id, .. } => order_id.as_deref(),
        }
    }
}

/// Sender half of a stream channel: transport adapters (Axum postback webhook
/// handlers, WebSocket readers) push canonical events through this sink, which
/// the OMS consumes via [`ChannelExecutionStream`].
#[derive(Debug, Clone)]
pub struct ExecutionEventSink {
    tx: mpsc::Sender<BrokerExecutionEvent>,
}

impl ExecutionEventSink {
    /// Push one canonical execution event into the stream.
    ///
    /// Fails with [`BrokerError::Transport`] when the consumer side is dropped.
    pub async fn send(&self, event: BrokerExecutionEvent) -> Result<(), BrokerError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| BrokerError::Transport("execution stream consumer dropped".into()))
    }

    /// Non-blocking push for synchronous webhook handlers.
    pub fn try_send(&self, event: BrokerExecutionEvent) -> Result<(), BrokerError> {
        self.tx
            .try_send(event)
            .map_err(|_| BrokerError::Transport("execution stream full or consumer dropped".into()))
    }
}

/// mpsc-backed [`ExecutionStream`] used by postback/webhook listeners and
/// order-feed WebSocket readers to feed ingestion-facing consumers.
#[derive(Debug)]
pub struct ChannelExecutionStream {
    rx: mpsc::Receiver<BrokerExecutionEvent>,
}

impl ChannelExecutionStream {
    /// Create a bounded (capacity) channel pair.
    pub fn channel(capacity: usize) -> (Self, ExecutionEventSink) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (Self { rx }, ExecutionEventSink { tx })
    }
}

#[async_trait]
impl ExecutionStream for ChannelExecutionStream {
    async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| BrokerError::Transport("execution stream closed".into()))
    }
}
