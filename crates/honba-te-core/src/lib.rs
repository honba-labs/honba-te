//! Broker execution trait.
use async_trait::async_trait;
use honba_kokoro_types::{Exchange, OrderSide, OrderType, Quantity, TimeInForce};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: String,
    pub exchange: Exchange,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: Quantity,
    pub tif: TimeInForce,
    pub algo_id: String,
}

#[derive(Debug, Clone)]
pub struct OrderAck {
    pub order_id: String,
    pub correlation_id: Uuid,
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("auth")]
    Auth,
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("transport: {0}")]
    Transport(String),
}

#[async_trait]
pub trait BrokerExecutor: Send + Sync {
    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError>;
    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError>;
}
