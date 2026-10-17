//! Zerodha execution adapter.
use async_trait::async_trait;
use honba_te_core::{BrokerError, BrokerExecutor, OrderAck, OrderRequest};

pub struct ZerodhaExecutor {
    pub api_key: String,
    pub access_token: String,
}

impl ZerodhaExecutor {
    pub fn new(k: impl Into<String>, t: impl Into<String>) -> Self {
        Self { api_key: k.into(), access_token: t.into() }
    }
}

#[async_trait]
impl BrokerExecutor for ZerodhaExecutor {
    async fn place_order(&self, _req: OrderRequest) -> Result<OrderAck, BrokerError> {
        Err(BrokerError::Transport("not implemented".into()))
    }
    async fn cancel_order(&self, _oid: &str) -> Result<(), BrokerError> {
        Err(BrokerError::Transport("not implemented".into()))
    }
}
