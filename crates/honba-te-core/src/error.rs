//! Broker execution error taxonomy.

use thiserror::Error;

/// Errors encountered during order execution across broker adapters.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Authentication or session token failure.
    #[error("auth failure: {0}")]
    Auth(String),

    /// Order rejected by broker or exchange.
    #[error("order rejected by broker: {0}")]
    Rejected(String),

    /// Order rejected by the synchronous pre-trade risk gate. Boxed because
    /// `honba_mon::RiskRefusal` is a rich error record that would otherwise
    /// balloon the size of every `Result<_, BrokerError>` return.
    #[error("pre-trade risk refusal: {0}")]
    RiskRefusal(Box<honba_mon::RiskRefusal>),

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

    /// Local token-bucket rate limiter refused or exhausted the request.
    #[error("rate limiter exhausted: {0}")]
    RateLimited(String),
}

impl From<honba_mon::RiskRefusal> for BrokerError {
    fn from(value: honba_mon::RiskRefusal) -> Self {
        Self::RiskRefusal(Box::new(value))
    }
}
