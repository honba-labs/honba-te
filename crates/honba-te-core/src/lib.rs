//! # 本場・手 honba-te-core
//!
//! Canonical broker execution interfaces for the 本場・手 (honba-te) subsystem:
//! exact-decimal domain types, the `BrokerExecutor` trait v2, push-based
//! [`ExecutionStream`], the canonical Indian [`ProductType`] taxonomy, and the
//! prioritized [`TokenBucketRateLimiter`] queue used to shield every broker
//! adapter from HTTP 429 bans.
//!
//! # Module map
//! - [`types`] — [`ProductType`], broker health, order snapshots, positions, margins
//! - [`executor`] — [`BrokerExecutor`] trait v2, [`RiskGatedExecutor`], mock adapter
//! - [`stream`] — [`ExecutionStream`] push trait + channel transport
//! - [`ratelimit`] — prioritized token bucket + [`RateLimitedBroker`] wrapper
//! - [`decimal`] — exact decimal wire formatting (no floats, ever)
//! - [`error`] — [`BrokerError`] taxonomy

pub mod decimal;
pub mod error;
pub mod executor;
pub mod ratelimit;
pub mod stream;
pub mod types;

pub use decimal::{format_fixed_exact, quantity_to_u32_exact};
pub use error::BrokerError;
pub use executor::{BrokerExecutor, MockBrokerExecutor, RiskGatedExecutor};
pub use ratelimit::{
    BrokerRateCatalog, RateLimitedBroker, RateLimiterConfig, RequestPriority,
    TokenBucketRateLimiter,
};
pub use stream::{
    BrokerExecutionEvent, ChannelExecutionStream, ExecutionEventSink, ExecutionStream,
};
pub use types::{
    BrokerHealth, BrokerMargins, BrokerPosition, OrderSnapshot, PositionSide, ProductType,
};

// Canonical domain re-exports (unified strictly on honba-core).
pub use honba_core::{
    Exchange, FillReport, InstrumentId, Money, OrderAck, OrderRequest, OrderSide, OrderStatus,
    OrderType, Price, PriceBand, Quantity, TimeInForce,
};
pub use honba_mon::{RiskEngine, RiskRefusal};
