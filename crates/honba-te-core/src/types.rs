//! Canonical execution-domain value types: Indian product taxonomy and
//! broker-facing order/position/margin/health snapshots.
//!
//! All monetary and quantity fields use [`rust_decimal::Decimal`] backed
//! canonical types; wire serialization explicitly avoids floating-point.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use honba_core::{InstrumentId, Money, OrderSide, OrderStatus, OrderType, Price, Quantity};

/// Canonical Indian product taxonomy mapped to broker-specific wire tokens.
///
/// SEBI/exchange margining rules vary per product: customers trade equity
/// delivery (`CncDelivery`), margin intraday (`MisIntraday`), F&O overnight
/// margin (`NrmlDerivative`) or Margin Trading Facility (`MtfMargin`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProductType {
    /// Cash & Carry / Demat Delivery (Zerodha: CNC, Upstox: D, Angel: DELIVERY)
    CncDelivery,
    /// Margin Intraday Square-off (Zerodha: MIS, Upstox: I, Angel: INTRADAY)
    MisIntraday,
    /// Normal F&O Overnight Margin (Zerodha: NRML, Upstox: M, Angel: CARRYFORWARD)
    NrmlDerivative,
    /// Margin Trading Facility (Zerodha: MTF, Upstox: MTF, Angel: MTF)
    MtfMargin,
}

impl ProductType {
    /// Zerodha Kite Connect v3 product code.
    pub fn zerodha(self) -> &'static str {
        match self {
            Self::CncDelivery => "CNC",
            Self::MisIntraday => "MIS",
            Self::NrmlDerivative => "NRML",
            Self::MtfMargin => "MTF",
        }
    }

    /// Upstox v2 product code.
    pub fn upstox(self) -> &'static str {
        match self {
            Self::CncDelivery => "D",
            Self::MisIntraday => "I",
            Self::NrmlDerivative => "M",
            Self::MtfMargin => "MTF",
        }
    }

    /// Angel One SmartAPI product code.
    pub fn angelone(self) -> &'static str {
        match self {
            Self::CncDelivery => "DELIVERY",
            Self::MisIntraday => "INTRADAY",
            Self::NrmlDerivative => "CARRYFORWARD",
            Self::MtfMargin => "MTF",
        }
    }

    /// Broker token for the given adapter name ("zerodha", "upstox", "angelone").
    pub fn broker_token(self, broker: &str) -> Option<&'static str> {
        match broker {
            "zerodha" => Some(self.zerodha()),
            "upstox" => Some(self.upstox()),
            "angelone" => Some(self.angelone()),
            _ => None,
        }
    }

    /// Parse a Zerodha product token back into the canonical taxonomy.
    pub fn from_zerodha(token: &str) -> Option<Self> {
        match token {
            "CNC" => Some(Self::CncDelivery),
            "MIS" => Some(Self::MisIntraday),
            "NRML" => Some(Self::NrmlDerivative),
            "MTF" => Some(Self::MtfMargin),
            _ => None,
        }
    }

    /// Parse an Upstox product token back into the canonical taxonomy.
    pub fn from_upstox(token: &str) -> Option<Self> {
        match token {
            "D" => Some(Self::CncDelivery),
            "I" => Some(Self::MisIntraday),
            "M" => Some(Self::NrmlDerivative),
            "MTF" => Some(Self::MtfMargin),
            _ => None,
        }
    }

    /// Parse an Angel One product token back into the canonical taxonomy.
    pub fn from_angelone(token: &str) -> Option<Self> {
        match token {
            "DELIVERY" => Some(Self::CncDelivery),
            "INTRADAY" => Some(Self::MisIntraday),
            "CARRYFORWARD" => Some(Self::NrmlDerivative),
            "MTF" => Some(Self::MtfMargin),
            _ => None,
        }
    }
}

/// Direction of a net broker position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    /// Net long (bought above sold).
    Long,
    /// Net short (sold above bought, e.g. intraday/F&O short).
    Short,
    /// Square-off: no open exposure.
    Flat,
}

/// Current connection and session health of a broker adapter.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BrokerHealth {
    /// Broker identifier
    pub broker: String,
    /// Whether the broker session is live and authenticated
    pub session_valid: bool,
    /// Authenticated user / client code (when known)
    pub authenticated_user: Option<String>,
    /// Round-trip latency observed during the health probe
    pub latency: Option<Decimal>,
    /// Human-readable detail (e.g. last error, session expiry)
    pub detail: String,
}

impl BrokerHealth {
    /// A healthy broker session with no authenticated user recorded.
    pub fn session_ok(broker: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            session_valid: true,
            authenticated_user: None,
            latency: None,
            detail: detail.into(),
        }
    }
}

/// Point-in-time snapshot of a single order resting at the broker
/// (from the broker order book / pending orders API).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OrderSnapshot {
    /// Broker-assigned order ID
    pub order_id: String,
    /// Client correlation ID (when reported by broker)
    pub client_order_id: Option<Uuid>,
    /// Exchange-assigned order ID (when reported by broker)
    pub exchange_order_id: Option<String>,
    /// Traded instrument
    pub instrument: InstrumentId,
    /// Order direction
    pub side: OrderSide,
    /// Pricing instructions
    pub order_type: OrderType,
    /// Original order quantity
    pub quantity: Quantity,
    /// Quantity filled so far
    pub filled_quantity: Quantity,
    /// Limit price (None for market orders)
    pub price: Option<Price>,
    /// Trigger price (None for non-stop orders)
    pub trigger_price: Option<Price>,
    /// Lifecycle status
    pub status: OrderStatus,
    /// Indian product taxonomy
    pub product: Option<ProductType>,
    /// Snapshot timestamp
    pub timestamp: DateTime<Utc>,
}

/// A net position as reported by the broker (Demat + intraday combined view).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BrokerPosition {
    /// Traded instrument
    pub instrument: InstrumentId,
    /// Indian product taxonomy
    pub product: ProductType,
    /// Direction of the net exposure
    pub side: PositionSide,
    /// Unsigned net quantity
    pub quantity: Quantity,
    /// Weighted average entry price
    pub average_price: Price,
    /// Realised P&L for the day
    pub realized_pnl: Option<Money>,
    /// Latest market price, when known
    pub last_price: Option<Price>,
}

impl BrokerPosition {
    /// Net open exposure in INR (average price x quantity).
    pub fn notional(&self) -> Money {
        Money::notional(self.average_price, self.quantity)
    }
}

/// Real-time available cash and margin balances reported by the broker.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BrokerMargins {
    /// Free cash available for new orders
    pub available_cash: Money,
    /// Margin currently used by open positions/orders
    pub used_margin: Money,
    /// Collateral (Demat pledge + F&O margainable stock)
    pub collateral: Money,
}
