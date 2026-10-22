//! Broker brokerage cost models for the cost-optimized routing policy
//! (plan §3.5).
//!
//! Indian transaction costs differ by broker class: zero-brokerage accounts
//! for equity delivery (`CncDelivery`), flat-fee direct-access brokers for
//! high-frequency intraday futures/options, and classic percentage brokerage.
//! The router ranks candidates by modeled order cost in exact `Decimal`
//! arithmetic.

use rust_decimal::Decimal;

use honba_te_core::ProductType;

/// Static brokerage model of one registered broker account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerCostModel {
    /// Zero brokerage on equity delivery; non-delivery products ride at
    /// `intraday_bps` of the order notional.
    ZeroDelivery {
        /// Basis points per rupee of notional for intraday/derivative legs.
        intraday_bps: Decimal,
    },
    /// Flat fee per order regardless of notional (direct-access brokers).
    FlatFee {
        /// Per-order brokerage in INR.
        fee: Decimal,
    },
    /// Classic percentage brokerage: `bps` of notional for every product.
    PercentOfNotional {
        /// Basis points per rupee of notional.
        bps: Decimal,
    },
}

impl BrokerCostModel {
    /// Modeled brokerage for one order.
    ///
    /// `notional` is the parent notional (`limit price × quantity`); market
    /// orders carry no known notional (`0`), where percentage models
    /// degenerate to zero-cost and flat-fee models remain accurate.
    pub fn order_cost(&self, product: ProductType, notional: Decimal) -> Decimal {
        match self {
            Self::ZeroDelivery { intraday_bps } => {
                if matches!(product, ProductType::CncDelivery) {
                    Decimal::ZERO
                } else {
                    notional * intraday_bps / Decimal::from(10_000)
                }
            }
            Self::FlatFee { fee } => *fee,
            Self::PercentOfNotional { bps } => notional * bps / Decimal::from(10_000),
        }
    }
}
