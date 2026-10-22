//! Exact decimal wire-format helpers.
//!
//! SEBI/exchange gateways reject malformed prices and quantities. Broker wire
//! payloads must therefore carry exact decimal strings with an explicit scale,
//! never floating-point conversions. These helpers format
//! [`rust_decimal::Decimal`] values losslessly and reject values that cannot
//! be represented exactly in the target precision.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::error::BrokerError;

/// Format an exact decimal with a fixed number of fractional digits.
///
/// Trailing zeros are added to reach exactly `scale` fractional digits, so
/// wire payloads are deterministic across adapters (e.g. `2500.5 -> "2500.50"`).
/// Rounding is forbidden: if the value cannot be represented exactly at
/// `scale` fractional digits (e.g. tick misalignment), an error is returned so
/// upstream slicing / tick validation can reject the malformed order instead
/// of silently sending a different price to the exchange.
pub fn format_fixed_exact(value: Decimal, scale: u32) -> Result<String, BrokerError> {
    // normalize() strips trailing zeros; any remaining fractional precision
    // beyond the requested scale means the value is NOT exactly representable.
    let normalized = value.normalize();
    if normalized.scale() > scale {
        return Err(BrokerError::InvalidRequest(format!(
            "value {value} is not exactly representable at scale {scale} (tick misalignment?)"
        )));
    }
    // With exactness verified, rescaling only pads trailing zero decimals —
    // the wire string always carries exactly `scale` fractional digits.
    let mut scaled = value;
    scaled.rescale(scale);
    Ok(scaled.to_string())
}

/// Convert an exact decimal quantity into an unsigned 32-bit integer
/// without losing precision (exchange quantities are share/lot counts).
pub fn quantity_to_u32_exact(quantity: Decimal) -> Result<u32, BrokerError> {
    if quantity <= Decimal::ZERO {
        return Err(BrokerError::InvalidRequest(format!(
            "quantity must be positive, got {quantity}"
        )));
    }
    let integral = quantity.trunc();
    if integral != quantity {
        return Err(BrokerError::InvalidRequest(format!(
            "quantity {quantity} is not an integral share/lot count"
        )));
    }
    integral
        .to_u32()
        .ok_or_else(|| BrokerError::InvalidRequest(format!("quantity {quantity} overflows u32")))
}
