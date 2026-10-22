//! Broker wire-format decoding helpers shared by every execution adapter.
//!
//! Brokers serialize prices both ways: as JSON numbers with float semantics
//! (`average_price: 470.25`) and as strings (`"19500.00"`). This module
//! extracts exact [`Decimal`] values from either encoding **without any
//! floating-point arithmetic reaching the domain layer**: integral numbers
//! convert exactly, and fractional numbers round-trip through serde_json's
//! shortest decimal representation, which is the same text the broker emitted.
//!
//! Indian exchanges report timestamps in exchange-local IST; the helpers here
//! normalize them to UTC `DateTime` values.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::Value;

use honba_core::{Price, Quantity};

/// Fixed Indian Standard Time offset (+05:30) in seconds.
pub const IST_OFFSET_SECONDS: i32 = 5 * 3600 + 1800;

/// Extract an exact [`Decimal`] from a JSON value.
///
/// Accepts integral/decimal JSON numbers and numeric strings; `null`, booleans
/// and other types yield `None`.
pub fn decimal_from_json(value: &Value) -> Option<Decimal> {
    match value {
        Value::Number(n) => decimal_from_number(n),
        Value::String(s) => s.trim().parse::<Decimal>().ok(),
        _ => None,
    }
}

/// Extract an exact [`Decimal`] from a serde_json number token.
///
/// Integers convert exactly. Fractional numbers are decoded from serde_json's
/// Display textual form (shortest representation that round-trips), so no IEEE
/// artifact ever becomes a price; e.g. the wire text `470.25` yields exactly
/// `470.25` (scale 2).
pub fn decimal_from_number(number: &serde_json::Number) -> Option<Decimal> {
    if let Some(i) = number.as_i64() {
        return Some(Decimal::from(i));
    }
    if let Some(u) = number.as_u64() {
        return Some(Decimal::from(u));
    }
    number.to_string().parse::<Decimal>().ok()
}

/// Extract a [`Price`] from a JSON value (see [`decimal_from_json`]).
pub fn price_from_json(value: &Value) -> Option<Price> {
    decimal_from_json(value).map(Price::new)
}

/// Extract a [`Quantity`] from a JSON value (see [`decimal_from_json`]).
pub fn quantity_from_json(value: &Value) -> Option<Quantity> {
    decimal_from_json(value).map(Quantity::new)
}

/// Parse a broker-reported timestamp into UTC, treating naive wall-clock text
/// as Indian Standard Time (+05:30), as Indian exchanges do.
///
/// Recognized formats:
/// - RFC 3339 with explicit offset: `2024-02-21T09:10:02Z`, `2024-02-21T14:40:02+05:30`
/// - Exchange naive datetimes (IST): `2024-02-21 14:40:02`, `2024-02-21T14:40:02`,
///   optionally with fractional seconds: `2024-02-21 09:24:25.327`
/// - Date-only: `2016-07-14` (interpreted midnight IST)
pub fn parse_broker_ist_datetime(text: &str) -> Option<DateTime<Utc>> {
    let text = text.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(dt.with_timezone(&Utc));
    }
    // Strip trailing marker used by Angel One ("2016-07-14T09:20:00 IST").
    let cleaned = text.strip_suffix(" IST").unwrap_or(text);
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ];
    for fmt in formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(cleaned, fmt) {
            return Some(naive_to_utc_ist(naive));
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(cleaned, "%Y-%m-%d") {
        return Some(naive_to_utc_ist(date.and_hms_opt(0, 0, 0)?));
    }
    None
}

/// Reconstruct a UTC instant from a time-of-day string reported without a date
/// (SmartStream order updates carry `lastmodifiedtime: "09:45:11"`),
/// combined with the given IST calendar date.
pub fn parse_ist_time_of_day(text: &str, ist_date: NaiveDate) -> Option<DateTime<Utc>> {
    let time = chrono::NaiveTime::parse_from_str(text.trim(), "%H:%M:%S").ok()?;
    Some(naive_to_utc_ist(ist_date.and_time(time)))
}

/// IST calendar date for the given UTC instant (`ist_today(Utc::now())` in
/// production; inject any date in tests).
pub fn ist_today(now: DateTime<Utc>) -> NaiveDate {
    now.with_ist().date_naive()
}

trait WithIst {
    fn with_ist(self) -> DateTime<chrono::FixedOffset>;
}

impl WithIst for DateTime<Utc> {
    fn with_ist(self) -> DateTime<chrono::FixedOffset> {
        let ist = chrono::FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("IST offset is valid");
        Utc.from_utc_datetime(&self.naive_utc()).with_timezone(&ist)
    }
}

fn naive_to_utc_ist(naive: NaiveDateTime) -> DateTime<Utc> {
    let ist = chrono::FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("IST offset is valid");
    ist.from_local_datetime(&naive)
        .single()
        .expect("IST has no DST jumps; naive local time maps uniquely")
        .with_timezone(&Utc)
}

/// Parse Kite/Upstox quantity-like counts that may arrive as numbers or
/// strings (`"1"`, `1`) into exact integers, rejecting fractional parts.
pub fn integer_from_json(value: &Value) -> Option<i64> {
    let d = decimal_from_json(value)?;
    if d.fract().is_zero() {
        d.try_into().ok()
    } else {
        None
    }
}
