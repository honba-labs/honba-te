//! Scrip freeze-quantity catalog (NSE/BSE master data).
//!
//! Exchange scrip-level and contract-level freeze quantities cap the quantity
//! of a single exchange order. Regulatory constants (plan §3.4):
//! - NIFTY index derivatives: 1,800 (36 lots) max per order
//! - BANKNIFTY: 900 (30 lots) max per order
//! - FINNIFTY / MIDCPNIFTY: 1,800 max per order
//! - Single-stock equities: vary per scrip, loaded from instrument master
//!   (bhavcopy / kite instruments dump) via [`FreezeLimitCatalog::from_csv`]
//!   or [`FreezeLimitCatalog::from_json`].
//!
//! Lookups prefer the broker instrument token (exact) and fall back to the
//! canonical upper-case symbol on the instrument's declared exchange.

use std::collections::HashMap;

use honba_core::{Exchange, InstrumentId};
use honba_te_core::BrokerError;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// One catalog record: the freeze cap for a single instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreezeLimitRecord {
    /// Canonical upper-case trading symbol (index or scrip).
    pub symbol: String,
    /// Exchange the limit is enforced on.
    pub exchange: Exchange,
    /// Optional broker instrument token (exact-match lookup preferred).
    pub token: Option<String>,
    /// Maximum quantity of a single exchange order.
    pub freeze_quantity: u64,
    /// Optional lot size; when present, freeze is guaranteed to be a lot
    /// multiple and the slicer can verify child lot-multiplicity.
    pub lot_size: Option<u64>,
}

impl FreezeLimitRecord {
    fn from_parts(
        exchange: Exchange,
        symbol: impl Into<String>,
        token: Option<String>,
        freeze_quantity: u64,
        lot_size: Option<u64>,
    ) -> Result<Self, BrokerError> {
        if freeze_quantity == 0 {
            return Err(BrokerError::InvalidRequest(
                "freeze quantity must be positive".into(),
            ));
        }
        let symbol = symbol.into().trim().to_uppercase();
        if symbol.is_empty() {
            return Err(BrokerError::InvalidRequest("empty catalog symbol".into()));
        }
        if let Some(lot) = lot_size {
            if lot == 0 {
                return Err(BrokerError::InvalidRequest(
                    "lot size must be positive".into(),
                ));
            }
            if !freeze_quantity.is_multiple_of(lot) {
                return Err(BrokerError::InvalidRequest(format!(
                    "freeze quantity {freeze_quantity} is not a multiple of lot {lot}"
                )));
            }
        }
        Ok(Self {
            symbol,
            exchange,
            token,
            freeze_quantity,
            lot_size,
        })
    }
}

/// Lookup result: the applicable freeze limit plus optional lot size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreezeLimit {
    /// Maximum single-order quantity.
    pub freeze_quantity: u64,
    /// Lot size when the record carries one.
    pub lot_size: Option<u64>,
}

/// Catalog of per-instrument freeze quantities with optional default cap.
///
/// # Semantics
/// - `slicing_limit(instrument)`: returns the applicable cap — token match
///   first, then symbol match; `None` means "no catalog entry, no default"
///   and the slicer passes the parent through unsliced.
/// - Loads reject conflicting duplicate keys; identical duplicates are
///   accepted idempotently so repeated master-data refreshes are safe.
#[derive(Debug, Clone, Default)]
pub struct FreezeLimitCatalog {
    records: Vec<FreezeLimitRecord>,
    by_symbol: HashMap<(Exchange, String), usize>,
    by_token: HashMap<(Exchange, String), usize>,
    default_freeze: Option<u64>,
}

impl FreezeLimitCatalog {
    /// Empty catalog (no default cap).
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert one record; conflicting duplicates are rejected loudly.
    pub fn insert(&mut self, record: FreezeLimitRecord) -> Result<(), BrokerError> {
        Self::record_insert(
            &mut self.records,
            &mut self.by_symbol,
            &mut self.by_token,
            record,
        )
    }

    fn record_insert(
        records: &mut Vec<FreezeLimitRecord>,
        by_symbol: &mut HashMap<(Exchange, String), usize>,
        by_token: &mut HashMap<(Exchange, String), usize>,
        record: FreezeLimitRecord,
    ) -> Result<(), BrokerError> {
        let symbol_key = (record.exchange, record.symbol.clone());
        if let Some(&existing) = by_symbol.get(&symbol_key) {
            if records[existing] != record {
                return Err(BrokerError::InvalidRequest(format!(
                    "conflicting freeze records for {} {}",
                    symbol_key.1, symbol_key.0
                )));
            }
            return Ok(());
        }
        if let Some(token) = &record.token {
            let token_key = (record.exchange, token.to_uppercase());
            if let Some(&existing) = by_token.get(&token_key) {
                if records[existing] != record {
                    return Err(BrokerError::InvalidRequest(format!(
                        "conflicting freeze token {token} on {}",
                        symbol_key.1
                    )));
                }
                return Ok(());
            }
        }
        let index = records.len();
        by_symbol.insert(symbol_key, index);
        if let Some(token) = record.token.clone() {
            by_token.insert((record.exchange, token.to_uppercase()), index);
        }
        records.push(record);
        Ok(())
    }

    /// Set the fallback cap applied when no record matches. `None` disables
    /// the fallback (unsliced pass-through).
    pub fn with_default_freeze(mut self, zero_fraction: Option<u64>) -> Self {
        self.default_freeze = zero_fraction;
        self
    }

    /// Exact record for an instrument (token first, then canonical symbol).
    pub fn record_for(&self, instrument: &InstrumentId) -> Option<&FreezeLimitRecord> {
        if let Some(&index) = instrument.token.as_deref().and_then(|token| {
            self.by_token
                .get(&(instrument.exchange, token.to_uppercase()))
        }) {
            return self.records.get(index);
        }
        let symbol = instrument.symbol.trim().to_uppercase();
        self.by_symbol
            .get(&(instrument.exchange, symbol))
            .and_then(|&index| self.records.get(index))
    }

    /// Applicable freeze limit: exact record, else the default cap.
    pub fn slicing_limit(&self, instrument: &InstrumentId) -> Option<FreezeLimit> {
        match self.record_for(instrument) {
            Some(record) => Some(FreezeLimit {
                freeze_quantity: record.freeze_quantity,
                lot_size: record.lot_size,
            }),
            None => self.default_freeze.map(|freeze| FreezeLimit {
                freeze_quantity: freeze,
                lot_size: None,
            }),
        }
    }

    /// Number of loaded records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// True when no records are loaded.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Iterator over exact records.
    pub fn iter(&self) -> impl Iterator<Item = &FreezeLimitRecord> {
        self.records.iter()
    }

    /// Parse an NSE/BSE-style instrument-master CSV.
    ///
    /// Header names are matched case-insensitively; recognised spellings:
    /// `SYMBOL` / `TRADINGSYMBOL` / `INSTRUMENT`, `EXCHANGE`, `FREEZE`
    /// (`FREEZE_QUANTITY`, `FRZ_QTY`, `FREEZE_QTY`), `LOT` (`LOT_SIZE`,
    /// `LOT_SIZE_UNITS`), `TOKEN` (`INSTRUMENT_TOKEN`). Unknown columns are
    /// ignored; field-level parse failures are hard errors.
    pub fn from_csv(text: &str) -> Result<Self, BrokerError> {
        let mut rows = text.lines().peekable();
        let header_line = rows
            .next()
            .ok_or_else(|| BrokerError::InvalidRequest("freeze CSV: empty input".into()))?;
        let columns = parse_csv_row(header_line);
        let find = |names: &[&str]| -> Option<usize> {
            columns
                .iter()
                .position(|c| names.iter().any(|n| c.eq_ignore_ascii_case(n)))
        };
        let symbol_col = find(&[
            "symbol",
            "tradingsymbol",
            "trading_symbol",
            "instrument",
            "tradingsymbolname",
        ])
        .ok_or_else(|| BrokerError::InvalidRequest("freeze CSV: no SYMBOL column".into()))?;
        let exchange_col = find(&["exchange", "segment"])
            .ok_or_else(|| BrokerError::InvalidRequest("freeze CSV: no EXCHANGE column".into()))?;
        let freeze_col = find(&[
            "freeze",
            "freezeqty",
            "frzqty",
            "freezequantity",
            "freeze_qty",
            "frz_qty",
        ])
        .ok_or_else(|| {
            BrokerError::InvalidRequest("freeze CSV: no FREEZE quantity column".into())
        })?;
        let lot_col = find(&[
            "lot",
            "lotsize",
            "lotunits",
            "lot_size",
            "lotsizeunits",
            "lot_size_units",
            "lot_units",
        ]);
        let token_col = find(&["token", "instrumenttoken", "instrument_token"]);

        let mut catalog = Self::new();
        for (line_number, line) in rows.enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let fields = parse_csv_row(line);
            let field = |index: usize, what: &str| -> Result<&str, BrokerError> {
                fields.get(index).map(String::as_str).ok_or_else(|| {
                    BrokerError::InvalidRequest(format!(
                        "freeze CSV line {}: missing {what} field",
                        line_number + 2
                    ))
                })
            };
            let exchange = parse_exchange(field(exchange_col, "exchange")?)?;
            let symbol = field(symbol_col, "symbol")?;
            let freeze = parse_u64(field(freeze_col, "freeze qty")?, "freeze qty")?;
            let lot = match lot_col {
                Some(index) => Some(parse_u64(field(index, "lot size")?, "lot size")?),
                None => None,
            };
            let token = match token_col {
                Some(index) => {
                    let raw = field(index, "token")?.trim().to_string();
                    if raw.is_empty() || raw == "-1" {
                        None
                    } else {
                        Some(raw)
                    }
                }
                None => None,
            };
            let record = FreezeLimitRecord::from_parts(exchange, symbol, token, freeze, lot)?;
            catalog.insert(record)?;
        }
        Ok(catalog)
    }

    /// Parse a JSON array of records with mirror-friendly keys.
    ///
    /// Accepted keys: `symbol`|`trading_symbol`|`tradingsymbol`, `exchange`,
    /// `freeze_quantity`|`freeze_qty`, `lot_size`|`lot`, `token`.
    /// Numeric values may arrive as JSON numbers or decimal strings — both are
    /// decoded through exact decimal-to-integer paths (no floats).
    pub fn from_json(text: &str) -> Result<Self, BrokerError> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| BrokerError::InvalidRequest(format!("freeze JSON: {e}")))?;
        let items = match value {
            serde_json::Value::Array(items) => items,
            _ => {
                return Err(BrokerError::InvalidRequest(
                    "freeze JSON: expected an array of records".into(),
                ));
            }
        };
        let mut catalog = Self::new();
        for (index, item) in items.iter().enumerate() {
            let string_key = |names: &[&str]| -> Option<String> {
                names
                    .iter()
                    .find_map(|k| item.get(*k))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            };
            let number_key = |names: &[&str]| -> Result<Option<u64>, BrokerError> {
                let raw = names.iter().find_map(|k| item.get(*k));
                match raw {
                    None | Some(serde_json::Value::Null) => Ok(None),
                    Some(serde_json::Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
                        BrokerError::InvalidRequest(format!(
                            "freeze JSON record {index}: non-positive integer {n}"
                        ))
                    }),
                    Some(serde_json::Value::String(s)) => {
                        Ok(Some(parse_u64(s.trim(), "freeze JSON numeric field")?))
                    }
                    Some(other) => Err(BrokerError::InvalidRequest(format!(
                        "freeze JSON record {index}: unexpected numeric {other}"
                    ))),
                }
            };
            let record = FreezeLimitRecord::from_parts(
                parse_exchange(&string_key(&["exchange", "segment"]).ok_or_else(|| {
                    BrokerError::InvalidRequest("freeze JSON: record missing exchange".into())
                })?)?,
                string_key(&["symbol", "trading_symbol", "tradingsymbol"]).ok_or_else(|| {
                    BrokerError::InvalidRequest("freeze JSON: record missing symbol".into())
                })?,
                string_key(&["token", "instrument_token", "symboltoken"]),
                number_key(&["freeze_quantity", "freeze_qty"])?.ok_or_else(|| {
                    BrokerError::InvalidRequest(
                        "freeze JSON: record missing freeze quantity".into(),
                    )
                })?,
                number_key(&["lot_size", "lot", "lotsize"])?,
            )?;
            catalog.insert(record)?;
        }
        Ok(catalog)
    }
}

/// Canonical SEBI-relevant index freeze constants (plan §3.4); master-data
/// loaders should override these with live values when available.
pub fn builtin_index_freezes() -> Vec<FreezeLimitRecord> {
    let mut records = Vec::new();
    for (exchange, symbol, freeze, lot) in [
        (Exchange::Nse, "NIFTY", 1800u64, Some(50u64)),
        (Exchange::Nse, "BANKNIFTY", 900, Some(30)),
        (Exchange::Nse, "FINNIFTY", 1800, Some(25)),
        (Exchange::Nse, "MIDCPNIFTY", 1800, Some(75)),
        (Exchange::Nse, "NIFTYNXT50", 1800, Some(25)),
    ] {
        records.push(
            FreezeLimitRecord::from_parts(exchange, symbol, None, freeze, lot)
                .expect("builtin index freeze is valid"),
        );
    }
    records
}

fn parse_exchange(text: &str) -> Result<Exchange, BrokerError> {
    // NSE master dumps leave SEGMENT empty for equity rows — default NSE.
    match text.trim().to_uppercase().as_str() {
        "" => Ok(Exchange::Nse),
        "NSE" | "NFO" | "NFO-FUT" | "FNO" | "CDS" => Ok(Exchange::Nse),
        "BSE" | "BFO" => Ok(Exchange::Bse),
        "MCX" => Ok(Exchange::Mcx),
        other => Err(BrokerError::InvalidRequest(format!(
            "freeze catalog: unknown exchange {other}"
        ))),
    }
}

fn parse_u64(text: &str, what: &str) -> Result<u64, BrokerError> {
    let digits: Decimal = text
        .trim()
        .parse::<Decimal>()
        .map_err(|e| BrokerError::InvalidRequest(format!("freeze catalog {what}: {e}")))?;
    if digits < Decimal::ZERO || digits.fract() != Decimal::ZERO {
        return Err(BrokerError::InvalidRequest(format!(
            "freeze catalog {what}: {digits} is not a non-negative integer"
        )));
    }
    digits
        .to_u64()
        .ok_or_else(|| BrokerError::InvalidRequest(format!("freeze catalog {what}: overflow")))
}

/// Minimal CSV field parser (NSE-side quoted-label tolerance).
fn parse_csv_row(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                fields.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_string());
    fields
}
