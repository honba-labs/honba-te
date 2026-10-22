//! SEBI Annexure CSV exporter (plan §3.6, M5).
//!
//! Renders the fixed Annexure column set with RFC-4180-compliant escaping
//! (quote fields containing `,`, `"`, newline, or CR; double embedded
//! quotes). Export is a pure function of sealed records — run
//! [`crate::hash_chain::verify_chain`] first (the CLI does) so tampered
//! journals cannot masquerade as compliance evidence.

use crate::entry::AuditRecord;

/// Fixed SEBI Annexure column order (plan §3.6 mandatory fields 1–7).
pub const ANNEXURE_COLUMNS: [&str; 17] = [
    "Seq",
    "Timestamp (UTC)",
    "Client Order ID",
    "Exchange Order ID",
    "Algo ID",
    "Client Account ID",
    "Client PAN/Demat",
    "Side",
    "Product",
    "Order Type",
    "Quantity",
    "Price",
    "Event",
    "System Meta (JSON)",
    "Raw Payload",
    "Previous Hash",
    "Entry Hash",
];

/// RFC-4180 escaping: quote only when required, doubling embedded quotes.
#[must_use]
pub fn escape_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

/// Render the sealed records as a complete SEBI Annexure CSV document
/// (header + one row per record, in journal order).
#[must_use]
pub fn to_annexure_csv(records: &[AuditRecord]) -> String {
    let mut csv = String::new();
    csv.push_str(&ANNEXURE_COLUMNS.join(","));
    csv.push('\n');
    for record in records {
        let fields = [
            record.seq.to_string(),
            record.captured_at_text(),
            record.client_order_id_text(),
            record.exchange_order_id.clone().unwrap_or_default(),
            record.algo_id.clone(),
            record.client_account_id.clone(),
            record.client_pan.clone(),
            record.side.clone(),
            record.product.clone(),
            record.order_type.clone(),
            record.quantity.clone(),
            record.price.clone(),
            record.event.as_label().to_owned(),
            serde_json::to_string(&record.system).unwrap_or_else(|_| "{}".into()),
            record.raw_payload.clone(),
            record.prev_hash.clone(),
            record.hash.clone(),
        ];
        let row = fields
            .iter()
            .map(|field| escape_field(field))
            .collect::<Vec<_>>()
            .join(",");
        csv.push_str(&row);
        csv.push('\n');
    }
    csv
}
