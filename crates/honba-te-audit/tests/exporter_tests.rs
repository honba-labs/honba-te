//! M5 SEBI Annexure CSV exporter tests: fixed column set, canonical row
//! layout, RFC-4180 escaping, and determinism.

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use uuid::Uuid;

use honba_te_audit::{
    ANNEXURE_COLUMNS, AuditEvent, AuditRecord, SystemMeta, escape_field, genesis, to_annexure_csv,
};

fn sealed_record(seq: u64, payload: &str) -> AuditRecord {
    let captured_at: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 1, 2, 10, 30, 0).unwrap()
        + ChronoDuration::microseconds(seq as i64);
    AuditRecord {
        seq,
        captured_at,
        client_order_id: Uuid::from_u128(seq.into()),
        exchange_order_id: Some("EXO-7".into()),
        algo_id: "ALGO-SEBI-0001".into(),
        client_account_id: "ACCT-1001".into(),
        client_pan: "AAAPA1234F".into(),
        side: "SELL".into(),
        product: "NRML".into(),
        order_type: "MARKET".into(),
        quantity: "5".into(),
        price: "198.25".into(),
        event: AuditEvent::Placed,
        system: SystemMeta::new(
            Some("203.0.113.9".into()),
            None,
            Some("aa:bb:cc:dd:ee:ff".into()),
            Some(9090),
            None,
        ),
        raw_payload: payload.to_owned(),
        prev_hash: String::new(),
        hash: String::new(),
    }
    .finalize(genesis())
}

#[test]
fn header_is_the_fixed_column_set() {
    let empty = to_annexure_csv(&[]);
    assert_eq!(empty, format!("{}\n", ANNEXURE_COLUMNS.join(",")));
}

#[test]
fn each_record_renders_one_row() {
    let csv = to_annexure_csv(&[
        sealed_record(1, "place-req-body"),
        sealed_record(2, "fill-report-body"),
    ]);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len(), 3, "header + 2 rows");
    assert!(lines[0].starts_with("Seq,Timestamp (UTC),Client Order ID"));
    assert!(lines[1].starts_with("1,2026-01-02T10:30:00.000001Z"));
    assert!(lines[1].contains("ALGO-SEBI-0001"));
    assert!(lines[1].contains("placed"), "canonical event label");
    assert!(lines[1].contains("AAAPA1234F"));
    assert!(lines[2].starts_with("2,"));
}

#[test]
fn special_characters_are_rfc4180_escaped() {
    assert_eq!(escape_field("plain"), "plain");
    assert_eq!(escape_field("a,b"), "\"a,b\"", "commas force quoting");
    assert_eq!(
        escape_field("say \"hi\""),
        "\"say \"\"hi\"\"\"",
        "quotes doubled"
    );
    assert_eq!(escape_field("line1\nline2"), "\"line1\nline2\"");
    assert_eq!(escape_field("cr\r"), "\"cr\r\"");

    let hostile = "k,\"q\"\nz";
    let csv = to_annexure_csv(&[sealed_record(1, hostile)]);
    assert!(
        csv.contains("\"k,\"\"q\"\"\nz\""),
        "payload ships quoted with doubled quotes: {csv}"
    );
}

#[test]
fn export_is_deterministic() {
    let csv = to_annexure_csv(&[sealed_record(1, "p1"), sealed_record(2, "p2")]);
    assert_eq!(
        csv,
        to_annexure_csv(&[sealed_record(1, "p1"), sealed_record(2, "p2")])
    );
}
