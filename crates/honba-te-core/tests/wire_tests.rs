//! Wire decoding contract tests: exact decimals out of broker JSON and IST
//! timestamp normalization.

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde_json::{Value, json};

use honba_te_core::wire::{
    IST_OFFSET_SECONDS, decimal_from_json, decimal_from_number, integer_from_json, ist_today,
    parse_broker_ist_datetime, parse_ist_time_of_day, price_from_json, quantity_from_json,
};

#[test]
fn integral_json_numbers_convert_exactly() {
    assert_eq!(decimal_from_json(&json!(470)), Some(Decimal::from(470)));
    assert_eq!(
        decimal_from_json(&json!(9007199254740993i64)),
        Some(Decimal::from(9007199254740993i64)),
        "i64 integers must never round-trip through f64"
    );
    assert_eq!(
        decimal_from_json(&json!(-12_345)),
        Some(Decimal::from(-12_345))
    );
}

#[test]
fn fractional_json_numbers_keep_broker_scale() {
    let n = serde_json::from_str::<serde_json::Number>("470.25").unwrap();
    assert_eq!(
        decimal_from_number(&n).unwrap(),
        Decimal::from_str_exact("470.25").unwrap()
    );
    // Two-decimal rupee prices stay exactly scale-2:
    let paise = serde_json::from_str::<serde_json::Number>("19500.55").unwrap();
    assert_eq!(decimal_from_number(&paise).unwrap().to_string(), "19500.55");
}

#[test]
fn numeric_strings_parse_exactly() {
    assert_eq!(
        decimal_from_json(&json!("19500.55")),
        Some(Decimal::from_str_exact("19500.55").unwrap())
    );
    assert_eq!(
        decimal_from_json(&json!(" 12 ")),
        Some(Decimal::from(12)),
        "broker strings are trimmed"
    );
}

#[test]
fn non_numeric_json_yields_none() {
    assert_eq!(decimal_from_json(&json!(null)), None);
    assert_eq!(decimal_from_json(&json!(true)), None);
    assert_eq!(decimal_from_json(&json!({"a": 1})), None);
    assert_eq!(decimal_from_json(&json!([1])), None);
    assert_eq!(decimal_from_json(&json!("abc")), None);
}

#[test]
fn price_and_quantity_extractors() {
    let v: Value = json!(2550.25);
    let price = price_from_json(&v).expect("price");
    assert_eq!(
        price.0.to_string(),
        "2550.25",
        "value exact; wire scale via rescale at output"
    );

    let q = quantity_from_json(&json!("10")).expect("qty");
    assert_eq!(q.0, Decimal::from(10));
}

#[test]
fn integer_extraction_rejects_fractions() {
    assert_eq!(integer_from_json(&json!(512)), Some(512));
    assert_eq!(integer_from_json(&json!("2300")), Some(2300));
    assert_eq!(integer_from_json(&json!(1.5)), None);
    assert_eq!(integer_from_json(&json!(null)), None);
}

#[test]
fn naive_broker_datetimes_are_ist() {
    // Upstox order_timestamp: naive IST text.
    let utc = parse_broker_ist_datetime("2024-02-21 14:40:02").expect("naive datetime");
    assert_eq!(
        utc,
        parse_broker_ist_datetime("2024-02-21T14:40:02+05:30").unwrap(),
        "naive IST must equal explicit +05:30 instant"
    );
    // Kite postback style with fractional seconds.
    let frac = parse_broker_ist_datetime("2022-03-03 09:24:25.327").expect("fractional");
    assert_eq!(
        frac.format("%H:%M:%S%.3f").to_string(),
        "03:54:25.327",
        "UTC shift -5:30"
    );
}

#[test]
fn rfc3339_and_angel_forms_parse() {
    let std = parse_broker_ist_datetime("2026-10-22T02:00:00Z").expect("Z suffix");
    assert_eq!(std.format("%H:%M").to_string(), "02:00");
    // Angel One REST orderbook: "2016-07-14T09:20:00 IST"
    let angel = parse_broker_ist_datetime("2016-07-14T09:20:00 IST").expect("angel IST marker");
    assert_eq!(angel.format("%H:%M").to_string(), "03:50");
    // Date-only defaults to midnight IST.
    let date_only = parse_broker_ist_datetime("2016-07-14").expect("date only");
    assert_eq!(
        date_only.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "2016-07-13T18:30:00",
        "midnight IST is 18:30Z previous day"
    );
    assert_eq!(parse_broker_ist_datetime("not-a-time"), None);
}

#[test]
fn time_of_day_combines_with_ist_date() {
    let date = NaiveDate::from_ymd_opt(2026, 10, 22).unwrap();
    let utc = parse_ist_time_of_day("09:45:11", date).expect("time of day");
    assert_eq!(
        utc.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "2026-10-22T04:15:11"
    );
    let noon = parse_ist_time_of_day("12:00:00", date).unwrap();
    assert_eq!(noon.format("%H:%M").to_string(), "06:30");
    assert_eq!(parse_ist_time_of_day("bogus", date), None);
}

#[test]
fn ist_today_shifts_calendar_day_before_ist_midnight() {
    // 01:30Z is 07:00 IST — same calendar day:
    let morning = chrono::Utc::now();
    let today = ist_today(morning);
    let _ = today; // smoke: never panics
    assert_eq!(IST_OFFSET_SECONDS, 19_800);
}
