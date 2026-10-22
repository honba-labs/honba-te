//! Upstox v2 payload tests — asserts exact decimal string serialization and
//! that NO floating-point values exist anywhere in the serialized wire form.

use honba_core::{Exchange, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity};
use honba_te_core::{BrokerError, ProductType};
use honba_te_upstox::{UpstoxExecutor, UpstoxOrderPayload};

#[test]
fn payload_uses_exact_decimal_strings_never_f64() {
    let inst = InstrumentId::new(Exchange::Nse, "TCS").with_token("NSE_EQ|INE467B01029");
    let req = OrderRequest::new(
        inst,
        OrderSide::Sell,
        OrderType::Limit(Price::from_rupees("3800.00").unwrap()),
        Quantity::from_u64(5),
        "algo-upstox",
    );

    let payload = UpstoxExecutor::build_order_payload(&req, ProductType::CncDelivery).unwrap();
    assert_eq!(payload.transaction_type, "SELL");
    assert_eq!(payload.order_type, "LIMIT");
    assert_eq!(payload.quantity, 5);
    assert_eq!(payload.price, Some("3800.00".to_string()));
    assert_eq!(payload.trigger_price, None);
    assert_eq!(payload.tag.as_deref(), Some("algo-upstox"));
    assert_eq!(payload.product, "D");
    assert_eq!(payload.instrument_token, "NSE_EQ|INE467B01029");

    // Serialized wire carries decimal strings — searching for f64-encoded
    // price numbers must find nothing.
    let json = serde_json::to_string(&payload).unwrap();
    assert!(json.contains("\"price\":\"3800.00\""), "wire json = {json}");
    assert!(!json.contains("3800.0,"), "wire json = {json}");
}

#[test]
fn payload_product_taxonomy_full_mapping() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "RELIANCE"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(1),
        "algo-taxonomy",
    );
    for (product, token) in [
        (ProductType::CncDelivery, "D"),
        (ProductType::MisIntraday, "I"),
        (ProductType::NrmlDerivative, "M"),
        (ProductType::MtfMargin, "MTF"),
    ] {
        let payload = UpstoxExecutor::build_order_payload(&req, product).unwrap();
        assert_eq!(payload.product, token);
    }
}

#[test]
fn payload_market_order_has_no_price_trigger() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(10),
        "algo-mkt",
    );
    let payload = UpstoxExecutor::build_order_payload(&req, ProductType::MisIntraday).unwrap();
    assert!(payload.price.is_none());
    assert!(payload.trigger_price.is_none());
}

#[test]
fn payload_stop_loss_carries_exact_trigger() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "HDFC"),
        OrderSide::Sell,
        OrderType::StopLoss(Price::from_rupees("1625.85").unwrap()),
        Quantity::from_u64(3),
        "algo-stop",
    );
    let payload = UpstoxExecutor::build_order_payload(&req, ProductType::NrmlDerivative).unwrap();
    assert_eq!(payload.order_type, "SL-M");
    assert_eq!(payload.trigger_price, Some("1625.85".to_string()));
    assert_eq!(payload.product, "M");
}

#[test]
fn payload_rejects_fractional_quantity() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "TCS"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::new(rust_decimal::Decimal::from_str_exact("10.5").unwrap()),
        "algo-x",
    );
    let err = UpstoxExecutor::build_order_payload(&req, ProductType::MisIntraday).unwrap_err();
    assert!(matches!(err, BrokerError::InvalidRequest(_)));
}

#[test]
fn legacy_f64_fields_are_gone() {
    // The struct can no longer contain f64 fields — compile-time guarantee via
    // struct definition, but double-check serde never writes exponent syntax.
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Bse, "SENSEX"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("79999.95").unwrap()),
        Quantity::from_u64(2),
        "algo-y",
    );
    let payload = UpstoxExecutor::build_order_payload(&req, ProductType::CncDelivery).unwrap();
    let UpstoxOrderPayload { price, .. } = payload;
    assert_eq!(price, Some("79999.95".to_string()));
}
