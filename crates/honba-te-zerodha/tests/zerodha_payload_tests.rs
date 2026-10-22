//! Zerodha Kite Connect v3 payload construction tests (exact decimals,
//! canonical product taxonomy, health-check parsing).

use honba_core::{Exchange, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity};
use honba_te_core::{BrokerError, ProductType};
use honba_te_zerodha::ZerodhaExecutor;

#[test]
fn payload_construction_is_exact_decimal() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let req = OrderRequest::new(
        inst,
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("2500.5").unwrap()),
        Quantity::from_u64(10),
        "algo-alpha",
    );

    let payload = ZerodhaExecutor::build_order_payload(&req, ProductType::CncDelivery).unwrap();
    assert_eq!(payload.get("tradingsymbol").unwrap(), "RELIANCE");
    assert_eq!(payload.get("exchange").unwrap(), "NSE");
    assert_eq!(payload.get("transaction_type").unwrap(), "BUY");
    assert_eq!(payload.get("order_type").unwrap(), "LIMIT");
    // Kite wire price carries explicit 2-decimal scale deterministically.
    assert_eq!(payload.get("price").unwrap(), "2500.50");
    assert_eq!(payload.get("quantity").unwrap(), "10");
    assert_eq!(payload.get("product").unwrap(), "CNC");
    assert_eq!(payload.get("tag").unwrap(), "algo-alpha");
}

#[test]
fn payload_products_map_all_taxonomy_variants() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "TCS"),
        OrderSide::Sell,
        OrderType::Market,
        Quantity::from_u64(1),
        "algo-taxonomy",
    );
    for (product, token) in [
        (ProductType::CncDelivery, "CNC"),
        (ProductType::MisIntraday, "MIS"),
        (ProductType::NrmlDerivative, "NRML"),
        (ProductType::MtfMargin, "MTF"),
    ] {
        let payload = ZerodhaExecutor::build_order_payload(&req, product).unwrap();
        assert_eq!(payload.get("product").unwrap(), token);
    }
}

#[test]
fn payload_rejects_fractional_and_misaligned_values() {
    // Fractional quantity
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "TCS"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::new(rust_decimal::Decimal::from_str_exact("10.5").unwrap()),
        "algo-x",
    );
    let err = ZerodhaExecutor::build_order_payload(&req, ProductType::MisIntraday).unwrap_err();
    assert!(matches!(err, BrokerError::InvalidRequest(_)));

    // Tick-misaligned limit price must be refused, never rounded silently.
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "TCS"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("2500.517").unwrap()),
        Quantity::from_u64(10),
        "algo-x",
    );
    let err = ZerodhaExecutor::build_order_payload(&req, ProductType::CncDelivery).unwrap_err();
    assert!(err.to_string().contains("tick misalignment"));
}

#[test]
fn stop_limit_payload_carries_both_prices() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY"),
        OrderSide::Sell,
        OrderType::StopLimit {
            trigger: Price::from_rupees("1500").unwrap(),
            limit: Price::from_rupees("1499.25").unwrap(),
        },
        Quantity::from_u64(100),
        "algo-stop",
    );
    let payload = ZerodhaExecutor::build_order_payload(&req, ProductType::MisIntraday).unwrap();
    assert_eq!(payload.get("order_type").unwrap(), "SL");
    assert_eq!(payload.get("price").unwrap(), "1499.25");
    assert_eq!(payload.get("trigger_price").unwrap(), "1500.00");
    assert_eq!(payload.get("transaction_type").unwrap(), "SELL");
}

#[test]
fn adapter_default_product_is_cnc_delivery() {
    use honba_te_core::BrokerExecutor;
    let exec = ZerodhaExecutor::new("key", "token");
    assert_eq!(exec.name(), "zerodha");
    assert_eq!(exec.product, ProductType::CncDelivery);
    let exec = exec.with_product(ProductType::MisIntraday);
    assert_eq!(exec.product, ProductType::MisIntraday);
}
