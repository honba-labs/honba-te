//! Angel One SmartAPI payload tests (canonical product taxonomy + exact prices).

use honba_core::{Exchange, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity};
use honba_te_angelone::AngeloneExecutor;
use honba_te_core::ProductType;

#[test]
fn payload_construction_with_product_taxonomy() {
    let inst = InstrumentId::new(Exchange::Nse, "HDFCBANK").with_token("1333");
    let req = OrderRequest::new(
        inst,
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("1650").unwrap()),
        Quantity::from_u64(20),
        "algo-angel",
    );

    let payload = AngeloneExecutor::build_order_payload(&req, ProductType::CncDelivery).unwrap();
    assert_eq!(payload.tradingsymbol, "HDFCBANK");
    assert_eq!(payload.symboltoken, "1333");
    assert_eq!(payload.transactiontype, "BUY");
    assert_eq!(payload.ordertype, "LIMIT");
    assert_eq!(payload.quantity, "20");
    // Deterministic 2-decimal price scale.
    assert_eq!(payload.price, "1650.00");
    assert_eq!(payload.producttype, "DELIVERY");
    assert_eq!(payload.exchange, "NSE");
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
        (ProductType::CncDelivery, "DELIVERY"),
        (ProductType::MisIntraday, "INTRADAY"),
        (ProductType::NrmlDerivative, "CARRYFORWARD"),
        (ProductType::MtfMargin, "MTF"),
    ] {
        let payload = AngeloneExecutor::build_order_payload(&req, product).unwrap();
        assert_eq!(payload.producttype, token);
    }
}

#[test]
fn market_order_carries_zero_price() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY").with_token("1594"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(10),
        "algo-mkt",
    );
    let payload = AngeloneExecutor::build_order_payload(&req, ProductType::MisIntraday).unwrap();
    assert_eq!(payload.ordertype, "MARKET");
    assert_eq!(payload.price, "0");
    assert_eq!(payload.quantity, "10");
    assert_eq!(payload.producttype, "INTRADAY");
}

#[test]
fn tick_misaligned_price_is_rejected() {
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "RELIANCE"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("2500.571").unwrap()),
        Quantity::from_u64(1),
        "algo-x",
    );
    let err = AngeloneExecutor::build_order_payload(&req, ProductType::CncDelivery).unwrap_err();
    assert!(err.to_string().contains("tick misalignment"));
}

#[test]
fn adapter_headers_and_defaults() {
    use honba_te_core::BrokerExecutor;
    let exec = AngeloneExecutor::new("key", "jwt", "A123456");
    assert_eq!(exec.name(), "angelone");
    assert_eq!(exec.product, ProductType::CncDelivery);
    assert_eq!(exec.client_code, "A123456");
}
