//! Golden Conformance Tests for Angel One SmartAPI 2.0 adapter (plan §7.1).
//!
//! Verifies recorded broker response fixtures:
//! - Order placement success (200 OK)
//! - Validation refusal (400 Bad Request)
//! - Rate limit ban (429 Too Many Requests)
//! - Server downtime (503 Service Unavailable)
//! - Real-world fill reports (complete and partial fills)
//! - Margin snapshot responses
//! - Order book snapshot responses
//!
//! Asserts 100% loss-free conversion to canonical `OrderAck`, `FillReport`,
//! `BrokerMargins`, and error taxonomies.

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use rust_decimal::Decimal;
use std::str::FromStr;

use honba_core::{
    Exchange, InstrumentId, Money, OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType,
    Price, Quantity, TraceContext,
};
use honba_te_angelone::{AngeloneExecutor, SmartStreamOrderStream};
use honba_te_core::{BrokerError, BrokerExecutionEvent, BrokerExecutor, ExecutionStream};

async fn spawn_mock(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn conformance_order_placement_200_ok() {
    let fixture = include_str!("fixtures/order_placement_200.json");
    let fixture_json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/placeOrder",
        post(move || {
            let body = fixture_json.clone();
            async move { axum::Json(body) }
        }),
    );
    let base_url = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("key", "jwt", "client", base_url);

    let tc = TraceContext::new([1u8; 16], [2u8; 8], TraceContext::SAMPLED);
    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("1450.50").unwrap()),
        Quantity::from_u64(100),
        "algo-alpha",
    )
    .with_trace_context(tc);

    let ack: OrderAck = executor
        .place_order(req.clone())
        .await
        .expect("place_order ack");
    assert_eq!(ack.order_id, "201020000000001");
    assert_eq!(ack.client_order_id, req.client_order_id);
    assert_eq!(ack.status, OrderStatus::New);
    assert_eq!(ack.trace_context, Some(tc));
}

#[tokio::test]
async fn conformance_validation_refusal_400() {
    let fixture = include_str!("fixtures/validation_refusal_400.json");
    let fixture_json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/placeOrder",
        post(move || {
            let body = fixture_json.clone();
            async move { (StatusCode::BAD_REQUEST, axum::Json(body)).into_response() }
        }),
    );
    let base_url = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("key", "jwt", "client", base_url);

    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("1450.50").unwrap()),
        Quantity::from_u64(100),
        "algo-alpha",
    );
    let err = executor.place_order(req).await.unwrap_err();
    match err {
        BrokerError::InvalidRequest(msg) => {
            assert!(msg.contains("Invalid Order Quantity"));
        }
        other => panic!("expected BrokerError::InvalidRequest, found {other:?}"),
    }
}

#[tokio::test]
async fn conformance_rate_limit_429() {
    let fixture = include_str!("fixtures/rate_limit_429.json");
    let fixture_json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/placeOrder",
        post(move || {
            let body = fixture_json.clone();
            async move { (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response() }
        }),
    );
    let base_url = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("key", "jwt", "client", base_url);

    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(10),
        "algo-alpha",
    );
    let err = executor.place_order(req).await.unwrap_err();
    match err {
        BrokerError::RateLimited(msg) => {
            assert!(msg.contains("Exceeded access rate limit"));
        }
        other => panic!("expected BrokerError::RateLimited, found {other:?}"),
    }
}

#[tokio::test]
async fn conformance_server_downtime_503() {
    let fixture = include_str!("fixtures/server_downtime_503.json");
    let fixture_json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/placeOrder",
        post(move || {
            let body = fixture_json.clone();
            async move { (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response() }
        }),
    );
    let base_url = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("key", "jwt", "client", base_url);

    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "INFY"),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(10),
        "algo-alpha",
    );
    let err = executor.place_order(req).await.unwrap_err();
    match err {
        BrokerError::Transport(msg) => {
            assert!(msg.contains("Gateway Maintenance"));
        }
        other => panic!("expected BrokerError::Transport, found {other:?}"),
    }
}

#[tokio::test]
async fn conformance_complete_fill_golden_conversion() {
    let fixture = include_str!("fixtures/fill_complete.json");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = SmartStreamOrderStream::new(rx);

    tx.send(fixture.to_string()).unwrap();
    let event = stream.next_event().await.expect("fill event");

    match event {
        BrokerExecutionEvent::Fill(fill) => {
            assert_eq!(fill.order_id, "201020000000001");
            assert_eq!(fill.fill_id, "201020000000001:100");
            assert_eq!(fill.instrument.symbol, "INFY");
            assert_eq!(fill.instrument.exchange, Exchange::Nse);
            assert_eq!(fill.side, OrderSide::Buy);
            assert_eq!(fill.price, Price::from_rupees("1450.50").unwrap());
            assert_eq!(fill.quantity, Quantity::from_u64(100));
        }
        other => panic!("expected BrokerExecutionEvent::Fill, found {other:?}"),
    }
}

#[tokio::test]
async fn conformance_partial_fill_golden_conversion() {
    let fixture = include_str!("fixtures/fill_partial.json");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = SmartStreamOrderStream::new(rx);

    tx.send(fixture.to_string()).unwrap();
    let event = stream.next_event().await.expect("partial fill event");

    match event {
        BrokerExecutionEvent::Fill(fill) => {
            assert_eq!(fill.order_id, "201020000000002");
            assert_eq!(fill.fill_id, "201020000000002:50");
            assert_eq!(fill.instrument.symbol, "RELIANCE");
            assert_eq!(fill.instrument.exchange, Exchange::Nse);
            assert_eq!(fill.side, OrderSide::Buy);
            assert_eq!(fill.price, Price::from_rupees("2850.75").unwrap());
            assert_eq!(fill.quantity, Quantity::from_u64(50));
        }
        other => panic!("expected BrokerExecutionEvent::Fill, found {other:?}"),
    }
}

#[tokio::test]
async fn conformance_margins_snapshot_conversion() {
    let fixture = include_str!("fixtures/margins_snapshot.json");
    let fixture_json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    let app = Router::new().route(
        "/rest/secure/angelbroking/user/v1/getRms",
        get(move || {
            let body = fixture_json.clone();
            async move { axum::Json(body) }
        }),
    );
    let base_url = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("key", "jwt", "client", base_url);

    let margins = executor.fetch_margins().await.expect("fetch_margins");
    assert_eq!(
        margins.available_cash,
        Money::inr(Decimal::from_str("100000.50").unwrap())
    );
    assert_eq!(
        margins.used_margin,
        Money::inr(Decimal::from_str("40000.50").unwrap())
    );
    assert_eq!(
        margins.collateral,
        Money::inr(Decimal::from_str("25000.00").unwrap())
    );
}

#[tokio::test]
async fn conformance_order_book_snapshot_conversion() {
    let fixture = include_str!("fixtures/order_book.json");
    let fixture_json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/getOrderBook",
        get(move || {
            let body = fixture_json.clone();
            async move { axum::Json(body) }
        }),
    );
    let base_url = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("key", "jwt", "client", base_url);

    let orders = executor.fetch_order_book().await.expect("fetch_order_book");
    assert_eq!(orders.len(), 1);
    let o = &orders[0];
    assert_eq!(o.order_id, "201020000000001");
    assert_eq!(o.status, OrderStatus::Filled);
    assert_eq!(o.instrument.symbol, "INFY");
    assert_eq!(o.instrument.exchange, Exchange::Nse);
    assert_eq!(o.side, OrderSide::Buy);
    assert_eq!(o.price, Some(Price::from_rupees("1450.50").unwrap()));
    assert_eq!(o.quantity, Quantity::from_u64(100));
    assert_eq!(o.filled_quantity, Quantity::from_u64(100));
}
