//! M2 Angel One REST contract tests against a loopback axum mock.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use honba_core::{
    Exchange, InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price, Quantity,
};
use honba_te_angelone::{AngeloneExecutor, map_smartapi_error};
use honba_te_core::{BrokerError, BrokerExecutor, PositionSide, ProductType};

type Logged = Arc<Mutex<Vec<(String, Value)>>>;

async fn spawn_mock(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn order_book_fixture() -> Value {
    json!({
        "status": true,
        "message": "SUCCESS",
        "errorcode": "",
        "data": [
            {
                "variety": "NORMAL",
                "orderid": "201130000006424",
                "exchange": "NSE",
                "symboltoken": "3045",
                "producttype": "DELIVERY",
                "tradingsymbol": "SBIN-EQ",
                "duration": "DAY",
                "price": "19500.55",
                "triggerprice": "-1",
                "quantity": "1",
                "transactiontype": "BUY",
                "ordertype": "LIMIT",
                "status": "REJECTED",
                "averageprice": "0",
                "filledshares": "0",
                "lastmodifiedtime": "2016-07-14T09:20:00 IST",
                "exchangetimestamp": "2016-07-14T09:20:00 IST"
            },
            {
                "variety": "NORMAL",
                "orderid": "201130000006425",
                "exchange": "MCX",
                "symboltoken": "4300",
                "producttype": "CARRYFORWARD",
                "tradingsymbol": "GOLD",
                "duration": "DAY",
                "price": "0",
                "triggerprice": "51200",
                "quantity": "2",
                "transactiontype": "SELL",
                "ordertype": "STOPLOSS_MARKET",
                "status": "COMPLETE",
                "averageprice": "51300",
                "filledshares": "2",
                "lastmodifiedtime": "09:20:00 IST"
            }
        ]
    })
}

#[tokio::test]
async fn place_order_posts_exact_smartapi_payload() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/placeOrder",
        post(|State(log): State<Logged>, Json(body): Json<Value>| async move {
            log.lock().unwrap().push(("PLACE".into(), body.clone()));
            Json(json!({"status": true, "message": "SUCCESS", "data": {"orderid": "20113000006424", "uniqueorderid": "abc"}}))
        }),
    )
    .with_state(logged.clone());
    let base = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("k", "jwt-token", "A1234", &base)
        .with_product(ProductType::MisIntraday);

    let req = OrderRequest::new(
        InstrumentId {
            exchange: Exchange::Nse,
            symbol: "SBIN".into(),
            token: Some("3045".into()),
            isin: None,
        },
        OrderSide::Buy,
        OrderType::StopLimit {
            trigger: Price::from_rupees("1545.5").unwrap(),
            limit: Price::from_rupees("1550.00").unwrap(),
        },
        Quantity::from_u64(10),
        "algo-m2",
    );
    let ack = executor.place_order(req).await.expect("ack");
    assert_eq!(ack.order_id, "20113000006424");
    assert_eq!(ack.status, OrderStatus::New);

    let body = { logged.lock().unwrap()[0].1.clone() };
    let obj = body.as_object().unwrap();
    assert_eq!(obj["variety"], "NORMAL");
    assert_eq!(obj["tradingsymbol"], "SBIN");
    assert_eq!(obj["symboltoken"], "3045");
    assert_eq!(obj["exchange"], "NSE");
    assert_eq!(obj["ordertype"], "STOPLOSS_LIMIT");
    assert_eq!(obj["producttype"], "INTRADAY");
    assert_eq!(obj["price"], "1550.00");
    assert_eq!(obj["triggerprice"], "1545.50");
    assert_eq!(obj["quantity"], "10");
    assert_eq!(obj["transactiontype"], "BUY");
    assert_eq!(obj["duration"], "DAY");
}

#[tokio::test]
async fn modify_scans_book_then_patches_full_definition() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let log_put = logged.clone();
    let app = Router::new()
        .route(
            "/rest/secure/angelbroking/order/v1/getOrderBook",
            get(|| async { Json(order_book_fixture()) }),
        )
        .route(
            "/rest/secure/angelbroking/order/v1/modifyOrder",
            put(
                move |State(log): State<Logged>, Json(body): Json<Value>| async move {
                    log.lock().unwrap().push(("PUT".into(), body.clone()));
                    Json(json!({"status": true, "data": {"orderid": "201130000006424"}}))
                },
            )
            .with_state(log_put.clone()),
        )
        .with_state(logged.clone());
    let base = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("k", "jwt", "A1234", &base);

    let ack = executor
        .modify_order(
            "201130000006424",
            Some(Price::from_rupees("19600.25").unwrap()),
            None,
        )
        .await
        .expect("modify ack");
    assert_eq!(ack.order_id, "201130000006424");

    let entries = logged.lock().unwrap();
    assert_eq!(entries[0].0, "PUT", "modify only (book read is internal)");
    let obj = entries[0].1.as_object().unwrap();
    assert_eq!(obj["orderid"], "201130000006424");
    assert_eq!(obj["price"], "19600.25");
    assert_eq!(obj["symboltoken"], "3045", "token threaded from the book");
    assert_eq!(obj["quantity"], "1", "inherit current quantity");
    assert_eq!(obj["producttype"], "DELIVERY");
    assert_eq!(obj["triggerprice"], "0", "limit order has no trigger");
    drop(entries);
}

#[tokio::test]
async fn order_book_maps_sentinels_statuses_and_symbols() {
    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/getOrderBook",
        get(|| async { Json(order_book_fixture()) }),
    );
    let base = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("k", "jwt", "A1234", base);
    let book = executor.fetch_order_book().await.expect("book");

    assert_eq!(book.len(), 2);

    let rejected = &book[0];
    assert_eq!(rejected.status, OrderStatus::Rejected);
    assert_eq!(rejected.instrument.exchange, Exchange::Nse);
    assert_eq!(rejected.instrument.symbol, "SBIN", "-EQ suffix stripped");
    assert_eq!(rejected.price.unwrap().0.to_string(), "19500.55");
    assert!(
        rejected.trigger_price.is_none(),
        "the leading -1 sentinel timer decodes to None"
    );
    assert_eq!(
        rejected.timestamp.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "2016-07-14T03:50:00",
        "IST suffix trailing -5:30"
    );

    let complete = &book[1];
    assert_eq!(complete.status, OrderStatus::Filled);
    assert!(matches!(complete.order_type, OrderType::StopLoss(_)));
    assert_eq!(complete.instrument.exchange, Exchange::Mcx);
    assert_eq!(complete.filled_quantity.0, Decimal::from(2));
}

#[tokio::test]
async fn positions_map_short_side_and_rms_pnl() {
    let app = Router::new().route(
        "/rest/secure/angelbroking/order/v1/getPositions",
        get(|| async {
            Json(json!({
                "status": true,
                "data": [
                    {
                        "exchange": "NSE",
                        "symbolname": "SBIN",
                        "symboltoken": "3045",
                        "producttype": "DELIVERY",
                        "netqty": "-2",
                        "netprice": "19500.55",
                        "pnl": "-10.5"
                    },
                    {
                        "exchange": "MCX",
                        "symbolname": "GOLD",
                        "symboltoken": "4300",
                        "producttype": "CARRYFORWARD",
                        "netqty": "0",
                        "netprice": "51300",
                        "pnl": "0"
                    }
                ]
            }))
        }),
    );
    let base = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("k", "jwt", "A1234", base);
    let positions = executor.fetch_positions().await.expect("positions");

    assert_eq!(positions[0].side, PositionSide::Short);
    assert_eq!(positions[0].quantity.0, Decimal::from(2));
    assert_eq!(positions[0].average_price.0.to_string(), "19500.55");
    let pnl = positions[0].realized_pnl.unwrap().amount;
    assert_eq!(pnl * Decimal::from(100), Decimal::from(-1050));
    assert_eq!(positions[0].instrument.token.as_deref(), Some("3045"));
    assert_eq!(positions[1].side, PositionSide::Flat);
}

#[tokio::test]
async fn margins_derive_used_from_rms_limits() {
    let app = Router::new().route(
        "/rest/secure/angelbroking/user/v1/getRms",
        get(|| async {
            Json(json!({
                "status": true,
                "data": {
                    "equityamount": "2340.4",
                    "equitylimit": "10000.00",
                    "commodityamount": "100"
                }
            }))
        }),
    );
    let base = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("k", "jwt", "A1234", base);
    let margins = executor.fetch_margins().await.expect("margins");

    assert_eq!(
        margins.available_cash.amount * Decimal::from(100),
        Decimal::from(244_040),
        "equity + commodity exactly"
    );
    assert_eq!(
        margins.used_margin.amount * Decimal::from(100),
        Decimal::from(765_960),
        "limit - equity"
    );
}

#[test]
fn error_taxonomy_maps_smartapi_codes() {
    let cases: Vec<(&str, u16, &str, &'static str)> = vec![
        ("AB1004", 401, "invalid token", "Auth"),
        ("AB1010", 429, "request limit reached", "RateLimited"),
        ("AB2001", 400, "invalid checksum", "InvalidRequest"),
        ("AG8001", 500, "generic error", "Rejected"),
    ];
    for (code, status, message, expected) in cases {
        let err = map_smartapi_error(status, code, message);
        assert_eq!(classify(&err), expected, "{err}");
    }
    // Message-text fallbacks:
    let token_msg = map_smartapi_error(400, "AG8001", "Your session has expired");
    assert_eq!(classify(&token_msg), "Auth");
    // Unknown codes stay Rejected:
    let unknown = map_smartapi_error(404, "ZZ9999", "route missing");
    assert_eq!(classify(&unknown), "Rejected");
}

fn classify(err: &BrokerError) -> &'static str {
    match err {
        BrokerError::Auth(_) => "Auth",
        BrokerError::Rejected(_) => "Rejected",
        BrokerError::Transport(_) => "Transport",
        BrokerError::InvalidRequest(_) => "InvalidRequest",
        BrokerError::RateLimited(_) => "RateLimited",
        _ => "Other",
    }
}

#[tokio::test]
async fn non_json_gateway_body_is_transport_error() {
    let app = Router::new().route(
        "/rest/secure/angelbroking/user/v1/getProfile",
        post(|| async { (axum::http::StatusCode::BAD_GATEWAY, "<html>down</html>") }),
    );
    let base = spawn_mock(app).await;
    let executor = AngeloneExecutor::with_base_url("k", "jwt", "A1234", base);
    match executor.health_check().await {
        Err(BrokerError::Transport(msg)) => assert!(msg.contains("non-JSON")),
        other => panic!("expected transport error, got {other:?}"),
    }
}
